#!/usr/bin/env python3
"""
pictorial_etl.py

– Builds mapping from legislators-current.json → pictorial→bioguide map
– Fetches GPO Pictorial API for portraits
– Downloads images to `portraits/{bioguide}.jpg`
– Writes debug JSON report
– Updates `legislators.portrait_url` in Postgres
– Emits a single end-of-run summary log
"""
import sys
import json
from pathlib import Path
import config
from logger import setup_logger
from utils import fetch_with_retry, write_json, get_cursor
from datetime import datetime
import hashlib  # Added for image integrity check

# Initialize structured logger
logger = setup_logger("pictorial_etl")

# ── CONFIGURE ───────────────────────────────────────────────────────────────
LEGIS_URL   = config.LEGIS_JSON_URL
GPO_API_URL = config.GPO_API_URL
OUT_DIR     = config.PORTRAITS_DIR
DEBUG_JSON  = config.PICT_DEBUG_JSON
DB_URL      = config.DATABASE_URL

# Ensure output directory exists
OUT_DIR.mkdir(parents=True, exist_ok=True)
logger.info("Output directory ready", extra={"out_dir": str(OUT_DIR)})

# ── FETCH LEGISLATORS JSON ───────────────────────────────────────────────────
resp = fetch_with_retry(LEGIS_URL)
if not resp:
    logger.error("Failed to fetch legislators list", extra={"url": LEGIS_URL})
    sys.exit(1)
try:
    legislators = resp.json()
    total_legislators = len(legislators)
    logger.info("Fetched legislators list", extra={"count": total_legislators})
except Exception:
    logger.exception("Failed parsing legislators JSON")
    sys.exit(1)

# Build pictorial → bioguide map
pict2bio = {
    str(p.get("id", {}).get("pictorial")): p.get("id", {}).get("bioguide")
    for p in legislators
    if p.get("id", {}).get("pictorial") and p.get("id", {}).get("bioguide")
}
if not pict2bio:
    logger.error("No pictorial→bioguide mappings found; aborting ETL")
    sys.exit(1)
logger.info("Built pictorial→bioguide map", extra={"mappings": len(pict2bio)})

# ── FETCH GPO MEMBER COLLECTION ──────────────────────────────────────────────
# Added: Check freshness (example using last-modified; adjust if API supports)
if Path("last_gpo_etag.txt").exists():  # Placeholder for ETag/last-modified
    # Implement check logic here; skip if unchanged
    pass  # For now, always fetch
resp = fetch_with_retry(GPO_API_URL)
if not resp:
    logger.error("Failed to fetch GPO member collection", extra={"url": GPO_API_URL})
    sys.exit(1)
try:
    members = resp.json().get("memberCollection", [])
    total_members = len(members)
    logger.info("Fetched GPO member collection", extra={"count": total_members})
except Exception:
    logger.exception("Failed parsing GPO JSON")
    sys.exit(1)

# ── DOWNLOAD HEADSHOTS ────────────────────────────────────────────────────────
downloaded = []
failed = []
for m in members:
    pict_id = str(m.get("memberId", ""))
    img_url = m.get("imageUrl") or ""
    bio_id  = pict2bio.get(pict_id)
    if not bio_id or not img_url:
        failed.append(pict_id)
        continue

    out_path = OUT_DIR / f"{bio_id}.jpg"
    resp = fetch_with_retry(img_url)
    if not resp or "image" not in resp.headers.get("Content-Type", ""):
        failed.append(bio_id)
        continue
    try:
        content = bytearray()
        for chunk in resp.iter_content(1024):
            content.extend(chunk)
        # Added: Basic integrity check (non-zero size)
        if len(content) == 0:
            raise ValueError("Empty image content")
        # Optional: Hash check if previous exists
        with open(out_path, "wb") as f:
            f.write(content)
        downloaded.append(bio_id)
    except Exception:
        logger.exception(f"Failed to download/write image for {bio_id}")
        failed.append(bio_id)

# ── WRITE DEBUG JSON ─────────────────────────────────────────────────────────
debug_data = {"downloaded": downloaded, "failed": failed, "timestamp": datetime.utcnow().isoformat(),
              "source_urls": {"legis": LEGIS_URL, "gpo": GPO_API_URL}}  # Added: Audit trail
try:
    write_json(DEBUG_JSON, debug_data)
    logger.info("Wrote debug JSON report", extra={"path": str(DEBUG_JSON),
                 "downloaded": len(downloaded), "failed": len(failed)})
except Exception:
    logger.exception("Failed to write debug JSON report")

# ── UPDATE DATABASE ──────────────────────────────────────────────────────────
updated_count = 0
if DB_URL:
    with get_cursor() as (conn, cur):  # Assume conn for commit if needed
        updates = []
        for bio_id in downloaded:
            portrait_url = f"/portraits/{bio_id}.jpg"
            updates.append((portrait_url, bio_id))
        try:
            # Changed: Batch update with executemany
            cur.executemany(
                "UPDATE legislators SET portrait_url = %s WHERE bioguide_id = %s",
                updates
            )
            updated_count = cur.rowcount  # Note: rowcount for executemany is total affected
            conn.commit()  # Explicit commit if not auto
        except Exception:
            logger.exception("DB update failed")
            if conn:
                conn.rollback()
    logger.info("Database update complete", extra={"updated_records": updated_count})
else:
    logger.warning("DATABASE_URL not set; skipped DB update")  # Changed: Warning log

# ── END-OF-RUN SUMMARY ────────────────────────────────────────────────────────
logger.info("Pictorial ETL summary", extra={
    "total_legislators": total_legislators,
    "map_size": len(pict2bio),
    "total_members": total_members,
    "downloaded_count": len(downloaded),
    "failed_count": len(failed),
    "db_updated": updated_count
})
# Added: Alert if failed > threshold (e.g., email, per ETL blueprint)
if len(failed) > 10:  # Arbitrary threshold
    logger.critical("High failure count; alert admins", extra={"failed": failed})
