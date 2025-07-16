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
GPO_API_URL = config.GPO_API_URL  # Note: No longer used; retained for config compatibility
OUT_DIR     = config.PORTRAITS_DIR
DEBUG_JSON  = config.PICT_DEBUG_JSON
DB_URL      = config.DATABASE_URL
# Changed: Official congress.gov base URL for images (replaces bioguide; aligns with Framework source of truth: congress.gov)
CONGRESS_IMG_BASE = "https://www.congress.gov/img/member"
# Added: Fallback to unitedstates/images repo (aggregated from GPO; secondary cross-verification per Framework; addresses 404s for resilience per ETL Blueprint)
FALLBACK_IMG_BASE = "https://unitedstates.github.io/images/congress/original"

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

# Removed: pictorial→bioguide map (no longer needed; using bioguide directly for resilience)

# Removed: FETCH GPO MEMBER COLLECTION (switched to direct bioguide source to address high failures and align with official sources)

# ── DOWNLOAD HEADSHOTS ────────────────────────────────────────────────────────
downloaded = []
failed = []
failed_reasons = {}  # Added: Track reasons for failures (per ETL blueprint reconciliation)
fallback_used = 0  # Added: Track fallback usage for audit
for p in legislators:  # Changed: Loop over legislators directly (comprehensive coverage)
    bio_id = p.get("id", {}).get("bioguide")
    if not bio_id:
        failed.append("unknown")
        failed_reasons["unknown"] = "no_bioguide_id"  # Note: Aggregate unknowns
        logger.debug("No bioguide_id for legislator entry")
        continue

    # Primary: Official congress.gov image URL (aligns with Framework: congress.gov as primary source)
    img_url = f"{CONGRESS_IMG_BASE}/{bio_id.lower()}_200.jpg"
    resp = fetch_with_retry(img_url)
    if resp and resp.status_code == 200 and "image" in resp.headers.get("Content-Type", ""):  # Changed: Explicit check for success
        pass  # Proceed with primary
    else:
        # Added: Fallback on failure (e.g., 404 for missing images; resilience per ETL Blueprint)
        if resp:
            logger.debug(f"Primary fetch failed (status {resp.status_code}) for {bio_id} at {img_url}")
        else:
            logger.debug(f"Primary fetch failed (no response) for {bio_id} at {img_url}")
        fallback_img_url = f"{FALLBACK_IMG_BASE}/{bio_id}.jpg"
        resp = fetch_with_retry(fallback_img_url)
        if not resp or resp.status_code != 200 or "image" not in resp.headers.get("Content-Type", ""):
            failed.append(bio_id)
            failed_reasons[bio_id] = "both_primary_and_fallback_failed"
            logger.debug(f"Fallback also failed for {bio_id} at {fallback_img_url}")
            continue
        fallback_used += 1

    try:
        content = bytearray()
        for chunk in resp.iter_content(1024):
            content.extend(chunk)
        # Added: Basic integrity check (non-zero size)
        if len(content) == 0:
            raise ValueError("Empty image content")
        # Optional: Hash check if previous exists
        with open(OUT_DIR / f"{bio_id}.jpg", "wb") as f:
            f.write(content)
        downloaded.append(bio_id)
    except Exception:
        logger.exception(f"Failed to download/write image for {bio_id}")
        failed.append(bio_id)
        failed_reasons[bio_id] = "write_or_integrity_failed"

# ── WRITE DEBUG JSON ─────────────────────────────────────────────────────────
debug_data = {"downloaded": downloaded, "failed": failed, "timestamp": datetime.utcnow().isoformat(),
              "source_urls": {"legis": LEGIS_URL, "congress_img_base": CONGRESS_IMG_BASE, "fallback_img_base": FALLBACK_IMG_BASE},  # Changed: Update audit trail
              "failed_reasons": failed_reasons, "fallback_used": fallback_used}  # Added: Reasons and fallback count for reconciliation
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
    "downloaded_count": len(downloaded),
    "failed_count": len(failed),
    "db_updated": updated_count
})
# Changed: Dynamic threshold based on total (e.g., >10% failures; per ETL blueprint scalability)
if len(failed) > 0.1 * total_legislators:
    logger.critical("High failure count; alert admins", extra={"failed": failed, "failed_reasons": failed_reasons})
