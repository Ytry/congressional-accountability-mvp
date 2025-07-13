#!/usr/bin/env python3
"""
pictorial_etl.py

– Builds mapping from pictorial IDs to bioguide IDs
– Downloads headshots via GPO Pictorial API
– Writes debug report
– Updates legislators.portrait_url in Postgres
"""
import sys
import json
from pathlib import Path
import config
from logger import setup_logger
from utils import fetch_with_retry, write_json, get_cursor

# Initialize structured logger
logger = setup_logger("pictorial_etl")

# ── Configured constants ─────────────────────────────────────────────────────
LEGIS_URL    = config.LEGIS_JSON_URL
GPO_API_URL  = config.GPO_API_URL
OUT_DIR      = config.PORTRAITS_DIR
DEBUG_JSON   = config.PICT_DEBUG_JSON
DB_URL       = config.DATABASE_URL

# Ensure directories exist
OUT_DIR.mkdir(parents=True, exist_ok=True)
logger.info("Output directory ready", extra={"out_dir": str(OUT_DIR)})

# ── 1) Build pictorial_id → bioguide_id map ─────────────────────────────────
resp = fetch_with_retry(LEGIS_URL)
if not resp:
    logger.error("Failed to fetch legislators JSON", extra={"url": LEGIS_URL})
    sys.exit(1)
try:
    legislators = resp.json()
    logger.info("Fetched legislators list", extra={"count": len(legislators)})
except Exception:
    logger.exception("Failed parsing legislators JSON")
    sys.exit(1)

pict2bio = {
    str(p.get("id", {}).get("pictorial")): p.get("id", {}).get("bioguide")
    for p in legislators
    if p.get("id", {}).get("pictorial") and p.get("id", {}).get("bioguide")
}
if not pict2bio:
    logger.error("No pictorial→bioguide mappings found")
    sys.exit(1)
logger.info("Built pict2bio map", extra={"mappings": len(pict2bio)})

# ── 2) Fetch GPO Pictorial API ────────────────────────────────────────────────
resp = fetch_with_retry(GPO_API_URL)
if not resp:
    logger.error("Failed to fetch GPO member collection", extra={"url": GPO_API_URL})
    sys.exit(1)
try:
    members = resp.json().get("memberCollection", [])
    logger.info("Fetched GPO member collection", extra={"count": len(members)})
except Exception:
    logger.exception("Failed parsing GPO JSON")
    sys.exit(1)

# ── 3) Download each headshot ─────────────────────────────────────────────────
downloaded = []
failed = []
for m in members:
    pict_id = str(m.get("memberId", ""))
    img_url = m.get("imageUrl") or ""
    bio_id  = pict2bio.get(pict_id)
    if not bio_id or not img_url:
        logger.warning("Skipping member; missing mapping or URL", extra={"pict_id": pict_id, "bio_id": bio_id})
        failed.append(pict_id)
        continue

    out_path = OUT_DIR / f"{bio_id}.jpg"
    resp = fetch_with_retry(img_url)
    if not resp:
        logger.warning("Failed to download portrait", extra={"bio_id": bio_id, "img_url": img_url})
        failed.append(bio_id)
        continue
    try:
        if "image" not in resp.headers.get("Content-Type", ""):
            raise ValueError("Content-Type not image")
        with open(out_path, "wb") as f:
            for chunk in resp.iter_content(1024):
                f.write(chunk)
        downloaded.append(bio_id)
        logger.info("Downloaded portrait", extra={"bio_id": bio_id})
    except Exception:
        logger.exception("Error writing portrait file", extra={"bio_id": bio_id})
        failed.append(bio_id)

# ── 4) Write debug JSON report ────────────────────────────────────────────────
try:
    write_json(DEBUG_JSON, {"downloaded": downloaded, "failed": failed})
except Exception:
    sys.exit(1)

# ── 5) Update database portrait_url ───────────────────────────────────────────
if not DB_URL:
    logger.info("DATABASE_URL not set; skipping DB update")
    sys.exit(0)

updated_count = 0
with get_cursor() as (conn, cur):
    for bio_id in downloaded:
        portrait_url = f"/portraits/{bio_id}.jpg"
        cur.execute(
            "UPDATE legislators SET portrait_url = %s WHERE bioguide_id = %s",
            (portrait_url, bio_id)
        )
        updated_count += cur.rowcount
logger.info("Database portrait_url update complete", extra={"updated_records": updated_count})
