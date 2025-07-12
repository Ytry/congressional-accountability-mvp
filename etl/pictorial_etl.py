#!/usr/bin/env python3
"""
pictorial_etl.py

– GET legislators-current.json → build pictorial→bioguide map
– GET GPO Pictorial API for 118th Congress members
– Download each imageUrl → portraits/{bioguide}.jpg
– Write debug report
– UPDATE legislators.portrait_url in Postgres
"""
import sys
import json
import requests
import psycopg2
from logger import setup_logger
import config

# Initialize structured logger
logger = setup_logger("pictorial_etl")

# ─── CONFIG ───────────────────────────────────────────────────────────────────
LEGIS_URL   = config.LEGIS_JSON_URL
GPO_API_URL = config.GPO_API_URL
OUT_DIR     = config.PORTRAITS_DIR
DEBUG_JSON  = config.PICT_DEBUG_JSON
DB_URL      = config.DATABASE_URL

# Ensure output directory exists
OUT_DIR.mkdir(parents=True, exist_ok=True)
logger.info("Output directory ready", extra={"out_dir": str(OUT_DIR)})

# ─── 1) Build pictorial_id → bioguide_id map ────────────────────────────────
try:
    resp = requests.get(LEGIS_URL, timeout=config.HTTP_TIMEOUT)
    resp.raise_for_status()
    legislators = resp.json()
    logger.info("Fetched legislators list", extra={"source": LEGIS_URL, "count": len(legislators)})
except Exception:
    logger.exception("Failed to fetch legislators list from JSON endpoint")
    sys.exit(1)

pict2bio = {str(p.get("id", {}).get("pictorial")): p.get("id", {}).get("bioguide")
            for p in legislators
            if p.get("id", {}).get("pictorial") and p.get("id", {}).get("bioguide")}

if not pict2bio:
    logger.error("No pictorial→bioguide mappings found")
    sys.exit(1)
logger.info("Built pict2bio map", extra={"mappings": len(pict2bio)})

# ─── 2) Fetch GPO Pictorial API ──────────────────────────────────────────────
try:
    resp = requests.get(GPO_API_URL, timeout=config.HTTP_TIMEOUT)
    resp.raise_for_status()
    members = resp.json().get("memberCollection", [])
    logger.info("Fetched GPO member collection", extra={"api_url": GPO_API_URL, "count": len(members)})
except Exception:
    logger.exception("Failed to fetch GPO Pictorial API data")
    sys.exit(1)

# ─── 3) Download each headshot ───────────────────────────────────────────────
downloaded = []
failed     = []
for m in members:
    pict_id = str(m.get("memberId"))
    img_url = m.get("imageUrl") or ""
    bio_id  = pict2bio.get(pict_id)

    if not bio_id or not img_url:
        logger.warning("Skipping member; missing mapping or URL", extra={"pict_id": pict_id, "bio_id": bio_id})
        failed.append(pict_id)
        continue

    out_path = OUT_DIR / f"{bio_id}.jpg"
    try:
        r = requests.get(img_url, stream=True, timeout=config.HTTP_TIMEOUT)
        r.raise_for_status()
        if "image" not in r.headers.get("Content-Type", ""):
            raise ValueError("Content-Type not image")
        with open(out_path, "wb") as f:
            for chunk in r.iter_content(1024):
                f.write(chunk)
        downloaded.append(bio_id)
        logger.info("Downloaded portrait", extra={"bio_id": bio_id, "img_url": img_url})
    except Exception as e:
        logger.warning("Failed to download portrait", extra={"bio_id": bio_id, "img_url": img_url, "error": str(e)})
        failed.append(pict_id)

# ─── 4) Write debug JSON ─────────────────────────────────────────────────────
try:
    with open(DEBUG_JSON, "w") as df:
        json.dump({"downloaded": downloaded, "failed": failed}, df, indent=2)
    logger.info("Wrote debug JSON report", extra={"path": str(DEBUG_JSON), "downloaded": len(downloaded), "failed": len(failed)})
except Exception:
    logger.exception("Failed to write debug JSON report")

# ─── 5) Update Postgres legislators.portrait_url ──────────────────────────────
if not DB_URL:
    logger.info("DATABASE_URL not set; skipping DB update")
    sys.exit(0)

try:
    conn = psycopg2.connect(DB_URL)
    cur  = conn.cursor()
    updated_count = 0
    for bio_id in downloaded:
        portrait_url = f"/portraits/{bio_id}.jpg"
        cur.execute(
            "UPDATE legislators SET portrait_url = %s WHERE bioguide_id = %s",
            (portrait_url, bio_id)
        )
        updated_count += cur.rowcount
    conn.commit()
    cur.close()
    conn.close()
    logger.info("Database portrait_url update complete", extra={"updated_records": updated_count})
except Exception:
    logger.exception("Failed to update database portrait URLs")
    sys.exit(1)
