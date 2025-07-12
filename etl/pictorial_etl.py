#!/usr/bin/env python3
"""
pictorial_etl.py

– GET legislators‑current.json → build pictorial→bioguide map
– GET GPO Pictorial API for 118th Congress members
– Download each imageUrl → portraits/{bioguide}.jpg
– Write debug report
– UPDATE legislators.portrait_url in Postgres
"""

import os
import sys
import json
import requests
import psycopg2
from logger import setup_logger

# Initialize structured logger
logger = setup_logger("pictorial_etl")

# ─── CONFIG ───────────────────────────────────────────────────────────────────
SCRIPT_DIR   = os.path.dirname(__file__)
LEGIS_URL    = "https://unitedstates.github.io/congress-legislators/legislators-current.json"
GPO_API_URL  = "https://pictorialapi.gpo.gov/api/GuideMember/GetMembers/118"
BACKEND_DIR  = os.path.abspath(os.path.join(SCRIPT_DIR, "..", "backend"))
OUT_DIR      = os.path.abspath(os.path.join(BACKEND_DIR, "..", "portraits"))
DEBUG_JSON   = os.path.join(SCRIPT_DIR, "pictorial_etl_debug.json")
DB_ENV_VAR   = "DATABASE_URL"

# Ensure output directory exists
os.makedirs(OUT_DIR, exist_ok=True)
logger.info("Output directory ready", extra={"out_dir": OUT_DIR})

# ─── 1) Build pictorial_id → bioguide_id map ────────────────────────────────
try:
    resp = requests.get(LEGIS_URL, timeout=15)
    resp.raise_for_status()
    legislators = resp.json()
    logger.info("Fetched legislators list", extra={"source": LEGIS_URL, "count": len(legislators)})
except Exception:
    logger.exception("Failed to fetch legislators-current.json")
    sys.exit(1)

pict2bio = {}
for person in legislators:
    ids = person.get("id", {})
    pict = ids.get("pictorial")
    bio  = ids.get("bioguide")
    if pict and bio:
        pict2bio[str(pict)] = bio

if not pict2bio:
    logger.error("No pictorial→bioguide mappings found")
    sys.exit(1)
logger.info("Built pict2bio map", extra={"mappings": len(pict2bio)})

# ─── 2) Fetch GPO Pictorial API ──────────────────────────────────────────────
try:
    resp = requests.get(GPO_API_URL, timeout=15)
    resp.raise_for_status()
    members = resp.json().get("memberCollection", [])
    logger.info("Fetched GPO member collection", extra={"api_url": GPO_API_URL, "count": len(members)})
except Exception:
    logger.exception("Failed to fetch GPO Pictorial API")
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

    out_path = os.path.join(OUT_DIR, f"{bio_id}.jpg")
    try:
        r = requests.get(img_url, stream=True, timeout=15)
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
    logger.info("Wrote debug JSON report", extra={"path": DEBUG_JSON, "downloaded": len(downloaded), "failed": len(failed)})
except Exception:
    logger.exception("Failed to write debug JSON report")

# ─── 5) Update Postgres legislators.portrait_url ──────────────────────────────
db_url = os.getenv(DB_ENV_VAR)
if not db_url:
    logger.info("DATABASE_URL not set; skipping DB update")
    sys.exit(0)

try:
    conn = psycopg2.connect(db_url)
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
    logger.exception("Failed to update database portraits")
    sys.exit(1)
