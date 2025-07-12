#!/usr/bin/env python3
"""
pictorial_etl.py

– GET legislators‐current.json → build pictorial→bioguide map
– GET GPO Pictorial API for 118th Congress members
– Download each imageUrl → backend/public/portraits/{bioguide}.jpg
– Write debug report
– UPDATE legislators.portrait_url in Postgres
"""

import os
import sys
import json
import requests
import psycopg2

# ─── CONFIG ───────────────────────────────────────────────────────────────────
SCRIPT_DIR     = os.path.dirname(__file__)                      # .../src/etl
LEGIS_URL      = "https://unitedstates.github.io/congress-legislators/legislators-current.json"
GPO_API_URL    = "https://pictorialapi.gpo.gov/api/GuideMember/GetMembers/118"
BACKEND_DIR    = os.path.abspath(os.path.join(SCRIPT_DIR, "..", "backend"))
OUT_DIR        = os.path.join(BACKEND_DIR, "public", "portraits")
DEBUG_JSON     = os.path.join(SCRIPT_DIR, "pictorial_etl_debug.json")
DB_ENV_VAR     = "DATABASE_URL"

# ensure output folder exists
os.makedirs(OUT_DIR, exist_ok=True)

# ─── 1) Build pictorial_id → bioguide_id map ────────────────────────────────
resp = requests.get(LEGIS_URL, timeout=15)
resp.raise_for_status()
legislators = resp.json()

pict2bio = {}
for person in legislators:
    ids = person.get("id", {})
    pict = ids.get("pictorial")
    bio  = ids.get("bioguide")
    if pict and bio:
        # pictorial API uses numeric strings; ensure matching types
        pict2bio[str(pict)] = bio

if not pict2bio:
    print("[ERROR] No pictorial→bioguide mappings found!", file=sys.stderr)
    sys.exit(1)

# ─── 2) Fetch GPO Pictorial API ──────────────────────────────────────────────
resp = requests.get(GPO_API_URL, timeout=15)
resp.raise_for_status()
members = resp.json().get("memberCollection", [])

downloaded = []
failed     = []

# ─── 3) Download each headshot ───────────────────────────────────────────────
for m in members:
    pict_id  = str(m.get("memberId"))
    img_url  = m.get("imageUrl") or ""
    bio_id   = pict2bio.get(pict_id)
    if not bio_id or not img_url:
        failed.append(pict_id)
        continue

    out_path = os.path.join(OUT_DIR, f"{bio_id}.jpg")
    try:
        r = requests.get(img_url, stream=True, timeout=15)
        r.raise_for_status()
        # sanity: check a JPEG
        if "image" not in r.headers.get("Content-Type", ""):
            raise ValueError("Not an image")
        with open(out_path, "wb") as f:
            for chunk in r.iter_content(1024):
                f.write(chunk)
        downloaded.append(bio_id)
        print(f"[OK] {bio_id} ← {img_url}", file=sys.stderr)
    except Exception as e:
        print(f"[WARN] {bio_id}: failed to download ({e})", file=sys.stderr)
        failed.append(bio_id)

# ─── 4) Write debug JSON ─────────────────────────────────────────────────────
with open(DEBUG_JSON, "w") as df:
    json.dump({"downloaded": downloaded, "failed": failed}, df, indent=2)
print(f"[INFO] Debug report → {DEBUG_JSON}", file=sys.stderr)

# ─── 5) Update Postgres legislators.portrait_url ──────────────────────────────
db_url = os.environ.get(DB_ENV_VAR)
if not db_url:
    print(f"[INFO] {DB_ENV_VAR} not set; skipping DB update", file=sys.stderr)
    sys.exit(0)

conn = psycopg2.connect(db_url)
cur  = conn.cursor()
for bio_id in downloaded:
    portrait_url = f"/portraits/{bio_id}.jpg"
    cur.execute(
        "UPDATE legislators SET portrait_url = %s WHERE bioguide_id = %s",
        (portrait_url, bio_id)
    )
conn.commit()
cur.close()
conn.close()
print("[INFO] Database portrait_url update complete", file=sys.stderr)
