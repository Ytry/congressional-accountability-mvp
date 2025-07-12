#!/usr/bin/env python3
"""
pictorial_etl.py

– Downloads the official Pictorial Directory PDF
– Extracts each headshot image
– OCRs the caption to pull “First Last”
– Looks up the bioguide_id in name_to_bioguide.json
– Saves each portrait as /portraits/{bioguide_id}.jpg under your backend’s static folder
– Updates legislators.portrait_url in Postgres
"""

import os
import io
import json
import sys
import requests
import fitz            # PyMuPDF
import pytesseract
from PIL import Image
import psycopg2

# ─── CONFIG ───────────────────────────────────────────────────────────────────
SCRIPT_DIR = os.path.dirname(__file__)

PDF_URL   = "https://www.govinfo.gov/content/pkg/GPO-PICTDIR-118/pdf/GPO-PICTDIR-118.pdf"
PDF_LOCAL = os.path.join(SCRIPT_DIR, "pictorial_118.pdf")

# ←── FIXED: write into src/backend/public/portraits (inside the deployed container)
# SCRIPT_DIR = .../src/etl
OUT_DIR = os.path.abspath(
    os.path.join(SCRIPT_DIR, "..", "public", "portraits")
)

MAP_FILE   = os.path.join(SCRIPT_DIR, "name_to_bioguide.json")
DB_ENV_VAR = "DATABASE_URL"

# ensure the output directory exists
os.makedirs(OUT_DIR, exist_ok=True)

# ─── Load name→bioguide map ──────────────────────────────────────────────────
if not os.path.exists(MAP_FILE):
    print(f"[ERROR] Mapping file not found: {MAP_FILE}", file=sys.stderr)
    sys.exit(1)
with open(MAP_FILE, "r") as mf:
    name_map = json.load(mf)

# ─── Download the PDF if missing ─────────────────────────────────────────────
if not os.path.exists(PDF_LOCAL):
    print("[INFO] Downloading Pictorial Directory PDF...", file=sys.stderr)
    resp = requests.get(PDF_URL, stream=True)
    resp.raise_for_status()
    with open(PDF_LOCAL, "wb") as f:
        for chunk in resp.iter_content(4096):
            f.write(chunk)
    print(f"[INFO] PDF saved to {PDF_LOCAL}", file=sys.stderr)

# ─── Extract, OCR, save portraits ────────────────────────────────────────────
doc      = fitz.open(PDF_LOCAL)
mapped   = {}
unmapped = []

for page_idx in range(doc.page_count):
    page   = doc.load_page(page_idx)
    images = page.get_images(full=True)

    for img_idx, img_info in enumerate(images, start=1):
        xref      = img_info[0]
        img_bytes = doc.extract_image(xref)["image"]
        pil_img   = Image.open(io.BytesIO(img_bytes))

        # OCR the caption beneath the headshot
        try:
            bbox    = page.get_image_bbox(xref)
            clip    = fitz.Rect(
                bbox.x0, bbox.y1 + 2,
                bbox.x1, bbox.y1 + bbox.height * 0.3
            )
            pix     = page.get_pixmap(clip=clip)
            cap_img = Image.frombytes("RGB", [pix.width, pix.height], pix.samples)
            text    = pytesseract.image_to_string(cap_img, config="--psm 7").strip()
            name    = text.splitlines()[0] if text else None
        except Exception:
            name = None

        if not name:
            unmapped.append(f"page{page_idx+1}_img{img_idx}")
            continue

        norm     = " ".join(name.split()).title()
        bioguide = name_map.get(norm)
        if not bioguide:
            unmapped.append(norm)
            continue

        out_path = os.path.join(OUT_DIR, f"{bioguide}.jpg")
        pil_img.save(out_path)
        mapped[norm] = bioguide
        print(f"[OK] Saved {norm} → {bioguide}.jpg", file=sys.stderr)

# ─── Write debug report ──────────────────────────────────────────────────────
debug_path = os.path.join(SCRIPT_DIR, "pictorial_etl_debug.json")
with open(debug_path, "w") as df:
    json.dump({"mapped": mapped, "unmapped": unmapped}, df, indent=2)
print(f"[INFO] Wrote debug report to {debug_path}", file=sys.stderr)

# ─── Update Postgres legislators.portrait_url ────────────────────────────────
db_url = os.environ.get(DB_ENV_VAR)
if not db_url:
    print(f"[INFO] {DB_ENV_VAR} not set; skipping DB update", file=sys.stderr)
    sys.exit(0)

conn = psycopg2.connect(db_url)
cur  = conn.cursor()
for bg in mapped.values():
    portrait_url = f"/portraits/{bg}.jpg"
    cur.execute(
        "UPDATE legislators SET portrait_url = %s WHERE bioguide_id = %s",
        (portrait_url, bg)
    )
conn.commit()
cur.close()
conn.close()
print("[INFO] Database portrait_url update complete", file=sys.stderr)
