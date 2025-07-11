#!/usr/bin/env python3
"""
Drop-in pictorial_etl.py for:
 - downloading the Pictorial Directory PDF
 - extracting each headshot image + OCR’ing the caption
 - mapping “First Last” → bioguide_id via name_to_bioguide.json
 - saving portraits as public/portraits/{bioguide_id}.jpg
 - updating legislators.portrait_url in your Postgres DB
"""

import os
import io
import sys
import json
import requests
import fitz            # PyMuPDF
import pytesseract
from PIL import Image
import psycopg2

# ─── Configuration ───────────────────────────────────────────────────────────
SCRIPT_DIR   = os.path.dirname(__file__)
PDF_URL      = "https://www.govinfo.gov/content/pkg/GPO-PICTDIR-118/pdf/GPO-PICTDIR-118.pdf"
PDF_LOCAL    = os.path.join(SCRIPT_DIR, "pictorial_118.pdf")
OUT_DIR      = os.path.join(SCRIPT_DIR, "..", "public", "portraits")
MAPPING_FILE = os.path.join(SCRIPT_DIR, "name_to_bioguide.json")
DB_ENV_VAR   = "DATABASE_URL"

# ─── Prep output folder ──────────────────────────────────────────────────────
os.makedirs(OUT_DIR, exist_ok=True)

# ─── Load your name→BioGuide map ─────────────────────────────────────────────
if not os.path.exists(MAPPING_FILE):
    print(f"[ERROR] Mapping file not found: {MAPPING_FILE}", file=sys.stderr)
    sys.exit(1)

with open(MAPPING_FILE) as mf:
    name_map = json.load(mf)

# ─── Download the PDF if it’s missing ────────────────────────────────────────
if not os.path.exists(PDF_LOCAL):
    print(f"[INFO] Downloading pictorial PDF…", file=sys.stderr)
    resp = requests.get(PDF_URL, stream=True)
    resp.raise_for_status()
    with open(PDF_LOCAL, "wb") as f:
        for chunk in resp.iter_content(1024):
            f.write(chunk)
    print(f"[INFO] PDF saved to {PDF_LOCAL}", file=sys.stderr)

# ─── Open PDF & iterate images ───────────────────────────────────────────────
doc     = fitz.open(PDF_LOCAL)
mapped  = {}   # name → bioguide
unmapped = []

for page_index in range(doc.page_count):
    page   = doc.load_page(page_index)
    images = page.get_images(full=True)

    for img_idx, img_info in enumerate(images, start=1):
        xref     = img_info[0]
        img_dict = doc.extract_image(xref)
        img_bytes= img_dict["image"]
        img      = Image.open(io.BytesIO(img_bytes))

        # ─ OCR the caption region ───────────────────────────────────
        try:
            bbox    = page.get_image_bbox(xref)
            cap_rect= fitz.Rect(
                bbox.x0,
                bbox.y1 + 2,
                bbox.x1,
                bbox.y1 + bbox.height * 0.3
            )
            cap_pix = page.get_pixmap(clip=cap_rect)
            cap_img = Image.frombytes(
                "RGB", [cap_pix.width, cap_pix.height], cap_pix.samples
            )
            text    = pytesseract.image_to_string(cap_img, config="--psm 7").strip()
            name    = text.splitlines()[0] if text else None
        except Exception as e:
            name = None
            print(f"[WARN] OCR failed page {page_index+1} img{img_idx}: {e}", file=sys.stderr)

        if not name:
            unmapped.append(f"page{page_index+1}_img{img_idx}")
            continue

        # ─ Normalize & lookup Bioguide ─────────────────────────────
        normalized = " ".join(name.split()).title()
        bioguide   = name_map.get(normalized)
        if not bioguide:
            unmapped.append(normalized)
            print(f"[WARN] No Bioguide for “{normalized}”", file=sys.stderr)
            continue

        # ─ Save as {BIOGUIDE}.jpg ────────────────────────────────────
        out_fn  = f"{bioguide}.jpg"
        out_path= os.path.join(OUT_DIR, out_fn)
        img.save(out_path)
        mapped[normalized] = bioguide
        print(f"[OK] {normalized} → {out_fn}", file=sys.stderr)

# ─── Write a debug report ───────────────────────────────────────────────────
debug_path = os.path.join(SCRIPT_DIR, "pictorial_etl_debug.json")
with open(debug_path, "w") as df:
    json.dump({"mapped": mapped, "unmapped": unmapped}, df, indent=2)
print(f"[INFO] Debug mapping: {debug_path}", file=sys.stderr)

# ─── Update Postgres ➔ legislators.portrait_url ─────────────────────────────
db_url = os.environ.get(DB_ENV_VAR)
if not db_url:
    print(f"[INFO] {DB_ENV_VAR} not set; skipping DB update", file=sys.stderr)
    sys.exit(0)

conn = psycopg2.connect(db_url)
cur  = conn.cursor()
for _, bioguide in mapped.items():
    portrait_url = f"/portraits/{bioguide}.jpg"
    cur.execute(
        "UPDATE legislators SET portrait_url = %s WHERE bioguide_id = %s",
        (portrait_url, bioguide)
    )
    print(f"[DB] Updated {bioguide}: {cur.rowcount} row(s)", file=sys.stderr)

conn.commit()
cur.close()
conn.close()
print("[INFO] Database portrait_url update complete", file=sys.stderr)
