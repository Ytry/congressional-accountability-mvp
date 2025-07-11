#!/usr/bin/env python3
import os
import io
import json
import requests
import fitz            # PyMuPDF
import pytesseract
from PIL import Image
import psycopg2
import sys

# ── CONFIG ───────────────────────────────────────────────────────────────────
SCRIPT_DIR   = os.path.dirname(__file__)
PDF_URL      = "https://www.govinfo.gov/content/pkg/GPO-PICTDIR-118/pdf/GPO-PICTDIR-118.pdf"
PDF_LOCAL    = os.path.join(SCRIPT_DIR, "pictorial_118.pdf")
OUT_DIR      = os.path.join(SCRIPT_DIR, "..", "public", "portraits")
MAP_FILE     = os.path.join(SCRIPT_DIR, "name_to_bioguide.json")
DB_ENV_VAR   = "DATABASE_URL"

os.makedirs(OUT_DIR, exist_ok=True)

# ── Load Name→BioGuide JSON ──────────────────────────────────────────────────
if not os.path.exists(MAP_FILE):
    print(f"[ERROR] Mapping file not found: {MAP_FILE}", file=sys.stderr)
    sys.exit(1)
with open(MAP_FILE) as mf:
    name_map = json.load(mf)

# ── Download PDF if needed ───────────────────────────────────────────────────
if not os.path.exists(PDF_LOCAL):
    resp = requests.get(PDF_URL, stream=True)
    resp.raise_for_status()
    with open(PDF_LOCAL, "wb") as f:
        for chunk in resp.iter_content(4096):
            f.write(chunk)
    print("[INFO] PDF downloaded")

# ── Extract images, OCR, save as {BIOGUIDE}.jpg ──────────────────────────────
doc     = fitz.open(PDF_LOCAL)
mapped  = {}
unmapped= []

for p in range(doc.page_count):
    page   = doc.load_page(p)
    for idx, img in enumerate(page.get_images(full=True), start=1):
        xref      = img[0]
        img_bytes = doc.extract_image(xref)["image"]
        pil_img   = Image.open(io.BytesIO(img_bytes))

        # OCR the caption box
        try:
            bbox    = page.get_image_bbox(xref)
            clip    = fitz.Rect(bbox.x0, bbox.y1 + 2, bbox.x1, bbox.y1 + bbox.height * 0.3)
            pix     = page.get_pixmap(clip=clip)
            cap_img = Image.frombytes("RGB", [pix.width, pix.height], pix.samples)
            name    = pytesseract.image_to_string(cap_img, config="--psm 7").strip().splitlines()[0]
        except Exception as e:
            name = None

        if not name:
            unmapped.append(f"page{p+1}_img{idx}")
            continue

        norm     = " ".join(name.split()).title()
        bioguide = name_map.get(norm)
        if not bioguide:
            unmapped.append(norm)
            continue

        out_path = os.path.join(OUT_DIR, f"{bioguide}.jpg")
        pil_img.save(out_path)
        mapped[norm] = bioguide

# ── Debug dump ───────────────────────────────────────────────────────────────
with open(os.path.join(SCRIPT_DIR, "pictorial_etl_debug.json"), "w") as df:
    json.dump({"mapped": mapped, "unmapped": unmapped}, df, indent=2)

# ── Update Postgres portrait_url ─────────────────────────────────────────────
db_url = os.environ.get(DB_ENV_VAR)
if db_url:
    conn = psycopg2.connect(db_url)
    cur  = conn.cursor()
    for bg in mapped.values():
        cur.execute(
            "UPDATE legislators SET portrait_url = %s WHERE bioguide_id = %s",
            (f"/portraits/{bg}.jpg", bg)
        )
    conn.commit()
    cur.close()
    conn.close()
else:
    print("[INFO] DATABASE_URL not set; skipping DB update")
