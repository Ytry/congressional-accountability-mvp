#!/usr/bin/env python3
import os
import io
import json
import requests
import fitz      # PyMuPDF
import pytesseract
from PIL import Image
import psycopg2

# ----- Configuration -----
PDF_URL        = "https://www.govinfo.gov/content/pkg/GPO-PICTDIR-118/pdf/GPO-PICTDIR-118.pdf"
PDF_LOCAL      = "pictorial_118.pdf"
OUT_DIR        = os.path.join("public", "portraits")
DEBUG_MAP_FILE = "name_to_bioguide.json"
DB_ENV_VAR     = "DATABASE_URL"

# ----- Name→BioGuide map (you must provide this) -----
# Expects a module `generate_name_map.py` next to this script:
#    name_map = { "John Smith": "S000123", ... }
from generate_name_map import name_map

# Ensure output directory exists
os.makedirs(OUT_DIR, exist_ok=True)

# 1) Download PDF if needed
if not os.path.exists(PDF_LOCAL):
    print(f"Downloading PDF to {PDF_LOCAL}…")
    resp = requests.get(PDF_URL, stream=True)
    resp.raise_for_status()
    with open(PDF_LOCAL, "wb") as f:
        for chunk in resp.iter_content(1024):
            f.write(chunk)
    print("Download complete.")

# 2) Open PDF
doc = fitz.open(PDF_LOCAL)

# 3) Extract, OCR & save portraits
mapped = {}     # for debug: name → bioguide
unmapped = []   # names we saw but couldn’t map

for page_num in range(len(doc)):
    page = doc[page_num]
    images = page.get_images(full=True)

    for idx, img_info in enumerate(images, start=1):
        xref     = img_info[0]
        img_dict = doc.extract_image(xref)
        img_bytes= img_dict["image"]
        ext      = img_dict.get("ext", "jpg")
        img      = Image.open(io.BytesIO(img_bytes))

        # 3a) OCR caption region
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
            print(f"[WARN] OCR failed for page{page_num+1}_img{idx}: {e}")
            name = None

        if not name:
            unmapped.append(f"page{page_num+1}_img{idx}")
            continue

        # 3b) Normalize & lookup BioGuide
        norm = " ".join(name.split()).title()
        bioguide = name_map.get(norm)
        if not bioguide:
            print(f"[WARN] No BioGuide mapping for '{norm}'")
            unmapped.append(norm)
            continue

        # 3c) Save as {bioguide}.jpg
        out_fn  = f"{bioguide}.jpg"
        out_path= os.path.join(OUT_DIR, out_fn)
        img.save(out_path)
        print(f"[OK] Saved portrait for {norm} → {out_fn}")
        mapped[norm] = bioguide

# 4) (Optional) write debug map
with open(DEBUG_MAP_FILE, "w") as dm:
    json.dump({"mapped": mapped, "unmapped": unmapped}, dm, indent=2)
print(f"Debug mapping written to {DEBUG_MAP_FILE}")

# 5) Update PostgreSQL if configured
if DB_ENV_VAR not in os.environ:
    print(f"[INFO] {DB_ENV_VAR} not set – skipping DB update")
    exit(0)

conn = psycopg2.connect(os.environ[DB_ENV_VAR])
cur  = conn.cursor()
print("Updating legislators.portrait_url…")
for name, bg in mapped.items():
    portrait_url = f"/portraits/{bg}.jpg"
    # Use bioguide_id in WHERE for reliability
    cur.execute(
        """
        UPDATE legislators
           SET portrait_url = %s
         WHERE bioguide_id = %s
        """,
        (portrait_url, bg)
    )
    print(
        cur.rowcount
        and f"  ✔ {name} ({bg})"
        or f"  ✖ no row for {name} ({bg})"
    )

conn.commit()
cur.close()
conn.close()
print("Database update complete.")
