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
PDF_URL      = (
    "https://www.govinfo.gov/pagelookup?package=GPO-Congressional%20Pictorial%20Directory/118th%20Congress&download=pdf"
)
PDF_LOCAL    = "pictorial_118.pdf"
OUT_DIR      = "portraits"
MAPPING_FILE = "name_mapping.json"
DB_ENV_VAR   = "DATABASE_URL"

# Ensure output directory exists
os.makedirs(OUT_DIR, exist_ok=True)

# 1) Download the PDF if not present
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

# 3) Extract images and OCR captions
mapping = {}
for page_num in range(len(doc)):
    page = doc[page_num]
    pix_page = page.get_pixmap()  # full page for cropping
    page_img = Image.frombytes(
        "RGB", [pix_page.width, pix_page.height], pix_page.samples
    )
    images = page.get_images(full=True)

    for idx, img_info in enumerate(images, start=1):
        xref = img_info[0]
        img_dict = doc.extract_image(xref)
        img_bytes = img_dict["image"]
        ext = img_dict.get("ext", "png")

        # save headshot
        filename = f"page{page_num+1:02d}_img{idx:02d}.{ext}"
        out_path = os.path.join(OUT_DIR, filename)
        with open(out_path, "wb") as img_file:
            img_file.write(img_bytes)
        print(f"Saved headshot: {out_path}")

        # get bbox for cropping caption region
        try:
            bbox = page.get_image_bbox(xref)
            # crop just below image (30% of its height)
            cap_rect = fitz.Rect(
                bbox.x0,
                bbox.y1 + 2,
                bbox.x1,
                bbox.y1 + (bbox.height * 0.3)
            )
            cap_pix = page.get_pixmap(clip=cap_rect)
            cap_img = Image.frombytes(
                "RGB", [cap_pix.width, cap_pix.height], cap_pix.samples
            )
            # OCR single-line text
            text = pytesseract.image_to_string(cap_img, config='--psm 7').strip()
            name = text.splitlines()[0] if text else None
            if name:
                mapping[name] = filename
                print(f"Mapped: '{name}' → {filename}")
            else:
                print(f"No OCR text for {filename}")
        except Exception as e:
            print(f"OCR error for {filename}: {e}")

# 4) Save mapping
with open(MAPPING_FILE, 'w') as mf:
    json.dump(mapping, mf, indent=2)
print(f"Name mapping saved: {MAPPING_FILE}")

# 5) Update database
if DB_ENV_VAR not in os.environ:
    print(f"Error: env var {DB_ENV_VAR} not set. Cannot update DB.")
    exit(1)

db_url = os.environ[DB_ENV_VAR]
conn = psycopg2.connect(db_url)
cur = conn.cursor()

print("Updating legislators.portrait_url from mapping…")
for full_name, fname in mapping.items():
    portrait_url = f"/portraits/{fname}"
    cur.execute(
        """
        UPDATE legislators
           SET portrait_url = %s
         WHERE full_name = %s
        """,
        (portrait_url, full_name)
    )
    if cur.rowcount:
        print(f"Updated {full_name}")
    else:
        print(f"No match for {full_name}")

conn.commit()
cur.close()
conn.close()
print("Database update complete.")
