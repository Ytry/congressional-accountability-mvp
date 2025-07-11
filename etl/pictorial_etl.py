#!/usr/bin/env python3
import os
import io
import json
import requests
import fitz      # PyMuPDF
import pytesseract
from PIL import Image

PDF_URL       = (
    "https://www.govinfo.gov/pagelookup?package=GPO-Congressional%20Pictorial%20Directory/118th%20Congress&download=pdf"
)
PDF_LOCAL     = "pictorial_118.pdf"
OUT_DIR       = "portraits"
MAPPING_FILE  = "name_mapping.json"

# Ensure output directory exists
os.makedirs(OUT_DIR, exist_ok=True)

# 1) Download the PDF if we don't have it
if not os.path.exists(PDF_LOCAL):
    print(f"Downloading PDF to {PDF_LOCAL}…")
    resp = requests.get(PDF_URL, stream=True)
    resp.raise_for_status()
    with open(PDF_LOCAL, "wb") as f:
        for chunk in resp.iter_content(1024):
            f.write(chunk)
    print("Download complete.")

# 2) Open PDF with PyMuPDF
doc = fitz.open(PDF_LOCAL)

# 3) Prepare mapping dict
mapping = {}

# 4) Loop pages & extract images + captions
for page_number in range(len(doc)):
    page = doc[page_number]
    images = page.get_images(full=True)
    # render full page for caption cropping
    page_pix = page.get_pixmap()
    page_img = Image.frombytes("RGB", [page_pix.width, page_pix.height], page_pix.samples)

    for img_index, img_info in enumerate(images):
        xref = img_info[0]
        # 4a) Extract and save the headshot image
        base_image = doc.extract_image(xref)
        img_bytes  = base_image["image"]
        ext        = base_image["ext"]
        img = Image.open(io.BytesIO(img_bytes))

        img_filename = f"page{page_number+1:02d}_img{img_index+1:02d}.{ext}"
        img_path     = os.path.join(OUT_DIR, img_filename)
        img.save(img_path)
        print(f"📸 Saved headshot {img_path}")

        # 4b) Locate the image bbox and crop just below for caption
        try:
            bbox = page.get_image_bbox(xref)
            # extend bbox downwards for caption region (30% of image height)
            caption_rect = fitz.Rect(
                bbox.x0,
                bbox.y1 + 2,
                bbox.x1,
                bbox.y1 + bbox.height * 0.3
            )
            cap_pix = page.get_pixmap(clip=caption_rect)
            cap_img = Image.frombytes(
                "RGB", [cap_pix.width, cap_pix.height], cap_pix.samples
            )

            # 4c) OCR the caption region to get the name
            text = pytesseract.image_to_string(cap_img, config='--psm 7').strip()
            name = text.splitlines()[0] if text else ''
            if name:
                mapping[name] = img_filename
                print(f"🔖 Mapped '{name}' → {img_filename}")
            else:
                print(f"⚠️ No text found for caption of {img_filename}")
        except Exception as err:
            print(f"⚠️ OCR failed for {img_filename}: {err}")

# 5) Save the name → filename map
with open(MAPPING_FILE, "w") as f:
    json.dump(mapping, f, indent=2)
print(f"✅ Mapping saved to {MAPPING_FILE}")
