#!/usr/bin/env python3
import os
import fitz      # PyMuPDF
import requests

PDF_URL   = "https://www.govinfo.gov/pagelookup?package=GPO-Congressional%20Pictorial%20Directory/118th%20Congress&download=pdf"
PDF_LOCAL = "pictorial_118.pdf"
OUT_DIR   = "portraits"

# 1) Download the PDF if we don't have it
if not os.path.exists(PDF_LOCAL):
    resp = requests.get(PDF_URL, stream=True)
    resp.raise_for_status()
    with open(PDF_LOCAL, "wb") as f:
        for chunk in resp.iter_content(1024):
            f.write(chunk)

# 2) Open with PyMuPDF
doc = fitz.open(PDF_LOCAL)
os.makedirs(OUT_DIR, exist_ok=True)

# 3) Loop pages & extract images
for page_number in range(len(doc)):
    page = doc[page_number]
    image_list = page.get_images(full=True)
    for img_index, img_info in enumerate(image_list):
        xref = img_info[0]
        base_image = doc.extract_image(xref)
        image_bytes = base_image["image"]
        ext = base_image["ext"]  # e.g. 'jpeg'
        
        # 4) Name the file by page & index
        filename = f"page{page_number+1:02d}_img{img_index+1:02d}.{ext}"
        path = os.path.join(OUT_DIR, filename)
        with open(path, "wb") as img_file:
            img_file.write(image_bytes)

        print(f"📸 saved {path}")

# 5) (Optional) OCR the page captions to auto‐map names to filenames,
#    or manually build a small JSON map: {"Daniel Sutherland": "page01_img03.jpg", …}
