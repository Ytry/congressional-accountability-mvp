#!/usr/bin/env python3
"""
ETL script to download the Congressional Pictorial Directory PDF for the 118th Congress,
extract each portrait image, OCR the page to capture the legislator's name, and
produce a JSON map of full_name → [filenames].
"""
import os
import re
import json
import fitz      # PyMuPDF
import requests
from PIL import Image
import pytesseract

# Constants
PDF_URL   = (
    "https://www.govinfo.gov/pagelookup?"
    "package=GPO-Congressional%20Pictorial%20Directory/118th%20Congress&download=pdf"
)
PDF_LOCAL = "pictorial_118.pdf"
OUT_DIR   = "portraits"
MAP_FILE  = "portrait_map.json"
DPI        = 150


def download_pdf():
    """Download the PDF if it's not already present."""
    if not os.path.exists(PDF_LOCAL):
        print(f"Downloading PDF to {PDF_LOCAL}...")
        resp = requests.get(PDF_URL, stream=True)
        resp.raise_for_status()
        with open(PDF_LOCAL, "wb") as f:
            for chunk in resp.iter_content(1024):
                f.write(chunk)
        print("Download complete.")


def sanitize_name(name: str) -> str:
    """Convert a full name into a filesystem-safe base filename."""
    # replace any non-alphanumeric runs with underscore, trim
    return re.sub(r'[^0-9A-Za-z]+', '_', name).strip('_')


def extract_images_and_captions():
    """Open the PDF, OCR each page for the caption name, extract images, and map them."""
    doc = fitz.open(PDF_LOCAL)
    os.makedirs(OUT_DIR, exist_ok=True)
    mapping = {}

    for page_num in range(len(doc)):
        page = doc[page_num]

        # 1) Render the page to an image for OCR
        pix = page.get_pixmap(dpi=DPI)
        img = Image.frombytes("RGB", [pix.width, pix.height], pix.samples)
        text = pytesseract.image_to_string(img)

        # 2) Find the first line that looks like a 'First Last' pattern
        name = None
        for line in text.splitlines():
            line = line.strip()
            if re.match(r'^[A-Z][a-z]+(?: [A-Z][a-z]+)+$', line):
                name = line
                break
        if not name:
            name = f"page{page_num+1}"
            print(f"⚠️  No name OCR’d on page {page_num+1}, using '{name}'")

        base = sanitize_name(name)
        mapping[name] = []

        # 3) Extract every image on the page
        for idx, img_info in enumerate(page.get_images(full=True)):
            xref = img_info[0]
            base_image = doc.extract_image(xref)
            img_bytes  = base_image["image"]
            ext        = base_image["ext"]  # e.g. 'jpeg'

            filename = f"{base}_{idx+1}.{ext}"
            out_path = os.path.join(OUT_DIR, filename)
            with open(out_path, "wb") as f:
                f.write(img_bytes)
            print(f"✅ Saved {out_path}")
            mapping[name].append(filename)

    # 4) Write out the JSON map
    with open(MAP_FILE, "w") as mf:
        json.dump(mapping, mf, indent=2)
    print(f"Mapping written to {MAP_FILE}")


if __name__ == "__main__":
    download_pdf()
    extract_images_and_captions()
