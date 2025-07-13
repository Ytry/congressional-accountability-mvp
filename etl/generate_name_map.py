#!/usr/bin/env python3
"""
generate_name_map.py

Fetch the current legislators JSON and build a mapping of full Senate member names to Bioguide IDs.
Writes to a default path from config.NAME_TO_BIO_MAP or an optional CLI argument.
"""
import sys
from pathlib import Path

import config
from logger import setup_logger
from utils import load_json_from_url, write_json

# Initialize structured logger
logger = setup_logger("generate_name_map")

# Configuration
LEGIS_URL = config.LEGIS_JSON_URL
OUTPUT_DEFAULT = config.NAME_TO_BIO_MAP


def build_name_to_bioguide(output_path: Path):
    logger.info("Building name_to_bioguide mapping", extra={"output_path": str(output_path)})
    try:
        legislators = load_json_from_url(LEGIS_URL)
        logger.info("Fetched legislators list", extra={"source": LEGIS_URL, "count": len(legislators)})
    except Exception:
        logger.exception("Failed to fetch legislators JSON")
        sys.exit(1)

    mapping = {}
    for person in legislators:
        terms = person.get("terms", [])
        # Only include those with a Senate term
        if not any(t.get("type") == "sen" for t in terms):
            continue

        biog_id = person.get("id", {}).get("bioguide")
        if not biog_id:
            continue

        # Construct full name (first, middle, last)
        name = person.get("name", {})
        parts = [name.get(k, "") for k in ("first", "middle", "last")]
        full_name = " ".join([p for p in parts if p]).strip()
        if full_name:
            mapping[full_name] = biog_id

    try:
        write_json(output_path, mapping)
        logger.info("Wrote name_to_bioguide file", extra={"entries": len(mapping), "path": str(output_path)})
    except Exception:
        logger.exception("Failed to write name_to_bioguide file")
        sys.exit(1)


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(
        description="Generate mapping of Senate member names to Bioguide IDs"
    )
    parser.add_argument(
        "output",
        nargs='?', 
        type=str,
        default=str(OUTPUT_DEFAULT),
        help="Output path for name_to_bioguide.json"
    )
    args = parser.parse_args()
    build_name_to_bioguide(Path(args.output))
