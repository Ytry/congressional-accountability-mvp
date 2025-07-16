#!/usr/bin/env python3
"""
generate_name_map.py

Fetch the current legislators JSON and build a mapping of full Senate member names to Bioguide IDs.
Writes to a default path from config.NAME_TO_BIO_MAP or an optional CLI argument.
"""
import sys
from pathlib import Path
from typing import Dict

import config
from logger import setup_logger
from utils import load_json_from_url, write_json

# Initialize structured logger
logger = setup_logger("generate_name_map")

# Configuration
LEGIS_URL = config.LEGIS_JSON_URL
OUTPUT_DEFAULT = config.NAME_TO_BIO_MAP


def build_name_to_bioguide(output_path: Path) -> None:
    logger.info("Building name_to_bioguide mapping", extra={"output_path": str(output_path)})
    try:
        legislators = load_json_from_url(LEGIS_URL)
        if not legislators:
            raise ValueError("Fetched legislators list is empty")
        logger.info("Fetched legislators list", extra={"source": LEGIS_URL, "count": len(legislators)})
    except Exception:
        logger.exception("Failed to fetch legislators JSON")
        sys.exit(1)

    mapping: Dict[str, str] = {}
    skipped_count = 0
    for person in legislators:
        terms = person.get("terms", [])
        # Only include those with a Senate term
        if not any(t.get("type") == "sen" for t in terms):
            continue

        biog_id = person.get("id", {}).get("bioguide")
        if not biog_id:
            logger.warning("Skipping person without bioguide ID", extra={"person": person.get("name")})
            skipped_count += 1
            continue

        # Construct full name, preferring official_full
        name = person.get("name", {})
        full_name = name.get("official_full", "").strip()
        if not full_name:
            parts = [name.get("first", ""), name.get("middle", ""), name.get("last", "")]
            full_name = " ".join(p for p in parts if p).strip()
            if name.get("suffix"):
                full_name += f" {name['suffix']}"

        if not full_name:
            logger.warning("Skipping person without valid full name", extra={"biog_id": biog_id})
            skipped_count += 1
            continue

        # Add primary full name
        _add_to_mapping(mapping, full_name, biog_id)

        # Add variants for better reconciliation (aligns with ETL name variants)
        # Variant 1: First + Last (common short form)
        short_name = f"{name.get('first', '')} {name.get('last', '')}".strip()
        if short_name != full_name:
            _add_to_mapping(mapping, short_name, biog_id)

        # Variant 2: Nickname + Last (if present)
        if name.get("nickname"):
            nick_name = f"{name['nickname']} {name.get('last', '')}".strip()
            if nick_name != full_name and nick_name != short_name:
                _add_to_mapping(mapping, nick_name, biog_id)

    if skipped_count > 0:
        logger.info("Skipped entries during mapping", extra={"skipped_count": skipped_count})

    try:
        write_json(output_path, mapping)
        logger.info("Wrote name_to_bioguide file", extra={"entries": len(mapping), "path": str(output_path)})
    except Exception:
        logger.exception("Failed to write name_to_bioguide file")
        sys.exit(1)


def _add_to_mapping(mapping: Dict[str, str], key: str, biog_id: str) -> None:
    if key in mapping:
        if mapping[key] != biog_id:
            logger.warning("Name conflict detected; overwriting", extra={"name": key, "existing_id": mapping[key], "new_id": biog_id})
    mapping[key] = biog_id


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(
        description="Generate mapping of Senate member names to Bioguide IDs"
    )
    parser.add_argument(
        "output",
        nargs="?",
        type=str,
        default=str(OUTPUT_DEFAULT),
        help="Output path for name_to_bioguide.json"
    )
    args = parser.parse_args()
    build_name_to_bioguide(Path(args.output))
