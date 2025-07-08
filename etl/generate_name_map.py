#!/usr/bin/env python3
import requests
import json
import logging
import sys

# URLs for current + historical members (using 'main' branch)
CURRENT_URL = (
    "https://unitedstates.github.io/congress-legislators/legislators-current.json"
)
HISTORICAL_URL = (
    "https://unitedstates.github.io/congress-legislators/legislators-historical.json"
)


def fetch_legislators(url: str) -> list:
    logging.info(f"Fetching legislators from {url}")
    try:
        resp = requests.get(url, timeout=10)
        resp.raise_for_status()
        return resp.json()
    except Exception as e:
        logging.error(f"Failed to fetch {url}: {e}")
        sys.exit(1)


def build_name_to_bioguide(output_path: str = "name_to_bioguide.json"):
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")
    name_to_biog = {}

    for url in (CURRENT_URL, HISTORICAL_URL):
        for person in fetch_legislators(url):
            ids = person.get("id", {})
            biog = ids.get("bioguide")
            if not biog:
                continue

            first  = person["name"]["first"]
            middle = person["name"].get("middle", "")
            last   = person["name"]["last"]

            full = f"{first} {middle + ' ' if middle else ''}{last}".strip()
            if full in name_to_biog and name_to_biog[full] != biog:
                logging.warning(f"Collision: {full} → {name_to_biog[full]} overwritten by {biog}")
            name_to_biog[full] = biog

    with open(output_path, "w") as f:
        json.dump(name_to_biog, f, indent=2)
    logging.info(f"Wrote {len(name_to_biog)} entries to {output_path}")


if __name__ == "__main__":
    build_name_to_bioguide()
