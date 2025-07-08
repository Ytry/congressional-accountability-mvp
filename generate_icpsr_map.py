#!/usr/bin/env python3
import requests
import json
import logging
import sys

# Raw GitHub URLs (main branch)
CURRENT_URL    = (
    "https://raw.githubusercontent.com/"
    "unitedstates/congress-legislators/main/legislators-current.json"
)
HISTORICAL_URL = (
    "https://raw.githubusercontent.com/"
    "unitedstates/congress-legislators/main/legislators-historical.json"
)

def fetch_legislators(url: str) -> list:
    logging.info(f"Fetching legislators from {url}")
    try:
        r = requests.get(url, timeout=10)
        r.raise_for_status()
        return r.json()
    except Exception as e:
        logging.error(f"Failed to fetch {url}: {e}")
        sys.exit(1)

def build_icpsr_to_bioguide(output_path="icpsr_to_bioguide_full.json"):
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")
    mapping = {}

    for url in (CURRENT_URL, HISTORICAL_URL):
        for person in fetch_legislators(url):
            ids = person.get("id", {})
            icpsr    = ids.get("icpsr")
            bioguide = ids.get("bioguide")
            if icpsr and bioguide:
                # ICPSR in the JSON are integers, but your existing map keys
                # might be strings—convert to str to match.
                mapping[str(icpsr)] = bioguide

    with open(output_path, "w") as f:
        json.dump(mapping, f, indent=2)
    logging.info(f"Wrote {len(mapping)} ICPSR→BioGuide entries to {output_path}")

if __name__ == "__main__":
    build_icpsr_to_bioguide()
