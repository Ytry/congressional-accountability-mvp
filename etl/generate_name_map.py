#!/usr/bin/env python3
import requests
import json
import logging

# URLs for current + historical members
CURRENT_URL    = "https://theunitedstates.io/congress-legislators/legislators-current.json"
HISTORICAL_URL = "https://theunitedstates.io/congress-legislators/legislators-historical.json"

def fetch_legislators(url):
    logging.info(f"Fetching legislators from {url}")
    r = requests.get(url, timeout=10)
    r.raise_for_status()
    return r.json()

def build_name_to_bioguide(output_path="name_to_bioguide.json"):
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

            # build full name key
            if middle:
                full = f"{first} {middle} {last}"
            else:
                full = f"{first} {last}"

            if full in name_to_biog and name_to_biog[full] != biog:
                logging.warning(f"Collision: {full} → {name_to_biog[full]} overwritten by {biog}")
            name_to_biog[full] = biog

    # write out
    with open(output_path, "w") as f:
        json.dump(name_to_biog, f, indent=2)
    logging.info(f"Wrote {len(name_to_biog)} entries to {output_path}")

if __name__ == "__main__":
    build_name_to_bioguide()
