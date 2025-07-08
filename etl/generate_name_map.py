#!/usr/bin/env python3
import requests, json, logging, sys

CURRENT_URL = (
  "https://unitedstates.github.io/congress-legislators/legislators-current.json"
)

def fetch_legislators(url):
    r = requests.get(url, timeout=10)
    r.raise_for_status()
    return r.json()

def build_name_to_bioguide(output="name_to_bioguide.json"):
    logging.basicConfig(level=logging.INFO,format="%(asctime)s %(levelname)s: %(message)s")
    name_to_biog = {}

    for person in fetch_legislators(CURRENT_URL):
        # only map those with a Senate term
        terms = person.get("terms", [])
        if not any(t["type"]=="sen" for t in terms):
            continue

        biog = person["id"].get("bioguide")
        if not biog:
            continue

        first = person["name"]["first"]
        middle = person["name"].get("middle","")
        last = person["name"]["last"]
        full = f"{first} {middle+' ' if middle else ''}{last}".strip()

        name_to_biog[full] = biog

    with open(output,"w") as f:
        json.dump(name_to_biog, f, indent=2)
    logging.info(f"Wrote {len(name_to_biog)} entries")

if __name__=="__main__":
    build_name_to_bioguide()
