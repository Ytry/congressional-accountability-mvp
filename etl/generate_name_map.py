#!/usr/bin/env python3
import os
import requests
import json
import logging
import sys

# URL for the official GitHub JSON of current legislators
CURRENT_URL = (
    "https://unitedstates.github.io/congress-legislators/legislators-current.json"
)


def fetch_legislators(url):
    """
    Fetch the legislators JSON from the provided URL.
    """
    r = requests.get(url, timeout=10)
    r.raise_for_status()
    return r.json()


def build_name_to_bioguide(output_path=None):
    """
    Build a mapping of full names to Bioguide IDs for current senators.

    If output_path is None, write to name_to_bioguide.json next to this script.
    """
    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s: %(message)s"
    )

    # Determine output file location
    if output_path is None:
        script_dir = os.path.dirname(__file__)
        output_path = os.path.join(script_dir, "name_to_bioguide.json")

    logging.info(f"Writing mapping to {output_path}")
    name_to_biog = {}

    # Fetch and filter only those with a Senate term
    legislators = fetch_legislators(CURRENT_URL)
    for person in legislators:
        terms = person.get("terms", [])
        # Only include if they have served in the Senate
        if not any(t.get("type") == "sen" for t in terms):
            continue

        biog_id = person.get("id", {}).get("bioguide")
        if not biog_id:
            continue

        first = person.get("name", {}).get("first", "")
        middle = person.get("name", {}).get("middle", "")
        last = person.get("name", {}).get("last", "")

        # Build full name
        parts = [first]
        if middle:
            parts.append(middle)
        if last:
            parts.append(last)
        full_name = " ".join(parts).strip()

        if full_name:
            name_to_biog[full_name] = biog_id

    # Write out the mapping
    with open(output_path, "w") as f:
        json.dump(name_to_biog, f, indent=2)

    logging.info(f"Wrote {len(name_to_biog)} entries")


if __name__ == "__main__":
    # Allow optional custom output path via CLI
    user_path = sys.argv[1] if len(sys.argv) > 1 else None
    build_name_to_bioguide(user_path)
