import requests
import psycopg2
import json
import yaml
import os
import logging
from typing import Optional

# --- Logging Config ---
logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")

# --- Database Config ---
DB_NAME = os.getenv("dbname")
DB_USER = os.getenv("user")
DB_PASSWORD = os.getenv("password")
DB_HOST = os.getenv("host")
DB_PORT = os.getenv("port")

DATA_SOURCE_URL = "https://raw.githubusercontent.com/unitedstates/congress-legislators/main/legislators-current.yaml"

# --- Connect to DB ---
def connect():
    return psycopg2.connect(
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
        host=DB_HOST,
        port=DB_PORT
    )

# --- Extract YAML ---
def extract_legislators():
    logging.info("Fetching legislator YAML data...")
    res = requests.get(DATA_SOURCE_URL)
    res.raise_for_status()
    return yaml.safe_load(res.text)

# --- Parse Individual Legislator ---
def parse_legislator(raw) -> Optional[dict]:
    try:
        bioguide_id = raw["id"]["bioguide"]
        last_term = raw["terms"][-1]

        if not last_term.get("end"):
            logging.debug(f"Skipping {bioguide_id}: no end date on last term")
            return None

        full_name = f"{raw['name'].get('first', '')} {raw['name'].get('last', '')}".strip()
        party = last_term.get("party", "")[0]
        chamber = last_term.get("type", "").capitalize()
        state = last_term.get("state")
        district = last_term.get("district") if chamber == "House" else None
        portrait_url = f"https://theunitedstates.io/images/congress/450x550/{bioguide_id}.jpg"
        website = last_term.get("url")

        contact = {
            "address": last_term.get("address"),
            "phone": last_term.get("ph_
