import requests
import yaml
import os
import psycopg2
import json
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

LEGISLATORS_YAML_URL = "https://raw.githubusercontent.com/unitedstates/congress-legislators/main/legislators-current.yaml"

def fetch_legislators():
    try:
        response = requests.get(LEGISLATORS_YAML_URL)
        response.raise_for_status()
        return yaml.safe_load(response.text)
    except Exception as e:
        print(f"Error fetching YAML: {e}")
        return []

def connect_db():
    try:
        return psycopg2.connect(
            host=os.getenv("DB_HOST"),
            dbname=os.getenv("DB_NAME"),
            user=os.getenv("DB_USER"),
            password=os.getenv("DB_PASSWORD"),
            port=os.getenv("DB_PORT"),
        )
    except Exception as e:
        print(f"Database connection error: {e}")
        return None

def truncate_string(value, max_length):
    if not value:
        return None
    return str(value)[:max_length]

def extract_bio_snapshot(bio, terms):
    birth = bio.get("birthday", "")
    gender = bio.get("gender", "")
    last_term = terms[-1] if terms else {}
    return f"{birth} - {gender}" if birth or gender else None

def extract_office_contact(term):
    contact = {
        "phone": term.get("phone"),
        "address": term.get("address"),
        "office": term.get("office"),
        "fax": term.get("fax"),
        "contact_form": term.get("contact_form"),
    }
    return json.dumps({k: v for k, v in contact.items() if v})

def insert_legislator(cur, legislator):
    try:
        bio = legislator.get("bio", {})
        id_info = legislator.get("id", {})
        terms = legislator.get("terms", [])
        last_term = terms[-1] if terms else {}

        if not (id_info.get("bioguide") and bio.get("first") and bio.get("last") and last_term):
            return "skipped", "Missing required fields"

        full_name = f"{bio.get('first', '')} {bio.get('last', '')}"
        party = truncate_string(last_term.get("party", ""), 1)
        chamber = last_term.get("type", "").capitalize()
        state = truncate_string(last_term.get("state", ""), 2)
        district = int(last_term["district"]) if chamber == "House" and "district" in last_term else None
        portrait_url = f"https://theunitedstates.io/images/congress/450x550/{id_info['bioguide']}.jpg"
        website = last_term.get("url")
        contact = extract_office_contact(last_term)
        snapshot = extract_bio_snapshot(bio, terms)
        created_at = datetime.utcnow()

        cur.execute("""
            INSERT INTO legislators (
                bioguide_id, full_name, party, chamber, state, district,
                portrait_url, official_website_url, office_contact, bio_snapshot, created_at
            ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        """, (
            id_info['bioguide'], full_name, party, chamber, state, district,
            portrait_url, website, contact, snapshot, created_at
        ))

        return "inserted", None

    except Exception as e:
        return "failed", str(e)

def run_etl():
    data = fetch_legislators()
    if not data:
        return {"inserted": 0, "skipped": 0, "failed": 0, "errors": ["No data fetched"]}

    conn = connect_db()
    if not conn:
        return {"inserted": 0, "skipped": 0, "failed": 0, "errors": ["DB connection failed"]}

    summary = {"inserted": 0, "skipped": 0, "failed": 0, "errors": []}
    with conn:
        with conn.cursor() as cur:
            for legislator in data:
                status, msg = insert_legislator(cur, legislator)
                summary[status] += 1
                if status == "failed":
                    bioguide = legislator.get("id", {}).get("bioguide", "UNKNOWN")
                    summary["errors"].append(f"DB error for {bioguide}: {msg}")
                elif status == "skipped":
                    bioguide = legislator.get("id", {}).get("bioguide", "UNKNOWN")
                    summary["errors"].append(f"Skipped {bioguide}: {msg}")
    return summary

if __name__ == "__main__":
    result = run_etl()
    print(json.dumps({"message": "ETL script completed successfully", "output": result}, indent=2))
