import os
import yaml
import requests
import psycopg2
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

DATABASE_URL = os.getenv("DATABASE_URL")
YAML_URL = "https://raw.githubusercontent.com/unitedstates/congress-legislators/main/legislators-current.yaml"

def fetch_yaml_data():
    try:
        response = requests.get(YAML_URL)
        response.raise_for_status()
        return yaml.safe_load(response.text)
    except Exception as e:
        raise RuntimeError(f"Error fetching/parsing YAML: {e}")

def extract_legislator_fields(record):
    try:
        bio = record.get("id", {})
        name = record.get("name", {})
        terms = record.get("terms", [])
        if not bio.get("bioguide") or not terms:
            raise ValueError("Missing bioguide or terms")

        last_term = terms[-1]
        if last_term.get("type") not in ["rep", "sen"]:
            raise ValueError("Non-rep/senator skipped")

        return {
            "bioguide_id": bio.get("bioguide"),
            "full_name": f"{name.get('first', '')} {name.get('last', '')}".strip(),
            "party": last_term.get("party"),
            "chamber": "House" if last_term["type"] == "rep" else "Senate",
            "state": last_term.get("state"),
            "district": last_term.get("district") if last_term["type"] == "rep" else None,
            "portrait_url": f"https://theunitedstates.io/images/congress/450x550/{bio.get('bioguide')}.jpg",
            "official_website_url": last_term.get("url"),
            "office_contact": {
                "phone": last_term.get("phone"),
                "address": last_term.get("address")
            },
            "bio_snapshot": {
                "birthdate": bio.get("birthday"),
                "gender": bio.get("gender", "unknown"),
                "twitter": bio.get("twitter", ""),
                "youtube": bio.get("youtube", "")
            },
            "created_at": datetime.utcnow()
        }
    except Exception as e:
        raise ValueError(f"Extraction error: {e}")

def insert_legislators(records):
    inserted, skipped, failed = 0, 0, 0
    errors = []

    with psycopg2.connect(DATABASE_URL) as conn:
        with conn.cursor() as cur:
            for rec in records:
                try:
                    fields = extract_legislator_fields(rec)
                    cur.execute("""
                        INSERT INTO legislators (
                            bioguide_id, full_name, party, chamber, state, district,
                            portrait_url, official_website_url, office_contact, bio_snapshot, created_at
                        )
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (bioguide_id) DO NOTHING
                    """, (
                        fields["bioguide_id"],
                        fields["full_name"],
                        fields["party"],
                        fields["chamber"],
                        fields["state"],
                        fields["district"],
                        fields["portrait_url"],
                        fields["official_website_url"],
                        json.dumps(fields["office_contact"]),
                        json.dumps(fields["bio_snapshot"]),
                        fields["created_at"]
                    ))
                    inserted += cur.rowcount
                except ValueError as ve:
                    skipped += 1
                    errors.append(f"Skipped record: {ve}")
                except Exception as e:
                    failed += 1
                    errors.append(f"DB error for {rec.get('id', {}).get('bioguide', 'unknown')}: {e}")

    return {"inserted": inserted, "skipped": skipped, "failed": failed, "errors": errors[:10]}

def main():
    try:
        yaml_data = fetch_yaml_data()
        result = insert_legislators(yaml_data)
        print({
            "message": "ETL script completed successfully",
            "output": result
        })
    except Exception as e:
        print({
            "message": "ETL script failed",
            "error": str(e)
        })

if __name__ == "__main__":
    import json
    main()
