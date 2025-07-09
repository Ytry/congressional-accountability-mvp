import requests
import psycopg2
import json
import yaml
import os
import logging
from typing import Optional, List

# --- Logging setup ---
logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")

# --- Environment config ---
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")

CURRENT_URL = "https://raw.githubusercontent.com/unitedstates/congress-legislators/main/legislators-current.yaml"
HISTORICAL_URL = "https://raw.githubusercontent.com/unitedstates/congress-legislators/main/legislators-historical.yaml"

def connect():
    return psycopg2.connect(
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
        host=DB_HOST,
        port=DB_PORT
    )

def fetch_yaml_data(url: str) -> List[dict]:
    logging.info(f"📥 Downloading YAML from: {url}")
    response = requests.get(url)
    response.raise_for_status()
    return yaml.safe_load(response.text)

def parse_legislator(raw) -> Optional[dict]:
    try:
        ids = raw.get("id", {})
        bioguide_id = ids.get("bioguide")
        icpsr_id = str(ids.get("icpsr")) if ids.get("icpsr") else None
        if not bioguide_id:
            return None

        name = raw.get("name", {})
        first = name.get("first", "")
        last = name.get("last", "")
        full_name = f"{first} {last}".strip()

        bio = raw.get("bio", {})
        birthday = bio.get("birthday")
        gender = bio.get("gender")

        terms = raw.get("terms", [])
        if not terms:
            return None

        last_term = terms[-1]
        party = last_term.get("party")
        chamber_type = last_term.get("type", "").lower()
        chamber = "House" if chamber_type == "rep" else "Senate" if chamber_type == "sen" else None
        if not chamber:
            return None

        state = last_term.get("state")
        district = last_term.get("district") if chamber == "House" else None
        portrait_url = f"https://theunitedstates.io/images/congress/450x550/{bioguide_id}.jpg"
        official_website_url = last_term.get("url")

        contact = {
            "address": last_term.get("address"),
            "phone": last_term.get("phone")
        }

        bio_snapshot = f"{birthday or ''} – {gender or ''}"

        return {
            "bioguide_id": bioguide_id,
            "icpsr_id": icpsr_id,
            "first_name": first,
            "last_name": last,
            "full_name": full_name,
            "gender": gender,
            "birthday": birthday,
            "party": party,
            "state": state,
            "district": district,
            "chamber": chamber,
            "portrait_url": portrait_url,
            "official_website_url": official_website_url,
            "office_contact": contact,
            "bio_snapshot": bio_snapshot,
            "terms": terms
        }
    except Exception as e:
        logging.error(f"❌ Failed to parse legislator: {e}")
        return None

def insert_legislator(cur, leg):
    cur.execute("""
        INSERT INTO legislators (
            bioguide_id, icpsr_id, first_name, last_name, full_name,
            gender, birthday, party, state, district, chamber,
            portrait_url, official_website_url, office_contact, bio_snapshot
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb, %s)
        ON CONFLICT (bioguide_id) DO UPDATE SET
            icpsr_id = EXCLUDED.icpsr_id,
            first_name = EXCLUDED.first_name,
            last_name = EXCLUDED.last_name,
            full_name = EXCLUDED.full_name,
            gender = EXCLUDED.gender,
            birthday = EXCLUDED.birthday,
            party = EXCLUDED.party,
            state = EXCLUDED.state,
            district = EXCLUDED.district,
            chamber = EXCLUDED.chamber,
            portrait_url = EXCLUDED.portrait_url,
            official_website_url = EXCLUDED.official_website_url,
            office_contact = EXCLUDED.office_contact,
            bio_snapshot = EXCLUDED.bio_snapshot
        RETURNING id;
    """, (
        leg["bioguide_id"], leg["icpsr_id"], leg["first_name"], leg["last_name"],
        leg["full_name"], leg["gender"], leg["birthday"], leg["party"], leg["state"],
        leg["district"], leg["chamber"], leg["portrait_url"],
        leg["official_website_url"], json.dumps(leg["office_contact"]),
        leg["bio_snapshot"]
    ))
    return cur.fetchone()[0]

def insert_service_history(cur, legislator_id, terms):
    for term in terms:
        cur.execute("""
            INSERT INTO service_history (
                legislator_id, start_date, end_date, chamber, state, district, party
            ) VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (legislator_id, start_date) DO NOTHING;
        """, (
            legislator_id,
            term.get("start"),
            term.get("end"),
            "House" if term.get("type") == "rep" else "Senate",
            term.get("state"),
            term.get("district") if term.get("type") == "rep" else None,
            term.get("party")
        ))

def insert_committee_roles(cur, legislator_id, terms):
    for term in terms:
        for committee in term.get("committees", []):
            cur.execute("""
                INSERT INTO committee_assignments (
                    legislator_id, congress, committee_name,
                    subcommittee_name, role
                ) VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT DO NOTHING;
            """, (
                legislator_id,
                term.get("congress"),
                committee.get("name"),
                committee.get("subcommittee"),
                committee.get("position", "Member")
            ))

def insert_leadership_roles(cur, legislator_id, terms):
    for term in terms:
        role = term.get("leadership_title")
        if role:
            cur.execute("""
                INSERT INTO leadership_roles (
                    legislator_id, congress, role
                ) VALUES (%s, %s, %s)
                ON CONFLICT (legislator_id, congress, role) DO NOTHING;
            """, (
                legislator_id,
                term.get("congress"),
                role
            ))

def run():
    logging.info("🚀 Starting legislator ETL job...")
    try:
        conn = connect()
        cur = conn.cursor()
        current_data = fetch_yaml_data(CURRENT_URL)
        historical_data = fetch_yaml_data(HISTORICAL_URL)
        combined = current_data + historical_data
    except Exception as e:
        logging.critical(f"❌ Startup failure: {e}")
        return

    success = skipped = failed = 0

    for raw in combined:
        leg = parse_legislator(raw)
        if not leg:
            skipped += 1
            continue
        try:
            legislator_id = insert_legislator(cur, leg)
            insert_service_history(cur, legislator_id, leg["terms"])
            insert_committee_roles(cur, legislator_id, leg["terms"])
            insert_leadership_roles(cur, legislator_id, leg["terms"])
            conn.commit()
            logging.info(f"✅ {leg['bioguide_id']} ({leg['full_name']})")
            success += 1
        except Exception as e:
            logging.error(f"❌ Failed for {leg['bioguide_id']}: {e}")
            conn.rollback()
            failed += 1

    cur.close()
    conn.close()

    logging.info("🏁 ETL Summary")
    logging.info(f"✅ Inserted: {success}")
    logging.info(f"⏭️ Skipped: {skipped}")
    logging.info(f"❌ Failed: {failed}")

if __name__ == "__main__":
    run()
