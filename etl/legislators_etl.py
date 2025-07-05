import requests
import psycopg2
import json
import yaml
import os
import logging
from typing import Optional

# --- Config ---
logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")

DB_NAME = os.getenv("dbname")
DB_USER = os.getenv("user")
DB_PASSWORD = os.getenv("password")
DB_HOST = os.getenv("host")
DB_PORT = os.getenv("port")

DATA_SOURCE_URL = "https://raw.githubusercontent.com/unitedstates/congress-legislators/main/legislators-current.yaml"

# --- Database Connection ---
def connect():
    return psycopg2.connect(
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
        host=DB_HOST,
        port=DB_PORT
    )

# --- Extract YAML from remote source ---
def extract_legislators():
    logging.info("Fetching legislators from YAML...")
    res = requests.get(DATA_SOURCE_URL)
    res.raise_for_status()
    return yaml.safe_load(res.text)

# --- Transform raw legislator to structured dict ---
def parse_legislator(raw) -> Optional[dict]:
    try:
        bioguide_id = raw["id"]["bioguide"]
        last_term = raw["terms"][-1]

        if not last_term.get("end"):  # Skip if not current
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
            "phone": last_term.get("phone")
        }

        bio_snapshot = f"{raw['bio'].get('birthday', '')} – {raw['bio'].get('gender', '')}"

        return {
            "bioguide_id": bioguide_id,
            "full_name": full_name,
            "party": party,
            "chamber": chamber,
            "state": state,
            "district": district,
            "portrait_url": portrait_url,
            "official_website_url": website,
            "office_contact": contact,
            "bio_snapshot": bio_snapshot,
            "terms": raw["terms"]
        }
    except KeyError as e:
        logging.warning(f"Skipping legislator due to missing key: {e}")
        return None

# --- Insert into DB ---
def insert_legislator(cur, leg):
    cur.execute("""
        INSERT INTO legislators (
            bioguide_id, full_name, party, chamber, state, district,
            portrait_url, official_website_url, office_contact, bio_snapshot
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb, %s)
        ON CONFLICT (bioguide_id) DO UPDATE SET
            full_name = EXCLUDED.full_name,
            party = EXCLUDED.party,
            chamber = EXCLUDED.chamber,
            state = EXCLUDED.state,
            district = EXCLUDED.district,
            portrait_url = EXCLUDED.portrait_url,
            official_website_url = EXCLUDED.official_website_url,
            office_contact = EXCLUDED.office_contact,
            bio_snapshot = EXCLUDED.bio_snapshot
        RETURNING id;
    """, (
        leg["bioguide_id"], leg["full_name"], leg["party"], leg["chamber"],
        leg["state"], leg["district"], leg["portrait_url"], leg["official_website_url"],
        json.dumps(leg["office_contact"]), leg["bio_snapshot"]
    ))
    return cur.fetchone()[0]

def insert_service_history(cur, legislator_id, terms):
    for term in terms:
        try:
            cur.execute("""
                INSERT INTO service_history (legislator_id, term_start, term_end)
                VALUES (%s, %s, %s)
                ON CONFLICT (legislator_id, term_start) DO NOTHING;
            """, (legislator_id, term.get("start"), term.get("end")))
        except Exception as e:
            logging.warning(f"Service history insert failed: {e}")

def insert_committee_roles(cur, legislator_id, terms):
    for term in terms:
        for committee in term.get("committees", []):
            try:
                cur.execute("""
                    INSERT INTO committee_assignments (
                        legislator_id, congress, committee_name,
                        subcommittee_name, role
                    )
                    VALUES (%s, %s, %s, %s, %s)
                    ON CONFLICT DO NOTHING;
                """, (
                    legislator_id,
                    term.get("congress"),
                    committee.get("name"),
                    committee.get("subcommittee"),
                    committee.get("position", "Member")
                ))
            except Exception as e:
                logging.warning(f"Committee assignment failed: {e}")

def insert_leadership_roles(cur, legislator_id, terms):
    for term in terms:
        role = term.get("leadership_title")
        if role:
            try:
                cur.execute("""
                    INSERT INTO leadership_roles (legislator_id, congress, role)
                    VALUES (%s, %s, %s)
                    ON CONFLICT (legislator_id, congress, role) DO NOTHING;
                """, (legislator_id, term.get("congress"), role))
            except Exception as e:
                logging.warning(f"Leadership insert failed: {e}")

# --- Main ETL Logic ---
def run():
    logging.info("Starting ETL process for legislators...")
    conn = connect()
    cur = conn.cursor()
    data = extract_legislators()

    success = skipped = failed = 0

    for raw in data:
        leg = parse_legislator(raw)
        if not leg:
            skipped += 1
            continue
        try:
            legislator_id = insert_legislator(cur, leg)
            insert_service_history(cur, legislator_id, leg["terms"])
            insert_committee_roles(cur, legislator_id, leg["terms"])
            insert_leadership_roles(cur, legislator_id, leg["terms"])
        except Exception as e:
            logging.error(f"Insert failed for {leg['bioguide_id']}: {e}")
            conn.rollback()
            failed += 1
        else:
            conn.commit()
            success += 1

    cur.close()
    conn.close()

    logging.info(f"ETL Summary: ✅ {success} inserted | ❌ {failed} failed | ⏭️ {skipped} skipped")

if __name__ == "__main__":
    run()
