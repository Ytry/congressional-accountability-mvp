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
DB_NAME = os.getenv("dbname")
DB_USER = os.getenv("user")
DB_PASSWORD = os.getenv("password")
DB_HOST = os.getenv("host")
DB_PORT = os.getenv("port")

CURRENT_URL = "https://raw.githubusercontent.com/unitedstates/congress-legislators/main/legislators-current.yaml"
HISTORICAL_URL = "https://raw.githubusercontent.com/unitedstates/congress-legislators/main/legislators-historical.yaml"

# --- Database Connection ---
def connect():
    try:
        return psycopg2.connect(
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD,
            host=DB_HOST,
            port=DB_PORT
        )
    except Exception as e:
        logging.critical(f"🚨 Database connection failed: {e}")
        raise

# --- Extract YAML data ---
def fetch_yaml_data(url: str) -> List[dict]:
    logging.info(f"📥 Downloading YAML from: {url}")
    response = requests.get(url)
    response.raise_for_status()
    return yaml.safe_load(response.text)

# --- Parse a single legislator record ---
def parse_legislator(raw) -> Optional[dict]:
    try:
        ids = raw.get("id", {})
        bioguide_id = ids.get("bioguide")
        icpsr = ids.get("icpsr")  # optional but included

        if not bioguide_id:
            logging.warning(f"⚠️ Missing bioguide_id. Skipping.")
            return None

        if not raw.get("terms"):
            logging.warning(f"⚠️ Missing 'terms' for {bioguide_id}")
            return None

        last_term = raw["terms"][-1]

        first = raw['name'].get('first', '')
        last = raw['name'].get('last', '')
        full_name = f"{first} {last}".strip()

        party = last_term.get("party", "")[0]

        chamber_raw = last_term.get("type", "").lower()
        chamber = "House" if chamber_raw == "rep" else "Senate" if chamber_raw == "sen" else None
        if chamber is None:
            logging.warning(f"⚠️ Unknown chamber type '{chamber_raw}' for {bioguide_id}. Skipping.")
            return None

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
            "icpsr": str(icpsr) if icpsr else None,
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
    except Exception as e:
        logging.error(f"❌ Failed to parse legislator: {e}")
        return None

# --- Insert logic ---
def insert_legislator(cur, leg):
    cur.execute("""
        INSERT INTO legislators (
            bioguide_id, icpsr, full_name, party, chamber, state, district,
            portrait_url, official_website_url, office_contact, bio_snapshot
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb, %s)
        ON CONFLICT (bioguide_id) DO UPDATE SET
            icpsr = EXCLUDED.icpsr,
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
        leg["bioguide_id"], leg["icpsr"], leg["full_name"], leg["party"],
        leg["chamber"], leg["state"], leg["district"], leg["portrait_url"],
        leg["official_website_url"], json.dumps(leg["office_contact"]),
        leg["bio_snapshot"]
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
            logging.warning(f"⚠️ Service history insert failed: {e}")

def insert_committee_roles(cur, legislator_id, terms):
    for term in terms:
        for committee in term.get("committees", []):
            try:
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
            except Exception as e:
                logging.warning(f"⚠️ Committee insert failed: {e}")

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
                logging.warning(f"⚠️ Leadership insert failed: {e}")

# --- Main run block ---
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
