import os
import logging
import requests
import xml.etree.ElementTree as ET
import psycopg2
from psycopg2.extras import execute_batch
from dotenv import load_dotenv

# Load environment variables
load_dotenv()
DB_URL = os.getenv("DATABASE_URL")

# Setup logging
logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")

VOTE_MAP = {
    "Yea": "Yea",
    "Nay": "Nay",
    "Present": "Present",
    "Not Voting": "Not Voting",
    "Absent": "Absent",
    "Unknown": "Unknown"
}

def get_vote_session_map(cursor):
    cursor.execute("SELECT vote_id, id FROM vote_sessions;")
    result = cursor.fetchall()
    logging.info(f"📥 Loaded {len(result)} vote sessions from DB.")
    return {vote_id: id_ for vote_id, id_ in result}

def get_legislator_map(cursor):
    cursor.execute("SELECT bioguide_id, id FROM legislators;")
    result = cursor.fetchall()
    logging.info(f"👥 Loaded {len(result)} legislators from DB.")
    return {bio.upper(): id_ for bio, id_ in result}

def is_valid_roll(congress, roll):
    url = f"https://clerk.house.gov/evs/{congress}/roll{str(roll).zfill(3)}.xml"
    try:
        response = requests.head(url, timeout=5)
        return response.status_code == 200
    except Exception:
        return False

def discover_valid_rolls(congress, max_roll=999):
    logging.info("🔍 Discovering valid roll calls from Clerk API...")
    valid_rolls = []
    for roll in range(1, max_roll + 1):
        if is_valid_roll(congress, roll):
            valid_rolls.append(roll)
        elif roll > 50 and len(valid_rolls) == 0:
            logging.warning("❌ No valid roll files found in the first 50 attempts. Check your congress/session values.")
            break
    logging.info(f"📦 Discovered {len(valid_rolls)} valid roll XMLs.")
    return valid_rolls

def parse_vote_records(congress, session, roll, session_map, legislator_map):
    vote_id = f"house-{congress}-{session}-{roll}"
    url = f"https://clerk.house.gov/evs/{congress}/roll{str(roll).zfill(3)}.xml"
    logging.info(f"📄 Processing vote XML: {url}")

    try:
        response = requests.get(url)
        response.raise_for_status()
        root = ET.fromstring(response.content)
    except Exception as e:
        logging.error(f"❌ Failed to fetch or parse XML from {url}: {e}")
        return []

    vote_session_id = session_map.get(vote_id)
    if not vote_session_id:
        logging.warning(f"⚠️ Vote session ID not found for {vote_id}. Skipping.")
        return []

    records = []
    skipped_missing_bio = 0
    skipped_unknown_legislator = 0

    for record in root.findall(".//recorded-vote"):
        legislator = record.find("legislator")
        if legislator is None:
            skipped_missing_bio += 1
            continue

        bioguide_id = legislator.attrib.get("bioGuideId") or legislator.attrib.get("name-id")
        if not bioguide_id:
            skipped_missing_bio += 1
            continue

        bioguide_id = bioguide_id.strip().upper()
        vote_cast = record.findtext("vote", default="Unknown").strip()
        normalized_vote = VOTE_MAP.get(vote_cast, "Unknown")

        legislator_id = legislator_map.get(bioguide_id)
        if not legislator_id:
            skipped_unknown_legislator += 1
            continue

        records.append((vote_session_id, legislator_id, normalized_vote))

    logging.info(
        f"🧾 Roll {roll}: Total={len(root.findall('.//recorded-vote'))} | Insert={len(records)} | "
        f"MissingBio={skipped_missing_bio} | UnknownLegislator={skipped_unknown_legislator}"
    )
    return records

def main():
    logging.info("🏁 Starting Vote Records ETL with dynamic roll discovery...")
    try:
        conn = psycopg2.connect(DB_URL)
        cursor = conn.cursor()

        session_map = get_vote_session_map(cursor)
        legislator_map = get_legislator_map(cursor)

        congress = 118
        session = 1
        valid_rolls = discover_valid_rolls(congress)

        total_inserted = 0
        for roll in valid_rolls:
            records = parse_vote_records(congress, session, roll, session_map, legislator_map)
            if records:
                execute_batch(cursor, """
                    INSERT INTO vote_records (vote_session_id, legislator_id, vote_cast)
                    VALUES (%s, %s, %s)
                    ON CONFLICT DO NOTHING;
                """, records)
                conn.commit()
                total_inserted += len(records)

        logging.info(f"✅ ETL complete. Inserted: {total_inserted} vote records from {len(valid_rolls)} rolls.")
    except Exception as e:
        logging.error(f"❌ Fatal error in ETL: {e}")
    finally:
        if 'cursor' in locals(): cursor.close()
        if 'conn' in locals(): conn.close()
        logging.info("🔚 Database connection closed.")

if __name__ == "__main__":
    main()
