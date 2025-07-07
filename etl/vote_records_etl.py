import os
import logging
import requests
import xml.etree.ElementTree as ET
import psycopg2
from psycopg2.extras import execute_batch
from dotenv import load_dotenv
from datetime import datetime

# Load environment variables
load_dotenv()
DB_URL = os.getenv("DATABASE_URL")

# Setup logging
logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")

# Normalize vote cast options
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
    logging.info(f"Loaded {len(result)} vote sessions from DB.")
    return {vote_id: id_ for vote_id, id_ in result}

def get_legislator_map(cursor):
    cursor.execute("SELECT bioguide_id, id FROM legislators;")
    result = cursor.fetchall()
    logging.info(f"Loaded {len(result)} legislators from DB.")
    return {bio.upper(): id_ for bio, id_ in result}

def parse_vote_records(congress, session, roll, session_map, legislator_map):
    vote_id = f"house-{congress}-{session}-{roll}"
    url = f"https://clerk.house.gov/evs/{congress}/roll{str(roll).zfill(3)}.xml"
    logging.info(f"📄 Processing vote XML: {url}")

    try:
        response = requests.get(url)
        response.raise_for_status()
    except Exception as e:
        logging.error(f"❌ Failed to fetch XML from {url}: {e}")
        return []

    try:
        root = ET.fromstring(response.content)
        vote_session_id = session_map.get(vote_id)
        if not vote_session_id:
            logging.warning(f"⚠️ Skipping vote {vote_id} — not found in vote_sessions table.")
            return []

        records = []
        for record in root.findall(".//recorded-vote"):
            legislator = record.find("legislator")
            if legislator is None:
                continue
            bioguide_id = legislator.attrib.get("bioGuideId") or legislator.attrib.get("name-id")
            if not bioguide_id:
                logging.warning("⚠️ Skipping record with missing bioguide_id")
                continue

            bioguide_id = bioguide_id.strip().upper()
            vote_cast = record.findtext("vote", default="Unknown").strip()
            normalized_vote = VOTE_MAP.get(vote_cast, "Unknown")

            legislator_id = legislator_map.get(bioguide_id)
            if not legislator_id:
                logging.warning(f"⚠️ Legislator ID '{bioguide_id}' not found in DB — skipping.")
                continue

            records.append((vote_session_id, legislator_id, normalized_vote))
        return records
    except Exception as e:
        logging.error(f"❌ Failed to parse XML from {url}: {e}")
        return []

def main():
    logging.info("📥 Starting Vote Records ETL...")

    try:
        conn = psycopg2.connect(DB_URL)
        cursor = conn.cursor()

        session_map = get_vote_session_map(cursor)
        legislator_map = get_legislator_map(cursor)

        congress = 118
        session = 1
        roll_calls = list(range(1, 11))  # Expand as needed

        total_inserted = 0
        for roll in roll_calls:
            records = parse_vote_records(congress, session, roll, session_map, legislator_map)
            if records:
                logging.info(f"✅ Inserting {len(records)} vote records for roll {roll}")
                execute_batch(cursor, """
                    INSERT INTO vote_records (vote_session_id, legislator_id, vote_cast)
                    VALUES (%s, %s, %s)
                    ON CONFLICT DO NOTHING;
                """, records)
                total_inserted += len(records)
                conn.commit()
            else:
                logging.warning(f"⚠️ No records to insert for roll {roll}")

        logging.info(f"🎉 Finished inserting vote records. Total inserted: {total_inserted}")
    except Exception as e:
        logging.error(f"❌ ETL process failed: {e}")
    finally:
        if 'cursor' in locals():
            cursor.close()
        if 'conn' in locals():
            conn.close()
        logging.info("🏁 ETL connection closed.")

if __name__ == "__main__":
    main()
