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
    return {vote_id: id_ for vote_id, id_ in cursor.fetchall()}

def get_legislator_map(cursor):
    cursor.execute("SELECT bioguide_id, id FROM legislators;")
    return {bio.upper(): id_ for bio, id_ in cursor.fetchall()}

def parse_vote_records(congress, session, roll, session_map, legislator_map):
    vote_id = f"house-{congress}-{session}-{roll}"
    url = f"https://clerk.house.gov/evs/2023/roll{str(roll).zfill(3)}.xml"
    logging.info(f"📄 Processing: {url}")

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
            logging.warning(f"⚠️ Skipping vote {vote_id} (not found in vote_sessions)")
            return []

        records = []
        for record in root.findall(".//recorded-vote"):
            legislator = record.find("legislator")
            if legislator is None:
                continue
            bioguide_id = legislator.attrib.get("bioGuideId") or legislator.attrib.get("name-id")
            if not bioguide_id:
                logging.warning("⚠️ Missing bioguide_id in recorded vote")
                continue
            bioguide_id = bioguide_id.strip().upper()
            vote_cast = record.findtext("vote", default="Unknown").strip()
            normalized_vote = VOTE_MAP.get(vote_cast, "Unknown")

            legislator_id = legislator_map.get(bioguide_id)
            if not legislator_id:
                logging.warning(f"⚠️ Legislator ID '{bioguide_id}' not found in database.")
                continue

            records.append((vote_session_id, legislator_id, normalized_vote))
        return records
    except Exception as e:
        logging.error(f"❌ Error parsing XML from {url}: {e}")
        return []

def main():
    logging.info("📥 Starting Vote Records ETL")

    conn = psycopg2.connect(DB_URL)
    cursor = conn.cursor()
    session_map = get_vote_session_map(cursor)
    legislator_map = get_legislator_map(cursor)

    congress = 118
    session = 1
    roll_calls = [1, 2]  # Can be expanded

    batch_data = []
    for roll in roll_calls:
        records = parse_vote_records(congress, session, roll, session_map, legislator_map)
        batch_data.extend(records)

    if batch_data:
        logging.info(f"✅ Inserting {len(batch_data)} vote records...")
        execute_batch(cursor, """
            INSERT INTO vote_records (vote_session_id, legislator_id, vote_cast)
            VALUES (%s, %s, %s)
            ON CONFLICT DO NOTHING;
        """, batch_data)
        conn.commit()
        logging.info("✅ Insert complete.")
    else:
        logging.warning("⚠️ No valid vote records to insert.")

    cursor.close()
    conn.close()
    logging.info("🏁 Vote Records ETL completed.")

if __name__ == "__main__":
    main()
