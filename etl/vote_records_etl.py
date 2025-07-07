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
logging.basicConfig(
    level=logging.INFO,
    format="%(levelname)s: %(message)s"
)

# Normalize vote cast options
VOTE_MAP = {
    "Yea": "Yea",
    "Nay": "Nay",
    "Present": "Present",
    "Not Voting": "Not Voting",
    "Absent": "Absent",
    "Unknown": "Unknown"
}

def parse_house_vote_xml(xml_url, vote_id_map):
    try:
        response = requests.get(xml_url)
        response.raise_for_status()
    except Exception as e:
        logging.error(f"Failed to fetch XML from {xml_url}: {e}")
        return []

    try:
        root = ET.fromstring(response.content)
        vote_number = root.findtext(".//roll-call-num")
        session = root.findtext(".//congress")
        vote_id = f"{session}_house_{vote_number.zfill(3)}"

        vote_id_fk = vote_id_map.get(vote_id)
        if not vote_id_fk:
            logging.warning(f"Skipping vote {vote_id} (not found in vote_id_map)")
            return []

        records = []
        for voter in root.findall(".//recorded-vote"):
            bioguide_id = voter.findtext("legislator")
            vote_cast = voter.findtext("vote")
            normalized_vote = VOTE_MAP.get(vote_cast, "Unknown")
            records.append((vote_id_fk, bioguide_id, normalized_vote))
        return records
    except Exception as e:
        logging.error(f"Error parsing XML from {xml_url}: {e}")
        return []

def get_vote_id_map(cursor):
    cursor.execute("SELECT vote_id, id FROM votes;")
    return {vote_id: id_ for vote_id, id_ in cursor.fetchall()}

def get_legislator_id_map(cursor):
    cursor.execute("SELECT bioguide_id, id FROM legislators;")
    return {bio: id_ for bio, id_ in cursor.fetchall()}

def main():
    logging.info("📥 Starting Vote Records ETL")

    conn = psycopg2.connect(DB_URL)
    cursor = conn.cursor()

    vote_id_map = get_vote_id_map(cursor)
    legislator_id_map = get_legislator_id_map(cursor)

    vote_record_urls = [
        "https://clerk.house.gov/evs/2023/roll001.xml",
        "https://clerk.house.gov/evs/2023/roll002.xml",
        # Add more URLs as needed
    ]

    batch_data = []
    for url in vote_record_urls:
        logging.info(f"📄 Processing: {url}")
        records = parse_house_vote_xml(url, vote_id_map)

        for vote_fk, bioguide_id, vote_cast in records:
            legislator_fk = legislator_id_map.get(bioguide_id)
            if not legislator_fk:
                logging.warning(f"⚠️ Bioguide ID '{bioguide_id}' not found in legislators table.")
                continue
            batch_data.append((vote_fk, legislator_fk, vote_cast))

    if batch_data:
        logging.info(f"🟢 Inserting {len(batch_data)} vote records into vote_records...")
        execute_batch(cursor, """
            INSERT INTO vote_records (vote_id, legislator_id, vote_cast)
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
