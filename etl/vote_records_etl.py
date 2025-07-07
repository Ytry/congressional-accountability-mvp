
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

# Vote cast normalization
VOTE_MAP = {
    "Yea": "Yea",
    "Nay": "Nay",
    "Present": "Present",
    "Not Voting": "Not Voting",
    "Absent": "Absent",
    "Unknown": "Unknown"
}

def parse_house_vote_xml(xml_url, vote_id_map):
    response = requests.get(xml_url)
    root = ET.fromstring(response.content)
    vote_id = root.findtext(".//roll-call-num")
    vote_id_fk = vote_id_map.get(vote_id)

    if not vote_id_fk:
        logging.warning(f"Skipping vote {vote_id} (not in vote_id_map)")
        return []

    records = []
    for voter in root.findall(".//recorded-vote"):
        bioguide_id = voter.findtext("legislator")
        vote_cast = voter.findtext("vote")
        normalized_vote = VOTE_MAP.get(vote_cast, "Unknown")

        records.append((vote_id_fk, bioguide_id, normalized_vote))

    return records

def get_vote_id_map(cursor):
    cursor.execute("SELECT vote_id, id FROM votes;")
    return {row[0]: row[1] for row in cursor.fetchall()}

def get_legislator_id_map(cursor):
    cursor.execute("SELECT bioguide_id, id FROM legislators;")
    return {row[0]: row[1] for row in cursor.fetchall()}

def main():
    logging.info("📥 Starting Vote Records ETL")

    conn = psycopg2.connect(DB_URL)
    cursor = conn.cursor()

    vote_id_map = get_vote_id_map(cursor)
    legislator_id_map = get_legislator_id_map(cursor)

    vote_record_urls = [
        "https://clerk.house.gov/evs/2023/roll001.xml",
        "https://clerk.house.gov/evs/2023/roll002.xml"
    ]

    batch_data = []
    for url in vote_record_urls:
        logging.info(f"📄 Parsing {url}")
        records = parse_house_vote_xml(url, vote_id_map)

        for vote_fk, bioguide_id, vote_cast in records:
            legislator_fk = legislator_id_map.get(bioguide_id)
            if not legislator_fk:
                logging.warning(f"Bioguide ID {bioguide_id} not found.")
                continue
            batch_data.append((vote_fk, legislator_fk, vote_cast))

    if batch_data:
        logging.info(f"🟢 Inserting {len(batch_data)} vote records...")
        execute_batch(cursor,
            "INSERT INTO vote_records (vote_id, legislator_id, vote_cast) VALUES (%s, %s, %s) ON CONFLICT DO NOTHING",
            batch_data)
        conn.commit()
    else:
        logging.warning("No vote records to insert.")

    cursor.close()
    conn.close()
    logging.info("✅ Vote Records ETL completed.")

if __name__ == "__main__":
    main()
