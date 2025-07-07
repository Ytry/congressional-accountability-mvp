# votes_etl.py — Dual Chamber ETL (House first, Senate fallback)

import os, requests, psycopg2, xml.etree.ElementTree as ET, csv, json, logging
from datetime import datetime
from dotenv import load_dotenv
from typing import List, Dict

load_dotenv()
logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")

DB_CONFIG = {
    "dbname": os.getenv("dbname"),
    "user": os.getenv("user"),
    "password": os.getenv("password"),
    "host": os.getenv("host"),
    "port": os.getenv("port"),
}

with open("icpsr_to_bioguide_full.json", "r") as f:
    ICPSR_TO_BIOGUIDE = json.load(f)

HOUSE_URL = "https://clerk.house.gov/evs/2023/roll{roll}.xml"
SENATE_URL = "https://www.senate.gov/legislative/LIS/roll_call_votes/vote{congress}{session}/vote_{congress}_{session}_{roll}.csv"

def db_connection():
    return psycopg2.connect(**DB_CONFIG)

def is_valid_url(url: str, expected_type: str) -> bool:
    try:
        resp = requests.get(url, timeout=5)
        if resp.status_code != 200:
            return False
        if expected_type == "xml" and not resp.content.strip().startswith(b"<?xml"):
            return False
        if expected_type == "csv" and "<!DOCTYPE" in resp.text:
            return False
        return True
    except Exception:
        return False

def vote_exists(cur, vote_id) -> bool:
    cur.execute("SELECT 1 FROM votes WHERE vote_id = %s", (vote_id,))
    return cur.fetchone() is not None

def parse_house_vote(congress: int, session: int, roll: int) -> Dict:
    url = HOUSE_URL.format(roll=str(roll).zfill(3))
    logging.info(f"📥 Fetching House vote from {url}")
    resp = requests.get(url)
    if resp.status_code != 200 or not resp.content.strip().startswith(b"<?xml"):
        logging.warning(f"⚠️ Invalid or missing House XML at {url}")
        return None

    root = ET.fromstring(resp.content)
    try:
        return {
            "vote_id": f"house-{congress}-{session}-{roll}",
            "congress": congress,
            "chamber": "house",
            "date": datetime.strptime(root.findtext(".//action-date"), "%d-%b-%Y"),
            "question": root.findtext(".//question-text"),
            "description": root.findtext(".//vote-desc"),
            "result": root.findtext(".//vote-result"),
            "bill_id": root.findtext(".//legis-num")
        }
    except Exception as e:
        logging.error(f"❌ Failed parsing House roll {roll}: {e}")
        return None

def parse_senate_vote(congress: int, session: int, roll: int) -> Dict:
    url = SENATE_URL.format(congress=congress, session=session, roll=str(roll).zfill(5))
    logging.info(f"📥 Fetching Senate vote from {url}")
    try:
        resp = requests.get(url)
        if resp.status_code != 200 or "<!DOCTYPE" in resp.text:
            logging.warning(f"⚠️ Invalid or HTML Senate content at {url}")
            return None
        rows = list(csv.DictReader(resp.content.decode().splitlines()))
        if not rows:
            return None
        return {
            "vote_id": f"senate-{congress}-{session}-{roll}",
            "congress": congress,
            "chamber": "senate",
            "date": datetime.strptime(rows[0]["Vote Date"], "%m/%d/%Y"),
            "question": rows[0].get("Vote Question"),
            "description": rows[0].get("Vote Title"),
            "result": rows[0].get("Result"),
            "bill_id": rows[0].get("Measure Number")
        }
    except Exception as e:
        logging.error(f"❌ Failed parsing Senate roll {roll}: {e}")
        return None

def insert_vote(vote: Dict):
    conn = db_connection()
    cur = conn.cursor()
    if vote_exists(cur, vote["vote_id"]):
        logging.info(f"⚠️ Skipping duplicate vote: {vote['vote_id']}")
        cur.close()
        conn.close()
        return False
    try:
        cur.execute("""
            INSERT INTO votes (
                vote_id, congress, chamber, date, question,
                description, result, bill_id
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """, (
            vote["vote_id"], vote["congress"], vote["chamber"], vote["date"],
            vote["question"], vote["description"], vote["result"], vote["bill_id"]
        ))
        conn.commit()
        logging.info(f"✅ Inserted vote: {vote['vote_id']}")
        return True
    except Exception as e:
        logging.error(f"❌ Insert failed for {vote['vote_id']}: {e}")
        conn.rollback()
        return False
    finally:
        cur.close()
        conn.close()

def run():
    logging.info("🚀 Starting Vote ETL")
    congress, session = 118, 1
    max_misses = 20
    consecutive_misses = 0
    inserted_count = 0

    for roll in range(1, 2000):
        if consecutive_misses >= max_misses:
            logging.info("🛑 Too many consecutive misses. Ending.")
            break

        vote_data = None
        if is_valid_url(HOUSE_URL.format(roll=str(roll).zfill(3)), "xml"):
            vote_data = parse_house_vote(congress, session, roll)
        elif is_valid_url(SENATE_URL.format(congress=congress, session=session, roll=str(roll).zfill(5)), "csv"):
            vote_data = parse_senate_vote(congress, session, roll)

        if vote_data:
            if insert_vote(vote_data):
                inserted_count += 1
            consecutive_misses = 0
        else:
            consecutive_misses += 1

    logging.info(f"🎯 ETL Complete. Votes Inserted: {inserted_count}")

if __name__ == "__main__":
    run()
