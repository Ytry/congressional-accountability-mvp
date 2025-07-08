# votes_etl.py — Final Patched Version with Senate URL Fix

import os
import requests
import psycopg2
import xml.etree.ElementTree as ET
import json
import logging
from datetime import datetime
from dotenv import load_dotenv
from typing import Dict, Optional

# Load environment variables
load_dotenv()

# Logging
logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler()]
)

# DB config
DB_CONFIG = {
    "dbname": os.getenv("dbname"),
    "user": os.getenv("user"),
    "password": os.getenv("password"),
    "host": os.getenv("host"),
    "port": os.getenv("port"),
}

# Bioguide Map
with open("icpsr_to_bioguide_full.json", "r") as f:
    ICPSR_TO_BIOGUIDE = json.load(f)

HOUSE_URL = "https://clerk.house.gov/evs/2023/roll{roll}.xml"
SENATE_URL = "https://www.senate.gov/legislative/LIS/roll_call_votes/vote_{congress}_{session}/vote_{congress}_{session}_{roll}.xml"

def db_connection():
    return psycopg2.connect(**DB_CONFIG)

def vote_exists(cur, vote_id: str) -> bool:
    cur.execute("SELECT 1 FROM votes WHERE vote_id = %s", (vote_id,))
    return cur.fetchone() is not None

def parse_house_vote(congress: int, session: int, roll: int) -> Optional[Dict]:
    url = HOUSE_URL.format(roll=str(roll).zfill(3))
    logging.info(f"🏛️ HOUSE Roll {roll}: {url}")
    try:
        resp = requests.get(url, timeout=10)
        if resp.status_code != 200 or not resp.content.strip().startswith(b"<?xml"):
            logging.debug(f"House roll {roll} unavailable or invalid.")
            return None
        root = ET.fromstring(resp.content)
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
        logging.warning(f"⚠️ Failed to parse House roll {roll}: {e}")
        return None

def parse_senate_vote(congress: int, session: int, roll: int) -> Optional[Dict]:
    roll_str = str(roll).zfill(5)
    url = SENATE_URL.format(congress=congress, session=session, roll=roll_str)
    logging.info(f"🏛️ SENATE Roll {roll}: {url}")
    try:
        resp = requests.get(url, timeout=10)
        if "roll-call-vote-not-available" in resp.text.lower():
            logging.debug(f"Senate roll {roll} page says vote not available.")
            return None
        if resp.status_code != 200 or not resp.content.strip().startswith(b"<?xml"):
            logging.debug(f"Senate roll {roll} unavailable or invalid.")
            return None
        root = ET.fromstring(resp.content)
        return {
            "vote_id": f"senate-{congress}-{session}-{roll}",
            "congress": congress,
            "chamber": "senate",
            "date": datetime.strptime(root.findtext(".//vote_date"), "%m/%d/%Y"),
            "question": root.findtext(".//question"),
            "description": root.findtext(".//title"),
            "result": root.findtext(".//result"),
            "bill_id": root.findtext(".//measure_number")
        }
    except Exception as e:
        logging.warning(f"⚠️ Failed to parse Senate roll {roll}: {e}")
        return None

def insert_vote(vote: Dict) -> bool:
    conn = db_connection()
    cur = conn.cursor()
    try:
        if vote_exists(cur, vote["vote_id"]):
            logging.info(f"⏩ Already exists: {vote['vote_id']}")
            return False
        cur.execute("""
            INSERT INTO votes (
                vote_id, congress, chamber, date, question,
                description, result, bill_id
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """, (
            vote["vote_id"], vote["congress"], vote["chamber"],
            vote["date"], vote["question"], vote["description"],
            vote["result"], vote["bill_id"]
        ))
        conn.commit()
        logging.info(f"✅ Inserted: {vote['vote_id']}")
        return True
    except Exception as e:
        conn.rollback()
        logging.error(f"❌ Failed to insert vote {vote['vote_id']}: {e}")
        return False
    finally:
        cur.close()
        conn.close()

def run():
    logging.info("🚀 Starting votes_etl run")
    congress, session = 118, 1
    max_rolls = 3000
    max_misses = 50
    misses = 0
    inserted = 0

    for roll in range(1, max_rolls + 1):
        if misses >= max_misses:
            logging.warning("🛑 Too many consecutive misses — stopping early.")
            break

        logging.debug(f"🔎 Processing roll {roll}")
        vote = parse_house_vote(congress, session, roll)
        if not vote:
            logging.debug(f"🏛️ House vote {roll} missing — trying Senate")
            vote = parse_senate_vote(congress, session, roll)

        if vote:
            if insert_vote(vote):
                inserted += 1
            misses = 0
        else:
            misses += 1
            logging.info(f"📭 No vote found for roll {roll} (miss {misses})")

    logging.info(f"🎯 ETL finished. Total inserted: {inserted}")

if __name__ == "__main__":
    run()
