# votes_etl.py — Robust Final Version

import os
import requests
import psycopg2
import xml.etree.ElementTree as ET
import csv
import json
import logging
from datetime import datetime
from dotenv import load_dotenv
from typing import Dict, Optional

# Load environment variables
load_dotenv()

# Logging setup
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

# Load ICPSR -> Bioguide map
with open("icpsr_to_bioguide_full.json", "r") as f:
    ICPSR_TO_BIOGUIDE = json.load(f)

# URLs
HOUSE_URL = "https://clerk.house.gov/evs/2023/roll{roll}.xml"
SENATE_URL = "https://www.senate.gov/legislative/LIS/roll_call_votes/vote{congress}{session}/vote_{congress}_{session}_{roll}.csv"

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
    url = SENATE_URL.format(congress=congress, session=session, roll=str(roll).zfill(5))
    logging.info(f"🏛️ SENATE Roll {roll}: {url}")
    try:
        resp = requests.get(url, timeout=10)
        if resp.status_code != 200 or "roll-call-vote-not-available" in resp.text.lower():
            logging.debug(f"Senate roll {roll} unavailable.")
            return None
        lines = resp.content.decode("utf-8").splitlines()
        reader = list(csv.DictReader(lines))
        if not reader or "Vote Date" not in reader[0]:
            logging.debug(f"Senate CSV structure unexpected for roll {roll}.")
            return None
        row = reader[0]
        return {
            "vote_id": f"senate-{congress}-{session}-{roll}",
            "congress": congress,
            "chamber": "senate",
            "date": datetime.strptime(row["Vote Date"], "%m/%d/%Y"),
            "question": row.get("Vote Question"),
            "description": row.get("Vote Title"),
            "result": row.get("Result"),
            "bill_id": row.get("Measure Number")
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
    logging.info("🚀 Running votes_etl for House and Senate")
    congress, session = 118, 1
    max_rolls = 3000
    max_misses = 30
    misses = 0
    inserted = 0

    for roll in range(1, max_rolls + 1):
        if misses >= max_misses:
            logging.warning(f"🛑 Reached max {max_misses} consecutive misses.")
            break

        logging.debug(f"🔍 Checking roll {roll}")
        vote = parse_house_vote(congress, session, roll)
        if not vote:
            logging.debug(f"❌ House vote {roll} not found — fallback to Senate.")
            vote = parse_senate_vote(congress, session, roll)

        if vote:
            if insert_vote(vote):
                inserted += 1
            misses = 0
        else:
            misses += 1
            logging.info(f"🚫 No valid vote data for roll {roll} (miss {misses})")

    logging.info(f"🎯 ETL complete. Total votes inserted: {inserted}")

if __name__ == "__main__":
    run()
