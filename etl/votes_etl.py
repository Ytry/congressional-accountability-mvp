# votes_etl_debug.py

import os, requests, psycopg2, xml.etree.ElementTree as ET, csv, json, logging
from datetime import datetime
from dotenv import load_dotenv
from typing import Dict

# Setup
load_dotenv()
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

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

def vote_exists(cur, vote_id) -> bool:
    cur.execute("SELECT 1 FROM votes WHERE vote_id = %s", (vote_id,))
    return cur.fetchone() is not None

def parse_house_vote(congress, session, roll) -> Dict or None:
    url = HOUSE_URL.format(roll=str(roll).zfill(3))
    logging.info(f"🏛️ Trying House vote {roll}: {url}")
    try:
        resp = requests.get(url, timeout=10)
        if resp.status_code != 200 or not resp.content.strip().startswith(b"<?xml"):
            logging.debug(f"House vote not found or not XML: {url}")
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
        logging.error(f"House parse failed for roll {roll}: {e}")
        return None

def parse_senate_vote(congress, session, roll) -> Dict or None:
    url = SENATE_URL.format(congress=congress, session=session, roll=str(roll).zfill(5))
    logging.info(f"🏛️ Trying Senate vote {roll}: {url}")
    try:
        resp = requests.get(url, timeout=10)
        if resp.status_code != 200 or "<!DOCTYPE" in resp.text:
            logging.debug(f"Senate vote not found or invalid: {url}")
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
        logging.error(f"Senate parse failed for roll {roll}: {e}")
        return None

def insert_vote(vote: Dict):
    conn = db_connection()
    cur = conn.cursor()
    try:
        if vote_exists(cur, vote["vote_id"]):
            logging.warning(f"⚠️ Duplicate vote skipped: {vote['vote_id']}")
            return False
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
        logging.info(f"✅ Vote inserted: {vote['vote_id']}")
        return True
    except Exception as e:
        conn.rollback()
        logging.error(f"❌ Insert error for {vote['vote_id']}: {e}")
        return False
    finally:
        cur.close()
        conn.close()

def run():
    logging.info("🚀 Vote ETL starting")
    congress, session = 118, 1
    max_rolls = 2000
    max_misses = 20

    inserted_total = 0
    consecutive_misses = 0

    for roll in range(1, max_rolls):
        if consecutive_misses >= max_misses:
            logging.info(f"🛑 Ending after {max_misses} consecutive misses")
            break

        # House first
        vote = parse_house_vote(congress, session, roll)
        if not vote:
            logging.info(f"House vote {roll} missing, trying Senate...")
            vote = parse_senate_vote(congress, session, roll)

        if vote:
            if insert_vote(vote):
                inserted_total += 1
            consecutive_misses = 0
        else:
            consecutive_misses += 1

    logging.info(f"🎉 Total votes inserted: {inserted_total}")

if __name__ == "__main__":
    run()
