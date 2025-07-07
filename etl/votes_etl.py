# votes_etl.py — Debug & Senate Support Included

import os, requests, psycopg2, xml.etree.ElementTree as ET, csv, json, logging
from datetime import datetime
from dotenv import load_dotenv
from typing import Dict

# Load environment
load_dotenv()

# Logging setup
logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler()]
)

# DB credentials
DB_CONFIG = {
    "dbname": os.getenv("dbname"),
    "user": os.getenv("user"),
    "password": os.getenv("password"),
    "host": os.getenv("host"),
    "port": os.getenv("port"),
}

# Static mappings
with open("icpsr_to_bioguide_full.json", "r") as f:
    ICPSR_TO_BIOGUIDE = json.load(f)

# Base URLs
HOUSE_URL = "https://clerk.house.gov/evs/2023/roll{roll}.xml"
SENATE_URL = "https://www.senate.gov/legislative/LIS/roll_call_votes/vote{congress}{session}/vote_{congress}_{session}_{roll}.csv"

# Helpers
def db_connection():
    return psycopg2.connect(**DB_CONFIG)

def vote_exists(cur, vote_id) -> bool:
    cur.execute("SELECT 1 FROM votes WHERE vote_id = %s", (vote_id,))
    return cur.fetchone() is not None

def parse_house_vote(congress, session, roll) -> Dict or None:
    url = HOUSE_URL.format(roll=str(roll).zfill(3))
    logging.info(f"🏛️ HOUSE Roll {roll}: {url}")
    try:
        resp = requests.get(url, timeout=10)
        if resp.status_code != 200 or not resp.content.strip().startswith(b"<?xml"):
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
        logging.warning(f"⚠️ Failed House roll {roll}: {e}")
        return None

def parse_senate_vote(congress, session, roll) -> Dict or None:
    url = SENATE_URL.format(congress=congress, session=session, roll=str(roll).zfill(5))
    logging.info(f"🏛️ SENATE Roll {roll}: {url}")
    try:
        resp = requests.get(url, timeout=10)
        if resp.status_code != 200 or "<!DOCTYPE" in resp.text:
            return None
        reader = list(csv.DictReader(resp.content.decode().splitlines()))
        if not reader or "Vote Date" not in reader[0]:
            return None
        return {
            "vote_id": f"senate-{congress}-{session}-{roll}",
            "congress": congress,
            "chamber": "senate",
            "date": datetime.strptime(reader[0]["Vote Date"], "%m/%d/%Y"),
            "question": reader[0].get("Vote Question"),
            "description": reader[0].get("Vote Title"),
            "result": reader[0].get("Result"),
            "bill_id": reader[0].get("Measure Number")
        }
    except Exception as e:
        logging.warning(f"⚠️ Failed Senate roll {roll}: {e}")
        return None

def insert_vote(vote: Dict) -> bool:
    conn = db_connection()
    cur = conn.cursor()
    try:
        if vote_exists(cur, vote["vote_id"]):
            logging.debug(f"↪️ Skipping duplicate vote: {vote['vote_id']}")
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
        logging.error(f"❌ Insert failed for {vote['vote_id']}: {e}")
        return False
    finally:
        cur.close()
        conn.close()

def run():
    logging.info("🚀 ETL Start for House + Senate Votes")
    congress, session = 118, 1
    max_rolls = 2000
    max_consec_misses = 20
    consec_misses = 0
    inserted_total = 0

    for roll in range(1, max_rolls):
        if consec_misses >= max_consec_misses:
            logging.warning("🛑 Too many misses, ending early.")
            break

        vote = parse_house_vote(congress, session, roll)
        if not vote:
            logging.debug(f"House roll {roll} not found — trying Senate...")
            vote = parse_senate_vote(congress, session, roll)

        if vote:
            if insert_vote(vote):
                inserted_total += 1
            consec_misses = 0
        else:
            logging.debug(f"Roll {roll} missing from both chambers.")
            consec_misses += 1

    logging.info(f"🏁 ETL Done: Inserted {inserted_total} votes.")

if __name__ == "__main__":
    run()
