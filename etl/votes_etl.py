# votes_etl.py — Senate parsing fixes

import os
import json
import logging
import time
import requests
import psycopg2
from bs4 import BeautifulSoup
from datetime import datetime
from dotenv import load_dotenv
from typing import Optional, Dict

load_dotenv()
logging.basicConfig(level=logging.DEBUG, format="%(asctime)s [%(levelname)s] %(message)s")

DB_CONFIG = {
    "dbname":   os.getenv("dbname"),
    "user":     os.getenv("user"),
    "password": os.getenv("password"),
    "host":     os.getenv("host"),
    "port":     os.getenv("port"),
}

# House XML URL (unchanged)
HOUSE_URL  = "https://clerk.house.gov/evs/{year}/roll{roll:03}.xml"
# Senate HTML URL — directory has no underscores
SENATE_URL = "https://www.senate.gov/legislative/LIS/roll_call_votes/vote{congress}{session}/vote_{congress}_{session}_{roll:05}.htm"

MAX_RETRIES            = 3
RETRY_DELAY            = 1
MAX_CONSECUTIVE_MISSES = 25

# Map for BioGuide IDs (unchanged)
with open("icpsr_to_bioguide_full.json") as f:
    ICPSR_TO_BIOGUIDE = json.load(f)

def connect_db():
    return psycopg2.connect(**DB_CONFIG)

def vote_exists(cur, vote_id: str) -> bool:
    cur.execute("SELECT 1 FROM votes WHERE vote_id = %s", (vote_id,))
    return cur.fetchone() is not None

def fetch_with_retry(url: str) -> Optional[requests.Response]:
    for i in range(1, MAX_RETRIES + 1):
        try:
            logging.debug(f"Fetching URL: {url} (Attempt {i})")
            r = requests.get(url, timeout=10)
            # skip the “not available” fallback page
            if r.status_code == 200 and "vote-not-available" not in r.url:
                return r
        except Exception as e:
            logging.warning(f"Request error on attempt {i}: {e}")
        time.sleep(RETRY_DELAY)
    return None

def parse_senate_vote(congress: int, session: int, roll: int) -> Optional[Dict]:
    url = SENATE_URL.format(congress=congress, session=session, roll=roll)
    logging.info(f"🏛️ SENATE Roll {roll}: {url}")
    resp = fetch_with_retry(url)
    if not resp:
        return None

    soup  = BeautifulSoup(resp.text, "html.parser")
    text  = soup.get_text(separator="\n")
    lines = [ln.strip() for ln in text.splitlines() if ln.strip()]

    # map the human label -> our internal key
    FIELD_MAP = {
        "Vote Number":           "vote_number",
        "Vote Date":             "date_str",
        "Question":              "question",
        "Vote Result":           "result",
        "Statement of Purpose":  "description",
        "Nomination Description": "description",
        "Amendment Number":      "bill_id",
        "Measure Number":        "bill_id",
    }

    vote_data: Dict[str,str] = {}
    for idx, ln in enumerate(lines):
        # case A: broken-out label on its own line
        if ln.endswith(":") and ln[:-1].strip() in FIELD_MAP:
            key = FIELD_MAP[ln[:-1].strip()]
            # grab the very next non-empty line as the value
            if idx + 1 < len(lines):
                vote_data[key] = lines[idx+1].strip()
            continue

        # case B: inline label and value
        if ":" in ln:
            label, val = (part.strip() for part in ln.split(":", 1))
            if label in FIELD_MAP:
                vote_data[FIELD_MAP[label]] = val

    # bail if we never got a date
    ds = vote_data.get("date_str", "")
    if not ds:
        logging.debug(f"⚠️ No date found for Senate roll {roll}, skipping.")
        return None

    # parse "July 19, 2023, 05:04 PM"
    try:
        vote_date = datetime.strptime(ds, "%B %d, %Y, %I:%M %p")
    except ValueError:
        logging.error(f"❌ Couldn't parse date '{ds}' for roll {roll}")
        return None

    return {
        "vote_id":     f"senate-{congress}-{session}-{roll}",
        "congress":    congress,
        "chamber":     "senate",
        "date":        vote_date,
        "question":    vote_data.get("question", ""),
        "description": vote_data.get("description", ""),
        "result":      vote_data.get("result", ""),
        "bill_id":     vote_data.get("bill_id", ""),
    }


def parse_house_vote(congress: int, session: int, roll: int) -> Optional[Dict]:
    # (unchanged from your current implementation)
    year = datetime.now().year
    url = HOUSE_URL.format(year=year, roll=roll)
    logging.info(f"🏛️ HOUSE Roll {roll}: {url}")
    resp = fetch_with_retry(url)
    if not resp or not resp.content.startswith(b"<?xml"):
        return None
    # ... your existing XML parsing here ...

def insert_vote(vote: Dict) -> bool:
    conn = connect_db()
    cur  = conn.cursor()
    try:
        if vote_exists(cur, vote["vote_id"]):
            logging.info(f"⏩ Skipped existing vote {vote['vote_id']}")
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
        logging.info(f"✅ Inserted vote: {vote['vote_id']}")
        return True
    except Exception as e:
        conn.rollback()
        logging.error(f"❌ Insert failed for {vote['vote_id']}: {e}")
        return False
    finally:
        cur.close()
        conn.close()

def run_etl():
    logging.info("🚀 Starting vote ETL process")
    congress, session = 118, 1
    inserted = 0
    misses   = 0

    for roll in range(1, 3000):
        if misses >= MAX_CONSECUTIVE_MISSES:
            logging.warning("🛑 Too many consecutive misses — exiting early.")
            break

        vote = parse_house_vote(congress, session, roll)
        if vote is None:
            vote = parse_senate_vote(congress, session, roll)

        if vote:
            if insert_vote(vote):
                inserted += 1
            misses = 0
        else:
            misses += 1
            logging.debug(f"📭 No vote found for roll {roll} (miss {misses})")

    logging.info(f"🎯 ETL complete. Total inserted: {inserted}")

if __name__ == "__main__":
    run_etl()
