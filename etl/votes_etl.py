# votes_etl.py — Final Patched Version (Senate .htm parsing fixed and cleaned)

import os
import json
import logging
import time
import requests
import psycopg2
import xml.etree.ElementTree as ET
from bs4 import BeautifulSoup
from datetime import datetime
from dotenv import load_dotenv
from typing import Optional, Dict

# Load environment variables
load_dotenv()

# Logging setup
logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s [%(levelname)s] %(message)s",
)

# Database configuration
DB_CONFIG = {
    "dbname": os.getenv("dbname"),
    "user": os.getenv("user"),
    "password": os.getenv("password"),
    "host": os.getenv("host"),
    "port": os.getenv("port"),
}

# URL Templates
HOUSE_URL = "https://clerk.house.gov/evs/{year}/roll{roll:03}.xml"
SENATE_URL = "https://www.senate.gov/legislative/LIS/roll_call_votes/vote_{congress}_{session}/vote_{congress}_{session}_{roll:05}.htm"

# Retry and control limits
MAX_RETRIES = 5
RETRY_DELAY = 2
MAX_CONSECUTIVE_MISSES = 50

# Load ICPSR to Bioguide ID map
with open("icpsr_to_bioguide_full.json", "r") as f:
    ICPSR_TO_BIOGUIDE = json.load(f)

def connect_db():
    return psycopg2.connect(**DB_CONFIG)

def vote_exists(cursor, vote_id: str) -> bool:
    cursor.execute("SELECT 1 FROM votes WHERE vote_id = %s", (vote_id,))
    return cursor.fetchone() is not None

def fetch_with_retry(url: str) -> Optional[requests.Response]:
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            logging.debug(f"Fetching URL: {url} (Attempt {attempt})")
            resp = requests.get(url, timeout=10)
            if resp.status_code == 200 and "vote-not-available" not in resp.url:
                return resp
        except Exception as e:
            logging.warning(f"Request error on attempt {attempt}: {e}")
        time.sleep(RETRY_DELAY)
    return None

def parse_house_vote(congress: int, session: int, roll: int) -> Optional[Dict]:
    year = datetime.now().year
    url = HOUSE_URL.format(year=year, roll=roll)
    logging.info(f"🏛️ HOUSE Roll {roll}: {url}")
    response = fetch_with_retry(url)
    if not response or not response.content.startswith(b"<?xml"):
        return None
    try:
        root = ET.fromstring(response.content)
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
        logging.warning(f"⚠️ House vote parse failed for roll {roll}: {e}")
        return None

def parse_senate_vote(congress: int, session: int, roll: int) -> Optional[Dict]:
    url = SENATE_URL.format(congress=congress, session=session, roll=roll)
    logging.info(f"🏛️ SENATE Roll {roll}: {url}")
    response = fetch_with_retry(url)
    if not response:
        return None
    try:
        soup = BeautifulSoup(response.text, "html.parser")
        vote_data = {}

        for row in soup.find_all("span", style=lambda val: val and "font-weight:bold" in val):
            label = row.get_text(strip=True)
            value = row.find_next("span").get_text(strip=True)
            if "Vote Date" in label:
                vote_data["date"] = value
            elif "Question" in label:
                vote_data["question"] = value
            elif "Measure Title" in label:
                vote_data["description"] = value
            elif "Vote Result" in label:
                vote_data["result"] = value
            elif "Measure Number" in label:
                vote_data["bill_id"] = value

        if "date" not in vote_data:
            logging.debug(f"⚠️ No vote table found for roll {roll}")
            return None

        return {
            "vote_id": f"senate-{congress}-{session}-{roll}",
            "congress": congress,
            "chamber": "senate",
            "date": datetime.strptime(vote_data["date"], "%B %d, %Y"),
            "question": vote_data.get("question", ""),
            "description": vote_data.get("description", ""),
            "result": vote_data.get("result", ""),
            "bill_id": vote_data.get("bill_id", "")
        }
    except Exception as e:
        logging.warning(f"⚠️ Senate vote parse failed for roll {roll}: {e}")
        return None

def insert_vote(vote: Dict) -> bool:
    conn = connect_db()
    cur = conn.cursor()
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
    misses = 0

    for roll in range(1, 3000):
        if misses >= MAX_CONSECUTIVE_MISSES:
            logging.warning("🛑 Too many consecutive misses — exiting early.")
            break

        vote = parse_house_vote(congress, session, roll)
        if not vote:
            vote = parse_senate_vote(congress, session, roll)

        if vote:
            if insert_vote(vote):
                inserted += 1
            misses = 0
        else:
            misses += 1
            logging.info(f"📭 No vote found for roll {roll} (miss {misses})")

    logging.info(f"🎯 ETL complete. Total inserted: {inserted}")

if __name__ == "__main__":
    run_etl()
