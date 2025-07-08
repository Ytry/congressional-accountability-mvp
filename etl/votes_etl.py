# votes_etl.py — Final Merged Version

import os
import json
import logging
import time
import requests
import psycopg2
import xml.etree.ElementTree as ET
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

# Constants
HOUSE_URL = "https://clerk.house.gov/evs/{year}/roll{roll:03}.xml"
SENATE_URL = "https://www.senate.gov/legislative/LIS/roll_call_votes/vote_{congress}_{session}/vote_{congress}_{session}_{roll:05}.xml"
UNAVAILABLE_URL_FRAGMENT = "roll-call-vote-not-available"
MAX_CONSECUTIVE_MISSES = 50
MAX_RETRIES = 10
RETRY_DELAY = 2

# Load Bioguide map
with open("icpsr_to_bioguide_full.json", "r") as f:
    ICPSR_TO_BIOGUIDE = json.load(f)


def db_connection():
    return psycopg2.connect(**DB_CONFIG)


def vote_exists(cur, vote_id: str) -> bool:
    cur.execute("SELECT 1 FROM votes WHERE vote_id = %s", (vote_id,))
    return cur.fetchone() is not None


def get_xml_with_retry(url: str) -> Optional[ET.Element]:
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            logging.debug(f"Fetching: {url} (Attempt {attempt})")
            resp = requests.get(url, timeout=10)
            if resp.status_code == 404 or UNAVAILABLE_URL_FRAGMENT in resp.url or UNAVAILABLE_URL_FRAGMENT in resp.text.lower():
                logging.debug("Vote unavailable or redirected")
                return None
            if resp.status_code == 200 and resp.content.strip().startswith(b"<?xml"):
                return ET.fromstring(resp.content)
        except Exception as e:
            logging.warning(f"Retry {attempt} failed for URL {url}: {e}")
        time.sleep(RETRY_DELAY)
    logging.error(f"Max retries exceeded for {url}")
    return None


def parse_house_vote(congress: int, session: int, roll: int) -> Optional[Dict]:
    year = datetime.now().year
    url = HOUSE_URL.format(year=year, roll=roll)
    logging.info(f"🏛️ HOUSE Roll {roll}: {url}")
    try:
        root = get_xml_with_retry(url)
        if root is None:
            return None
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
    url = SENATE_URL.format(congress=congress, session=session, roll=roll)
    logging.info(f"🏛️ SENATE Roll {roll}: {url}")
    try:
        root = get_xml_with_retry(url)
        if root is None:
            return None
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
    misses = 0
    inserted = 0

    for roll in range(1, max_rolls + 1):
        if misses >= MAX_CONSECUTIVE_MISSES:
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
