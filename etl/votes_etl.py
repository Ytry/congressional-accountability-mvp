# votes_etl.py — Senate .htm Parsing Fixed with BeautifulSoup span/div block parsing

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
from typing import Dict, Optional

# Load environment variables
load_dotenv()

# Logging setup
logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s [%(levelname)s] %(message)s",
)

# DB config
DB_CONFIG = {
    "dbname": os.getenv("dbname"),
    "user": os.getenv("user"),
    "password": os.getenv("password"),
    "host": os.getenv("host"),
    "port": os.getenv("port"),
}

HOUSE_URL = "https://clerk.house.gov/evs/{year}/roll{roll:03}.xml"
SENATE_HTM_URL = "https://www.senate.gov/legislative/LIS/roll_call_votes/vote_{congress}_{session}/vote_{congress}_{session}_{roll:05}.htm"
MAX_CONSECUTIVE_MISSES = 50
MAX_RETRIES = 5
RETRY_DELAY = 2

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
            logging.debug(f"Fetching XML: {url} (Attempt {attempt})")
            resp = requests.get(url, timeout=10)
            if resp.status_code == 200 and resp.content.startswith(b"<?xml"):
                return ET.fromstring(resp.content)
        except Exception as e:
            logging.warning(f"XML retry {attempt} failed: {e}")
        time.sleep(RETRY_DELAY)
    return None

def get_html_with_retry(url: str) -> Optional[str]:
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            logging.debug(f"Fetching HTML: {url} (Attempt {attempt})")
            resp = requests.get(url, timeout=10)
            if "vote-not-available" in resp.url or resp.status_code != 200:
                return None
            return resp.text
        except Exception as e:
            logging.warning(f"HTML retry {attempt} failed: {e}")
        time.sleep(RETRY_DELAY)
    return None

def parse_house_vote(congress: int, session: int, roll: int) -> Optional[Dict]:
    year = datetime.now().year
    url = HOUSE_URL.format(year=year, roll=roll)
    logging.info(f"🏛️ HOUSE Roll {roll}: {url}")
    root = get_xml_with_retry(url)
    if not root:
        return None
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
        logging.warning(f"⚠️ Failed to parse House vote {roll}: {e}")
        return None

def parse_senate_vote_htm(congress: int, session: int, roll: int) -> Optional[Dict]:
    url = SENATE_HTM_URL.format(congress=congress, session=session, roll=roll)
    logging.info(f"🏛️ SENATE Roll {roll}: {url}")
    html = get_html_with_retry(url)
    if not html:
        return None
    try:
        soup = BeautifulSoup(html, "html.parser")
        vote_data = {}

        for row in soup.find_all("span", style=lambda val: val and "font-weight:bold" in val):
            text = row.get_text(strip=True)
            if "Vote Date" in text:
                date_text = row.find_next("span").get_text(strip=True)
                vote_data["date"] = date_text
            elif "Vote Result" in text:
                vote_data["result"] = row.find_next("span").get_text(strip=True)
            elif "Question" in text:
                vote_data["question"] = row.find_next("span").get_text(strip=True)
            elif "Measure Title" in text:
                vote_data["description"] = row.find_next("span").get_text(strip=True)
            elif "Measure Number" in text:
                vote_data["bill_id"] = row.find_next("span").get_text(strip=True)

        return {
            "vote_id": f"senate-{congress}-{session}-{roll}",
            "congress": congress,
            "chamber": "senate",
            "date": datetime.strptime(vote_data.get("date", ""), "%B %d, %Y"),
            "question": vote_data.get("question", ""),
            "description": vote_data.get("description", ""),
            "result": vote_data.get("result", ""),
            "bill_id": vote_data.get("bill_id", "")
        }
    except Exception as e:
        logging.warning(f"⚠️ Failed to parse Senate .htm vote {roll}: {e}")
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
        logging.error(f"❌ Insert failed {vote['vote_id']}: {e}")
        return False
    finally:
        cur.close()
        conn.close()

def run():
    logging.info("🚀 Starting votes_etl with corrected Senate .htm parsing")
    congress, session = 118, 1
    misses = 0
    inserted = 0
    for roll in range(1, 3000):
        if misses >= MAX_CONSECUTIVE_MISSES:
            logging.warning("🛑 Too many misses, stopping.")
            break
        vote = parse_house_vote(congress, session, roll)
        if not vote:
            vote = parse_senate_vote_htm(congress, session, roll)
        if vote:
            if insert_vote(vote):
                inserted += 1
            misses = 0
        else:
            misses += 1
            logging.info(f"📭 Roll {roll} not found (miss {misses})")
    logging.info(f"🎯 ETL complete. Total votes inserted: {inserted}")

if __name__ == "__main__":
    run()
