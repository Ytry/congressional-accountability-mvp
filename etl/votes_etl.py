import os
import re
import json
import logging
import time
import requests
import psycopg2
from bs4 import BeautifulSoup
from datetime import datetime
from dotenv import load_dotenv
from typing import Optional, Dict, List

load_dotenv()
# set to INFO or DEBUG as needed
logging.basicConfig(level=logging.DEBUG, format="%(asctime)s [%(levelname)s] %(message)s")

# DB configuration
db_cfg = {
    "dbname":   os.getenv("dbname"),
    "user":     os.getenv("user"),
    "password": os.getenv("password"),
    "host":     os.getenv("host"),
    "port":     os.getenv("port"),
}

# URLs
HOUSE_URL    = "https://clerk.house.gov/evs/{year}/roll{roll:03}.xml"
SENATE_URL   = "https://www.senate.gov/legislative/LIS/roll_call_votes/vote{congress}{session}/vote_{congress}_{session}_{roll:05}.htm"
INDEX_URL    = "https://www.senate.gov/legislative/LIS/roll_call_votes/vote{congress}{session}/index.htm"

# performance tuning
MAX_RETRIES             = 2    # retry once on flake
RETRY_DELAY             = 1    # seconds between retries
MAX_CONSECUTIVE_MISSES  = 50   # only for House brute-force

# load ICPSR->BioGuide map
with open("icpsr_to_bioguide_full.json") as f:
    ICPSR_TO_BIOGUIDE = json.load(f)

# DB helpers
def connect_db():
    return psycopg2.connect(**db_cfg)

def vote_exists(cur, vote_id: str) -> bool:
    cur.execute("SELECT 1 FROM votes WHERE vote_id = %s", (vote_id,))
    return cur.fetchone() is not None

# HTTP fetch with fast-fail on 404, limited retries on 5xx/timeouts
def fetch_with_retry(url: str) -> Optional[requests.Response]:
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            logging.debug(f"[{attempt}] Fetching {url}")
            r = requests.get(url, timeout=10)
        except requests.RequestException as e:
            logging.warning(f"[{attempt}] Network error: {e}")
            time.sleep(RETRY_DELAY)
            continue

        if r.status_code == 404:
            logging.debug(f"🔍 404 Not Found: {url}")
            return None
        if 500 <= r.status_code < 600:
            logging.warning(f"[{attempt}] Server error {r.status_code}, retrying...")
            time.sleep(RETRY_DELAY)
            continue
        # skip fallback pages
        if r.status_code == 200 and "vote-not-available" in r.url:
            logging.debug(f"[{attempt}] "
                          f"Fallback page for {url}, retrying...")
            time.sleep(RETRY_DELAY)
            continue
        return r

    logging.debug(f"❌ Retries exhausted for {url}")
    return None

# parse list of actual Senate roll numbers
def list_senate_rolls(congress: int, session: int) -> List[int]:
    url = INDEX_URL.format(congress=congress, session=session)
    logging.info(f"🗂️ Fetching Senate index: {url}")
    r = fetch_with_retry(url)
    if not r:
        return []
    soup = BeautifulSoup(r.text, "html.parser")
    rolls = []
    for a in soup.select("a[href*='vote_']"):
        href = a.get('href', '')
        m = re.search(r"vote_\d+_\d+_(\d{5})", href)
        if m:
            rolls.append(int(m.group(1)))
    sorted_rolls = sorted(set(rolls))
    logging.info(f"Found {len(sorted_rolls)} Senate rolls")
    return sorted_rolls

# parse Senate roll call HTML pages
def parse_senate_vote(congress: int, session: int, roll: int) -> Optional[Dict]:
    url = SENATE_URL.format(congress=congress, session=session, roll=roll)
    logging.info(f"🏛️ SENATE Roll {roll}: {url}")
    resp = fetch_with_retry(url)
    if not resp:
        return None

    soup  = BeautifulSoup(resp.text, "html.parser")
    text  = soup.get_text(separator="\n")
    lines = [ln.strip() for ln in text.splitlines() if ln.strip()]

    FIELD_MAP = {
        "Vote Number":            "vote_number",
        "Vote Date":              "date_str",
        "Question":               "question",
        "Vote Result":            "result",
        "Statement of Purpose":   "description",
        "Nomination Description": "description",
        "Amendment Number":       "bill_id",
        "Measure Number":         "bill_id",
    }

    vote_data: Dict[str,str] = {}
    for idx, ln in enumerate(lines):
        # broken-out label
        if ln.endswith(":") and ln[:-1].strip() in FIELD_MAP:
            key = FIELD_MAP[ln[:-1].strip()]
            if idx + 1 < len(lines):
                vote_data[key] = lines[idx+1]
            continue
        # inline label:value
        if ":" in ln:
            label, val = (p.strip() for p in ln.split(":", 1))
            if label in FIELD_MAP:
                vote_data[FIELD_MAP[label]] = val

    ds = vote_data.get("date_str", "")
    if not ds:
        logging.debug(f"⚠️ No date for Senate roll {roll}")
        return None

    try:
        vote_date = datetime.strptime(ds, "%B %d, %Y, %I:%M %p")
    except ValueError:
        logging.error(f"❌ Can't parse date '{ds}' for roll {roll}")
        return None

    return {
        "vote_id":    f"senate-{congress}-{session}-{roll}",
        "congress":   congress,
        "chamber":    "senate",
        "date":       vote_date,
        "question":   vote_data.get("question", ""),
        "description":vote_data.get("description", ""),
        "result":     vote_data.get("result", ""),
        "bill_id":    vote_data.get("bill_id", ""),
    }

# parse House XML votes
def parse_house_vote(congress: int, session: int, roll: int) -> Optional[Dict]:
    year = datetime.now().year
    url = HOUSE_URL.format(year=year, roll=roll)
    logging.info(f"🏛️ HOUSE Roll {roll}: {url}")
    resp = fetch_with_retry(url)
    if not resp or not resp.content.startswith(b"<?xml"):
        return None
    # your existing XML parsing logic goes here
    # ...
    return None  # placeholder

# insert a vote record
def insert_vote(v: Dict) -> bool:
    conn = connect_db()
    cur  = conn.cursor()
    try:
        if vote_exists(cur, v["vote_id"]):
            logging.info(f"⏩ Skipped existing {v['vote_id']}")
            return False
        cur.execute(
            """
            INSERT INTO votes (
                vote_id, congress, chamber, date,
                question, description, result, bill_id
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (
                v["vote_id"], v["congress"], v["chamber"],
                v["date"], v["question"], v["description"],
                v["result"], v["bill_id"]
            )
        )
        conn.commit()
        logging.info(f"✅ Inserted vote: {v['vote_id']}")
        return True
    except Exception as e:
        conn.rollback()
        logging.error(f"❌ Insert failed for {v['vote_id']}: {e}")
        return False
    finally:
        cur.close()
        conn.close()

# orchestrate ETL
def run_etl():
    logging.info("🚀 Starting vote ETL process")
    congress, session = 118, 1
    inserted = 0

    # 1) House votes (brute-force)
    misses = 0
    for roll in range(1, 3000):
        if misses >= MAX_CONSECUTIVE_MISSES:
            logging.warning("🛑 Too many House misses, stopping.")
            break
        hv = parse_house_vote(congress, session, roll)
        if hv:
            if insert_vote(hv): inserted += 1
            misses = 0
        else:
            misses += 1

    # 2) Senate votes (index-driven)
    rolls = list_senate_rolls(congress, session)
    for roll in rolls:
        sv = parse_senate_vote(congress, session, roll)
        if sv and insert_vote(sv):
            inserted += 1

    logging.info(f"🎯 ETL complete. Total inserted: {inserted}")

if __name__ == "__main__":
    run_etl()
