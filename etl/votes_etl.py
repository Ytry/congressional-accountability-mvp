import os
import json
import logging
import time
import re
import requests
import psycopg2
from bs4 import BeautifulSoup
from datetime import datetime
from dotenv import load_dotenv
from typing import Optional, Dict, List
from concurrent.futures import ThreadPoolExecutor, as_completed

load_dotenv()
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

# Database configuration
DB_CONFIG = {
    "dbname":   os.getenv("dbname"),
    "user":     os.getenv("user"),
    "password": os.getenv("password"),
    "host":     os.getenv("host"),
    "port":     os.getenv("port"),
}

# URLs
HOUSE_URL   = "https://clerk.house.gov/evs/{year}/roll{roll:03}.xml"
SENATE_URL  = "https://www.senate.gov/legislative/LIS/roll_call_votes/vote_{congress}_{session}/vote_{congress}_{session}_{roll:05}.htm"
INDEX_URL   = "https://www.senate.gov/legislative/LIS/roll_call_votes/vote_{congress}_{session}/index.htm"

# Tuning constants
MAX_RETRIES            = 1       # only retry once on flake
RETRY_DELAY            = 0.5     # seconds between retries
MAX_CONSECUTIVE_MISSES = 50      # for House brute-force
PARALLEL_WORKERS       = 10      # threads for Senate parsing

# Load ICPSR->BioGuide map
with open("icpsr_to_bioguide_full.json") as f:
    ICPSR_TO_BIOGUIDE = json.load(f)

# Database helpers
def connect_db():
    return psycopg2.connect(**DB_CONFIG)

def vote_exists(cur, vote_id: str) -> bool:
    cur.execute("SELECT 1 FROM votes WHERE vote_id = %s", (vote_id,))
    return cur.fetchone() is not None

# Fetch with fast-fail on 404 and fallback pages
def fetch_with_retry(url: str) -> Optional[requests.Response]:
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            logging.debug(f"[{attempt}] GET {url}")
            r = requests.get(url, timeout=10)
        except requests.RequestException as e:
            logging.warning(f"[{attempt}] Network error: {e}")
            time.sleep(RETRY_DELAY)
            continue

        # no such page or fallback
        if r.status_code == 404 or "vote-not-available" in r.url:
            logging.debug(f"No vote page: {url} (status {r.status_code})")
            return None
        # retry on server errors
        if 500 <= r.status_code < 600:
            logging.warning(f"[{attempt}] Server error {r.status_code}, retrying...")
            time.sleep(RETRY_DELAY)
            continue
        return r

    logging.debug(f"Retries exhausted for {url}")
    return None

# Parse Senate index to list available roll numbers
def list_senate_rolls(congress: int, session: int) -> List[int]:
    url = INDEX_URL.format(congress=congress, session=session)
    logging.info(f"🗂️ Fetching Senate index: {url}")
    resp = fetch_with_retry(url)
    if not resp:
        return []
    soup = BeautifulSoup(resp.text, "html.parser")
    rolls: List[int] = []
    for a in soup.select("a[href*='vote_"]"):
        href = a.get('href', '')
        match = re.search(r"vote_\d+_\d+_(\d{5})", href)
        if match:
            rolls.append(int(match.group(1)))
    unique = sorted(set(rolls))
    logging.info(f"Found {len(unique)} Senate rolls")
    return unique

# Parse a single Senate roll call
def parse_senate_vote(congress: int, session: int, roll: int) -> Optional[Dict]:
    url = SENATE_URL.format(congress=congress, session=session, roll=roll)
    resp = fetch_with_retry(url)
    if not resp:
        return None

    soup = BeautifulSoup(resp.text, "html.parser")
    lines = [ln.strip() for ln in soup.get_text(separator="\n").splitlines() if ln.strip()]
    FIELD_MAP = {
        "Vote Date":             "date_str",
        "Question":              "question",
        "Vote Result":           "result",
        "Statement of Purpose":  "description",
        "Nomination Description": "description",
        "Amendment Number":      "bill_id",
        "Measure Number":        "bill_id",
    }

    data: Dict[str, str] = {}
    for idx, ln in enumerate(lines):
        if ln.endswith(":") and ln[:-1] in FIELD_MAP:
            key = FIELD_MAP[ln[:-1]]
            if idx + 1 < len(lines):
                data[key] = lines[idx + 1]
            continue
        if ":" in ln:
            label, val = (p.strip() for p in ln.split(":", 1))
            if label in FIELD_MAP:
                data[FIELD_MAP[label]] = val

    ds = data.get("date_str", "")
    if not ds:
        return None
    try:
        vote_date = datetime.strptime(ds, "%B %d, %Y, %I:%M %p")
    except ValueError:
        logging.error(f"Bad date '{ds}' for roll {roll}")
        return None

    return {
        "vote_id":    f"senate-{congress}-{session}-{roll}",
        "congress":   congress,
        "chamber":    "senate",
        "date":       vote_date,
        "question":   data.get("question", ""),
        "description":data.get("description", ""),
        "result":     data.get("result", ""),
        "bill_id":    data.get("bill_id", ""),
    }

# Parse a single House roll call (placeholder)
def parse_house_vote(congress: int, session: int, roll: int) -> Optional[Dict]:
    year = datetime.now().year
    url = HOUSE_URL.format(year=year, roll=roll)
    resp = fetch_with_retry(url)
    if not resp or not resp.content.startswith(b"<?xml"):
        return None
    # existing XML parsing goes here
    return None

# Insert vote record
def insert_vote(v: Dict) -> bool:
    conn = connect_db()
    cur = conn.cursor()
    try:
        if vote_exists(cur, v["vote_id"]):
            return False
        cur.execute(
            "INSERT INTO votes (vote_id, congress, chamber, date, question, description, result, bill_id)"
            " VALUES (%s, %s, %s, %s, %s, %s, %s, %s)",
            (v["vote_id"], v["congress"], v["chamber"], v["date"],
             v["question"], v["description"], v["result"], v["bill_id"]) )
        conn.commit()
        return True
    except Exception as e:
        conn.rollback()
        logging.error(f"Insert error for {v['vote_id']}: {e}")
        return False
    finally:
        cur.close()
        conn.close()

# Main ETL orchestration
def run_etl():
    logging.info("🚀 Starting vote ETL process")
    congress, session = 118, 1
    inserted = 0

    # 1) House votes (brute-force)
    misses = 0
    for roll in range(1, 3000):
        if misses >= MAX_CONSECUTIVE_MISSES:
            break
        hv = parse_house_vote(congress, session, roll)
        if hv and insert_vote(hv):
            inserted += 1
            misses = 0
        else:
            misses += 1

    # 2) Senate votes (index-driven, parallel)
    rolls = list_senate_rolls(congress, session)
    with ThreadPoolExecutor(max_workers=PARALLEL_WORKERS) as executor:
        futures = {executor.submit(parse_senate_vote, congress, session, r): r for r in rolls}
        for fut in as_completed(futures):
            sv = fut.result()
            if sv and insert_vote(sv):
                inserted += 1

    logging.info(f"🎯 ETL complete. Total inserted: {inserted}")

if __name__ == "__main__":
    run_etl()
