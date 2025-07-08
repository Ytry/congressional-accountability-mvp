import os
import json
import logging
import time
import requests
import psycopg2
from bs4 import BeautifulSoup
from datetime import datetime
from dotenv import load_dotenv
from typing import Optional, Dict, List

# Load environment
load_dotenv()
logging.basicConfig(level=logging.DEBUG, format="%(asctime)s [%(levelname)s] %(message)s")

# Database configuration
DB_CONFIG = {
    "dbname":   os.getenv("dbname"),
    "user":     os.getenv("user"),
    "password": os.getenv("password"),
    "host":     os.getenv("host"),
    "port":     os.getenv("port"),
}

# URLs for House XML and Senate HTML
HOUSE_URL  = "https://clerk.house.gov/evs/{year}/roll{roll:03}.xml"
SENATE_URL = (
    "https://www.senate.gov/legislative/LIS/roll_call_votes/"
    "vote{congress}{session}/vote_{congress}_{session}_{roll:05}.htm"
)

# Retry and miss configuration
MAX_RETRIES            = 1
RETRY_DELAY            = 0.5
MAX_CONSECUTIVE_MISSES = 10

# Load ICPSR → BioGuide map
with open("icpsr_to_bioguide_full.json") as f:
    ICPSR_TO_BIOGUIDE = json.load(f)

# Load Name → BioGuide map for Senate positions
try:
    with open("name_to_bioguide.json") as f:
        NAME_TO_BIOGUIDE = json.load(f)
    logging.debug("Loaded name_to_bioguide.json successfully")
except FileNotFoundError:
    logging.warning("⚠️ name_to_bioguide.json not found — names will be unmapped")
    NAME_TO_BIOGUIDE = {}


def connect_db():
    """Establish a new database connection."""
    return psycopg2.connect(**DB_CONFIG)


def vote_exists(cur, vote_id: str) -> bool:
    """Check if a given vote_id already exists in the votes table."""
    cur.execute("SELECT 1 FROM votes WHERE vote_id = %s", (vote_id,))
    return cur.fetchone() is not None


def fetch_with_retry(url: str) -> Optional[requests.Response]:
    """Fetch a URL with retries and skip fallback pages."""
    for i in range(1, MAX_RETRIES + 1):
        try:
            logging.debug(f"Fetching URL: {url} (Attempt {i})")
            r = requests.get(url, timeout=10)
            if r.status_code == 200 and "vote-not-available" not in r.url:
                return r
        except Exception as e:
            logging.warning(f"Request error on attempt {i}: {e}")
        time.sleep(RETRY_DELAY)
    return None


def parse_senate_vote(congress: int, session: int, roll: int) -> Optional[Dict]:
    """
    Parse Senate roll-call HTML page to extract vote metadata and individual positions.
    """
    url = SENATE_URL.format(congress=congress, session=session, roll=roll)
    logging.info(f"🏛️ SENATE Roll {roll}: {url}")
    resp = fetch_with_retry(url)
    if not resp:
        return None

    soup = BeautifulSoup(resp.text, "html.parser")
    text = soup.get_text(separator="\n")
    lines = [ln.strip() for ln in text.splitlines() if ln.strip()]

    # Map human labels to internal keys
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

    raw_data: Dict[str, str] = {}
    for idx, ln in enumerate(lines):
        if ln.endswith(":") and ln[:-1] in FIELD_MAP:
            key = FIELD_MAP[ln[:-1]]
            if idx + 1 < len(lines):
                raw_data[key] = lines[idx + 1]
            continue
        if ":" in ln:
            label, val = [p.strip() for p in ln.split(":", 1)]
            if label in FIELD_MAP:
                raw_data[FIELD_MAP[label]] = val

    ds = raw_data.get("date_str", "")
    if not ds:
        logging.debug(f"⚠️ No date found for Senate roll {roll}, skipping.")
        return None

    try:
        vote_date = datetime.strptime(ds, "%B %d, %Y, %I:%M %p")
    except ValueError:
        logging.error(f"❌ Could not parse date '{ds}' for roll {roll}")
        return None

    # Build metadata
    vote = {
        "vote_id":     f"senate-{congress}-{session}-{roll}",
        "congress":    congress,
        "chamber":     "senate",
        "date":        vote_date,
        "question":    raw_data.get("question", ""),
        "description": raw_data.get("description", ""),
        "result":      raw_data.get("result", ""),
        "bill_id":     raw_data.get("bill_id", ""),
    }

    # ── Extract roll-call tally ──
    tally: List[Dict[str, str]] = []
    table = soup.find("table", class_="roll_call")
    if not table:
        logging.warning(f"⚠️ No roll_call table for Senate roll {roll}")
    else:
        rows = table.find_all("tr")
        for tr in rows[1:]:  # skip header
            cols = tr.find_all("td")
            if len(cols) < 2:
                continue
            raw_name    = cols[0].get_text(strip=True)
            position    = cols[1].get_text(strip=True)
            # Normalize "Last, First" → "First Last"
            if "," in raw_name:
                last, first = [p.strip() for p in raw_name.split(",", 1)]
                norm_name    = f"{first} {last}"
            else:
                norm_name = raw_name
            bioguide_id = NAME_TO_BIOGUIDE.get(norm_name)
            if not bioguide_id:
                logging.warning(f"⚠️ Unmapped name '{norm_name}' in roll {roll}")
            tally.append({"bioguide_id": bioguide_id, "position": position})

    vote["tally"] = tally
    return vote


def parse_house_vote(congress: int, session: int, roll: int) -> Optional[Dict]:
    # Unchanged from existing implementation
    year = datetime.now().year
    url   = HOUSE_URL.format(year=year, roll=roll)
    logging.info(f"🏛️ HOUSE Roll {roll}: {url}")
    resp  = fetch_with_retry(url)
    if not resp or not resp.content.startswith(b"<?xml"):
        return None
    # ... your existing XML parsing here ...
    return None  # replace with actual return


def insert_vote_positions(vote_id: str, tally: List[Dict[str, str]], cur):
    """Bulk insert individual vote positions into vote_positions table."""
    rows = [
        (vote_id, pos["bioguide_id"], pos["position"]) for pos in tally if pos.get("bioguide_id")
    ]
    if not rows:
        return
    cur.executemany(
        "INSERT INTO vote_positions (vote_id, bioguide_id, position) VALUES (%s, %s, %s)",
        rows
    )
    logging.info(f"✅ Inserted {len(rows)} vote_positions for {vote_id}")


def insert_vote(vote: Dict) -> bool:
    """Insert a vote and its positions (if present) into the database."""
    conn = connect_db()
    cur  = conn.cursor()
    try:
        if vote_exists(cur, vote["vote_id"]):
            logging.info(f"⏩ Skipped existing vote {vote['vote_id']}")
            return False

        # Insert vote metadata
        cur.execute(
            """
            INSERT INTO votes (
                vote_id, congress, chamber, date, question,
                description, result, bill_id
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (
                vote["vote_id"], vote["congress"], vote["chamber"],
                vote["date"], vote["question"], vote["description"],
                vote["result"], vote["bill_id"]
            )
        )

        # Insert individual positions
        if vote.get("tally"):
            insert_vote_positions(vote["vote_id"], vote["tally"], cur)

        conn.commit()
        logging.info(f"✅ Inserted vote and positions: {vote['vote_id']}")
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
