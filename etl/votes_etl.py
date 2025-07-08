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
import xml.etree.ElementTree as ET

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
MAX_RETRIES            = 3
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
    return psycopg2.connect(**DB_CONFIG)


def vote_exists(cur, vote_id: str) -> bool:
    cur.execute("SELECT 1 FROM votes WHERE vote_id = %s", (vote_id,))
    return cur.fetchone() is not None


def fetch_with_retry(url: str) -> Optional[requests.Response]:
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
    url = SENATE_URL.format(congress=congress, session=session, roll=roll)
    logging.info(f"🏛️ SENATE Roll {roll}: {url}")
    resp = fetch_with_retry(url)
    if not resp:
        return None

    soup = BeautifulSoup(resp.text, "html.parser")
    text = soup.get_text(separator="\n")
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

    raw_data: Dict[str, str] = {}
    for idx, ln in enumerate(lines):
        if ln.endswith(":") and ln[:-1] in FIELD_MAP:
            key = FIELD_MAP[ln[:-1]]
            if idx + 1 < len(lines):
                raw_data[key] = lines[idx + 1]
        elif ":" in ln:
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

    table = soup.find("table", class_="roll_call")
    if not table:
        for tbl in soup.find_all("table"):
            headers = [th.get_text(strip=True).lower() for th in tbl.find_all("th")]
            if any(h in ["yea", "yeas", "nay", "nays", "not voting"] for h in headers):
                table = tbl
                logging.debug(f"🔄 Using fallback table for Senate roll {roll}")
                break
    if not table:
        logging.warning(f"⚠️ No roll_call table found for Senate roll {roll}")
        vote["tally"] = []
    else:
        tally: List[Dict[str, str]] = []
        rows = table.find_all("tr")
        for tr in rows[1:]:
            cols = tr.find_all("td")
            if len(cols) < 2:
                continue
            raw_name = cols[0].get_text(strip=True)
            position = cols[1].get_text(strip=True)
            if "," in raw_name:
                last, first = [p.strip() for p in raw_name.split(",", 1)]
                norm_name = f"{first} {last}"
            else:
                norm_name = raw_name
            bioguide_id = NAME_TO_BIOGUIDE.get(norm_name)
            if not bioguide_id:
                logging.warning(f"⚠️ Unmapped name '{norm_name}' in roll {roll}")
            tally.append({"bioguide_id": bioguide_id, "position": position})
        vote["tally"] = tally

    return vote


def parse_house_vote(congress: int, session: int, roll: int) -> Optional[Dict]:
    year = datetime.now().year
    url = HOUSE_URL.format(year=year, roll=str(roll).zfill(3))
    logging.info(f"🏛️ HOUSE Roll {roll}: {url}")
    resp = fetch_with_retry(url)
    if not resp or not resp.content.strip().startswith(b"<?xml"):
        logging.debug(f"House vote not found or invalid XML: {url}")
        return None
    try:
        root = ET.fromstring(resp.content)
    except Exception as e:
        logging.warning(f"⚠️ Failed to parse House XML for roll {roll}: {e}")
        return None
    vote = {
        "vote_id":    f"house-{congress}-{session}-{roll}",
        "congress":   congress,
        "chamber":    "house",
        "date":       datetime.strptime(root.findtext(".//action-date"), "%d-%b-%Y"),
        "question":   root.findtext(".//question-text") or "",
        "description":root.findtext(".//vote-desc") or "",
        "result":     root.findtext(".//vote-result") or "",
        "bill_id":    root.findtext(".//legis-num") or "",
        "tally":      []  # implement house positions if needed
    }
    return vote


def insert_vote_positions(vote_id: str, tally: List[Dict[str, str]], cur):
    rows = [(vote_id, pos["bioguide_id"], pos["position"]) for pos in tally if pos.get("bioguide_id")]
    if not rows:
        return
    cur.executemany(
        "INSERT INTO vote_positions (vote_id, bioguide_id, position) VALUES (%s, %s, %s);", rows
    )
    logging.info(f"✅ Inserted {len(rows)} positions for {vote_id}")


def insert_vote(vote: Dict) -> bool:
    conn = connect_db()
    cur  = conn.cursor()
    try:
        if vote_exists(cur, vote["vote_id"]):
            logging.info(f"⏩ Skipped existing vote {vote['vote_id']}")
            return False
        cur.execute(
            "INSERT INTO votes (vote_id, congress, chamber, date, question, description, result, bill_id) "
            "VALUES (%s, %s, %s, %s, %s, %s, %s, %s);", (
                vote["vote_id"], vote["congress"], vote["chamber"], vote["date"],
                vote["question"], vote["description"], vote["result"], vote["bill_id"]
            )
        )
        if vote.get("tally"):
            insert_vote_positions(vote["vote_id"], vote["tally"], cur)
        conn.commit()
        logging.info(f"✅ Inserted vote {vote['vote_id']}")
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
            logging.warning("🛑 Too many misses, exiting ETL loop")
            break
        vote = parse_house_vote(congress, session, roll) or parse_senate_vote(congress, session, roll)
        if vote and insert_vote(vote):
            inserted += 1
            misses = 0
        else:
            misses += 1
            logging.debug(f"📭 No vote for roll {roll} (miss {misses})")
    logging.info(f"🎯 ETL complete. Total inserted: {inserted}")

if __name__ == "__main__":
    run_etl()
