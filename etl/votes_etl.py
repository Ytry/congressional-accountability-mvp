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

# Load environment and configure logging
load_dotenv()
logging.basicConfig(level=logging.DEBUG, format="%(asctime)s [%(levelname)s] %(message)s")

# Database configuration from environment
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

# Retry and loop-stopping parameters
MAX_RETRIES            = 3
RETRY_DELAY            = 0.5  # seconds
MAX_CONSECUTIVE_MISSES = 10

# Load Name → BioGuide map for Senate (First Last → Bioguide)
try:
    with open("name_to_bioguide.json") as f:
        NAME_TO_BIOGUIDE = json.load(f)
    logging.debug("Loaded name_to_bioguide.json successfully")
except FileNotFoundError:
    logging.warning("⚠️ name_to_bioguide.json not found — Senate names will be unmapped")
    NAME_TO_BIOGUIDE = {}

# Load ICPSR → BioGuide map for House lookups
with open("icpsr_to_bioguide_full.json") as f:
    ICPSR_TO_BIOGUIDE = json.load(f)
logging.debug("Loaded icpsr_to_bioguide_full.json successfully")


def connect_db():
    """Get a new database connection."""
    return psycopg2.connect(**DB_CONFIG)


def vote_exists(cur, vote_id: str) -> bool:
    cur.execute("SELECT 1 FROM votes WHERE vote_id = %s", (vote_id,))
    return cur.fetchone() is not None


def fetch_with_retry(url: str) -> Optional[requests.Response]:
    """HTTP GET with simple retry logic."""
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            logging.debug(f"Fetching URL (attempt {attempt}): {url}")
            resp = requests.get(url, timeout=10)
            if resp.status_code == 200 and "vote-not-available" not in resp.url:
                return resp
        except Exception as e:
            logging.warning(f"Request error on {url}: {e}")
        time.sleep(RETRY_DELAY)
    return None


def parse_house_vote(congress: int, session: int, roll: int) -> Optional[Dict]:
    """
    Parse a House roll-call XML into metadata + individual positions.
    Returns None if no valid vote found.
    """
    year = datetime.now().year
    url  = HOUSE_URL.format(year=year, roll=roll)
    logging.info(f"🏛️ HOUSE Roll {roll}: {url}")

    resp = fetch_with_retry(url)
    if not resp or not resp.content.strip().startswith(b"<?xml"):
        return None

    try:
        root = ET.fromstring(resp.content)
    except Exception as e:
        logging.warning(f"⚠️ Failed to parse House XML for roll {roll}: {e}")
        return None

    # --- metadata ---
    try:
        vote_date = datetime.strptime(
            root.findtext(".//action-date"), "%d-%b-%Y"
        )
    except Exception:
        logging.error(f"❌ Could not parse date for House roll {roll}")
        return None

    vote = {
        "vote_id":    f"house-{congress}-{session}-{roll}",
        "congress":   congress,
        "chamber":    "house",
        "date":       vote_date,
        "question":   root.findtext(".//question-text") or "",
        "description":root.findtext(".//vote-desc") or "",
        "result":     root.findtext(".//vote-result") or "",
        "bill_id":    root.findtext(".//legis-num") or "",
    }

    # --- positions ---
    tally = []
    for rec in root.findall(".//recorded-vote"):
        biog = None

        # 1) If the <legislator> tag includes a Bioguide
        leg = rec.find("legislator")
        if leg is not None:
            biog = leg.attrib.get("bioGuideId") or leg.attrib.get("BioGuideID")

        # 2) Fallback: map ICPSR → Bioguide only if no bioGuideId
        if not biog:
            icpsr = None
            if leg is not None:
                icpsr = leg.attrib.get("icpsr-id")
            if not icpsr:
                icpsr = rec.attrib.get("member-id")
            if icpsr:
                biog = ICPSR_TO_BIOGUIDE.get(str(icpsr))

        if not biog:
            logging.warning(f"⚠️ Unmapped House member in roll {roll}")
            continue

        position = rec.findtext("vote") or ""
        tally.append({"bioguide_id": biog, "position": position})

    vote["tally"] = tally
    return vote



def parse_senate_vote(congress: int, session: int, roll: int) -> Optional[Dict]:
    """
    Parse a Senate roll-call HTML into metadata + individual positions.
    Returns None if no valid vote found.
    """
    url = SENATE_URL.format(congress=congress, session=session, roll=roll)
    logging.info(f"🏛️ SENATE Roll {roll}: {url}")

    resp = fetch_with_retry(url)
    if not resp:
        return None

    soup = BeautifulSoup(resp.text, "html.parser")
    text = soup.get_text(separator="\n")
    lines = [ln.strip() for ln in text.splitlines() if ln.strip()]

    # Map page labels → metadata keys
    FIELD_MAP = {
        "Vote Number":          "vote_number",
        "Vote Date":            "date_str",
        "Question":             "question",
        "Vote Result":          "result",
        "Statement of Purpose": "description",
        "Amendment Number":     "bill_id",
        "Measure Number":       "bill_id",
    }
    raw = {}
    for i, ln in enumerate(lines):
        if ln.endswith(":") and ln[:-1] in FIELD_MAP and i+1 < len(lines):
            raw[FIELD_MAP[ln[:-1]]] = lines[i+1]
        elif ":" in ln:
            lbl, val = [p.strip() for p in ln.split(":",1)]
            if lbl in FIELD_MAP:
                raw[FIELD_MAP[lbl]] = val

    ds = raw.get("date_str", "")
    if not ds:
        return None
    try:
        vote_date = datetime.strptime(ds, "%B %d, %Y, %I:%M %p")
    except ValueError:
        return None

    vote = {
        "vote_id":    f"senate-{congress}-{session}-{roll}",
        "congress":   congress,
        "chamber":    "senate",
        "date":       vote_date,
        "question":   raw.get("question",""),
        "description":raw.get("description",""),
        "result":     raw.get("result",""),
        "bill_id":    raw.get("bill_id",""),
    }

    # Locate roll-call table (with fallbacks)
    table = soup.find("table", class_="roll_call")
    if not table:
        # Fallback: headers containing yea/nay
        for tbl in soup.find_all("table"):
            hdrs = [h.get_text(strip=True).lower() for h in tbl.find_all("th")]
            if any(k in hdrs for k in ("yea","nay","not voting")):
                table = tbl
                break
    if not table and soup.find_all("table"):
        # Fallback: largest table
        table = max(soup.find_all("table"), key=lambda t: len(t.find_all("tr")))

    tally = []
    if table:
        rows = table.find_all("tr")[1:]
        for tr in rows:
            cols = tr.find_all("td")
            if len(cols) < 2:
                continue
            raw_name = cols[0].get_text(strip=True)
            position = cols[1].get_text(strip=True)
            # Normalize Last, First → First Last
            if "," in raw_name:
                last, first = [p.strip() for p in raw_name.split(",",1)]
                norm = f"{first} {last}"
            else:
                norm = raw_name
            biog = NAME_TO_BIOGUIDE.get(norm)
            if not biog:
                logging.warning(f"⚠️ Unmapped Senate name '{norm}' in roll {roll}")
            tally.append({"bioguide_id": biog, "position": position})

    vote["tally"] = tally
    return vote


def insert_vote(vote: Dict) -> bool:
    """Insert metadata & positions into votes + vote_positions tables."""
    conn = connect_db()
    cur  = conn.cursor()
    try:
        if vote_exists(cur, vote["vote_id"]):
            logging.info(f"⏩ Skipped existing {vote['vote_id']}")
            return False
        # Insert metadata
        cur.execute(
            "INSERT INTO votes (vote_id, congress, chamber, date, question, description, result, bill_id)"
            " VALUES (%s, %s, %s, %s, %s, %s, %s, %s)",
            (vote["vote_id"], vote["congress"], vote["chamber"], vote["date"],
             vote["question"], vote["description"], vote["result"], vote["bill_id"])  # noqa
        )
        # Insert positions
        rows = [(vote["vote_id"], rec["bioguide_id"], rec["position"]) for rec in vote["tally"] if rec.get("bioguide_id")]
        if rows:
            cur.executemany(
                "INSERT INTO vote_positions (vote_id, bioguide_id, position) VALUES (%s, %s, %s)",
                rows
            )
            logging.info(f"✅ Inserted {len(rows)} positions for {vote['vote_id']}")
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
    """Main ETL loop: iterate rolls, parse, and insert."""
    logging.info("🚀 Starting vote ETL process")
    congress, session = 118, 1
    inserted = 0
    misses   = 0

    for roll in range(1, 3000):
        if misses >= MAX_CONSECUTIVE_MISSES:
            logging.warning("🛑 Too many misses, exiting ETL")
            break

        # Try House first, then Senate
        vote = parse_house_vote(congress, session, roll) or parse_senate_vote(congress, session, roll)
        if not vote:
            misses += 1
            logging.debug(f"📭 No vote for roll {roll} (miss {misses})")
            continue

        # Insert and reset miss counter on success or skip
        if insert_vote(vote):
            inserted += 1
        misses = 0

    logging.info(f"🎯 ETL complete. Votes inserted: {inserted}")

if __name__ == "__main__":
    run_etl()
