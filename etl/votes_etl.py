# votes_etl.py — old-style parsing with ElementTree + BS4,
# upsert into vote_sessions/vote_records, now with normalization of vote_cast

import os
import json
import logging
import time
import requests
import psycopg2
import xml.etree.ElementTree as ET

from datetime import datetime
from typing import Dict, Optional
from bs4 import BeautifulSoup
from dotenv import load_dotenv

# ── CONFIGURATION ──────────────────────────────────────────────────────────────
load_dotenv()
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

DB = {
    "dbname":   os.getenv("DB_NAME"),
    "user":     os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
    "host":     os.getenv("DB_HOST"),
    "port":     os.getenv("DB_PORT"),
}

HOUSE_URL  = "https://clerk.house.gov/evs/{year}/roll{roll:03d}.xml"
SENATE_URL = (
    "https://www.senate.gov/legislative/LIS/roll_call_votes/"
    "vote{congress}{session}/vote_{congress}_{session}_{roll:05d}.htm"
)

MAX_RETRIES            = 3
RETRY_DELAY            = 0.5   # seconds
MAX_CONSECUTIVE_MISSES = 10

# Load Name→Bioguide map for Senate
try:
    with open("name_to_bioguide.json") as f:
        NAME_TO_BIOGUIDE = json.load(f)
except FileNotFoundError:
    logging.warning("name_to_bioguide.json not found; Senate names will skip")
    NAME_TO_BIOGUIDE = {}

# ── HELPER: NORMALIZE VOTE CAST ─────────────────────────────────────────────────

def normalize_vote(raw: str) -> str:
    s = raw.strip().lower()
    if s in ("yea", "yes", "y", "aye"):
        return "Yea"
    if s in ("nay", "no", "n"):
        return "Nay"
    if s in ("present", "p"):
        return "Present"
    if s in ("not voting", "nv", "notvote"):
        return "Not Voting"
    if s in ("absent", "a"):
        return "Absent"
    return "Unknown"

# ── DB HELPERS ─────────────────────────────────────────────────────────────────

def connect_db():
    return psycopg2.connect(**DB)

def vote_exists(cur, vote_id: str) -> bool:
    cur.execute("SELECT 1 FROM vote_sessions WHERE vote_id = %s", (vote_id,))
    return cur.fetchone() is not None

def upsert_vote(vote: Dict) -> bool:
    """Insert into vote_sessions + vote_records, skipping if vote exists."""
    conn = connect_db()
    cur  = conn.cursor()
    try:
        if vote_exists(cur, vote["vote_id"]):
            logging.debug(f"⏩ skip {vote['vote_id']}")
            return False

        # 1) Insert vote_session
        cur.execute(
            """
            INSERT INTO vote_sessions
              (vote_id, congress, chamber, date, question,
               description, result, bill_id)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
            RETURNING id
            """,
            (
                vote["vote_id"], vote["congress"], vote["chamber"], vote["date"],
                vote["question"], vote["description"], vote["result"], vote["bill_id"]
            )
        )
        vsid = cur.fetchone()[0]

        # 2) Lookup Bioguide→legislator_id
        cache: Dict[str, Optional[int]] = {}
        def get_leg_id(biog: Optional[str]) -> Optional[int]:
            if not biog:
                return None
            if biog in cache:
                return cache[biog]
            cur.execute("SELECT id FROM legislators WHERE bioguide_id = %s", (biog,))
            row = cur.fetchone()
            cache[biog] = row[0] if row else None
            return cache[biog]

        # 3) Build and insert vote_records with normalization
        rows = []
        for rec in vote["tally"]:
            leg_id = get_leg_id(rec.get("bioguide_id"))
            if leg_id:
                raw_pos = rec.get("position", "")
                norm_pos = normalize_vote(raw_pos)
                rows.append((vsid, leg_id, norm_pos))

        if rows:
            cur.executemany(
                "INSERT INTO vote_records (vote_session_id, legislator_id, vote_cast) VALUES (%s,%s,%s)",
                rows
            )

        conn.commit()
        logging.info(f"✅ {vote['vote_id']} (+{len(rows)} positions)")
        return True

    except Exception as e:
        conn.rollback()
        logging.error(f"❌ DB error for {vote['vote_id']}: {e}")
        return False

    finally:
        cur.close()
        conn.close()

# ── HTTP HELPER ────────────────────────────────────────────────────────────────

def fetch_with_retry(url: str) -> Optional[requests.Response]:
    for attempt in range(1, MAX_RETRIES+1):
        try:
            r = requests.get(url, timeout=10)
            if r.status_code == 200 and "vote-not-available" not in r.url:
                return r
        except Exception as e:
            logging.debug(f"Retry {attempt} failed for {url}: {e}")
        time.sleep(RETRY_DELAY)
    return None

# ── PARSERS (OLD STYLE) ─────────────────────────────────────────────────────────

def parse_house_vote(congress: int, session: int, roll: int) -> Optional[Dict]:
    # Map 118th Congress → year; adjust if needed
    year = 2023 if (congress==118 and session==1) else (2024 if (congress==118) else datetime.now().year)
    url  = HOUSE_URL.format(year=year, roll=roll)
    logging.info(f"🏛 HOUSE {roll}: {url}")
    resp = fetch_with_retry(url)
    if not resp or not resp.content.strip().startswith(b"<?xml"):
        return None

    try:
        root = ET.fromstring(resp.content)
    except Exception as e:
        logging.warning(f"House XML parse failed for roll {roll}: {e}")
        return None

    ds = root.findtext(".//action-date")
    qt = root.findtext(".//question-text")  or ""
    vd = root.findtext(".//vote-desc")       or ""
    vr = root.findtext(".//vote-result")     or ""
    bn = root.findtext(".//legis-num")       or ""

    try:
        vote_date = datetime.strptime(ds, "%d-%b-%Y")
    except Exception:
        return None

    vote = {
        "vote_id":    f"house-{congress}-{session}-{roll}",
        "congress":   congress,
        "chamber":    "house",
        "date":       vote_date,
        "question":   qt,
        "description":vd,
        "result":     vr,
        "bill_id":    bn,
        "tally":      [],
    }

    for rec in root.findall(".//recorded-vote"):
        leg = rec.find("legislator")
        biog = leg.attrib.get("name-id") if leg is not None else None
        pos  = rec.findtext("vote") or ""
        vote["tally"].append({"bioguide_id": biog, "position": pos})

    return vote

def parse_senate_vote(congress: int, session: int, roll: int) -> Optional[Dict]:
    url  = SENATE_URL.format(congress=congress, session=session, roll=roll)
    logging.info(f"🏛 SENATE {roll}: {url}")
    resp = fetch_with_retry(url)
    if not resp:
        return None

    soup = BeautifulSoup(resp.text, "html.parser")
    text = soup.get_text("\n")
    lines = [ln.strip() for ln in text.splitlines() if ln.strip()]

    FIELD_MAP = {
        "Vote Date":    "date_str",
        "Question":     "question",
        "Vote Result":  "result",
        "Statement of Purpose": "description",
        "Measure Number":      "bill_id",
        "Amendment Number":    "bill_id",
    }
    raw = {}
    for i, ln in enumerate(lines):
        if ln.endswith(":") and ln[:-1] in FIELD_MAP and i+1 < len(lines):
            raw[FIELD_MAP[ln[:-1]]] = lines[i+1]
        elif ":" in ln:
            k, v = [p.strip() for p in ln.split(":",1)]
            if k in FIELD_MAP:
                raw[FIELD_MAP[k]] = v

    ds = raw.get("date_str")
    try:
        vote_date = datetime.strptime(ds, "%B %d, %Y, %I:%M %p")
    except Exception:
        vote_date = datetime.now()

    vote = {
        "vote_id":    f"senate-{congress}-{session}-{roll}",
        "congress":   congress,
        "chamber":    "senate",
        "date":       vote_date,
        "question":   raw.get("question",""),
        "description":raw.get("description",""),
        "result":     raw.get("result",""),
        "bill_id":    raw.get("bill_id",""),
        "tally":      [],
    }

    table = soup.find("table", class_="roll_call") \
         or max(soup.find_all("table"), key=lambda t: len(t.find_all("tr")), default=None)
    if table:
        for tr in table.find_all("tr")[1:]:
            cols = tr.find_all("td")
            if len(cols) < 2: continue
            raw_name = cols[0].get_text(strip=True)
            pos      = cols[1].get_text(strip=True)
            if "," in raw_name:
                last, first = [s.strip() for s in raw_name.split(",",1)]
                name = f"{first} {last}"
            else:
                name = raw_name
            biog = NAME_TO_BIOGUIDE.get(name)
            vote["tally"].append({"bioguide_id": biog, "position": pos})

    return vote

# ── DRIVER ─────────────────────────────────────────────────────────────────────

def run_etl(congress: int = 118, session: int = 1):
    logging.info("🚀 Starting vote ETL")
    inserted = 0
    misses   = 0

    for roll in range(1, 3000):
        if misses >= MAX_CONSECUTIVE_MISSES:
            break

        v = parse_house_vote(congress, session, roll) or parse_senate_vote(congress, session, roll)
        if not v:
            misses += 1
            logging.debug(f"miss {misses} at roll {roll}")
            continue

        if upsert_vote(v):
            inserted += 1
        misses = 0

    logging.info(f"🎯 Done: inserted {inserted} votes")

if __name__ == "__main__":
    run_etl()
