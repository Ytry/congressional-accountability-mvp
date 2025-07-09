#!/usr/bin/env python3
# votes_etl.py — load House & Senate in separate passes so Session 2 actually inserts Senate votes

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

# ── CONFIG ──────────────────────────────────────────────────────────────────────
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
RETRY_DELAY            = 0.5       # seconds
MAX_CONSECUTIVE_MISSES = 10        # stop after this many empty rolls

# Load Name→Bioguide map for Senate lookups
try:
    with open("name_to_bioguide.json") as f:
        NAME_TO_BIOGUIDE = json.load(f)
except FileNotFoundError:
    logging.warning("⚠️ name_to_bioguide.json not found; Senate names will skip")
    NAME_TO_BIOGUIDE = {}

# ── HELPERS ─────────────────────────────────────────────────────────────────────

def normalize_vote(raw: str) -> str:
    s = raw.strip().lower()
    if s in ("yea","yes","y","aye"):       return "Yea"
    if s in ("nay","no","n"):              return "Nay"
    if s in ("present","p"):               return "Present"
    if s in ("not voting","nv","notvote"): return "Not Voting"
    if s in ("absent","a"):                return "Absent"
    return "Unknown"

def connect_db():
    return psycopg2.connect(**DB)

def upsert_vote(vote: Dict) -> bool:
    """Insert vote session + records; skip if already exists."""
    conn = connect_db()
    cur  = conn.cursor()
    try:
        # session insert (ON CONFLICT so repeat runs won’t error)
        cur.execute(
            """
            INSERT INTO vote_sessions
               (vote_id, congress, chamber, date, question,
                description, result, bill_id)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
            ON CONFLICT (vote_id) DO NOTHING
            RETURNING id
            """,
            (
                vote["vote_id"], vote["congress"], vote["chamber"], vote["date"],
                vote["question"], vote["description"], vote["result"], vote["bill_id"]
            )
        )
        row = cur.fetchone()
        if not row:
            return False
        vsid = row[0]

        # build & insert individual vote_records
        cache: Dict[str,Optional[int]] = {}
        def leg_id(biog: Optional[str]) -> Optional[int]:
            if not biog:
                return None
            if biog in cache:
                return cache[biog]
            cur.execute("SELECT id FROM legislators WHERE bioguide_id = %s", (biog,))
            r = cur.fetchone()
            cache[biog] = r[0] if r else None
            return cache[biog]

        records = []
        for rec in vote["tally"]:
            lid = leg_id(rec["bioguide_id"])
            if lid:
                records.append((vsid, lid, normalize_vote(rec["position"])))

        if records:
            cur.executemany(
                "INSERT INTO vote_records (vote_session_id, legislator_id, vote_cast) "
                "VALUES (%s,%s,%s) ON CONFLICT DO NOTHING",
                records
            )

        conn.commit()
        logging.info(f"✅ {vote['vote_id']} (+{len(records)} positions)")
        return True

    except Exception as e:
        conn.rollback()
        logging.error(f"❌ DB error for {vote['vote_id']}: {e}")
        return False

    finally:
        cur.close()
        conn.close()

def fetch_with_retry(url: str) -> Optional[requests.Response]:
    for i in range(1, MAX_RETRIES+1):
        try:
            r = requests.get(url, timeout=10)
            # skip the “vote-not-available” placeholder
            if r.status_code == 200 and "vote-not-available" not in r.url:
                return r
        except Exception:
            pass
        time.sleep(RETRY_DELAY)
    return None

# ── PARSERS ─────────────────────────────────────────────────────────────────────

def parse_house(congress: int, session: int, roll: int) -> Optional[Dict]:
    # map 118-1→2023, 118-2→2024
    if congress == 118:
        year = 2023 if session == 1 else 2024
    else:
        year = datetime.now().year

    url  = HOUSE_URL.format(year=year, roll=roll)
    logging.info(f"🏛 HOUSE {roll}: {url}")
    resp = fetch_with_retry(url)
    if not resp or not resp.content.lstrip().startswith(b"<?xml"):
        return None

    try:
        root = ET.fromstring(resp.content)
    except Exception as e:
        logging.warning(f"XML parse failed on House roll {roll}: {e}")
        return None

    ds = root.findtext(".//action-date") or ""
    try:
        date = datetime.strptime(ds, "%d-%b-%Y")
    except Exception:
        return None

    vote = {
        "vote_id":    f"house-{congress}-{session}-{roll}",
        "congress":   congress,
        "chamber":    "house",
        "date":       date,
        "question":   root.findtext(".//question-text", default=""),
        "description":root.findtext(".//vote-desc",      default=""),
        "result":     root.findtext(".//vote-result",    default=""),
        "bill_id":    root.findtext(".//legis-num",      default=None),
        "tally":      [],
    }
    for rec in root.findall(".//recorded-vote"):
        leg = rec.find("legislator")
        biog = leg.attrib.get("name-id") if leg is not None else None
        pos  = rec.findtext("vote", default="")
        vote["tally"].append({"bioguide_id": biog, "position": pos})

    return vote

def parse_senate(congress: int, session: int, roll: int) -> Optional[Dict]:
    url  = SENATE_URL.format(congress=congress, session=session, roll=roll)
    logging.info(f"🏛 SENATE {roll}: {url}")
    resp = fetch_with_retry(url)
    if not resp:
        return None

    soup = BeautifulSoup(resp.text, "html.parser")
    table = (
        soup.find("table", class_="roll_call")
        or max(soup.find_all("table"), key=lambda t: len(t.find_all("tr")), default=None)
    )
    if not table:
        return None

    vid    = f"senate-{congress}-{session}-{roll}"
    tally  = []
    for tr in table.find_all("tr")[1:]:
        cols = tr.find_all("td")
        if len(cols) < 2:
            continue
        raw = cols[0].get_text(strip=True)
        pos = cols[1].get_text(strip=True)
        # normalize "Last, First" → "First Last"
        if "," in raw:
            last, first = [x.strip() for x in raw.split(",", 1)]
            name = f"{first} {last}"
        else:
            name = raw
        biog = NAME_TO_BIOGUIDE.get(name)
        tally.append({"bioguide_id": biog, "position": pos})

    return {
        "vote_id":    vid,
        "congress":   congress,
        "chamber":    "senate",
        "date":       datetime.now(),  # metadata parsing can be added
        "question":   "",
        "description":"",
        "result":     "",
        "bill_id":    None,
        "tally":      tally,
    }

# ── DRIVER ─────────────────────────────────────────────────────────────────────

def run_chamber(name: str, parser, congress: int, session: int):
    logging.info(f"🚀 Loading {name.upper()} votes for {congress}-{session}")
    inserted = 0
    misses   = 0
    roll     = 1

    while misses < MAX_CONSECUTIVE_MISSES:
        vote = parser(congress, session, roll)
        if vote and vote["tally"]:
            if upsert_vote(vote):
                inserted += 1
            misses = 0
        else:
            misses += 1
            logging.debug(f"📭 no {name} vote at roll {roll} (miss {misses})")
        roll += 1

    logging.info(f"🏁 {name.capitalize()} done: inserted {inserted} sessions\n")

def main():
    import argparse
    p = argparse.ArgumentParser()
    p.add_argument("congress", type=int, nargs="?", default=118)
    p.add_argument("session", type=int, nargs="?", default=1)
    args = p.parse_args()

    # Run House first, then Senate
    run_chamber("house", parse_house,  args.congress, args.session)
    run_chamber("senate", parse_senate, args.congress, args.session)

if __name__ == "__main__":
    main()
