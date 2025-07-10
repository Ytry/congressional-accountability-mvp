#!/usr/bin/env python3
# votes_etl.py — ETL for House (XML) and Senate (XML) roll‑calls, with idempotent upserts and bulk inserts

import os
import json
import logging
import time
import requests
import psycopg2
import psycopg2.extras
import xml.etree.ElementTree as ET

from datetime import datetime
from typing import Dict, Optional
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

HOUSE_URL      = "https://clerk.house.gov/evs/{year}/roll{roll:03d}.xml"
SENATE_XML_URL = (
    "https://www.senate.gov/legislative/LIS/roll_call_votes/"
    "vote{congress}{session}/vote_{congress}_{session}_{roll:05d}.xml"
)

MAX_RETRIES            = 3
RETRY_DELAY            = 0.5       # seconds
MAX_CONSECUTIVE_MISSES = 10        # stop after this many empty rolls

# Load Name→Bioguide map for Senate lookups (first-last)
try:
    with open("name_to_bioguide.json") as f:
        NAME_TO_BIOGUIDE = json.load(f)
except FileNotFoundError:
    logging.warning("⚠️ name_to_bioguide.json not found; Senate names will skip")
    NAME_TO_BIOGUIDE = {}

# ── HELPERS ─────────────────────────────────────────────────────────────────────

def normalize_vote(raw: str) -> str:
    s = raw.strip().lower()
    if s in ("yea", "yes", "y", "aye"):       return "Yea"
    if s in ("nay", "no", "n"):               return "Nay"
    if s in ("present", "p"):                 return "Present"
    if s in ("not voting", "nv", "notvote"):  return "Not Voting"
    if s in ("absent", "a"):                  return "Absent"
    return "Unknown"


def connect_db():
    return psycopg2.connect(**DB)


def upsert_vote(vote: Dict) -> bool:
    """
    Insert or update a vote_session and its related vote_records in bulk.
    """
    conn = connect_db()
    cur = conn.cursor()
    try:
        # 1. Upsert vote_session
        cur.execute(
            """
            INSERT INTO vote_sessions
              (vote_id, congress, chamber, date, question, description, result, bill_id)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (vote_id) DO UPDATE
              SET congress     = EXCLUDED.congress,
                  chamber      = EXCLUDED.chamber,
                  date         = EXCLUDED.date,
                  question     = EXCLUDED.question,
                  description  = EXCLUDED.description,
                  result       = EXCLUDED.result,
                  bill_id      = EXCLUDED.bill_id
            RETURNING id
            """,
            (
                vote["vote_id"], vote["congress"], vote["chamber"], vote["date"],
                vote["question"], vote["description"], vote["result"], vote["bill_id"]
            )
        )
        res = cur.fetchone()
        if res:
            vsid = res[0]
        else:
            # If existed already, fetch the existing id
            cur.execute("SELECT id FROM vote_sessions WHERE vote_id = %s", (vote["vote_id"],))
            vsid = cur.fetchone()[0]

        # 2. Bulk insert or update vote_records
        # Prepare legislator lookup cache
        cache: Dict[str, Optional[int]] = {}
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
                records.append((vsid, lid, normalize_vote(rec["position"])) )

        if records:
            psycopg2.extras.execute_values(
                cur,
                """
                INSERT INTO vote_records (vote_session_id, legislator_id, vote_cast)
                VALUES %s
                ON CONFLICT (vote_session_id, legislator_id) DO UPDATE
                  SET vote_cast = EXCLUDED.vote_cast
                """,
                records,
                template=None,
                page_size=100
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
    for i in range(1, MAX_RETRIES + 1):
        try:
            r = requests.get(url, timeout=10)
            if r.status_code == 200:
                return r
        except Exception as e:
            logging.debug(f"Retry {i} for {url} failed: {e}")
        time.sleep(RETRY_DELAY)
    return None

# ── PARSERS ─────────────────────────────────────────────────────────────────────

# ... (parse_house and parse_senate remain unchanged) ...
def parse_house(congress: int, session: int, roll: int) -> Optional[Dict]:
    # map 118-1→2023, 118-2→2024
    year = 2023 if (congress==118 and session==1) else (2024 if (congress==118 and session==2) else datetime.now().year)
    url  = HOUSE_URL.format(year=year, roll=roll)
    logging.info(f"🏛 HOUSE {roll}: {url}")
    resp = fetch_with_retry(url)
    if not resp or not resp.content.lstrip().startswith(b"<?xml"):
        return None

    try:
        root = ET.fromstring(resp.content)
    except Exception as e:
        logging.warning(f"House XML parse failed for roll {roll}: {e}")
        return None

    ds = root.findtext(".//action-date") or ""
    try:
        date = datetime.strptime(ds, "%d-%b-%Y")
    except:
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
    url  = SENATE_XML_URL.format(congress=congress, session=session, roll=roll)
    logging.info(f"🏛 SENATE {roll}: {url}")
    resp = fetch_with_retry(url)
    if not resp or not resp.content.lstrip().startswith(b"<?xml"):
        return None

    try:
        root = ET.fromstring(resp.content)
    except Exception as e:
        logging.warning(f"Senate XML parse failed for roll {roll}: {e}")
        return None

    # parse metadata
    date_s    = root.findtext("vote_date") or ""
    try:
        # e.g. "January 8, 2024,  05:27 PM"
        date = datetime.strptime(date_s, "%B %d, %Y,  %I:%M %p")
    except:
        date = datetime.now()

    question  = root.findtext("vote_question_text") or root.findtext("question") or ""
    result    = root.findtext("vote_result") or root.findtext("vote_result_text") or ""
    description = root.findtext("vote_title") or ""
    vid       = f"senate-{congress}-{session}-{roll}"

    # collect each <member> entry
    tally = []
    for m in root.findall(".//members/member"):
        first = m.findtext("first_name","").strip()
        last  = m.findtext("last_name","").strip()
        name  = f"{first} {last}"
        pos   = m.findtext("vote_cast","").strip()
        biog  = NAME_TO_BIOGUIDE.get(name)
        tally.append({"bioguide_id": biog, "position": pos})

    return {
        "vote_id":    vid,
        "congress":   congress,
        "chamber":    "senate",
        "date":       date,
        "question":   question,
        "description":description,
        "result":     result,
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

    run_chamber("house", parse_house,  args.congress, args.session)
    run_chamber("senate", parse_senate, args.congress, args.session)


if __name__ == "__main__":
    main()
