# votes_etl.py — full-range roll-call ETL with correct parsing and miss-threshold stopping

import os
import csv
import json
import logging
import requests
import psycopg2
import time

from datetime import datetime
from typing import Dict, Optional, Tuple
from bs4 import BeautifulSoup
from dotenv import load_dotenv

# ── CONFIGURATION ──────────────────────────────────────────────────────────────
load_dotenv()

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
    "vote{cong}{sess}/csv/roll_call_vote_{cong}{sess}_{roll:05d}.csv"
)

MAX_RETRIES            = 3
RETRY_DELAY            = 0.5    # seconds between HTTP retries
MAX_CONSECUTIVE_MISSES = 10     # quit after this many empty rolls

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")


# ── HELPERS ─────────────────────────────────────────────────────────────────────

def connect():
    return psycopg2.connect(**DB)


def fetch_with_retry(url: str) -> Optional[requests.Response]:
    """Try up to MAX_RETRIES to fetch a URL, skipping non-200 or unavailable pages."""
    for i in range(1, MAX_RETRIES + 1):
        try:
            r = requests.get(url, timeout=10)
            if r.status_code == 200 and "vote-not-available" not in r.url:
                return r
        except Exception as e:
            logging.debug(f"Retry {i} error for {url}: {e}")
        time.sleep(RETRY_DELAY)
    return None


def is_key_vote(question: str, description: str) -> bool:
    """Simple keyword stub for §2.2 key-vote criteria."""
    kws = ("final passage", "conference report", "appropriations")
    txt = f"{question} {description}".lower()
    return any(k in txt for k in kws)


def vote_session_exists(cur, vid: str) -> bool:
    cur.execute("SELECT 1 FROM vote_sessions WHERE vote_id = %s", (vid,))
    return cur.fetchone() is not None


def upsert_vote(vote: Dict) -> bool:
    """Insert into vote_sessions + vote_records, skipping existing."""
    conn = connect()
    cur = conn.cursor()
    try:
        if vote_session_exists(cur, vote["vote_id"]):
            logging.debug(f"⏩ skip existing {vote['vote_id']}")
            return False

        # insert session metadata
        cur.execute(
            """
            INSERT INTO vote_sessions
              (vote_id, congress, chamber, date, question,
               description, result, key_vote, bill_id)
            VALUES
              (%(vote_id)s, %(congress)s, %(chamber)s, %(date)s,
               %(question)s, %(description)s, %(result)s,
               %(key_vote)s, %(bill_id)s)
            RETURNING id
            """,
            vote,
        )
        vsid = cur.fetchone()[0]

        # cache legislator_id lookups
        cache: Dict[str, Optional[int]] = {}
        def lk(biog: str) -> Optional[int]:
            if biog in cache:
                return cache[biog]
            cur.execute("SELECT id FROM legislators WHERE bioguide_id = %s", (biog,))
            row = cur.fetchone()
            cache[biog] = row[0] if row else None
            return cache[biog]

        # insert each recorded-vote
        records = [
            (vsid, lk(biog), pos)
            for biog, pos in vote["tally"]
            if lk(biog) is not None
        ]
        if records:
            cur.executemany(
                """
                INSERT INTO vote_records
                  (vote_session_id, legislator_id, vote_cast)
                VALUES (%s, %s, %s)
                ON CONFLICT DO NOTHING
                """,
                records,
            )

        conn.commit()
        logging.info(f"✅ {vote['vote_id']} (+{len(records)} positions)")
        return True

    except Exception as e:
        conn.rollback()
        logging.error(f"❌ db error for {vote['vote_id']}: {e}")
        return False

    finally:
        cur.close()
        conn.close()


# ── PARSERS ─────────────────────────────────────────────────────────────────────

def parse_house_roll(cong: int, sess: int, roll: int) -> Optional[Dict]:
    """Parse a House XML roll-call, skipping if any required tag is missing."""
    year = datetime.now().year
    url  = HOUSE_URL.format(year=year, roll=roll)
    resp = fetch_with_retry(url)
    if not resp or not resp.text.lstrip().startswith("<?xml"):
        return None

    soup = BeautifulSoup(resp.text, "xml")
    meta = soup.find("vote-metadata")
    if not meta:
        return None

    # correct element names per House XML spec
    date_str  = meta.findtext("action-date")
    q_text    = meta.findtext("question-text")
    result_txt = meta.findtext("vote-result")
    if not date_str or not q_text or not result_txt:
        logging.debug(f"🏳 missing metadata on House roll {roll}")
        return None

    try:
        date = datetime.strptime(date_str.strip(), "%d-%b-%Y")
    except ValueError:
        logging.warning(f"⚠️ invalid date '{date_str}' on roll {roll}")
        return None

    desc     = meta.findtext("vote-desc") or ""
    bill_num = meta.findtext("legis-num") or meta.findtext("bill/number") or ""
    vid      = f"house-{cong}-{sess}-{roll}"

    tally = []
    for rec in soup.find_all("recorded-vote"):
        leg = rec.find("legislator")
        pos = rec.find("vote")
        if not leg or not leg.has_attr("name-id") or not pos:
            continue
        tally.append((leg["name-id"], pos.text.strip()))

    return {
        "vote_id":    vid,
        "congress":   cong,
        "chamber":    "house",
        "date":       date,
        "question":   q_text.strip(),
        "description":desc.strip(),
        "result":     result_txt.strip(),
        "bill_id":    bill_num.strip(),
        "key_vote":   is_key_vote(q_text, desc),
        "tally":      tally,
    }


# load our mapping of "First Last" → Bioguide for Senate
try:
    with open("name_to_bioguide.json") as f:
        NAME_TO_BIOGUIDE = json.load(f)
except FileNotFoundError:
    logging.warning("⚠️ name_to_bioguide.json not found; unmapped Senate names will be skipped")
    NAME_TO_BIOGUIDE = {}


def parse_senate_roll(cong: int, sess: int, roll: int) -> Optional[Dict]:
    """Parse a Senate roll-call via CSV endpoint, skip if key fields missing."""
    url  = SENATE_URL.format(cong=str(cong).zfill(3), sess=sess, roll=roll)
    resp = fetch_with_retry(url)
    if not resp:
        return None

    rows = list(csv.DictReader(resp.text.splitlines()))
    if not rows:
        return None

    first = rows[0]
    for fld in ("vote_date", "question_text", "vote_result"):
        if not first.get(fld):
            logging.debug(f"🏳 missing '{fld}' on Senate roll {roll}")
            return None

    try:
        date = datetime.strptime(first["vote_date"], "%Y-%m-%d")
    except ValueError:
        logging.warning(f"⚠️ invalid Senate date '{first['vote_date']}' on roll {roll}")
        return None

    vid = f"senate-{cong}-{sess}-{roll}"
    vote = {
        "vote_id":    vid,
        "congress":   cong,
        "chamber":    "senate",
        "date":       date,
        "question":   first["question_text"].strip(),
        "description": first.get("vote_title","").strip(),
        "result":     first["vote_result"].strip(),
        "bill_id":    first.get("bill_number","").strip(),
        "key_vote":   is_key_vote(first["question_text"], first.get("vote_title","")),
    }

    # Build tally from CSV rows
    tally = [
        (r["member_id"], r["vote_cast"].title())
        for r in rows
        if r.get("member_id") and r.get("vote_cast")
    ]
    vote["tally"] = tally
    return vote


# ── DRIVER ─────────────────────────────────────────────────────────────────────

def run_etl(congress: int = 118, session: int = 1):
    logging.info("🚀 Starting vote ETL")
    misses = 0
    roll   = 1
    inserted = 0

    while misses < MAX_CONSECUTIVE_MISSES:
        # try House, then Senate
        vote = parse_house_roll(congress, session, roll) or parse_senate_roll(congress, session, roll)
        if vote:
            if upsert_vote(vote):
                inserted += 1
            misses = 0
        else:
            misses += 1
            logging.debug(f"📭 no vote at roll {roll} (miss {misses})")
        roll += 1

    logging.info(f"🎯 Done: {inserted} votes inserted (stopped after {misses} misses)")


if __name__ == "__main__":
    run_etl()
