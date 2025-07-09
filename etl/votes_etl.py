# votes_etl.py — full-range ETL with miss-threshold stopping and defensive parsing

import os
import csv
import logging
import requests
import psycopg2
import time
from datetime import datetime
from typing import Dict, Optional
from bs4 import BeautifulSoup
from dotenv import load_dotenv

# ── CONFIG ────────────────────────────────────────────────────────────────────
load_dotenv()
DB = dict(
    dbname   = os.getenv("DB_NAME"),
    user     = os.getenv("DB_USER"),
    password = os.getenv("DB_PASSWORD"),
    host     = os.getenv("DB_HOST"),
    port     = os.getenv("DB_PORT"),
)
HOUSE_XML    = "https://clerk.house.gov/evs/{year}/roll{roll:03d}.xml"
SENATE_CSV   = (
    "https://www.senate.gov/legislative/LIS/roll_call_votes/"
    "vote{cong}{sess}/csv/roll_call_vote_{cong}{sess}_{roll:05d}.csv"
)

MAX_RETRIES            = 3
RETRY_DELAY            = 0.5    # seconds between HTTP retries
MAX_CONSECUTIVE_MISSES = 10     # stop after this many empty rolls
# ──────────────────────────────────────────────────────────────────────────────

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")


def connect():
    return psycopg2.connect(**DB)


def fetch_with_retry(url: str) -> Optional[requests.Response]:
    """Try up to MAX_RETRIES to fetch a URL, skipping on non-200 or 'not-available'."""
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
    """Basic keyword-based stub for §2.2 key-vote criteria."""
    kws = ("final passage", "conference report", "appropriations")
    text = f"{question} {description}".lower()
    return any(k in text for k in kws)


def vote_session_exists(cur, vid: str) -> bool:
    cur.execute("SELECT 1 FROM vote_sessions WHERE vote_id = %s", (vid,))
    return cur.fetchone() is not None


def upsert_vote(vote: Dict) -> bool:
    """Insert a vote_session + its records, skipping if already present."""
    conn = connect()
    cur = conn.cursor()
    try:
        if vote_session_exists(cur, vote["vote_id"]):
            logging.debug(f"⏩ skip existing {vote['vote_id']}")
            return False

        # Insert session
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

        # Cache legislator_id lookups
        cache: Dict[str, Optional[int]] = {}

        def lk(biog: str) -> Optional[int]:
            if biog in cache:
                return cache[biog]
            cur.execute(
                "SELECT id FROM legislators WHERE bioguide_id = %s", (biog,)
            )
            row = cur.fetchone()
            cache[biog] = row[0] if row else None
            return cache[biog]

        # Insert each recorded vote
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


def parse_house_roll(cong: int, sess: int, roll: int) -> Optional[Dict]:
    """Parse a House roll-call XML, guarding against missing tags."""
    year = 2025 if cong == 118 else 2024
    url = HOUSE_XML.format(year=year, roll=roll)
    resp = fetch_with_retry(url)
    if not resp or not resp.text.strip().startswith("<?xml"):
        return None

    soup = BeautifulSoup(resp.text, "xml")
    meta = soup.find("vote-metadata")
    if not meta:
        return None

    # Required sub-tags
    date_tag = meta.find("date")
    question_tag = meta.find("question")
    result_tag = meta.find("result")
    if not date_tag or not question_tag or not result_tag:
        logging.debug(f"🏳 incomplete metadata on House roll {roll}, skipping")
        return None

    # Parse fields
    vid = f"house-{cong}-{sess}-{roll}"
    try:
        date = datetime.strptime(date_tag.text.strip(), "%m/%d/%Y")
    except ValueError:
        logging.warning(f"⚠️ invalid date '{date_tag.text}' on roll {roll}, skipping")
        return None

    description = ""
    desc_tag = meta.find("description")
    if desc_tag and desc_tag.text:
        description = desc_tag.text.strip()

    bill_id = None
    bill_tag = meta.find("bill")
    if bill_tag and bill_tag.text:
        bill_id = bill_tag.text.strip()

    # Collect votes
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
        "question":   question_tag.text.strip(),
        "description":description,
        "result":     result_tag.text.strip(),
        "bill_id":    bill_id,
        "key_vote":   is_key_vote(question_tag.text, description),
        "tally":      tally,
    }


def parse_senate_roll(cong: int, sess: int, roll: int) -> Optional[Dict]:
    """Parse a Senate roll-call via the official CSV endpoint."""
    url = SENATE_CSV.format(cong=str(cong).zfill(3), sess=sess, roll=roll)
    resp = fetch_with_retry(url)
    if not resp:
        return None

    rows = list(csv.DictReader(resp.text.splitlines()))
    if not rows:
        return None

    first = rows[0]
    # Guard required fields
    for fld in ("vote_date", "question_text", "vote_result"):
        if fld not in first or not first[fld]:
            logging.debug(f"🏳 missing '{fld}' on Senate roll {roll}, skipping")
            return None

    vid = f"senate-{cong}-{sess}-{roll}"
    try:
        date = datetime.strptime(first["vote_date"], "%Y-%m-%d")
    except ValueError:
        logging.warning(f"⚠️ invalid Senate date '{first['vote_date']}' on roll {roll}, skipping")
        return None

    tally = []
    for r in rows:
        if "member_id" in r and "vote_cast" in r and r["member_id"]:
            tally.append((r["member_id"], r["vote_cast"].title()))

    return {
        "vote_id":    vid,
        "congress":   cong,
        "chamber":    "senate",
        "date":       date,
        "question":   first["question_text"].strip(),
        "description": first.get("vote_title", "").strip(),
        "result":     first["vote_result"].strip(),
        "bill_id":    first.get("bill_number"),
        "key_vote":   is_key_vote(first["question_text"], first.get("vote_title", "")),
        "tally":      tally,
    }


def run_etl(congress: int = 118, session: int = 1):
    logging.info("🚀 Starting vote ETL")
    misses = 0
    roll = 1
    inserted = 0

    while misses < MAX_CONSECUTIVE_MISSES:
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
