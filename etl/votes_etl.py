# votes_etl.py — full-range roll-call ETL with robust BS4 parsing and miss-threshold stopping

import os
import csv
import logging
import time
import requests
import psycopg2

from datetime import datetime
from typing import Dict, Optional
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
HOUSE_URL    = "https://clerk.house.gov/evs/{year}/roll{roll:03d}.xml"
SENATE_CSV   = (
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
    """Retry up to MAX_RETRIES; skip non-200 or unavailable pages."""
    for i in range(1, MAX_RETRIES + 1):
        try:
            r = requests.get(url, timeout=10)
            if r.status_code == 200 and "vote-not-available" not in r.url:
                return r
        except Exception as e:
            logging.debug(f"Retry {i} error fetching {url}: {e}")
        time.sleep(RETRY_DELAY)
    return None


def is_key_vote(question: str, description: str) -> bool:
    """Keyword-based stub for §2.2 key-vote criteria."""
    kws = ("final passage", "conference report", "appropriations")
    txt = f"{question} {description}".lower()
    return any(k in txt for k in kws)


def vote_session_exists(cur, vid: str) -> bool:
    cur.execute("SELECT 1 FROM vote_sessions WHERE vote_id = %s", (vid,))
    return cur.fetchone() is not None


def upsert_vote(vote: Dict) -> bool:
    """Insert into vote_sessions + vote_records, skipping existing sessions."""
    conn = connect()
    cur  = conn.cursor()
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
            cur.execute(
                "SELECT id FROM legislators WHERE bioguide_id = %s", (biog,)
            )
            row = cur.fetchone()
            cache[biog] = row[0] if row else None
            return cache[biog]

        # insert each recorded vote
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
        logging.error(f"❌ DB error for {vote['vote_id']}: {e}")
        return False

    finally:
        cur.close()
        conn.close()


# ── PARSERS ─────────────────────────────────────────────────────────────────────

def parse_house_roll(cong: int, sess: int, roll: int) -> Optional[Dict]:
    """Parse a House roll-call XML, skipping if required tags missing."""
    year = datetime.now().year
    url  = HOUSE_URL.format(year=year, roll=roll)
    resp = fetch_with_retry(url)
    if not resp or not resp.text.lstrip().startswith("<?xml"):
        return None

    # use lxml-xml when available, fallback to html.parser
    try:
        soup = BeautifulSoup(resp.text, "lxml-xml")
    except Exception:
        soup = BeautifulSoup(resp.text, "html.parser")

    meta = soup.find("vote-metadata")
    if not meta:
        return None

    date_tag     = meta.find("action-date")
    question_tag = meta.find("question-text")
    result_tag   = meta.find("vote-result")
    if not date_tag or not question_tag or not result_tag:
        logging.debug(f"🏳 missing House metadata on roll {roll}")
        return None

    # parse date like "18-Jan-2025"
    try:
        date = datetime.strptime(date_tag.get_text(strip=True), "%d-%b-%Y")
    except ValueError:
        logging.warning(f"⚠️ invalid date '{date_tag.get_text()}' on roll {roll}")
        return None

    description = meta.find("vote-desc").get_text(strip=True) if meta.find("vote-desc") else ""
    # bill number: <legis-num> or <bill><number>
    legis_num = meta.find("legis-num")
    if legis_num and legis_num.get_text(strip=True):
        bill_id = legis_num.get_text(strip=True)
    else:
        bill_elem = meta.find("bill")
        if bill_elem and bill_elem.find("number"):
            bill_id = bill_elem.find("number").get_text(strip=True)
        else:
            bill_id = None

    vid   = f"house-{cong}-{sess}-{roll}"
    tally = []
    for rec in soup.find_all("recorded-vote"):
        leg = rec.find("legislator")
        pos = rec.find("vote")
        if not leg or not leg.has_attr("name-id") or not pos:
            continue
        tally.append((leg["name-id"], pos.get_text(strip=True)))

    return {
        "vote_id":    vid,
        "congress":   cong,
        "chamber":    "house",
        "date":       date,
        "question":   question_tag.get_text(strip=True),
        "description":description.strip(),
        "result":     result_tag.get_text(strip=True),
        "bill_id":    bill_id,
        "key_vote":   is_key_vote(question_tag.get_text(), description),
        "tally":      tally,
    }


def parse_senate_roll(cong: int, sess: int, roll: int) -> Optional[Dict]:
    """Parse a Senate roll-call via official CSV endpoint."""
    url  = SENATE_CSV.format(cong=str(cong).zfill(3), sess=sess, roll=roll)
    resp = fetch_with_retry(url)
    if not resp:
        return None

    rows = list(csv.DictReader(resp.text.splitlines()))
    if not rows:
        return None

    first = rows[0]
    # require key fields
    if not first.get("vote_date") or not first.get("question_text") or not first.get("vote_result"):
        logging.debug(f"🏳 missing Senate metadata on roll {roll}")
        return None

    try:
        date = datetime.strptime(first["vote_date"], "%Y-%m-%d")
    except ValueError:
        logging.warning(f"⚠️ invalid Senate date '{first['vote_date']}' on roll {roll}")
        return None

    vid   = f"senate-{cong}-{sess}-{roll}"
    tally = [
        (r["member_id"], r["vote_cast"].title())
        for r in rows
        if r.get("member_id") and r.get("vote_cast")
    ]

    return {
        "vote_id":    vid,
        "congress":   cong,
        "chamber":    "senate",
        "date":       date,
        "question":   first["question_text"].strip(),
        "description":first.get("vote_title", "").strip(),
        "result":     first["vote_result"].strip(),
        "bill_id":    first.get("bill_number"),
        "key_vote":   is_key_vote(first["question_text"], first.get("vote_title", "")),
        "tally":      tally,
    }


# ── DRIVER ─────────────────────────────────────────────────────────────────────

def run_etl(congress: int = 118, session: int = 1):
    logging.info("🚀 Starting vote ETL")
    misses   = 0
    roll     = 1
    inserted = 0

    while misses < MAX_CONSECUTIVE_MISSES:
        vote = parse_house_roll(congress, session, roll) \
               or parse_senate_roll(congress, session, roll)
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
