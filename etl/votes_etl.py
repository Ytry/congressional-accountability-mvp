# votes_etl.py — correct House XML parsing + HTML-based Senate fallback

import os
import json
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

HOUSE_URL  = "https://clerk.house.gov/evs/{year}/roll{roll:03d}.xml"
SENATE_URL = (
    "https://www.senate.gov/legislative/LIS/roll_call_votes/"
    "vote_{cong}_{sess}_{roll:05d}.htm"
)

MAX_RETRIES            = 3
RETRY_DELAY            = 0.5    # seconds between HTTP retries
MAX_CONSECUTIVE_MISSES = 10     # stop after this many empty rolls

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

# Load Name→Bioguide map for Senate
try:
    with open("name_to_bioguide.json") as f:
        NAME_TO_BIOGUIDE = json.load(f)
except FileNotFoundError:
    logging.warning("⚠️ name_to_bioguide.json not found; Senate names will skip")
    NAME_TO_BIOGUIDE = {}

# ── HELPERS ─────────────────────────────────────────────────────────────────────

def connect():
    return psycopg2.connect(**DB)

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

def is_key_vote(question: str, description: str) -> bool:
    kws = ("final passage", "conference report", "appropriations")
    txt = f"{question} {description}".lower()
    return any(k in txt for k in kws)

def vote_session_exists(cur, vid: str) -> bool:
    cur.execute("SELECT 1 FROM vote_sessions WHERE vote_id = %s", (vid,))
    return cur.fetchone() is not None

def upsert_vote(vote: Dict) -> bool:
    conn = connect(); cur = conn.cursor()
    try:
        if vote_session_exists(cur, vote["vote_id"]):
            return False

        # insert session
        cur.execute(
            """
            INSERT INTO vote_sessions
              (vote_id, congress, chamber, date, question,
               description, result, key_vote, bill_id)
            VALUES
              (%(vote_id)s,%(congress)s,%(chamber)s,%(date)s,
               %(question)s,%(description)s,%(result)s,
               %(key_vote)s,%(bill_id)s)
            RETURNING id
            """,
            vote
        )
        vsid = cur.fetchone()[0]

        # cache legislator_id lookups
        cache: Dict[str,Optional[int]] = {}
        def lk(biog: str) -> Optional[int]:
            if biog in cache: 
                return cache[biog]
            cur.execute("SELECT id FROM legislators WHERE bioguide_id=%s", (biog,))
            row = cur.fetchone()
            cache[biog] = row[0] if row else None
            return cache[biog]

        records = [
            (vsid, lk(biog), pos)
            for biog,pos in vote["tally"]
            if lk(biog) is not None
        ]
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

# ── PARSERS ─────────────────────────────────────────────────────────────────────

def parse_house_roll(cong: int, sess: int, roll: int) -> Optional[Dict]:
    # Map 118th Congress → correct year
    if cong == 118:
        year = 2023 if sess == 1 else 2024
    else:
        year = datetime.now().year

    url = HOUSE_URL.format(year=year, roll=roll)
    resp = fetch_with_retry(url)
    if not resp or not resp.text.lstrip().startswith("<?xml"):
        return None

    soup = BeautifulSoup(resp.text, "lxml-xml")
    meta = soup.find("vote-metadata")
    if not meta:
        return None

    # *** Correct tag names here ***
    date_tag       = meta.find("date")
    question_tag   = meta.find("question")
    result_tag     = meta.find("result")
    if not date_tag or not question_tag or not result_tag:
        logging.debug(f"Skipping House roll {roll}: missing metadata")
        return None

    # Parse date formatted like "02/13/2025"
    date_str = date_tag.get_text(strip=True)
    try:
        date = datetime.strptime(date_str, "%m/%d/%Y")
    except ValueError:
        logging.warning(f"⚠️ Bad date '{date_str}' on roll {roll}")
        return None

    question    = question_tag.get_text(strip=True)
    result      = result_tag.get_text(strip=True)
    description = (meta.find("description") or "").get_text(strip=True)
    bill_id     = (meta.find("bill") or "").get_text(strip=True) or None

    # Collect the recorded-vote entries
    tally = []
    for rec in soup.find_all("recorded-vote"):
        leg = rec.find("legislator")
        pos = rec.find("vote")
        if leg and leg.has_attr("name-id") and pos:
            tally.append((leg["name-id"], pos.get_text(strip=True)))

    if not tally:
        return None

    return {
        "vote_id":    f"house-{cong}-{sess}-{roll}",
        "congress":   cong,
        "chamber":    "house",
        "date":       date,
        "question":   question,
        "description":description,
        "result":     result,
        "bill_id":    bill_id,
        "key_vote":   is_key_vote(question, description),
        "tally":      tally,
    }


def parse_senate_roll(cong: int, sess: int, roll: int) -> Optional[Dict]:
    url  = SENATE_URL.format(cong=cong, sess=sess, roll=roll)
    resp = fetch_with_retry(url)
    if not resp:
        return None

    soup = BeautifulSoup(resp.text, "html.parser")
    table = soup.find("table", class_="roll_call") \
         or max(soup.find_all("table"), key=lambda t: len(t.find_all("tr")), default=None)
    if not table:
        return None

    vid   = f"senate-{cong}-{sess}-{roll}"
    tally = []
    for tr in table.find_all("tr")[1:]:
        cols = tr.find_all("td")
        if len(cols) < 2:
            continue
        raw = cols[0].get_text(strip=True)
        pos = cols[1].get_text(strip=True)
        # normalize "Last, First" → "First Last"
        if "," in raw:
            last, first = [s.strip() for s in raw.split(",",1)]
            name = f"{first} {last}"
        else:
            name = raw
        biog = NAME_TO_BIOGUIDE.get(name)
        if biog:
            tally.append((biog, pos))

    if not tally:
        return None

    # We don’t have full metadata parsing here; stub date + fields
    return {
        "vote_id":    vid,
        "congress":   cong,
        "chamber":    "senate",
        "date":       datetime.now(),
        "question":   "",
        "description":"",
        "result":     "",
        "bill_id":    None,
        "key_vote":   False,
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
