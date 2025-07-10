#!/usr/bin/env python3
# votes_etl.py — ETL for House (XML) and Senate (XML) roll-calls,
# with idempotent upserts, bulk inserts, connection pooling, and parallel execution

import os
import json
import logging
import time
import requests
import psycopg2
import psycopg2.extras
from psycopg2.pool import ThreadedConnectionPool
from contextlib import contextmanager
import xml.etree.ElementTree as ET
import concurrent.futures

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

# ── CONNECTION POOL ──────────────────────────────────────────────────────────────
try:
    conn_pool = ThreadedConnectionPool(minconn=1, maxconn=10, **DB)
except Exception as e:
    logging.error(f"Failed to create connection pool: {e}")
    raise

@contextmanager
def get_conn():
    conn = conn_pool.getconn()
    try:
        yield conn
    finally:
        conn_pool.putconn(conn)

# ── CONSTANTS ──────────────────────────────────────────────────────────────────
HOUSE_URL      = "https://clerk.house.gov/evs/{year}/roll{roll:03d}.xml"
SENATE_XML_URL = (
    "https://www.senate.gov/legislative/LIS/roll_call_votes/"
    "vote{congress}{session}/vote_{congress}_{session}_{roll:05d}.xml"
)
MAX_RETRIES            = 3
RETRY_DELAY            = 0.5
MAX_CONSECUTIVE_MISSES = 10

# Load Name→Bioguide map
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

# ── CORE DB OPERATION ──────────────────────────────────────────────────────────

def upsert_vote(vote: Dict) -> bool:
    with get_conn() as conn:
        try:
            with conn.cursor() as cur:
                # Upsert vote_session
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
                vsid = res[0] if res else cur.execute(
                    "SELECT id FROM vote_sessions WHERE vote_id = %s",
                    (vote["vote_id"],)
                ) or cur.fetchone()[0]

                # Bulk insert/update vote_records
                biogs = [r.get("bioguide_id") for r in vote["tally"] if r.get("bioguide_id")]
                cur.execute(
                    "SELECT bioguide_id, id FROM legislators WHERE bioguide_id = ANY(%s)",
                    (biogs,)
                )
                mapping = {b: i for b, i in cur.fetchall()}

                records = [
                    (vsid, mapping[b], normalize_vote(r["position"]))
                    for r in vote["tally"] if (b := r.get("bioguide_id")) in mapping
                ]

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
                        page_size=100
                    )
            conn.commit()
            logging.info(f"✅ {vote['vote_id']} (+{len(records)} positions)")
            return True
        except Exception as e:
            conn.rollback()
            logging.error(f"❌ DB error for {vote['vote_id']}: {e}")
            return False

# ── RETRYABLE FETCH ─────────────────────────────────────────────────────────────

def fetch_with_retry(url: str) -> Optional[requests.Response]:
    for i in range(1, MAX_RETRIES + 1):
        try:
            r = requests.get(url, timeout=10)
            if r.status_code == 200:
                return r
            if r.status_code == 404:
                return None
        except Exception as e:
            logging.debug(f"Retry {i} for {url} failed: {e}")
        time.sleep(RETRY_DELAY * (2 ** (i-1)))
    return None

# ── PARSERS ─────────────────────────────────────────────────────────────────────
# parse_house and parse_senate unchanged

def parse_house(congress: int, session: int, roll: int) -> Optional[Dict]:
    year = 2023 if (congress==118 and session==1) else (2024 if (congress==118 and session==2) else datetime.now().year)
    url  = HOUSE_URL.format(year=year, roll=roll)
    resp = fetch_with_retry(url)
    if not resp or not resp.content.lstrip().startswith(b"<?xml"):
        return None
    try:
        root = ET.fromstring(resp.content)
    except Exception as e:
        logging.warning(f"House XML parse failed for roll {roll}: {e}")
        return None

    try:
        date = datetime.strptime(root.findtext(".//action-date", ""), "%d-%b-%Y")
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
    url  = SENATE_XML_URL.format(congress=congress, session=session, roll=roll)
    resp = fetch_with_retry(url)
    if not resp or not resp.content.lstrip().startswith(b"<?xml"):
        return None
    try:
        root = ET.fromstring(resp.content)
    except Exception as e:
        logging.warning(f"Senate XML parse failed for roll {roll}: {e}")
        return None

    try:
        date = datetime.strptime(root.findtext("vote_date", ""), "%B %d, %Y,  %I:%M %p")
    except Exception:
        date = datetime.now()

    question   = root.findtext("vote_question_text") or root.findtext("question") or ""
    result     = root.findtext("vote_result") or root.findtext("vote_result_text") or ""
    description= root.findtext("vote_title") or ""
    vid        = f"senate-{congress}-{session}-{roll}"

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
        roll += 1
    logging.info(f"🏁 {name.capitalize()} done: inserted {inserted} sessions\n")


def main():
    import argparse
    p = argparse.ArgumentParser()
    p.add_argument("congress", type=int, nargs="?", default=118)
    p.add_argument("session", type=int, nargs="?", default=1)
    args = p.parse_args()

    # Parallelize House and Senate runs
    with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
        futures = [
            executor.submit(run_chamber, "house", parse_house, args.congress, args.session),
            executor.submit(run_chamber, "senate", parse_senate, args.congress, args.session)
        ]
        for future in concurrent.futures.as_completed(futures):
            if future.exception():
                logging.error(f"Parallel run error: {future.exception()}")

if __name__ == "__main__":
    main()
