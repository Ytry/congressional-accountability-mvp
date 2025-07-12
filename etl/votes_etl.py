#!/usr/bin/env python3
"""
votes_etl.py — ETL for House (XML) and Senate (XML) roll-calls,
with idempotent upserts, bulk inserts, connection pooling, and structured JSON logging
"""
import sys
import json
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

import config
from logger import setup_logger

# Initialize structured logger
logger = setup_logger("votes_etl")

# ── CONFIG ─────────────────────────────────────────────────────────────────────
DB_URL = config.DATABASE_URL
DB_POOL_MIN = config.DB_POOL_MIN
DB_POOL_MAX = config.DB_POOL_MAX

# ── CONNECTION POOL ───────────────────────────────────────────────────────────
try:
    if DB_URL:
        conn_pool = ThreadedConnectionPool(DB_POOL_MIN, DB_POOL_MAX, DB_URL)
    else:
        conn_pool = ThreadedConnectionPool(
            DB_POOL_MIN, DB_POOL_MAX,
            database=config.DB_NAME,
            user=config.DB_USER,
            password=config.DB_PASSWORD,
            host=config.DB_HOST,
            port=config.DB_PORT
        )
    logger.info("Connection pool created", extra={"minconn": DB_POOL_MIN, "maxconn": DB_POOL_MAX})
except Exception:
    logger.exception("Failed to create connection pool")
    raise

@contextmanager
def get_conn():
    conn = conn_pool.getconn()
    try:
        yield conn
    finally:
        conn_pool.putconn(conn)

# ── CONSTANTS ─────────────────────────────────────────────────────────────────
HOUSE_URL = config.HOUSE_ROLL_URL
SENATE_XML_URL = config.SENATE_ROLL_URL
MAX_RETRIES = config.HTTP_MAX_RETRIES
RETRY_DELAY = config.HTTP_RETRY_DELAY
MAX_CONSECUTIVE_MISSES = config.MAX_CONSECUTIVE_MISSES
THREAD_WORKERS = config.THREAD_WORKERS

# Load Name→Bioguide map
try:
    with open(str(config.NAME_TO_BIO_MAP)) as f:
        NAME_TO_BIOGUIDE = json.load(f)
    logger.info("Loaded name_to_bioguide map", extra={"entries": len(NAME_TO_BIOGUIDE)})
except FileNotFoundError:
    NAME_TO_BIOGUIDE = {}
    logger.warning("name_to_bioguide.json not found; Senate names may skip mapping")

# ── HELPERS ─────────────────────────────────────────────────────────────────────
def normalize_vote(raw: str) -> str:
    s = raw.strip().lower()
    return {
        **dict.fromkeys(("yea","yes","y","aye"), "Yea"),
        **dict.fromkeys(("nay","no","n"), "Nay"),
        **dict.fromkeys(("present","p"), "Present"),
        **dict.fromkeys(("not voting","nv","notvote"), "Not Voting"),
        **dict.fromkeys(("absent","a"), "Absent"),
    }.get(s, "Unknown")

# ── CORE DB OPERATION ──────────────────────────────────────────────────────────
def upsert_vote(vote: Dict) -> bool:
    with get_conn() as conn:
        try:
            with conn.cursor() as cur:
                # Upsert vote session
                cur.execute(
                    """
                    INSERT INTO vote_sessions
                      (vote_id, congress, chamber, date, question, description, result, bill_id)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (vote_id) DO UPDATE SET
                      congress     = EXCLUDED.congress,
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
                vsid = res[0] if res else None

                # Bulk insert/update records
                biogs = [r.get("bioguide_id") for r in vote.get("tally", []) if r.get("bioguide_id")]
                if biogs:
                    cur.execute(
                        "SELECT bioguide_id, id FROM legislators WHERE bioguide_id = ANY(%s)", (biogs,)
                    )
                    mapping = {b: i for b, i in cur.fetchall()}
                else:
                    mapping = {}

                records = []
                for r in vote.get("tally", []):
                    biog = r.get("bioguide_id")
                    pos = normalize_vote(r.get("position", ""))
                    if biog in mapping:
                        records.append((vsid, mapping[biog], pos))

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
            logger.info("Upserted vote session", extra={"vote_id": vote.get("vote_id"), "records": len(records)})
            return True
        except Exception:
            conn.rollback()
            logger.exception("DB error during vote upsert", extra={"vote_id": vote.get("vote_id")})
            return False

# ── RETRYABLE FETCH ─────────────────────────────────────────────────────────────
def fetch_with_retry(url: str) -> Optional[requests.Response]:
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = requests.get(url, timeout=config.HTTP_TIMEOUT)
            if resp.status_code == 200:
                return resp
            if resp.status_code == 404:
                return None
            logger.warning("Unexpected status code", extra={"url": url, "status": resp.status_code})
        except Exception as e:
            logger.debug("Fetch attempt failed", extra={"url": url, "attempt": attempt, "error": str(e)})
        time.sleep(RETRY_DELAY * (2 ** (attempt - 1)))
    logger.error("Failed to fetch URL after retries", extra={"url": url})
    return None

# ── PARSERS ─────────────────────────────────────────────────────────────────────
def parse_house(congress: int, session: int, roll: int) -> Optional[Dict]:
    url = HOUSE_URL.format(year=config.HOUSE_YEAR, roll=roll)
    resp = fetch_with_retry(url)
    if not resp or not resp.content.lstrip().startswith(b"<?xml"):
        return None
    try:
        root = ET.fromstring(resp.content)
        date = datetime.strptime(root.findtext(".//action-date", ""), "%d-%b-%Y")
    except Exception:
        logger.warning("House XML parse failed", extra={"roll": roll})
        return None

    vote = {
        "vote_id": f"house-{congress}-{session}-{roll}",
        "congress": congress,
        "chamber": "house",
        "date": date,
        "question": root.findtext(".//question-text", ""),
        "description": root.findtext(".//vote-desc", ""),
        "result": root.findtext(".//vote-result", ""),
        "bill_id": root.findtext(".//legis-num", None),
        "tally": []
    }
    for rec in root.findall(".//recorded-vote"):
        biog = rec.find("legislator").attrib.get("name-id") if rec.find("legislator") is not None else None
        pos = rec.findtext("vote", "")
        vote["tally"].append({"bioguide_id": biog, "position": pos})
    return vote


def parse_senate(congress: int, session: int, roll: int) -> Optional[Dict]:
    url = SENATE_XML_URL.format(congress=congress, session=session, roll=roll)
    resp = fetch_with_retry(url)
    if not resp or not resp.content.lstrip().startswith(b"<?xml"):
        return None
    try:
        root = ET.fromstring(resp.content)
    except Exception:
        logger.warning("Senate XML parse failed", extra={"roll": roll})
        return None
    try:
        date = datetime.strptime(root.findtext("vote_date", ""), "%B %d, %Y,  %I:%M %p")
    except Exception:
        date = datetime.now()

    tally = []
    for m in root.findall(".//members/member"):
        name = f"{m.findtext('first_name','').strip()} {m.findtext('last_name','').strip()}".strip()
        biog = NAME_TO_BIOGUIDE.get(name)
        tally.append({"bioguide_id": biog, "position": m.findtext("vote_cast","" )})
    return {
        "vote_id": f"senate-{congress}-{session}-{roll}",
        "congress": congress,
        "chamber": "senate",
        "date": date,
        "question": root.findtext("vote_question_text") or "",
        "description": root.findtext("vote_title") or "",
        "result": root.findtext("vote_result") or "",
        "bill_id": None,
        "tally": tally
    }

# ── DRIVER ─────────────────────────────────────────────────────────────────────
def run_chamber(name: str, parser, congress: int, session: int):
    logger.info("Starting chamber ETL", extra={"chamber": name, "congress": congress, "session": session})
    inserted = 0
    misses = 0
    roll = 1
    while misses < MAX_CONSECUTIVE_MISSES:
        vote = parser(congress, session, roll)
        if vote and vote.get("tally"):
            if upsert_vote(vote):
                inserted += 1
            misses = 0
        else:
            misses += 1
        roll += 1
    logger.info("Completed chamber ETL", extra={"chamber": name, "inserted": inserted})


def main():
    import argparse
    p = argparse.ArgumentParser(description="Run votes ETL for specified Congress and session")
    p.add_argument("congress", type=int, nargs="?", default=config.CONGRESS)
    p.add_argument("session", type=int, nargs="?", default=config.SESSION)
    args = p.parse_args()

    with concurrent.futures.ThreadPoolExecutor(max_workers=THREAD_WORKERS) as executor:
        futures = [
            executor.submit(run_chamber, "house", parse_house, args.congress, args.session),
            executor.submit(run_chamber, "senate", parse_senate, args.congress, args.session)
        ]
        for future in concurrent.futures.as_completed(futures):
            if future.exception():
                logger.exception("Parallel run error", extra={"error": str(future.exception())})

if __name__ == "__main__":
    main()
