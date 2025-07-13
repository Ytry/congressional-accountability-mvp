#!/usr/bin/env python3
"""
votes_etl.py — ETL for House (XML) and Senate (XML) roll-calls,
with idempotent upserts, bulk inserts, connection pooling, and structured JSON logging
"""
import concurrent.futures
import xml.etree.ElementTree as ET
from datetime import datetime
import json

import config
from logger import setup_logger
from utils import get_cursor, fetch_with_retry, bulk_upsert

# Initialize structured logger
logger = setup_logger("votes_etl")

# ── Configurable constants ───────────────────────────────────────────────────
HOUSE_URL              = config.HOUSE_ROLL_URL
SENATE_URL             = config.SENATE_ROLL_URL
MAX_CONSECUTIVE_MISSES = config.MAX_CONSECUTIVE_MISSES
THREAD_WORKERS         = config.THREAD_WORKERS

# Load Name→Bioguide map
try:
    with open(config.NAME_TO_BIO_MAP, 'r') as f:
        NAME_TO_BIOGUIDE = json.load(f)
    logger.info("Loaded name_to_bioguide map", extra={"entries": len(NAME_TO_BIOGUIDE)})
except Exception:
    NAME_TO_BIOGUIDE = {}
    logger.warning("name_to_bioguide.json not found; Senate names may skip mapping")

# ── Utility: normalize raw vote strings ───────────────────────────────────────
def normalize_vote(raw: str) -> str:
    mapping = {
        **dict.fromkeys(("yea","yes","y","aye"),      "Yea"),
        **dict.fromkeys(("nay","no","n"),               "Nay"),
        **dict.fromkeys(("present","p"),                 "Present"),
        **dict.fromkeys(("not voting","nv","notvote"), "Not Voting"),
        **dict.fromkeys(("absent","a"),                  "Absent"),
    }
    return mapping.get(raw.strip().lower(), "Unknown")

# ── Core DB operation: upsert vote session + records ─────────────────────────
def upsert_vote(vote: dict) -> bool:
    """Insert or update a vote session and its vote records."""
    with get_cursor() as (conn, cur):
        # Upsert vote_session
        cur.execute(
            """
            INSERT INTO vote_sessions
              (vote_id, congress, chamber, date, question, description, result, bill_id)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (vote_id) DO UPDATE SET
              congress    = EXCLUDED.congress,
              chamber     = EXCLUDED.chamber,
              date        = EXCLUDED.date,
              question    = EXCLUDED.question,
              description = EXCLUDED.description,
              result      = EXCLUDED.result,
              bill_id     = EXCLUDED.bill_id
            RETURNING id
            """,
            (
                vote["vote_id"], vote["congress"], vote["chamber"], vote["date"],
                vote["question"], vote["description"], vote["result"], vote.get("bill_id")
            )
        )
        vsid = cur.fetchone()[0]

        # Prepare vote_records
        biogs = [r.get("bioguide_id") for r in vote.get("tally", []) if r.get("bioguide_id")]
        mapping = {}
        if biogs:
            cur.execute(
                "SELECT bioguide_id, id FROM legislators WHERE bioguide_id = ANY(%s)",
                (biogs,)
            )
            mapping = {b: i for b, i in cur.fetchall()}

        records = [
            (vsid, mapping[b], normalize_vote(r.get("position", "")))
            for r in vote.get("tally", [])
            if (b := r.get("bioguide_id")) in mapping
        ]

        # Bulk upsert into vote_records
        if records:
            bulk_upsert(
                cur,
                table="vote_records",
                rows=records,
                columns=["vote_session_id", "legislator_id", "vote_cast"],
                conflict_cols=["vote_session_id", "legislator_id"]
            )

    logger.info(
        "Upserted vote session",
        extra={"vote_id": vote.get("vote_id"), "records": len(records)}
    )
    return True

# ── Parsing functions for XML sources ─────────────────────────────────────────
def parse_house(congress: int, session: int, roll: int) -> dict | None:
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
        "vote_id":    f"house-{congress}-{session}-{roll}",
        "congress":   congress,
        "chamber":    "house",
        "date":       date,
        "question":   root.findtext(".//question-text", ""),
        "description":root.findtext(".//vote-desc",      ""),
        "result":     root.findtext(".//vote-result",    ""),
        "bill_id":    root.findtext(".//legis-num",      None),
        "tally":      []
    }
    for rec in root.findall(".//recorded-vote"):
        biog = rec.find("legislator").attrib.get("name-id") if rec.find("legislator") is not None else None
        pos  = rec.findtext("vote", "")
        vote["tally"].append({"bioguide_id": biog, "position": pos})
    return vote


def parse_senate(congress: int, session: int, roll: int) -> dict | None:
    url = SENATE_URL.format(congress=congress, session=session, roll=roll)
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
        "vote_id":    f"senate-{congress}-{session}-{roll}",
        "congress":   congress,
        "chamber":    "senate",
        "date":       date,
        "question":   root.findtext("vote_question_text") or "",
        "description":root.findtext("vote_title") or "",
        "result":     root.findtext("vote_result") or "",
        "bill_id":    None,
        "tally":      tally
    }

# ── Driver ────────────────────────────────────────────────────────────────────
def run_chamber(name: str, parser, congress: int, session: int):
    logger.info("Starting chamber ETL", extra={"chamber": name, "congress": congress, "session": session})
    inserted = 0
    misses   = 0
    roll     = 1
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
