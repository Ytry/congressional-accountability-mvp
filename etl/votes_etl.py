#!/usr/bin/env python3
# votes_etl.py — old-style parsing (ElementTree + BS4),
# normalized vote_cast, upsert into vote_sessions/vote_records,
# now with CLI args for congress & session.

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

def vote_exists(cur, vote_id: str) -> bool:
    cur.execute("SELECT 1 FROM vote_sessions WHERE vote_id = %s", (vote_id,))
    return cur.fetchone() is not None

def upsert_vote(vote: Dict) -> bool:
    conn = connect_db(); cur = conn.cursor()
    try:
        if vote_exists(cur, vote["vote_id"]):
            return False

        # 1) Insert vote_sessions
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

        # 2) Resolve Bioguide → legislator_id
        cache: Dict[str,Optional[int]] = {}
        def leg_id(biog: Optional[str]) -> Optional[int]:
            if not biog:
                return None
            if biog in cache:
                return cache[biog]
            cur.execute("SELECT id FROM legislators WHERE bioguide_id = %s", (biog,))
            row = cur.fetchone()
            cache[biog] = row[0] if row else None
            return cache[biog]

        # 3) Build vote_records with normalized vote_cast
        rows = []
        for rec in vote["tally"]:
            lid = leg_id(rec.get("bioguide_id"))
            if lid:
                rows.append((vsid, lid, normalize_vote(rec.get("position",""))))

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
        cur.close(); conn.close()

def fetch_with_retry(url: str) -> Optional[requests.Response]:
    for i in range(1, MAX_RETRIES+1):
        try:
            r = requests.get(url, timeout=10)
            if r.status_code == 200 and "vote-not-available" not in r.url:
                return r
        except Exception as e:
            logging.debug(f"Retry {i} failed for {url}: {e}")
        time.sleep(RETRY_DELAY)
    return None

# ── PARSERS ─────────────────────────────────────────────────────────────────────

def parse_house_vote(congress: int, session: int, roll: int) -> Optional[Dict]:
    # Map session→year for House (118-1 → 2023, 118-2 → 2024)
    if congress == 118:
        year = 2023 if session == 1 else 2024
    else:
        year = datetime.now().year

    url = HOUSE_URL.format(year=year, roll=roll)
    logging.info(f"🏛 HOUSE {roll}: {url}")
    resp = fetch_with_retry(url)
    if not resp or not resp.content.strip().startswith(b"<?xml"):
        return None

    try:
        root = ET.fromstring(resp.content)
    except Exception as e:
        logging.warning(f"House XML parse failed for roll {roll}: {e}")
        return None

    # Extract exactly as original ETL
    ds = root.findtext(".//action-date")
    qt = root.findtext(".//question-text") or ""
    vd = root.findtext(".//vote-desc")      or ""
    vr = root.findtext(".//vote-result")    or ""
    bn = root.findtext(".//legis-num")      or ""

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
    url = SENATE_URL.format(congress=congress, session=session, roll=roll)
    logging.info(f"🏛 SENATE {roll}: {url}")
    resp = fetch_with_retry(url)
    if not resp:
        return None

    soup = BeautifulSoup(resp.text, "html.parser")
    table = soup.find("table", class_="roll_call") \
         or max(soup.find_all("table"), key=lambda t: len(t.find_all("tr")), default=None)
    if not table:
        return None

    vid = f"senate-{congress}-{session}-{roll}"
    tally = []
    for tr in table.find_all("tr")[1:]:
        cols = tr.find_all("td")
        if len(cols) < 2:
            continue
        raw_name = cols[0].get_text(strip=True)
        pos      = cols[1].get_text(strip=True)
        if "," in raw_name:
            last, first = [s.strip() for s in raw_name.split(",",1)]
            name = f"{first} {last}"
        else:
            name = raw_name
        biog = NAME_TO_BIOGUIDE.get(name)
        tally.append({"bioguide_id": biog, "position": pos})

    return {
        "vote_id":    vid,
        "congress":   congress,
        "chamber":    "senate",
        "date":       datetime.now(),  # Senate metadata parsing can be added
        "question":   "",
        "description":"",
        "result":     "",
        "bill_id":    None,
        "tally":      tally,
    }

# ── DRIVER ─────────────────────────────────────────────────────────────────────

def run_etl(congress: int = 118, session: int = 1):
    logging.info(f"🚀 Starting vote ETL for Congress {congress}, Session {session}")
    inserted = 0
    misses   = 0

    for roll in range(1, 3000):
        if misses >= MAX_CONSECUTIVE_MISSES:
            break

        v = parse_house_vote(congress, session, roll) \
            or parse_senate_vote(congress, session, roll)

        if not v:
            misses += 1
            logging.debug(f"📭 miss {misses} at roll {roll}")
            continue

        if upsert_vote(v):
            inserted += 1
        misses = 0

    logging.info(f"🎯 Done: inserted {inserted} sessions")

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="ETL roll-call votes into Postgres")
    parser.add_argument("congress", type=int, nargs="?", default=118,
                        help="Congress number (e.g. 118)")
    parser.add_argument("session", type=int, nargs="?", default=1,
                        help="Session number (1 or 2)")
    args = parser.parse_args()

    run_etl(args.congress, args.session)
