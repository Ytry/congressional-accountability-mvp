# votes_etl.py — full‐range ETL with miss‐threshold stopping

import os, csv, logging, requests, psycopg2, time
from datetime import datetime
from typing import Dict, List, Optional
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
SENATE_CSV   = ("https://www.senate.gov/legislative/LIS/roll_call_votes/"
                "vote{cong}{sess}/csv/roll_call_vote_{cong}{sess}_{roll:05d}.csv")

MAX_RETRIES            = 3
RETRY_DELAY            = 0.5   # seconds between HTTP retries
MAX_CONSECUTIVE_MISSES = 10    # stop after this many empty rolls
# ──────────────────────────────────────────────────────────────────────────────

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

def connect(): 
    return psycopg2.connect(**DB)

def fetch_with_retry(url: str) -> Optional[requests.Response]:
    for i in range(1, MAX_RETRIES+1):
        try:
            r = requests.get(url, timeout=10)
            if r.status_code == 200 and "vote-not-available" not in r.url:
                return r
        except Exception as e:
            logging.debug(f"Retry {i} error for {url}: {e}")
        time.sleep(RETRY_DELAY)
    return None

def is_key_vote(question: str, description: str) -> bool:
    kws = ("final passage","conference report","appropriations")
    text = f"{question} {description}".lower()
    return any(k in text for k in kws)

def vote_session_exists(cur, vid: str) -> bool:
    cur.execute("SELECT 1 FROM vote_sessions WHERE vote_id = %s", (vid,))
    return cur.fetchone() is not None

def upsert_vote(vote: Dict) -> bool:
    conn = connect(); cur = conn.cursor()
    try:
        if vote_session_exists(cur, vote["vote_id"]):
            logging.debug(f"⏩ skip existing {vote['vote_id']}")
            return False

        cur.execute(
            """INSERT INTO vote_sessions
               (vote_id, congress, chamber, date, question, description, result, key_vote, bill_id)
               VALUES (%(vote_id)s,%(congress)s,%(chamber)s,%(date)s,
                       %(question)s,%(description)s,%(result)s,%(key_vote)s,%(bill_id)s)
               RETURNING id""",
            vote
        )
        vsid = cur.fetchone()[0]

        # cache legislator_id lookups
        cache: Dict[str,int] = {}
        def lk(biog: str) -> Optional[int]:
            if biog in cache: return cache[biog]
            cur.execute("SELECT id FROM legislators WHERE bioguide_id=%s", (biog,))
            row = cur.fetchone()
            cache[biog] = row[0] if row else None
            return cache[biog]

        recs = [
            (vsid, lk(biog), pos)
            for biog,pos in vote["tally"]
            if lk(biog) is not None
        ]
        if recs:
            cur.executemany(
                "INSERT INTO vote_records (vote_session_id, legislator_id, vote_cast) VALUES (%s,%s,%s) "
                "ON CONFLICT DO NOTHING",
                recs
            )
        conn.commit()
        logging.info(f"✅ {vote['vote_id']} (+{len(recs)} positions)")
        return True

    except Exception as e:
        conn.rollback()
        logging.error(f"❌ db error for {vote['vote_id']}: {e}")
        return False
    finally:
        cur.close(); conn.close()

def parse_house_roll(cong: int, sess: int, roll: int) -> Optional[Dict]:
    year = 2025 if cong==118 else 2024
    url  = HOUSE_XML.format(year=year, roll=roll)
    resp = fetch_with_retry(url)
    if not resp or not resp.text.strip().startswith("<?xml"):
        return None

    soup = BeautifulSoup(resp.text, "xml")
    meta = soup.find("vote-metadata")
    if not meta: return None

    vid = f"house-{cong}-{sess}-{roll}"
    date = datetime.strptime(meta.date.text, "%m/%d/%Y")
    tally = [
        (rec.legislator["name-id"], rec.vote.text)
        for rec in soup.find_all("recorded-vote")
        if rec.legislator and rec.legislator.has_attr("name-id")
    ]

    return {
        "vote_id":    vid, "congress": cong, "chamber": "house",
        "date":       date, "question": meta.question.text,
        "description":meta.description.text if meta.description else "",
        "result":     meta.result.text, "bill_id": meta.bill.text if meta.bill else None,
        "key_vote":   is_key_vote(meta.question.text, meta.description.text if meta.description else ""),
        "tally":      tally,
    }

def parse_senate_roll(cong: int, sess: int, roll: int) -> Optional[Dict]:
    url = SENATE_CSV.format(cong=str(cong).zfill(3), sess=sess, roll=roll)
    resp = fetch_with_retry(url)
    if not resp: return None

    rows = list(csv.DictReader(resp.text.splitlines()))
    if not rows: return None

    m = rows[0]
    vid = f"senate-{cong}-{sess}-{roll}"
    date = datetime.strptime(m["vote_date"], "%Y-%m-%d")
    tally = [(r["member_id"], r["vote_cast"].title()) for r in rows]

    return {
        "vote_id":    vid, "congress": cong, "chamber": "senate",
        "date":       date, "question": m["question_text"],
        "description":m["vote_title"], "result": m["vote_result"],
        "bill_id":    m["bill_number"], "key_vote": is_key_vote(m["question_text"], m["vote_title"]),
        "tally":      tally,
    }

def run_etl(congress: int = 118, session: int = 1):
    logging.info("🚀 Starting vote ETL")
    misses = 0
    roll   = 1
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
