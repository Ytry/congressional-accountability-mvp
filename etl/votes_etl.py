# votes_etl.py  · schema-aligned, FastAPI-ready
import os, csv, logging, requests, psycopg2
from datetime import datetime
from typing import Dict, List
from bs4 import BeautifulSoup              # pip install beautifulsoup4
from dotenv import load_dotenv

load_dotenv()
DB = dict(
    dbname=os.getenv("DB_NAME"),
    user=os.getenv("DB_USER"),
    password=os.getenv("DB_PASSWORD"),
    host=os.getenv("DB_HOST"),
    port=os.getenv("DB_PORT"),
)

HOUSE_XML = "https://clerk.house.gov/evs/{year}/roll{roll:03d}.xml"
SENATE_CSV = "https://www.senate.gov/legislative/LIS/roll_call_votes/vote{cong}{sess}/csv/roll_call_vote_{cong}{sess}_{roll:05d}.csv"

###############################################################################
# helpers
###############################################################################
def connect():
    return psycopg2.connect(**DB)

def get_legislator_id(cur, bioguide_id: str) -> int | None:
    cur.execute("SELECT id FROM legislators WHERE bioguide_id = %s", (bioguide_id,))
    row = cur.fetchone()
    return row[0] if row else None

def is_key_vote(question: str, description: str) -> bool:
    """
    Very simple placeholder implementing § 2.2 'Key Vote' rules
    (final passage of major bills, etc.) :contentReference[oaicite:1]{index=1}
    """
    keywords = ("final passage", "conference report", "appropriations")
    text = f"{question} {description}".lower()
    return any(k in text for k in keywords)

def vote_session_exists(cur, vote_id: str) -> bool:
    cur.execute("SELECT 1 FROM vote_sessions WHERE vote_id = %s", (vote_id,))
    return cur.fetchone() is not None

###############################################################################
# House parser (XML)
###############################################################################
def parse_house_roll(congress: int, session: int, roll: int) -> Dict | None:
    url = HOUSE_XML.format(year=2025 if congress == 118 else 2024, roll=roll)  # quick dummy
    resp = requests.get(url, timeout=20)
    if resp.status_code != 200:
        logging.warning("House %s returned %s", url, resp.status_code)
        return None
    soup = BeautifulSoup(resp.text, "xml")
    meta = soup.find("vote-metadata")
    if not meta:
        return None

    vote_id = f"house-{congress}-{session}-{roll}"
    date = datetime.strptime(meta.date.text, "%m/%d/%Y")
    question = meta.question.text
    result   = meta.result.text
    bill_id  = meta.bill.text if meta.bill else None
    desc     = meta.description.text if meta.description else None

    tally = []
    for rec in soup.find_all("recorded-vote"):
        bioguide = rec.legislator["name-id"]
        pos      = rec.vote.text
        tally.append((bioguide, pos))

    return {
        "vote_id": vote_id,
        "congress": congress,
        "chamber": "house",
        "date": date,
        "question": question,
        "description": desc,
        "result": result,
        "bill_id": bill_id,
        "key_vote": is_key_vote(question, desc),
        "tally": tally,
    }

###############################################################################
# Senate parser (official CSV, stable since 1989)
###############################################################################
def parse_senate_roll(congress: int, session: int, roll: int) -> Dict | None:
    url = SENATE_CSV.format(cong=str(congress).zfill(3), sess=session, roll=roll)
    resp = requests.get(url, timeout=20)
    if resp.status_code != 200:
        logging.debug("Senate %s returned %s", roll, resp.status_code)
        return None
    lines = resp.text.splitlines()
    reader = csv.DictReader(lines)
    rows = list(reader)
    if not rows:
        return None

    meta_r = rows[0]
    vote_id = f"senate-{congress}-{session}-{roll}"
    date = datetime.strptime(meta_r["vote_date"], "%Y-%m-%d")
    question = meta_r["question_text"]
    result   = meta_r["vote_result"]
    bill_id  = meta_r["bill_number"]
    desc     = meta_r["vote_title"]

    tally = [(r["member_id"], r["vote_cast"].title()) for r in rows]

    return {
        "vote_id": vote_id,
        "congress": congress,
        "chamber": "senate",
        "date": date,
        "question": question,
        "description": desc,
        "result": result,
        "bill_id": bill_id,
        "key_vote": is_key_vote(question, desc),
        "tally": tally,
    }

###############################################################################
# DB ingest
###############################################################################
def upsert_vote(vote: Dict):
    conn = connect()
    cur  = conn.cursor()
    try:
        if vote_session_exists(cur, vote["vote_id"]):
            logging.info("⏭  %s already present", vote["vote_id"])
            return

        cur.execute(
            """INSERT INTO vote_sessions
               (vote_id, congress, chamber, date, question, description, result,
                key_vote, bill_id)
               VALUES (%(vote_id)s,%(congress)s,%(chamber)s,%(date)s,
                       %(question)s,%(description)s,%(result)s,%(key_vote)s,%(bill_id)s)
               RETURNING id""",
            vote,
        )
        vote_session_id = cur.fetchone()[0]

        # map bioguide→legislator_id once per ETL run for speed
        cache: dict[str, int] = {}

        def fk(biog: str) -> int | None:
            if biog in cache:
                return cache[biog]
            cur.execute("SELECT id FROM legislators WHERE bioguide_id = %s", (biog,))
            r = cur.fetchone()
            cache[biog] = r[0] if r else None
            return cache[biog]

        records = [
            (vote_session_id, fk(biog), pos)
            for biog, pos in vote["tally"]
            if fk(biog)
        ]

        cur.executemany(
            "INSERT INTO vote_records (vote_session_id, legislator_id, vote_cast) "
            "VALUES (%s,%s,%s) ON CONFLICT DO NOTHING",
            records,
        )
        conn.commit()
        logging.info("✅ inserted %s (%d positions)", vote["vote_id"], len(records))
    except Exception as e:
        conn.rollback()
        logging.error("❌ db error for %s: %s", vote["vote_id"], e)
    finally:
        cur.close()
        conn.close()

###############################################################################
# driver
###############################################################################
def run(congress=118, session=1, house_rolls=range(1, 11), senate_rolls=range(1, 11)):
    for roll in house_rolls:
        v = parse_house_roll(congress, session, roll)
        if v:
            upsert_vote(v)

    for roll in senate_rolls:
        v = parse_senate_roll(congress, session, roll)
        if v:
            upsert_vote(v)

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
    run()
