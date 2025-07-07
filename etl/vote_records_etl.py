# vote_records_etl.py — Robust with fallback

import os
import requests
import psycopg2
import xml.etree.ElementTree as ET
from dotenv import load_dotenv
from datetime import datetime
import logging

load_dotenv()
logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")

DB_CONFIG = {
    "dbname": os.getenv("dbname"),
    "user": os.getenv("user"),
    "password": os.getenv("password"),
    "host": os.getenv("host"),
    "port": os.getenv("port"),
}

HOUSE_URL = "https://clerk.house.gov/evs/{year}/roll{roll:03}.xml"

def db_connection():
    return psycopg2.connect(**DB_CONFIG)

def load_vote_sessions():
    conn = db_connection()
    cur = conn.cursor()
    cur.execute("SELECT roll, session, congress, id FROM vote_sessions")
    sessions = {(r, s, c): id for r, s, c, id in cur.fetchall()}
    conn.close()
    logging.info(f"📥 Loaded {len(sessions)} vote sessions from DB.")
    return sessions

def load_legislators():
    conn = db_connection()
    cur = conn.cursor()
    cur.execute("SELECT bioguide_id, id FROM legislators")
    legislator_map = {bid.strip().upper(): id for bid, id in cur.fetchall()}
    conn.close()
    logging.info(f"🧑‍🤝‍🧑Loaded {len(legislator_map)} legislators from DB.")
    return legislator_map

def fetch_roll(congress, session, roll):
    url = HOUSE_URL.format(year=2023, roll=roll)
    try:
        resp = requests.get(url)
        if resp.status_code == 200:
            return ET.fromstring(resp.content)
        else:
            logging.error(f"❌Failed to fetch XML from {url}: {resp.status_code}")
    except Exception as e:
        logging.error(f"❌Request error for roll {roll}: {e}")
    return None

def parse_votes(root, congress, session, roll, legislator_map, vote_sessions):
    records = []
    vote_id = f"house-{congress}-{session}-{roll}"
    try:
        bill = root.findtext(".//legis-num")
        question = root.findtext(".//question-text")
        description = root.findtext(".//vote-desc")
        result = root.findtext(".//vote-result")
        date = datetime.strptime(root.findtext(".//action-date"), "%d-%b-%Y")
    except Exception as e:
        logging.warning(f"⚠️Failed to parse metadata for roll {roll}: {e}")
        return records

    # Look up session ID
    session_key = (roll, session, congress)
    session_id = vote_sessions.get(session_key)
    if not session_id:
        logging.warning(f"⚠️Skipping vote {vote_id} (not found in vote_sessions)")
        return records

    tally = {"Yea": 0, "Nay": 0, "Present": 0, "Not Voting": 0}
    for vote in root.findall(".//recorded-vote"):
        bioguide = vote.find("legislator").attrib.get("bioGuideId", "").upper().strip()
        position = vote.findtext("vote")
        if not bioguide or bioguide not in legislator_map or position not in tally:
            continue
        tally[position] += 1
        records.append({
            "vote_id": vote_id,
            "legislator_id": legislator_map[bioguide],
            "vote_session_id": session_id,
            "bill": bill,
            "question": question,
            "description": description,
            "result": result,
            "position": position,
            "date": date,
            "tally_yea": tally["Yea"],
            "tally_nay": tally["Nay"],
            "tally_present": tally["Present"],
            "tally_not_voting": tally["Not Voting"],
        })
    return records

def insert_votes(records):
    conn = db_connection()
    cur = conn.cursor()
    inserted = skipped = 0

    for r in records:
        cur.execute("SELECT 1 FROM votes WHERE vote_id = %s AND legislator_id = %s", (r["vote_id"], r["legislator_id"]))
        if cur.fetchone():
            skipped += 1
            continue
        try:
            cur.execute("""
                INSERT INTO votes (
                    legislator_id, vote_id, vote_session_id,
                    bill_number, question_text, vote_description, vote_result,
                    position, date, tally_yea, tally_nay, tally_present, tally_not_voting, is_key_vote
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, false)
            """, (
                r["legislator_id"], r["vote_id"], r["vote_session_id"],
                r["bill"], r["question"], r["description"], r["result"],
                r["position"], r["date"], r["tally_yea"], r["tally_nay"],
                r["tally_present"], r["tally_not_voting"]
            ))
            inserted += 1
        except Exception as e:
            logging.error(f"❌Failed to insert vote {r['vote_id']}: {e}")
            conn.rollback()
    conn.commit()
    cur.close()
    conn.close()
    logging.info(f"✅ Inserted: {inserted} | ⏭️ Skipped: {skipped}")

def run_etl():
    logging.info("🛠️ Starting Vote Records ETL...")
    vote_sessions = load_vote_sessions()
    legislator_map = load_legislators()
    all_records = []

    for roll in range(1, 51):  # Check first 50 rolls
        root = fetch_roll(118, 1, roll)
        if root is None:
            continue
        records = parse_votes(root, 118, 1, roll, legislator_map, vote_sessions)
        if records:
            all_records.extend(records)

    insert_votes(all_records)
    logging.info("🧹ETL complete. Database connection closed.")

if __name__ == "__main__":
    run_etl()
