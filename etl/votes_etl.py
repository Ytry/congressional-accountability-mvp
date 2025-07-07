# votes_etl.py — with dynamic roll discovery and unique vote_id per roll

import os
import requests
import psycopg2
import xml.etree.ElementTree as ET
import csv
import json
import logging
from datetime import datetime
from dotenv import load_dotenv
from typing import List, Dict

# Load env vars
load_dotenv()

# Logging setup
logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")

# DB Config
DB_CONFIG = {
    "dbname": os.getenv("dbname"),
    "user": os.getenv("user"),
    "password": os.getenv("password"),
    "host": os.getenv("host"),
    "port": os.getenv("port"),
}

# Load mapping
with open("icpsr_to_bioguide_full.json", "r") as f:
    ICPSR_TO_BIOGUIDE = json.load(f)

HOUSE_URL = "https://clerk.house.gov/evs/{year}/roll{roll}.xml"

def db_connection():
    return psycopg2.connect(**DB_CONFIG)

def parse_house_vote(congress: int, session: int, roll: int) -> List[Dict]:
    year = 2023
    url = HOUSE_URL.format(year=year, roll=str(roll).zfill(3))
    logging.info(f"📥 Fetching House vote from {url}")
    resp = requests.get(url)
    if resp.status_code != 200 or not resp.content.strip().startswith(b'<?xml'):
        logging.warning(f"⚠️ Invalid or missing XML at {url}")
        return []

    root = ET.fromstring(resp.content)
    vote_data = []
    try:
        bill_number = root.findtext(".//legis-num")
        vote_desc = root.findtext(".//vote-desc")
        vote_result = root.findtext(".//vote-result")
        question = root.findtext(".//question-text")
        date = datetime.strptime(root.findtext(".//action-date"), "%d-%b-%Y")
    except Exception as e:
        logging.error(f"❌ Failed parsing metadata: {e}")
        return []

    tally = {"Yea": 0, "Nay": 0, "Present": 0, "Not Voting": 0}
    vote_id = f"house-{congress}-{session}-{roll}"

    for record in root.findall(".//recorded-vote"):
        legislator_elem = record.find("legislator")
        bioguide_id = (legislator_elem.attrib.get("bioGuideId") or legislator_elem.attrib.get("name-id", "")).strip().upper()
        position = record.findtext("vote")
        if not bioguide_id or position not in tally:
            continue
        tally[position] += 1
        vote_data.append({
            "vote_id": vote_id,
            "chamber": "House",
            "congress": congress,
            "session": session,
            "roll": roll,
            "bioguide_id": bioguide_id,
            "bill_number": bill_number,
            "question": question,
            "vote_description": vote_desc,
            "vote_result": vote_result,
            "position": position,
            "date": date,
            "tally_yea": tally["Yea"],
            "tally_nay": tally["Nay"],
            "tally_present": tally["Present"],
            "tally_not_voting": tally["Not Voting"],
            "is_key_vote": False
        })
    return vote_data

def vote_exists(cur, vote_id: str, legislator_id: int) -> bool:
    cur.execute("SELECT 1 FROM votes WHERE vote_id = %s AND legislator_id = %s", (vote_id, legislator_id))
    return cur.fetchone() is not None

def insert_votes(votes: List[Dict]):
    conn = db_connection()
    cur = conn.cursor()
    inserted, skipped = 0, 0

    for v in votes:
        try:
            bioguide_id = v["bioguide_id"].strip().upper()
            cur.execute("SELECT id FROM legislators WHERE bioguide_id = %s", (bioguide_id,))
            result = cur.fetchone()
            if not result:
                logging.warning(f"⏭️ No match for BioGuide ID {bioguide_id}")
                skipped += 1
                continue
            legislator_id = result[0]

            if vote_exists(cur, v["vote_id"], legislator_id):
                logging.info(f"⚠️ Duplicate vote_id {v['vote_id']} for legislator {legislator_id} — Skipping")
                skipped += 1
                continue

            cur.execute("""
                INSERT INTO votes (
                    legislator_id, vote_id, bill_number, question_text,
                    vote_description, vote_result, position, date,
                    tally_yea, tally_nay, tally_present, tally_not_voting, is_key_vote
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                legislator_id, v["vote_id"], v["bill_number"], v["question"],
                v["vote_description"], v["vote_result"], v["position"], v["date"],
                v["tally_yea"], v["tally_nay"], v["tally_present"],
                v["tally_not_voting"], v["is_key_vote"]
            ))
            inserted += 1
        except Exception as e:
            logging.error(f"❌ Insert failed for vote {v['vote_id']}: {e}")
            conn.rollback()

    conn.commit()
    cur.close()
    conn.close()
    logging.info(f"✅ Inserted: {inserted} | ⏭️ Skipped: {skipped}")

def run():
    logging.info("🚀 Starting Vote ETL process...")
    all_votes = []
    congress, session = 118, 1
    for roll in range(1, 101):  # Try rolls 1–100
        votes = parse_house_vote(congress, session, roll)
        if votes:
            logging.info(f"📦 Parsed {len(votes)} votes for roll {roll}")
            all_votes.extend(votes)
        else:
            logging.debug(f"❌ No valid data for roll {roll}")
    insert_votes(all_votes)

if __name__ == "__main__":
    run()
