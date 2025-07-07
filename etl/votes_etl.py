# votes_etl.py — Corrected Version

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

# Load environment variables
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

# Load ICPSR to BioGuide map
with open("icpsr_to_bioguide_full.json", "r") as f:
    ICPSR_TO_BIOGUIDE = json.load(f)

HOUSE_URL = "https://clerk.house.gov/evs/{year}/roll{roll}.xml"
SENATE_URL = "https://www.senate.gov/legislative/LIS/roll_call_votes/vote{congress}{session}/vote_{congress}_{session}_{roll}.csv"

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
    for record in root.findall(".//recorded-vote"):
        bioguide_id = record.findtext("legislator")
        position = record.findtext("vote")
        if position not in tally:
            continue
        tally[position] += 1

        vote_data.append({
            "vote_id": f"house-{congress}-{session}-{roll}",
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

def parse_senate_vote(congress: int, session: int, roll: int) -> List[Dict]:
    url = SENATE_URL.format(congress=congress, session=session, roll=str(roll).zfill(5))
    logging.info(f"📥 Fetching Senate vote from {url}")
    resp = requests.get(url)
    if resp.status_code != 200 or "<!DOCTYPE" in resp.text:
        logging.warning(f"⚠️ Invalid or HTML content at {url}")
        return []

    vote_data = []
    lines = resp.content.decode("utf-8").splitlines()
    reader = csv.DictReader(lines)

    tally = {"Yea": 0, "Nay": 0, "Present": 0, "Not Voting": 0}
    for row in reader:
        position = row.get("Vote") or row.get("Vote Cast")
        if not position or position not in tally:
            continue
        tally[position] += 1

        try:
            date = datetime.strptime(row["Vote Date"], "%m/%d/%Y")
        except Exception:
            continue

        icpsr = str(int(row.get("ICPSR", "0").strip()))
        bioguide_id = ICPSR_TO_BIOGUIDE.get(icpsr)
        if not bioguide_id:
            logging.warning(f"⚠️ No match for ICPSR {icpsr}, skipping.")
            continue

        vote_data.append({
            "vote_id": f"senate-{congress}-{session}-{roll}",
            "chamber": "Senate",
            "congress": congress,
            "session": session,
            "roll": roll,
            "bioguide_id": bioguide_id,
            "bill_number": row.get("Measure Number"),
            "question": row.get("Vote Question"),
            "vote_description": row.get("Vote Title"),
            "vote_result": row.get("Result"),
            "position": position,
            "date": date,
            "tally_yea": tally["Yea"],
            "tally_nay": tally["Nay"],
            "tally_present": tally["Present"],
            "tally_not_voting": tally["Not Voting"],
            "is_key_vote": False
        })
    return vote_data

def insert_votes(votes: List[Dict]):
    conn = db_connection()
    cur = conn.cursor()
    inserted, skipped = 0, 0

    for v in votes:
        try:
            cur.execute("SELECT id FROM legislators WHERE LOWER(bioguide_id) = LOWER(%s)", (v["bioguide_id"],))
            legislator = cur.fetchone()
            if not legislator:
                logging.warning(f"⏭️ No match for BioGuide ID {v['bioguide_id']}, skipping.")
                skipped += 1
                continue

            legislator_id = legislator[0]
            cur.execute("""
                INSERT INTO votes (
                    legislator_id, vote_id, bill_number, question_text,
                    vote_description, vote_result, position, date,
                    tally_yea, tally_nay, tally_present, tally_not_voting, is_key_vote
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT DO NOTHING;
            """, (
                legislator_id, v["vote_id"], v["bill_number"], v["question"],
                v["vote_description"], v["vote_result"], v["position"], v["date"],
                v["tally_yea"], v["tally_nay"], v["tally_present"],
                v["tally_not_voting"], v["is_key_vote"]
            ))
            inserted += 1
        except Exception as e:
            logging.error(f"❌ Failed to insert vote {v['vote_id']}: {e}")
            conn.rollback()

    conn.commit()
    cur.close()
    conn.close()
    logging.info("📊 Vote ETL Summary")
    logging.info(f"✅ Inserted: {inserted}")
    logging.info(f"⏭️ Skipped: {skipped}")
    logging.info("🏁 Vote ETL process complete.")

def run():
    logging.info("🚀 Starting Vote ETL process...")
    all_votes = []
    rolls = [(118, 1, 1), (118, 1, 2)]
    for congress, session, roll in rolls:
        all_votes.extend(parse_house_vote(congress, session, roll))
        all_votes.extend(parse_senate_vote(congress, session, roll))
    insert_votes(all_votes)

if __name__ == "__main__":
    run()
