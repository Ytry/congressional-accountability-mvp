# votes_etl.py — Dynamic roll discovery & deduplication

import os, requests, psycopg2, xml.etree.ElementTree as ET, csv, json, logging
from datetime import datetime
from dotenv import load_dotenv
from typing import List, Dict

load_dotenv()
logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")

DB_CONFIG = {
    "dbname": os.getenv("dbname"),
    "user": os.getenv("user"),
    "password": os.getenv("password"),
    "host": os.getenv("host"),
    "port": os.getenv("port"),
}

with open("icpsr_to_bioguide_full.json", "r") as f:
    ICPSR_TO_BIOGUIDE = json.load(f)

HOUSE_URL = "https://clerk.house.gov/evs/2023/roll{roll}.xml"
SENATE_URL = "https://www.senate.gov/legislative/LIS/roll_call_votes/vote{congress}{session}/vote_{congress}_{session}_{roll}.csv"

def db_connection():
    return psycopg2.connect(**DB_CONFIG)

def is_valid_url(url: str, expected_type: str) -> bool:
    try:
        resp = requests.get(url, timeout=5)
        if resp.status_code != 200:
            return False
        if expected_type == "xml" and not resp.content.strip().startswith(b"<?xml"):
            return False
        if expected_type == "csv" and "<!DOCTYPE" in resp.text:
            return False
        return True
    except Exception:
        return False

def parse_house_vote(congress: int, session: int, roll: int) -> List[Dict]:
    url = HOUSE_URL.format(roll=str(roll).zfill(3))
    logging.info(f"📥 Fetching House vote from {url}")
    resp = requests.get(url)
    if resp.status_code != 200 or not resp.content.strip().startswith(b"<?xml"):
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
        leg = record.find("legislator")
        bioguide = (leg.attrib.get("bioGuideId") or leg.attrib.get("name-id", "")).strip().upper()
        pos = record.findtext("vote")
        if not bioguide or pos not in tally:
            continue
        tally[pos] += 1
        vote_data.append({
            "vote_id": vote_id,
            "chamber": "House",
            "congress": congress,
            "session": session,
            "roll": roll,
            "bioguide_id": bioguide,
            "bill_number": bill_number,
            "question": question,
            "vote_description": vote_desc,
            "vote_result": vote_result,
            "position": pos,
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

    reader = csv.DictReader(resp.content.decode().splitlines())
    vote_id = f"senate-{congress}-{session}-{roll}"
    vote_data = []
    tally = {"Yea": 0, "Nay": 0, "Present": 0, "Not Voting": 0}

    for row in reader:
        pos = row.get("Vote") or row.get("Vote Cast")
        if not pos or pos not in tally:
            continue
        tally[pos] += 1
        try:
            date = datetime.strptime(row["Vote Date"], "%m/%d/%Y")
        except:
            continue
        try:
            icpsr = str(int(row.get("ICPSR", "").strip()))
        except:
            continue

        bioguide = ICPSR_TO_BIOGUIDE.get(icpsr)
        if not bioguide:
            continue

        vote_data.append({
            "vote_id": vote_id,
            "chamber": "Senate",
            "congress": congress,
            "session": session,
            "roll": roll,
            "bioguide_id": bioguide,
            "bill_number": row.get("Measure Number"),
            "question": row.get("Vote Question"),
            "vote_description": row.get("Vote Title"),
            "vote_result": row.get("Result"),
            "position": pos,
            "date": date,
            "tally_yea": tally["Yea"],
            "tally_nay": tally["Nay"],
            "tally_present": tally["Present"],
            "tally_not_voting": tally["Not Voting"],
            "is_key_vote": False
        })

    return vote_data

def vote_exists(cur, vote_id, legislator_id) -> bool:
    cur.execute("SELECT 1 FROM votes WHERE vote_id=%s AND legislator_id=%s", (vote_id, legislator_id))
    return cur.fetchone() is not None

def insert_votes(votes: List[Dict]):
    conn = db_connection()
    cur = conn.cursor()
    inserted, skipped = 0, 0

    for v in votes:
        bioguide = v["bioguide_id"].strip().upper()
        cur.execute("SELECT id FROM legislators WHERE bioguide_id = %s", (bioguide,))
        result = cur.fetchone()
        if not result:
            logging.warning(f"⏭️ No match for BioGuide ID {bioguide}")
            skipped += 1
            continue
        legislator_id = result[0]
        if vote_exists(cur, v["vote_id"], legislator_id):
            logging.info(f"⚠️ Duplicate vote_id {v['vote_id']} for legislator {legislator_id} — Skipping")
            skipped += 1
            continue
        try:
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
            logging.error(f"❌ Failed insert: {e}")
            conn.rollback()

    conn.commit()
    cur.close()
    conn.close()
    logging.info(f"✅ Inserted: {inserted} | ⏭️ Skipped: {skipped}")

def run():
    logging.info("🚀 Running Vote ETL")
    all_votes = []
    max_misses = 10
    consecutive_misses = 0
    congress, session = 118, 1

    for roll in range(1, 1000):
        if consecutive_misses >= max_misses:
            logging.info(f"🛑 Stopping after {max_misses} consecutive missing rolls.")
            break

        house_url = HOUSE_URL.format(roll=str(roll).zfill(3))
        senate_url = SENATE_URL.format(congress=congress, session=session, roll=str(roll).zfill(5))

        has_data = False
        if is_valid_url(house_url, "xml"):
            all_votes.extend(parse_house_vote(congress, session, roll))
            has_data = True
        if is_valid_url(senate_url, "csv"):
            all_votes.extend(parse_senate_vote(congress, session, roll))
            has_data = True

        consecutive_misses = 0 if has_data else consecutive_misses + 1

    insert_votes(all_votes)

if __name__ == "__main__":
    run()
