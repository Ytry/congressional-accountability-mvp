
import requests
import xml.etree.ElementTree as ET
import csv
import psycopg2
from datetime import datetime

DB_CONFIG = {
    "dbname": "yourdbname",
    "user": "youruser",
    "password": "yourpassword",
    "host": "localhost",
    "port": "5432"
}

HOUSE_BASE_URL = "https://clerk.house.gov/evs/{year}/roll{roll}.xml"
SENATE_BASE_URL = "https://www.senate.gov/legislative/LIS/roll_call_votes/vote{congress}{session}/vote_{congress}_{session}_{roll}.csv"

def db_connection():
    return psycopg2.connect(**DB_CONFIG)

def parse_house_vote(congress, session, roll):
    url = HOUSE_BASE_URL.format(year=2023, roll=str(roll).zfill(3))  # Replace 2023 dynamically if needed
    print(f"Fetching {url}")
    resp = requests.get(url)
    if resp.status_code != 200:
        print(f"Failed to fetch {url}")
        return []

    root = ET.fromstring(resp.content)
    vote_data = []

    bill_number = root.findtext(".//legis-num")
    vote_desc = root.findtext(".//vote-desc")
    vote_result = root.findtext(".//vote-result")
    date = root.findtext(".//action-date")
    question = root.findtext(".//question-text")

    tally = {
        "Yea": 0,
        "Nay": 0,
        "Present": 0,
        "Not Voting": 0
    }

    for record in root.findall(".//recorded-vote"):
        bioguide_id = record.findtext("legislator")
        position = record.findtext("vote")
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
            "date": datetime.strptime(date, "%Y-%m-%d"),
            "tally_yea": tally["Yea"],
            "tally_nay": tally["Nay"],
            "tally_present": tally["Present"],
            "tally_not_voting": tally["Not Voting"],
            "is_key_vote": False  # Enhance later
        })

    return vote_data

def parse_senate_vote(congress, session, roll):
    url = SENATE_BASE_URL.format(congress=congress, session=session, roll=str(roll).zfill(5))
    print(f"Fetching {url}")
    resp = requests.get(url)
    if resp.status_code != 200:
        print(f"Failed to fetch {url}")
        return []

    lines = resp.content.decode("utf-8").splitlines()
    reader = csv.DictReader(lines)
    vote_data = []
    tally = {
        "Yea": 0,
        "Nay": 0,
        "Present": 0,
        "Not Voting": 0
    }

    for row in reader:
        position = row["Vote"]
        tally[position] += 1
        vote_data.append({
            "vote_id": f"senate-{congress}-{session}-{roll}",
            "chamber": "Senate",
            "congress": congress,
            "session": session,
            "roll": roll,
            "bioguide_id": row["ICPSR"],
            "bill_number": row["Measure Number"],
            "question": row["Vote Question"],
            "vote_description": row["Vote Title"],
            "vote_result": row["Result"],
            "position": position,
            "date": datetime.strptime(row["Vote Date"], "%m/%d/%Y"),
            "tally_yea": tally["Yea"],
            "tally_nay": tally["Nay"],
            "tally_present": tally["Present"],
            "tally_not_voting": tally["Not Voting"],
            "is_key_vote": False
        })

    return vote_data

def insert_votes(vote_records):
    conn = db_connection()
    cur = conn.cursor()
    for v in vote_records:
        cur.execute("""
            SELECT id FROM legislators WHERE bioguide_id = %s
        """, (v["bioguide_id"],))
        legislator = cur.fetchone()
        if not legislator:
            print(f"No match for BioGuide ID {v['bioguide_id']}, skipping.")
            continue
        legislator_id = legislator[0]

        cur.execute("""
            INSERT INTO votes (legislator_id, vote_id, bill_number, question_text, vote_description, vote_result,
                position, date, tally_yea, tally_nay, tally_present, tally_not_voting, is_key_vote)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT DO NOTHING
        """, (
            legislator_id, v["vote_id"], v["bill_number"], v["question"], v["vote_description"],
            v["vote_result"], v["position"], v["date"], v["tally_yea"], v["tally_nay"],
            v["tally_present"], v["tally_not_voting"], v["is_key_vote"]
        ))

    conn.commit()
    cur.close()
    conn.close()

if __name__ == "__main__":
    all_votes = []
    all_votes.extend(parse_house_vote(congress=118, session=1, roll=1))  # Example: HR 1 vote
    all_votes.extend(parse_senate_vote(congress=118, session=1, roll=1))
    insert_votes(all_votes)
