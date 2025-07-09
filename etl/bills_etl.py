# bills_etl.py – inserts into bill_sponsorships
import os, json, logging, psycopg2, requests
from datetime import datetime
from dotenv import load_dotenv
load_dotenv()

DB = {k: os.getenv(k) for k in ("DB_NAME","DB_USER","DB_PASSWORD","DB_HOST","DB_PORT")}
API = "https://api.congress.gov/v3/member/{biog}/bills?format=json&offset={off}"

def connect(): return psycopg2.connect(**DB)

def get_legislator_ids():
    with connect() as c, c.cursor() as cur:
        cur.execute("SELECT id, bioguide_id FROM legislators")
        return dict(cur.fetchall())         # bioguide → id

def insert_bills(rows):
    with connect() as c, c.cursor() as cur:
        cur.executemany("""
            INSERT INTO bill_sponsorships
            (legislator_id, bill_number, sponsorship_type, title,
             status, policy_area, date_introduced)
            VALUES (%s,%s,%s,%s,%s,%s,%s)
            ON CONFLICT (legislator_id, bill_number, sponsorship_type) DO NOTHING
        """, rows)
        c.commit()

def crawl():
    biog2id = get_legislator_ids()
    for biog, legislator_id in biog2id.items():
        offset = 0
        while True:                # simple pagination loop
            url = API.format(biog=biog, off=offset)
            r = requests.get(url, timeout=20)
            data = r.json()
            bills = data.get("bills", [])
            if not bills: break
            rows = []
            for b in bills:
                rows.append((
                    legislator_id,
                    b["bill"]["number"],
                    "Sponsor" if b["bill"]["sponsor"]["bioguide_id"] == biog else "Cosponsor",
                    b["bill"]["title"],
                    b["bill"]["latestAction"]["status"],
                    b["bill"].get("policyArea", {}).get("name"),
                    datetime.strptime(b["bill"]["introducedDate"], "%Y-%m-%d")
                ))
            insert_bills(rows)
            offset += 250

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
    crawl()
