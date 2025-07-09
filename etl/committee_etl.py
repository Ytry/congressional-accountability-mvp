# committee_etl.py – scrapes congress.gov committee JSON
import os, logging, requests, psycopg2
from dotenv import load_dotenv
load_dotenv()

DB = {k: os.getenv(k) for k in ("DB_NAME","DB_USER","DB_PASSWORD","DB_HOST","DB_PORT")}
COMMITTEE_URL = "https://www.congress.gov/committee/{cong}/{type}/json"

def connect(): return psycopg2.connect(**DB)

def get_legislator_ids():
    with connect() as c, c.cursor() as cur:
        cur.execute("SELECT id, bioguide_id FROM legislators")
        return dict(cur.fetchall())

def upsert(rows):
    with connect() as c, c.cursor() as cur:
        cur.executemany("""
            INSERT INTO committee_assignments
            (legislator_id, congress, committee_name,
             subcommittee_name, role)
            VALUES (%s,%s,%s,%s,%s)
            ON CONFLICT DO NOTHING
        """, rows); c.commit()

def crawl(congress=118):
    biog2id = get_legislator_ids()
    for typ in ("house","senate"):
        data = requests.get(COMMITTEE_URL.format(cong=congress, type=typ)).json()
        for committee in data["committees"]:
            cname = committee["name"]
            for mem in committee["members"]:
                lid = biog2id.get(mem["bioguideId"])
                if not lid: continue
                rows = [(lid, congress, cname,
                         mem.get("subcommitteeName"), mem.get("role","Member"))]
                upsert(rows)

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
    crawl()
