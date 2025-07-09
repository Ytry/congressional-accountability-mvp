# finance_etl.py – OpenSecrets summary into campaign_finance
import os, logging, time, requests, psycopg2, json
from dotenv import load_dotenv
load_dotenv()

DB  = {k: os.getenv(k) for k in ("DB_NAME","DB_USER","DB_PASSWORD","DB_HOST","DB_PORT")}
KEY = os.getenv("OPENSECRETS_API_KEY")
URL = "https://www.opensecrets.org/api/?method=candSummary&cid={cid}&cycle={cyc}&apikey={key}&output=json"

def connect(): return psycopg2.connect(**DB)

def bioguide_to_id():
    with connect() as c, c.cursor() as cur:
        cur.execute("SELECT id, bioguide_id FROM legislators")
        return dict(cur.fetchall())

def upsert(rows):
    with connect() as c, c.cursor() as cur:
        cur.executemany("""
            INSERT INTO campaign_finance
            (legislator_id, cycle, total_raised, total_spent,
             top_donors, industry_breakdown)
            VALUES (%s,%s,%s,%s,%s::jsonb,%s::jsonb)
            ON CONFLICT (legislator_id, cycle)
            DO UPDATE SET total_raised=EXCLUDED.total_raised,
                          total_spent =EXCLUDED.total_spent,
                          top_donors  =EXCLUDED.top_donors,
                          industry_breakdown=EXCLUDED.industry_breakdown
        """, rows); c.commit()

def fetch_summary(cid, cycle):
    url = URL.format(cid=cid, cyc=cycle, key=KEY)
    r = requests.get(url, timeout=20)
    if r.status_code != 200: return None
    attrs = r.json()["response"]["summary"]["@attributes"]
    return {
        "total": attrs["total"],
        "spent": attrs["spent"],
        "top_donors": [],            # TODO extra API calls
        "industry_breakdown": []
    }

def run(cycles=(2024,2022,2020)):
    bmap = bioguide_to_id()
    rows=[]
    for biog,lid in bmap.items():
        for cyc in cycles:
            s = fetch_summary(biog,cyc)
            if not s: continue
            rows.append((lid, cyc, s["total"], s["spent"],
                         json.dumps(s["top_donors"]),
                         json.dumps(s["industry_breakdown"])))
            time.sleep(0.5)
    upsert(rows)

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
    run()
