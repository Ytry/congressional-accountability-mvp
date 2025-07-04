import requests
import psycopg2
import time

DB_CONFIG = {
    "dbname": "yourdbname",
    "user": "youruser",
    "password": "yourpassword",
    "host": "localhost",
    "port": "5432"
}

API_KEY = "YOUR_OPENSECRETS_API_KEY"
BASE_URL = "https://www.opensecrets.org/api/"

def get_finance_data(bioguide_id_list):
    finance_data = []
    for bioguide_id in bioguide_id_list:
        params = {
            "method": "candSummary",
            "cid": bioguide_id,
            "apikey": API_KEY,
            "output": "json"
        }
        response = requests.get(BASE_URL, params=params)
        if response.status_code == 200:
            try:
                summary = response.json().get("response", {}).get("summary", {}).get("@attributes", {})
                finance_data.append({
                    "bioguide_id": bioguide_id,
                    "total": summary.get("total", "0"),
                    "spent": summary.get("spent", "0"),
                    "cash_on_hand": summary.get("cash_on_hand", "0"),
                    "debt": summary.get("debt", "0"),
                    "last_updated": summary.get("last_updated", "")
                })
            except Exception as e:
                print(f"Error parsing data for {bioguide_id}: {e}")
        time.sleep(1)  # Respect OpenSecrets API rate limits
    return finance_data

def insert_finance_data(data):
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()

    for entry in data:
        cur.execute("""
            INSERT INTO campaign_finance (bioguide_id, total, spent, cash_on_hand, debt, last_updated)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (bioguide_id) DO UPDATE SET
                total = EXCLUDED.total,
                spent = EXCLUDED.spent,
                cash_on_hand = EXCLUDED.cash_on_hand,
                debt = EXCLUDED.debt,
                last_updated = EXCLUDED.last_updated;
        """, (
            entry["bioguide_id"],
            entry["total"],
            entry["spent"],
            entry["cash_on_hand"],
            entry["debt"],
            entry["last_updated"]
        ))

    conn.commit()
    cur.close()
    conn.close()

if __name__ == "__main__":
    bioguide_ids = ["N00007360", "N00030676", "N00033395"]  # Replace with real BioGuide IDs
    finance_data = get_finance_data(bioguide_ids)
    insert_finance_data(finance_data)
