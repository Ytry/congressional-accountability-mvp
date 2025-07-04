import requests
import psycopg2
from datetime import datetime

API_KEY = "5fjyzhIQuWMgjz2uInRG5NNAEkI7RkouiEsKElcf"
BASE_URL = "https://api.congress.gov/v3/bill"

# You can change this to pull more results, paginate, or iterate through multiple Congress sessions
PARAMS = {
    "api_key": API_KEY,
    "format": "json",
    "limit": 25
}

DB_CONFIG = {
    "dbname": "yourdbname",
    "user": "youruser",
    "password": "yourpassword",
    "host": "localhost",
    "port": "5432"
}

def fetch_bills():
    print("Fetching bills...")
    response = requests.get(BASE_URL, params=PARAMS)
    response.raise_for_status()
    data = response.json()
    return data.get("bills", [])

def extract_bill_data(bill):
    return {
        "bill_number": bill.get("number"),
        "title": bill.get("title", "No title provided"),
        "status": bill.get("latestAction", {}).get("text", "Unknown"),
        "policy_area": bill.get("policyArea", {}).get("name", "Uncategorized"),
        "date_introduced": bill.get("introducedDate", None),
        "sponsor": bill.get("sponsor", {}).get("bioguideId")
    }

def insert_into_db(bills):
    print(f"Inserting {len(bills)} bills into the database...")
    conn = psycopg2.connect(**DB_CONFIG)
    cursor = conn.cursor()

    for bill in bills:
        try:
            cursor.execute("""
                INSERT INTO sponsored_bills (
                    legislator_id,
                    bill_number,
                    title,
                    status,
                    policy_area,
                    date_introduced
                )
                VALUES (
                    (SELECT id FROM legislators WHERE bioguide_id = %s),
                    %s, %s, %s, %s, %s
                )
                ON CONFLICT (bill_number) DO NOTHING;
            """, (
                bill["sponsor"],
                bill["bill_number"],
                bill["title"],
                bill["status"],
                bill["policy_area"],
                bill["date_introduced"]
            ))
        except Exception as e:
            print(f"Error inserting bill {bill['bill_number']}: {e}")
            continue

    conn.commit()
    cursor.close()
    conn.close()

def main():
    bills_raw = fetch_bills()
    processed_bills = [extract_bill_data(b) for b in bills_raw]
    insert_into_db(processed_bills)
    print("Done.")

if __name__ == "__main__":
    main()
