import os
import requests
import psycopg2
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

API_KEY = os.getenv("CONGRESS_API_KEY")
DATABASE_URL = os.getenv("DATABASE_URL")

BASE_URL = "https://api.congress.gov/v3/member"

def fetch_legislators(offset=0):
    url = f"{BASE_URL}?api_key={API_KEY}&offset={offset}"
    response = requests.get(url)
    response.raise_for_status()
    return response.json()

def parse_legislator_data(member):
    return {
        'bioguide_id': member.get('bioguideId'),
        'full_name': member.get('name'),
        'party': member.get('party'),
        'state': member.get('state'),
        'district': member.get('district'),
        'chamber': member.get('chamber'),
        'portrait_url': member.get('urlPhoto'),
        'official_website_url': member.get('url'),
        'address': member.get('office'),
        'phone': member.get('phone'),
        'bio_snapshot': member.get('description')
    }

def insert_legislator(cursor, legislator):
    cursor.execute("""
        INSERT INTO legislators (
            bioguide_id, full_name, party, state, district, chamber,
            portrait_url, official_website_url, address, phone, bio_snapshot
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (bioguide_id) DO UPDATE SET
            full_name = EXCLUDED.full_name,
            party = EXCLUDED.party,
            state = EXCLUDED.state,
            district = EXCLUDED.district,
            chamber = EXCLUDED.chamber,
            portrait_url = EXCLUDED.portrait_url,
            official_website_url = EXCLUDED.official_website_url,
            address = EXCLUDED.address,
            phone = EXCLUDED.phone,
            bio_snapshot = EXCLUDED.bio_snapshot;
    """, (
        legislator['bioguide_id'], legislator['full_name'], legislator['party'],
        legislator['state'], legislator['district'], legislator['chamber'],
        legislator['portrait_url'], legislator['official_website_url'],
        legislator['address'], legislator['phone'], legislator['bio_snapshot']
    ))

def run():
    print("Starting legislators ETL process...")
    conn = psycopg2.connect(DATABASE_URL)
    cursor = conn.cursor()

    offset = 0
    while True:
        data = fetch_legislators(offset)
        members = data.get("members", [])
        if not members:
            break

        for member in members:
            legislator = parse_legislator_data(member)
            insert_legislator(cursor, legislator)

        conn.commit()
        offset += len(members)

    cursor.close()
    conn.close()
    print("ETL complete.")

if __name__ == "__main__":
    run()
