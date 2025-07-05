import requests
import psycopg2
import json
import os
import urllib3

DB_NAME = os.getenv("dbname")
DB_USER = os.getenv("user")
DB_PASSWORD = os.getenv("password")
DB_HOST = os.getenv("host")
DB_PORT = os.getenv("port")

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


DATA_SOURCE_URL = "https://raw.githubusercontent.com/unitedstates/congress-legislators/main/legislators-current.json"

def connect():
    return psycopg2.connect(
        dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD,
        host=DB_HOST, port=DB_PORT
    )

def extract_legislators():
    res = requests.get(DATA_SOURCE_URL, verify=False)
    res.raise_for_status()
    return res.json()

def parse_legislator(raw):
    bioguide_id = raw["id"].get("bioguide", None)
    full_name = f"{raw['name'].get('first', '')} {raw['name'].get('last', '')}"
    party = raw["terms"][-1].get("party", "")[0]
    chamber = raw["terms"][-1].get("type", "").capitalize()  # 'house' → 'House'
    state = raw["terms"][-1].get("state", "")
    district = raw["terms"][-1].get("district", None) if chamber == "House" else None
    portrait_url = f"https://theunitedstates.io/images/congress/450x550/{bioguide_id}.jpg"
    website = raw["terms"][-1].get("url", None)
    
    contact = {
        "address": raw["terms"][-1].get("address", None),
        "phone": raw["terms"][-1].get("phone", None)
    }

    bio_snapshot = f"{raw['bio'].get('birthday', '')} – {raw['bio'].get('gender', '')}"

    return {
        "bioguide_id": bioguide_id,
        "full_name": full_name,
        "party": party,
        "chamber": chamber,
        "state": state,
        "district": district,
        "portrait_url": portrait_url,
        "official_website_url": website,
        "office_contact": contact,
        "bio_snapshot": bio_snapshot,
        "terms": raw["terms"]
    }

def insert_legislator(cur, leg):
    cur.execute("""
        INSERT INTO legislators (
            bioguide_id, full_name, party, chamber, state, district,
            portrait_url, official_website_url, office_contact, bio_snapshot
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb, %s)
        ON CONFLICT (bioguide_id) DO UPDATE SET
            full_name = EXCLUDED.full_name,
            party = EXCLUDED.party,
            chamber = EXCLUDED.chamber,
            state = EXCLUDED.state,
            district = EXCLUDED.district,
            portrait_url = EXCLUDED.portrait_url,
            official_website_url = EXCLUDED.official_website_url,
            office_contact = EXCLUDED.office_contact,
            bio_snapshot = EXCLUDED.bio_snapshot
        RETURNING id;
    """, (
        leg["bioguide_id"], leg["full_name"], leg["party"], leg["chamber"],
        leg["state"], leg["district"], leg["portrait_url"], leg["official_website_url"],
        json.dumps(leg["office_contact"]), leg["bio_snapshot"]
    ))
    return cur.fetchone()[0]

def insert_service_history(cur, legislator_id, terms):
    for term in terms:
        start = term.get("start")
        end = term.get("end")
        cur.execute("""
            INSERT INTO service_history (legislator_id, term_start, term_end)
            VALUES (%s, %s, %s)
            ON CONFLICT (legislator_id, term_start) DO NOTHING;
        """, (legislator_id, start, end))

def insert_committee_roles(cur, legislator_id, terms):
    for term in terms:
        if "committees" in term:
            congress = term.get("congress", None)
            for committee in term["committees"]:
                cur.execute("""
                    INSERT INTO committee_assignments (
                        legislator_id, congress, committee_name,
                        subcommittee_name, role
                    )
                    VALUES (%s, %s, %s, %s, %s)
                    ON CONFLICT DO NOTHING;
                """, (
                    legislator_id,
                    congress,
                    committee.get("name"),
                    committee.get("subcommittee", None),
                    committee.get("position", "Member")
                ))

def insert_leadership_roles(cur, legislator_id, terms):
    for term in terms:
        congress = term.get("congress")
        role = term.get("leadership_title")
        if role:
            cur.execute("""
                INSERT INTO leadership_roles (legislator_id, congress, role)
                VALUES (%s, %s, %s)
                ON CONFLICT (legislator_id, congress, role) DO NOTHING;
            """, (legislator_id, congress, role))

def run():
    conn = connect()
    cur = conn.cursor()

    legislators_raw = extract_legislators()
    for raw in legislators_raw:
        try:
            leg = parse_legislator(raw)
            legislator_id = insert_legislator(cur, leg)
            insert_service_history(cur, legislator_id, leg["terms"])
            insert_committee_roles(cur, legislator_id, leg["terms"])
            insert_leadership_roles(cur, legislator_id, leg["terms"])
        except Exception as e:
            print(f"Error processing {raw['id'].get('bioguide')}: {e}")
            conn.rollback()
        else:
            conn.commit()

    cur.close()
    conn.close()

if __name__ == "__main__":
    run()
