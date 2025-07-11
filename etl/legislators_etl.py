#!/usr/bin/env python3
# legislators_etl_118.py — ETL for current Congress legislators only, with connection pooling and context managers

import os
import json
import logging
from typing import Optional, List

import requests
import yaml
import psycopg2
import psycopg2.extras
from psycopg2.pool import ThreadedConnectionPool
from contextlib import contextmanager
from dotenv import load_dotenv

# ── CONFIG & LOGGING ─────────────────────────────────────────────────────────────
load_dotenv()
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

# Database credentials from environment
DB = {
    "dbname":   os.getenv("DB_NAME"),
    "user":     os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
    "host":     os.getenv("DB_HOST"),
    "port":     os.getenv("DB_PORT"),
}

# ── CONNECTION POOL ──────────────────────────────────────────────────────────────
conn_pool = ThreadedConnectionPool(minconn=1, maxconn=5, **DB)

@contextmanager
def get_conn():
    conn = conn_pool.getconn()
    try:
        yield conn
    finally:
        conn_pool.putconn(conn)

@contextmanager
def get_cursor():
    with get_conn() as conn:
        with conn.cursor() as cur:
            yield conn, cur

# ── SOURCE: CURRENT MEMBERS ──────────────────────────────────────────────────────
CURRENT_URL = (
    "https://raw.githubusercontent.com/unitedstates/congress-legislators/"
    "main/legislators-current.yaml"
)

# ── FETCH & PARSE ───────────────────────────────────────────────────────────────
def fetch_yaml_data(url: str) -> List[dict]:
    logging.info(f"📥 Downloading YAML from: {url}")
    resp = requests.get(url, timeout=10)
    resp.raise_for_status()
    return yaml.safe_load(resp.text)


def parse_legislator(raw) -> Optional[dict]:
    ids = raw.get("id") or {}
    bioguide = ids.get("bioguide")
    terms = raw.get("terms") or []
    if not bioguide or not terms:
        return None

    # Select most recent term by start date
    valid_terms = [t for t in terms if t.get("start")]
    valid_terms.sort(key=lambda t: t["start"], reverse=True)
    current_term = valid_terms[0]

    chamber = (
        "House" if current_term.get("type") == "rep"
        else "Senate" if current_term.get("type") == "sen"
        else None
    )
    if not chamber:
        return None

    name = raw.get("name") or {}
    first = name.get("first", "")
    last_name = name.get("last", "")
    full_name = f"{first} {last_name}".strip()

    bio = raw.get("bio") or {}
    birthday = bio.get("birthday", "")
    gender = bio.get("gender", "")
    bio_snip = f"{birthday} – {gender}" if (birthday or gender) else ""

    contact = {"address": current_term.get("address"), "phone": current_term.get("phone")}

    return {
        "bioguide_id":          bioguide,
        "icpsr_id":             str(ids.get("icpsr")) if ids.get("icpsr") else None,
        "first_name":           first,
        "last_name":            last_name,
        "full_name":            full_name,
        "gender":               gender,
        "birthday":             birthday,
        "party":                current_term.get("party"),
        "state":                current_term.get("state"),
        "district":             current_term.get("district") if current_term.get("type") == "rep" else None,
        "chamber":              chamber,
        "portrait_url":         f"https://theunitedstates.io/images/congress/450x550/{bioguide}.jpg",
        "official_website_url": current_term.get("url"),
        "office_contact":       contact,
        "bio_snapshot":         bio_snip,
        "terms":                terms,
    }

# ── INSERT FUNCTIONS ────────────────────────────────────────────────────────────
def insert_legislator(cur, leg):
    # INSERT or UPDATE, then RETURN primary key
    cur.execute(
        """
        INSERT INTO legislators (
            bioguide_id, icpsr_id, first_name, last_name, full_name,
            gender, birthday, party, state, district, chamber,
            portrait_url, official_website_url, office_contact, bio_snapshot
        ) VALUES (
            %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s, %s,
            %s, %s, %s::jsonb, %s
        )
        ON CONFLICT (bioguide_id) DO UPDATE SET
            icpsr_id = EXCLUDED.icpsr_id,
            first_name = EXCLUDED.first_name,
            last_name = EXCLUDED.last_name,
            full_name = EXCLUDED.full_name,
            gender = EXCLUDED.gender,
            birthday = EXCLUDED.birthday,
            party = EXCLUDED.party,
            state = EXCLUDED.state,
            district = EXCLUDED.district,
            chamber = EXCLUDED.chamber,
            portrait_url = EXCLUDED.portrait_url,
            official_website_url = EXCLUDED.official_website_url,
            office_contact = EXCLUDED.office_contact,
            bio_snapshot = EXCLUDED.bio_snapshot
        RETURNING id;
        """,
        (
            leg["bioguide_id"], leg["icpsr_id"], leg["first_name"], leg["last_name"],
            leg["full_name"], leg["gender"], leg["birthday"], leg["party"], leg["state"],
            leg["district"], leg["chamber"], leg["portrait_url"], leg["official_website_url"],
            json.dumps(leg["office_contact"]), leg["bio_snapshot"]
        )
    )
    # fetchone() will now always return the id
    return cur.fetchone()[0]


def insert_service_history(cur, legislator_id, terms):
    records = []
    for t in terms:
        records.append((
            legislator_id,
            t.get("start"),
            t.get("end"),
            "House" if t.get("type") == "rep" else "Senate",
            t.get("state"),
            t.get("district") if t.get("type") == "rep" else None,
            t.get("party")
        ))
    psycopg2.extras.execute_values(
        cur,
        """
        INSERT INTO service_history (
            legislator_id, start_date, end_date,
            chamber, state, district, party
        ) VALUES %s
        ON CONFLICT (legislator_id, start_date) DO NOTHING;
        """,
        records,
        page_size=100
    )


def insert_committee_roles(cur, legislator_id, terms):
    records = []
    for t in terms:
        for c in t.get("committees", []):
            records.append((
                legislator_id,
                t.get("congress"),
                c.get("name"),
                c.get("subcommittee"),
                c.get("position", "Member")
            ))
    psycopg2.extras.execute_values(
        cur,
        """
        INSERT INTO committee_assignments (
            legislator_id, congress,
            committee_name, subcommittee_name, role
        ) VALUES %s
        ON CONFLICT (legislator_id, congress, committee_name, subcommittee_name) DO NOTHING;
        """,
        records,
        page_size=100
    )


def insert_leadership_roles(cur, legislator_id, terms):
    records = []
    for t in terms:
        role = t.get("leadership_title")
        if role:
            records.append((legislator_id, t.get("congress"), role))
    if records:
        psycopg2.extras.execute_values(
            cur,
            """
            INSERT INTO leadership_roles (
                legislator_id, congress, role
            ) VALUES %s
            ON CONFLICT (legislator_id, congress, role) DO NOTHING;
            """,
            records,
            page_size=100
        )

# ── ETL DRIVER ──────────────────────────────────────────────────────────────────
def run():
    logging.info("🚀 Starting legislator ETL (current roster)...")
    try:
        entries = fetch_yaml_data(CURRENT_URL)
        logging.info(f"ℹ️ {len(entries)} current legislators fetched.")
    except Exception as e:
        logging.critical(f"❌ Startup failure: {e}")
        return

    success = skipped = failed = 0
    with get_cursor() as (conn, cur):
        for raw in entries:
            leg = parse_legislator(raw)
            if not leg:
                skipped += 1
                continue
            try:
                lid = insert_legislator(cur, leg)
                insert_service_history(cur, lid, leg["terms"])
                insert_committee_roles(cur, lid, leg["terms"])
                insert_leadership_roles(cur, lid, leg["terms"])
                conn.commit()
                logging.info(f"✅ {leg['bioguide_id']} ({leg['full_name']})")
                success += 1
            except Exception as e:
                logging.error(f"❌Failed for {leg['bioguide_id']}: {e}")
                conn.rollback()
                failed += 1

    conn_pool.closeall()
    logging.info("🏁 ETL Summary")
    logging.info(f"✅Inserted: {success}")
    logging.info(f"⏭️Skipped: {skipped}")
    logging.info(f"❌Failed: {failed}")

if __name__ == "__main__":
    run()
