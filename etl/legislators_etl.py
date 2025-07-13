#!/usr/bin/env python3
"""
legislators_etl.py — ETL for current Congress legislators only,
using shared utils and structured JSON logging
"""
import json
from typing import Optional, List
import config
from logger import setup_logger
from utils import get_cursor, load_yaml_from_url, bulk_upsert

# Initialize structured logger
logger = setup_logger("legislators_etl")

# URL for legislators YAML
CURRENT_URL = config.LEGIS_YAML_URL


def parse_legislator(raw: dict) -> Optional[dict]:
    ids = raw.get("id", {})
    bioguide = ids.get("bioguide")
    terms = raw.get("terms", [])
    if not bioguide or not terms:
        return None

    # select most recent term
    valid = [t for t in terms if t.get("start")]
    valid.sort(key=lambda t: t["start"], reverse=True)
    term = valid[0]

    chamber = (
        "House" if term.get("type") == "rep" else
        "Senate" if term.get("type") == "sen" else
        None
    )
    if not chamber:
        return None

    name = raw.get("name", {})
    first = name.get("first", "")
    last = name.get("last", "")
    full_name = f"{first} {last}".strip()

    bio = raw.get("bio", {})
    birthday = bio.get("birthday", "")
    gender = bio.get("gender", "")
    snapshot = f"{birthday} – {gender}" if (birthday or gender) else ""

    return {
        "bioguide_id": bioguide,
        "icpsr_id": str(ids.get("icpsr")) if ids.get("icpsr") else None,
        "first_name": first,
        "last_name": last,
        "full_name": full_name,
        "gender": gender,
        "birthday": birthday,
        "party": term.get("party"),
        "state": term.get("state"),
        "district": term.get("district") if term.get("type") == "rep" else None,
        "chamber": chamber,
        "portrait_url": f"https://theunitedstates.io/images/congress/450x550/{bioguide}.jpg",
        "official_website_url": term.get("url"),
        "office_contact": term.get("address", {}),
        "bio_snapshot": snapshot,
        "terms": terms,
    }


# ── ETL DRIVER ────────────────────────────────────────────────────────────────

def run():
    logger.info("Starting legislator ETL run")
    try:
        entries = load_yaml_from_url(CURRENT_URL)
        logger.info("YAML data loaded", extra={"records": len(entries)})
    except Exception:
        logger.exception("Failed to load legislators YAML from URL")
        return

    success = skipped = failed = 0
    for raw in entries:
        leg = parse_legislator(raw)
        if not leg:
            skipped += 1
            continue
        bioguide = leg["bioguide_id"]
        try:
            with get_cursor() as (conn, cur):
                # Insert or update core legislator
                cur.execute(
                    """
                    INSERT INTO legislators (
                      bioguide_id, icpsr_id, first_name, last_name, full_name,
                      gender, birthday, party, state, district, chamber,
                      portrait_url, official_website_url, office_contact, bio_snapshot
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
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
                        leg["bioguide_id"], leg["icpsr_id"], leg["first_name"],
                        leg["last_name"],    leg["full_name"], leg["gender"],
                        leg["birthday"],     leg["party"],     leg["state"],
                        leg["district"],     leg["chamber"],   leg["portrait_url"],
                        leg["official_website_url"], json.dumps(leg.get("office_contact", {})),
                        leg["bio_snapshot"]
                    )
                )
                legislator_id = cur.fetchone()[0]

                # Service history
                records = [(
                    legislator_id,
                    t.get("start"),
                    t.get("end"),
                    "House" if t.get("type") == "rep" else "Senate",
                    t.get("state"),
                    t.get("district") if t.get("type") == "rep" else None,
                    t.get("party")
                ) for t in leg["terms"]]
                bulk_upsert(
                    cur,
                    table="service_history",
                    rows=records,
                    columns=["legislator_id","start_date","end_date","chamber","state","district","party"],
                    conflict_cols=["legislator_id","start_date"],
                    update_cols=[]
                )

                # Committee assignments
                records = []
                for t in leg["terms"]:
                    for c in t.get("committees", []):
                        records.append((
                            legislator_id,
                            t.get("congress"),
                            c.get("name"),
                            c.get("subcommittee"),
                            c.get("position", "Member")
                        ))
                bulk_upsert(
                    cur,
                    table="committee_assignments",
                    rows=records,
                    columns=["legislator_id","congress","committee_name","subcommittee_name","role"],
                    conflict_cols=["legislator_id","congress","committee_name","subcommittee_name"],
                    update_cols=[]
                )

                # Leadership roles
                records = [
                    (legislator_id, t.get("congress"), t.get("leadership_title"))
                    for t in leg["terms"] if t.get("leadership_title")
                ]
                bulk_upsert(
                    cur,
                    table="leadership_roles",
                    rows=records,
                    columns=["legislator_id","congress","role"],
                    conflict_cols=["legislator_id","congress","role"],
                    update_cols=[]
                )

            logger.info("Legislator processed successfully", extra={"bioguide_id": bioguide})
            success += 1
        except Exception:
            logger.exception("Failed processing legislator", extra={"bioguide_id": bioguide})
            failed += 1

    logger.info("ETL summary complete", extra={"inserted": success, "skipped": skipped, "failed": failed})


if __name__ == "__main__":
    run()
