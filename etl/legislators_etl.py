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
# URL for committee membership (align with blueprint for comprehensive assignments)
COMMITTEE_URL = config.COMMITTEE_YAML_URL  # e.g., "https://raw.githubusercontent.com/unitedstates/congress-legislators/main/committee-membership-current.yaml"
# URL for committees master (for names)
COMMITTEES_MASTER_URL = config.COMMITTEES_MASTER_YAML_URL  # e.g., "https://raw.githubusercontent.com/unitedstates/congress-legislators/main/committees-current.yaml"

def compute_congress_from_date(date_str: str) -> Optional[int]:
    """
    Compute Congress number from a date string (YYYY-MM-DD).
    Congress starts Jan 3 of odd years; e.g., 118th: 2023-01-03 to 2025-01-03.
    """
    try:
        year = int(date_str[:4])
        # Formula: Congress = ((year - 1789) // 2) + 1, but adjust for start.
        # Simplified: For year Y, if Y odd, Congress (Y+1)/2 + 88 (since 1st=1789-1791).
        base_year = 1789
        congress = ((year - base_year) // 2) + 1
        # Adjust if before Jan 3: belongs to prior Congress
        month_day = date_str[5:]
        if month_day < "01-03":
            congress -= 1
        return congress
    except Exception:
        return None

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

    # Build proper office_contact dict (align with blueprint Object: address, phone, etc.)
    office_contact = {
        "address": term.get("address", ""),
        "phone": term.get("phone", ""),
        "fax": term.get("fax", ""),
        "contact_form": term.get("contact_form", ""),
        "office": term.get("office", "")
    }

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
        "office_contact": office_contact,
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

    # Load committee membership YAML (to align with blueprint for assignments)
    try:
        committee_data = load_yaml_from_url(COMMITTEE_URL)
        logger.info("Committee membership YAML loaded", extra={"committees": len(committee_data)})
    except Exception:
        logger.exception("Failed to load committee membership YAML from URL")
        committee_data = {}  # Proceed without, but log

    # Load committees master YAML for names
    try:
        committees_master = load_yaml_from_url(COMMITTEES_MASTER_URL)
        logger.info("Committees master YAML loaded", extra={"committees": len(committees_master)})
    except Exception:
        logger.exception("Failed to load committees master YAML from URL")
        committees_master = []  # Proceed without

    # Build committee name maps
    parent_code_to_name = {}
    sub_code_to_name = {}
    for c in committees_master:
        if 'thomas_id' in c:
            parent_code_to_name[c['thomas_id']] = c.get('name', c['thomas_id'])
        if 'subcommittees' in c:
            for sub in c['subcommittees']:
                full_sub_code = c['thomas_id'] + sub['thomas_id']
                sub_code_to_name[full_sub_code] = sub.get('name', full_sub_code)

    # Invert committee_data to bioguide: list of assignments
    bioguide_to_committees = {}
    for comm_id, members in committee_data.items():
        for m in members:
            bg = m.get('bioguide')
            if not bg:
                continue
            if bg not in bioguide_to_committees:
                bioguide_to_committees[bg] = []
            if len(comm_id) == 4:
                committee_name = parent_code_to_name.get(comm_id, comm_id)
                subcommittee_name = None
            else:
                parent_id = comm_id[:4]
                committee_name = parent_code_to_name.get(parent_id, parent_id)
                subcommittee_name = sub_code_to_name.get(comm_id, comm_id)
            role = m.get('title', 'Member').capitalize()
            bioguide_to_committees[bg].append({
                'committee_name': committee_name,
                'subcommittee_name': subcommittee_name,
                'role': role
            })

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

                # Committee assignments (from inverted bioguide_to_committees; current congress only)
                records = []
                if bioguide in bioguide_to_committees:
                    current_congress = compute_congress_from_date(leg["terms"][0]["start"])
                    for c in bioguide_to_committees[bioguide]:
                        records.append((
                            legislator_id,
                            current_congress,
                            c['committee_name'],
                            c['subcommittee_name'],
                            c['role']
                        ))
                bulk_upsert(
                    cur,
                    table="committee_assignments",
                    rows=records,
                    columns=["legislator_id","congress","committee_name","subcommittee_name","role"],
                    conflict_cols=["legislator_id","congress","committee_name","subcommittee_name"],
                    update_cols=[]
                )

                # Leadership roles (fix key to leadership_role, compute congress)
                records = []
                for t in leg["terms"]:
                    role = t.get("leadership_role")
                    if role:
                        congress = compute_congress_from_date(t["start"])
                        if congress:
                            records.append((legislator_id, congress, role))
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
