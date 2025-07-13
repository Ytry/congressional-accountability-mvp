#!/usr/bin/env python3
"""
committee_etl.py — ETL for committee assignments via Congress.gov API
"""
import config
from logger import setup_logger
from utils import fetch_with_retry, get_cursor, fetch_legislator_map, bulk_upsert

# Initialize structured logger
logger = setup_logger("committee_etl")

# Template for committee API
COMMITTEE_URL = "https://www.congress.gov/committee/{cong}/{type}/json"


def crawl(congress: int = config.CONGRESS):
    logger.info("Starting committee ETL", extra={"congress": congress})

    # Build mapping bioguide_id -> internal legislator_id
    mapping = fetch_legislator_map(query="SELECT bioguide_id, id FROM legislators")
    if not mapping:
        logger.warning("No legislators found; skipping committee ETL")
        return
    logger.info("Loaded legislator map", extra={"entries": len(mapping)})

    # Process both House and Senate committees
    for chamber in ("house", "senate"):
        url = COMMITTEE_URL.format(cong=congress, type=chamber)
        resp = fetch_with_retry(url)
        if not resp:
            logger.error("Failed to fetch committee data", extra={"url": url, "chamber": chamber})
            continue
        try:
            data = resp.json()
        except Exception:
            logger.exception("Invalid JSON in committee response", extra={"url": url})
            continue

        rows = []
        for committee in data.get("committees", []):
            name = committee.get("name")
            for member in committee.get("members", []):
                biog_id = member.get("bioguideId")
                leg_id = mapping.get(biog_id)
                if not leg_id:
                    logger.debug("Skipping member; unknown bioguide", extra={"bioguide": biog_id})
                    continue
                rows.append((
                    leg_id,
                    congress,
                    name,
                    member.get("subcommitteeName"),
                    member.get("role", "Member")
                ))

        if rows:
            with get_cursor() as (conn, cur):
                bulk_upsert(
                    cur,
                    table="committee_assignments",
                    rows=rows,
                    columns=["legislator_id","congress","committee_name","subcommittee_name","role"],
                    conflict_cols=["legislator_id","congress","committee_name","subcommittee_name"],
                    update_cols=[]
                )
            logger.info("Upserted committee assignments", extra={"chamber": chamber, "rows": len(rows)})
        else:
            logger.info("No committee assignments to upsert", extra={"chamber": chamber})


if __name__ == "__main__":
    crawl()
