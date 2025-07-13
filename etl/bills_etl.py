#!/usr/bin/env python3
"""
bills_etl.py — ETL for bill_sponsorships via Congress.gov API with structured logging
"""
import json
from datetime import datetime

import config
from logger import setup_logger
from utils import fetch_with_retry, get_cursor, bulk_upsert, fetch_legislator_map

# Initialize structured logger
logger = setup_logger("bills_etl")

# API and pagination settings
API_URL_TPL = "https://api.congress.gov/v3/member/{biog}/bills?format=json&offset={off}"
PAGE_SIZE   = 250
TIMEOUT     = config.HTTP_TIMEOUT


def run():
    logger.info("Starting bills ETL run")

    # Map bioguide_id -> internal legislator_id
    biog2id = fetch_legislator_map(query="SELECT bioguide_id, id FROM legislators")
    if not biog2id:
        logger.warning("No legislators found; skipping bills ETL")
        return
    logger.info("Loaded legislator map", extra={"entries": len(biog2id)})

    for biog, legislator_id in biog2id.items():
        offset = 0
        while True:
            url = API_URL_TPL.format(biog=biog, off=offset)
            resp = fetch_with_retry(url, timeout=TIMEOUT)
            if not resp:
                logger.warning("Failed to fetch bills", extra={"bioguide": biog, "offset": offset})
                break
            try:
                data = resp.json()
            except Exception:
                logger.exception("Invalid JSON response for bills", extra={"url": url})
                break

            bills = data.get("bills", [])
            if not bills:
                break

            rows = []
            for b in bills:
                sponsor = b["bill"]["sponsor"].get("bioguide_id")
                sponsorship_type = "Sponsor" if sponsor == biog else "Cosponsor"
                rows.append((
                    legislator_id,
                    b["bill"]["number"],
                    sponsorship_type,
                    b["bill"]["title"],
                    b["bill"]["latestAction"]["status"],
                    b["bill"].get("policyArea", {}).get("name"),
                    datetime.strptime(b["bill"]["introducedDate"], "%Y-%m-%d")
                ))

            # Bulk upsert into bill_sponsorships
            with get_cursor() as (conn, cur):
                bulk_upsert(
                    cur,
                    table="bill_sponsorships",
                    rows=rows,
                    columns=[
                        "legislator_id", "bill_number", "sponsorship_type",
                        "title", "status", "policy_area", "date_introduced"
                    ],
                    conflict_cols=["legislator_id", "bill_number", "sponsorship_type"],
                    update_cols=[]
                )
            logger.info(
                "Upserted bills for legislator",
                extra={"bioguide": biog, "count": len(rows)}
            )

            offset += PAGE_SIZE


if __name__ == "__main__":
    run()
