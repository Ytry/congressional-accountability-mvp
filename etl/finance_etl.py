#!/usr/bin/env python3
"""
finance_etl.py — ETL for campaign_finance using OpenSecrets candSummary
"""
import time
import json

import config
from logger import setup_logger
from utils import (fetch_with_retry, get_cursor, bulk_upsert,
                   fetch_legislator_map)

# Initialize structured logger
logger = setup_logger("finance_etl")

# ── Configuration ────────────────────────────────────────────────────────────
OPENSECRETS_KEY = config.OPENSECRETS_API_KEY
API_URL_TPL     = (
    "https://www.opensecrets.org/api/?method=candSummary"
    "&cid={cid}&cycle={cycle}&apikey={key}&output=json"
).format(key=OPENSECRETS_KEY)
CYCLES          = getattr(config, "FINANCE_CYCLES", [2024, 2022, 2020])
SLEEP_DELAY     = config.HTTP_RETRY_DELAY

# ── Main ETL Function ─────────────────────────────────────────────────────────
def run():
    logger.info("Starting finance ETL run")

    # Build mapping of bioguide_id → internal legislator_id
    bmap = fetch_legislator_map(
        query="SELECT bioguide_id, id FROM legislators"
    )
    if not bmap:
        logger.warning("No legislators found in DB; skipping finance ETL")
        return
    logger.info("Loaded legislator map", extra={"entries": len(bmap)})

    payloads = []
    for bioguide, leg_id in bmap.items():
        for cycle in CYCLES:
            url = API_URL_TPL.format(cid=bioguide, cycle=cycle)
            resp = fetch_with_retry(url)
            if not resp:
                logger.warning("Failed to fetch finance summary", extra={"bioguide": bioguide, "cycle": cycle})
                continue
            try:
                attrs = resp.json()["response"]["summary"]["@attributes"]
                summary = {
                    "total_raised": attrs.get("total", ""),
                    "total_spent":  attrs.get("spent", ""),
                    "top_donors":         [],
                    "industry_breakdown": []
                }
                payloads.append((
                    leg_id,
                    cycle,
                    summary["total_raised"],
                    summary["total_spent"],
                    json.dumps(summary["top_donors"]),
                    json.dumps(summary["industry_breakdown"])
                ))
                logger.debug("Prepared finance payload", extra={"bioguide": bioguide, "cycle": cycle})
            except Exception:
                logger.exception("Error parsing finance summary JSON", extra={"bioguide": bioguide, "cycle": cycle})
            time.sleep(SLEEP_DELAY)

    # Bulk upsert into campaign_finance
    if payloads:
        with get_cursor() as (conn, cur):
            bulk_upsert(
                cur,
                table="campaign_finance",
                rows=payloads,
                columns=[
                    "legislator_id", "cycle", "total_raised",
                    "total_spent", "top_donors", "industry_breakdown"
                ],
                conflict_cols=["legislator_id", "cycle"]
            )
        logger.info("Upserted campaign_finance rows", extra={"count": len(payloads)})
    else:
        logger.warning("No finance data to upsert")


if __name__ == "__main__":
    run()
