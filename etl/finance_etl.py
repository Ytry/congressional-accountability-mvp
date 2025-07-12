#!/usr/bin/env python3
"""
finance_etl.py — ETL for campaign_finance using OpenSecrets candSummary
"""
import time
import json
import requests
import psycopg2
from contextlib import contextmanager

import config
from logger import setup_logger

# Initialize structured logger
logger = setup_logger("finance_etl")

# ── CONFIG ─────────────────────────────────────────────────────────────────────
DB_URL    = config.DATABASE_URL
DB_NAME   = config.DB_NAME
DB_USER   = config.DB_USER
DB_PASS   = config.DB_PASSWORD
DB_HOST   = config.DB_HOST
DB_PORT   = config.DB_PORT

OPENSECRETS_KEY = config.OPENSECRETS_API_KEY
API_URL_TPL     = "https://www.opensecrets.org/api/?method=candSummary" \
                  "&cid={cid}&cycle={cycle}&apikey={key}&output=json"

# Cycles to fetch (can be overridden via ENV if you extend config)
CYCLES = getattr(config, "FINANCE_CYCLES", [2024, 2022, 2020])

TIMEOUT     = config.HTTP_TIMEOUT
SLEEP_DELAY = config.HTTP_RETRY_DELAY  # throttle between calls

# ── DB CONNECTION ──────────────────────────────────────────────────────────────
@contextmanager
def get_conn():
    """Yield a psycopg2 connection, using DATABASE_URL if present."""
    if DB_URL:
        conn = psycopg2.connect(DB_URL)
    else:
        conn = psycopg2.connect(
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASS,
            host=DB_HOST,
            port=DB_PORT
        )
    try:
        yield conn
    finally:
        conn.close()

# ── HELPERS ─────────────────────────────────────────────────────────────────────
def bioguide_to_id():
    """Map bioguide_id → internal DB legislator_id."""
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute("SELECT bioguide_id, id FROM legislators")
        mapping = dict(cur.fetchall())
    logger.info("Loaded bioguide→id map", extra={"entries": len(mapping)})
    return mapping

def fetch_summary(bioguide: str, cycle: int):
    """Query OpenSecrets for a given bioguide+cycle; return totals or None."""
    url = API_URL_TPL.format(cid=bioguide, cycle=cycle, key=OPENSECRETS_KEY)
    try:
        resp = requests.get(url, timeout=TIMEOUT)
        resp.raise_for_status()
        attrs = resp.json()["response"]["summary"]["@attributes"]
        return {
            "total_raised": attrs.get("total", ""),
            "total_spent":  attrs.get("spent", ""),
            # placeholders until you wire in extra endpoints:
            "top_donors":         [],
            "industry_breakdown": []
        }
    except Exception as e:
        logger.warning("Failed fetch_summary", extra={
            "bioguide": bioguide, "cycle": cycle, "error": str(e)
        })
        return None

def upsert_finance(rows):
    """Bulk upsert into campaign_finance table."""
    with get_conn() as conn, conn.cursor() as cur:
        psycopg2.extras.execute_values(cur, """
            INSERT INTO campaign_finance
              (legislator_id, cycle, total_raised, total_spent,
               top_donors, industry_breakdown)
            VALUES %s
            ON CONFLICT (legislator_id, cycle) DO UPDATE SET
              total_raised=EXCLUDED.total_raised,
              total_spent =EXCLUDED.total_spent,
              top_donors  =EXCLUDED.top_donors,
              industry_breakdown=EXCLUDED.industry_breakdown
        """, rows, page_size=100)
        conn.commit()
    logger.info("Upserted campaign_finance rows", extra={"count": len(rows)})

# ── MAIN ETL ────────────────────────────────────────────────────────────────────
def run():
    logger.info("Starting finance ETL run")
    bmap = bioguide_to_id()

    payloads = []
    for bioguide, leg_id in bmap.items():
        for cyc in CYCLES:
            summary = fetch_summary(bioguide, cyc)
            if summary is None:
                continue
            payloads.append((
                leg_id,
                cyc,
                summary["total_raised"],
                summary["total_spent"],
                json.dumps(summary["top_donors"]),
                json.dumps(summary["industry_breakdown"])
            ))
            time.sleep(SLEEP_DELAY)

    if payloads:
        upsert_finance(payloads)
    else:
        logger.warn("No finance data to upsert")

if __name__ == "__main__":
    run()
