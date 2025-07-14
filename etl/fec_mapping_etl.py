#!/usr/bin/env python3
"""
fec_mapping_etl.py — ETL for mapping FEC candidates to bioguide IDs
with batched summary warnings, enriched name matching, and manual overrides
"""
import config
import utils
from logger import setup_logger
from datetime import datetime
from collections import Counter
import difflib

logger = setup_logger("fec_mapping_etl")

# FEC search endpoint template
SEARCH_ENDPOINT = f"{config.FEC_BASE_URL}/candidates/search/"

# Election cycles and offices
CYCLES = [2020, 2022, 2024]
OFFICES = ["H", "S"]

# Bulk upsert settings
TABLE = "fec_candidates"
COLUMNS = [
    "fec_id", "bioguide_id", "name", "office",
    "state", "district", "cycle", "last_updated"
]
CONFLICT_COLS = ["fec_id", "cycle"]

# Manual overrides: FEC ID -> bioguide
OVERRIDES = getattr(config, 'FEC_MANUAL_OVERRIDES', {})


def generate_name_variants(fec_name: str) -> list:
    """
    Generate name variants: Last, First Middle and swaps;
    strip suffixes and punctuation for matching.
    """
    variants = []
    if not fec_name:
        return variants
    raw = fec_name.strip()
    lower = raw.lower()
    # base forms
    variants.append(lower)
    # swap Last, First Middle -> First Middle Last
    if "," in raw:
        last, first = raw.split(",", 1)
        variants.append(f"{first.strip()} {last.strip()}".lower())
    # strip punctuation
    variants.append(lower.replace(',', '').replace('.', ''))
    # strip common suffixes
    for suffix in [' jr', ' sr', ' ii', ' iii', ' iv']:
        if lower.endswith(suffix):
            variants.append(lower[: -len(suffix)])
    # dedupe
    seen = set()
    out = []
    for v in variants:
        if v and v not in seen:
            seen.add(v)
            out.append(v)
    return out


def fetch_candidates(cycle: int, office: str) -> list:
    """Page through FEC candidates, logging fetch details."""
    candidates, page = [], 1
    while True:
        url = (
            f"{SEARCH_ENDPOINT}?api_key={config.FEC_API_KEY}"
            f"&cycle={cycle}&office={office}"
            f"&per_page={config.FEC_PAGE_SIZE}&page={page}"
        )
        logger.debug("Requesting FEC candidates", extra={"url": url, "cycle": cycle, "office": office, "page": page})
        try:
            data = utils.load_json_from_url(url)
        except Exception as e:
            logger.error("Error fetching FEC candidates", extra={"url": url, "error": str(e)})
            break
        results = data.get("results", [])
        if not results:
            logger.info("No more FEC candidates", extra={"cycle": cycle, "office": office, "page": page})
            break
        candidates.extend(results)
        pages = data.get("pagination", {}).get("pages", 1)
        if page >= pages:
            break
        page += 1
    logger.info("Fetched FEC candidates", extra={"cycle": cycle, "office": office, "count": len(candidates)})
    return candidates


def build_legislator_lookup():
    """Build lookup from (fullname, state, district) -> bioguide_id."""
    lookup = {}
    with utils.get_cursor(commit=False) as (_, cur):
        cur.execute("SELECT bioguide_id, full_name, state, district FROM legislators")
        rows = cur.fetchall()
    for biog, name, state, dist in rows:
        key = (name.lower().strip(), state, dist)
        lookup[key] = biog
    logger.info("Built legislator lookup", extra={"entries": len(lookup)})
    return lookup


def normalize_and_map(records, lookup, cycle, office):
    """
    Map FEC records to bioguide IDs with overrides, fallbacks, fuzziness.
    Summarize mismatches at end.
    """
    rows = []
    misses = Counter()
    for rec in records:
        fec_id   = rec.get("candidate_id")
        raw_name = rec.get("name", "").strip()
        state    = rec.get("state")
        dist_raw = rec.get("district")
        district = dist_raw if dist_raw not in (None, "", "00") else None

        # Manual override
        if fec_id in OVERRIDES:
            biog = OVERRIDES[fec_id]
            rows.append((fec_id, biog, raw_name, office, state, district, cycle, datetime.utcnow()))
            continue

        # Exact & fallback matching
        matched = None
        variants = generate_name_variants(raw_name)
        for var in variants:
            # exact state+district
            key = (var, state, district)
            if key in lookup:
                matched = lookup[key]
                break
            # fallback ignore district
            key2 = (var, state, None)
            if key2 in lookup:
                matched = lookup[key2]
                break
        # fuzzy match on name + state
        if not matched:
            names = [n for (n, st, dt) in lookup if st == state]
            close = difflib.get_close_matches(raw_name.lower(), names, n=3, cutoff=0.8)
            for name in close:
                for key in [(name, state, district), (name, state, None)]:
                    if key in lookup:
                        matched = lookup[key]
                        break
                if matched:
                    break

        if matched:
            rows.append((fec_id, matched, raw_name, office, state, district, cycle, datetime.utcnow()))
        else:
            misses[(raw_name, state, district)] += 1

    # Log summary of mismatches
    total_misses = sum(misses.values())
    if total_misses:
        top = misses.most_common(10)
        logger.warning(
            "FEC mapping unmatched candidates",
            extra={"total_unmatched": total_misses, "top_unmatched": top}
        )
    else:
        logger.info("All FEC candidates mapped successfully", extra={"cycle": cycle, "office": office})

    logger.info(
        "Normalized and mapped records",
        extra={"cycle": cycle, "office": office, "mapped_rows": len(rows)}
    )
    return rows


def main():
    logger.info("Starting FEC mapping ETL run")
    lookup = build_legislator_lookup()
    all_rows = []

    for cycle in CYCLES:
        for office in OFFICES:
            recs = fetch_candidates(cycle, office)
            mapped = normalize_and_map(recs, lookup, cycle, office)
            all_rows.extend(mapped)

    if not all_rows:
        logger.warning("No rows to upsert, exiting ETL")
        return

    logger.info("Upserting FEC mapping rows", extra={"total_rows": len(all_rows)})
    with utils.get_cursor() as (_, cur):
        utils.bulk_upsert(cur, TABLE, all_rows, COLUMNS, CONFLICT_COLS)
    logger.info("FEC mapping ETL completed successfully", extra={"total_rows": len(all_rows)})

if __name__ == '__main__':
    main()
