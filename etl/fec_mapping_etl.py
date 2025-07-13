import config
import utils
from logger import setup_logger
from datetime import datetime

logger = setup_logger("fec_mapping_etl")

# FEC search endpoint template
SEARCH_ENDPOINT = f"{config.FEC_BASE_URL}/candidates/search/"

# Election cycles to process
CYCLES = [2020, 2022, 2024]  # adjust as needed

# Offices: H=House, S=Senate
OFFICES = ["H", "S"]

# Bulk upsert settings
TABLE = "fec_candidates"
COLUMNS = [
    "fec_id",
    "bioguide_id",
    "name",
    "office",
    "state",
    "district",
    "cycle",
    "last_updated"
]
CONFLICT_COLS = ["fec_id", "cycle"]


def generate_name_variants(fec_name: str) -> list:
    """
    Generate possible name variants from FEC 'Last, First Middle' format.
    Returns list of lowercased name strings for matching.
    """
    variants = []
    if not fec_name:
        return variants
    name_lower = fec_name.lower().strip()
    variants.append(name_lower)

    # Swap 'Last, First' -> 'First Last'
    if "," in fec_name:
        last, first = fec_name.split(",", 1)
        swapped = f"{first.strip()} {last.strip()}".lower()
        variants.append(swapped)

    # Remove punctuation
    variants.append(name_lower.replace(',', ''))

    return list(dict.fromkeys(variants))  # dedupe preserving order


def fetch_candidates(cycle: int, office: str) -> list:
    """
    Page through FEC candidates for a given cycle and office.
    Logs each URL call and handles errors gracefully.
    Returns list of candidate records.
    """
    candidates = []
    page = 1
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
            logger.error(
                "Error fetching FEC candidates",
                extra={"url": url, "cycle": cycle, "office": office, "page": page, "error": str(e)}
            )
            break

        results = data.get("results", [])
        logger.debug("Received FEC response", extra={"cycle": cycle, "office": office, "page": page, "count": len(results)})
        if not results:
            logger.info("No more FEC candidates returned, ending pagination", extra={"cycle": cycle, "office": office, "page": page})
            break

        candidates.extend(results)
        pagination = data.get("pagination", {})
        total_pages = pagination.get("pages", 1)
        if page >= total_pages:
            break
        page += 1

    logger.info("Fetched candidates for cycle and office", extra={"cycle": cycle, "office": office, "total_fetched": len(candidates)})
    return candidates


def build_legislator_lookup():
    """
    Build a mapping of (full_name.lower(), state, district) -> bioguide_id
    """
    lookup = {}
    with utils.get_cursor(commit=False) as (_, cur):
        cur.execute("SELECT bioguide_id, full_name, state, district FROM legislators")
        rows = cur.fetchall()
        logger.info("Fetched legislators for lookup", extra={"count": len(rows)})
        for bioguide, full_name, state, district in rows:
            key = (full_name.lower().strip(), state, district)
            lookup[key] = bioguide
    logger.info("Built legislator lookup", extra={"entries": len(lookup)})
    return lookup


def normalize_and_map(records, lookup, cycle, office_code):
    """
    Normalize FEC records and map to bioguide IDs.
    Returns list of tuples for upsert.
    """
    rows = []
    for rec in records:
        fec_id = rec.get("candidate_id")
        raw_name = rec.get("name")
        state = rec.get("state")
        district = rec.get("district") if rec.get("district") not in (None, "", "00") else None

        # Generate name variants and attempt to match
        matched_bioguide = None
        for variant in generate_name_variants(raw_name):
            key = (variant, state, district)
            bioguide = lookup.get(key)
            if bioguide:
                matched_bioguide = bioguide
                break

        if not matched_bioguide:
            logger.warning(
                "No bioguide match for candidate",
                extra={"fec_id": fec_id, "name": raw_name, "state": state, "district": district, "variants": generate_name_variants(raw_name)}
            )
            continue

        rows.append((
            fec_id,
            matched_bioguide,
            raw_name,
            office_code,
            state,
            district,
            cycle,
            datetime.utcnow()
        ))
    logger.info("Normalized and mapped records", extra={"cycle": cycle, "office": office_code, "mapped_rows": len(rows)})
    return rows


def main():
    logger.info("Starting FEC mapping ETL run")
    lookup = build_legislator_lookup()
    all_rows = []

    for cycle in CYCLES:
        for office in OFFICES:
            logger.info("Processing cycle and office", extra={"cycle": cycle, "office": office})
            records = fetch_candidates(cycle, office)
            rows = normalize_and_map(records, lookup, cycle, office)
            all_rows.extend(rows)

    if not all_rows:
        logger.warning("No rows to upsert for FEC mapping ETL, exiting")
        return

    logger.info("Upserting rows into fec_candidates", extra={"total_rows": len(all_rows)})
    with utils.get_cursor() as (_, cur):
        utils.bulk_upsert(cur, TABLE, all_rows, COLUMNS, CONFLICT_COLS)
    logger.info("FEC mapping ETL completed successfully", extra={"total_rows": len(all_rows)})

if __name__ == '__main__':
    main()
