import json
from collections import Counter
import config
import utils
from logger import setup_logger

logger = setup_logger("fec_finance_etl")

# SQL to fetch FEC candidates
FETCH_CANDIDATES_SQL = "SELECT fec_id, bioguide_id, cycle FROM fec_candidates"

# FEC endpoints
TOTALS_ENDPOINT = f"{config.FEC_BASE_URL}/candidate/{{fec_id}}/totals/"
SCHEDULE_A_ENDPOINT = f"{config.FEC_BASE_URL}/schedules/schedule_a/"
SCHEDULE_B_ENDPOINT = f"{config.FEC_BASE_URL}/schedules/schedule_b/"

# Upsert config
TABLE = "campaign_finance"
COLUMNS = [
    "legislator_id",
    "cycle",
    "total_raised",
    "total_spent",
    "other_federal_receipts",
    "top_donors",
    "industry_breakdown"
]
CONFLICT_COLS = ["legislator_id", "cycle"]


def fetch_totals(fec_id: str, cycle: int) -> dict:
    """
    Fetch summary totals for a candidate and return key metrics.
    """
    url = f"{TOTALS_ENDPOINT.format(fec_id=fec_id)}?api_key={config.FEC_API_KEY}&cycle={cycle}"
    data = utils.load_json_from_url(url)
    if not data.get("results"):
        logger.warning("No totals data", extra={"fec_id": fec_id, "cycle": cycle})
        return {}
    rec = data["results"][0]
    return {
        "total_raised": rec.get("receipts", 0),
        "total_spent": rec.get("disbursements", 0),
        "other_federal_receipts": rec.get("other_federal_receipts", 0)
    }


def fetch_itemized(endpoint: str, fec_id: str, cycle: int, key: str) -> Counter:
    """
    Page through FEC itemized contributions/disbursements and aggregate by key field.
    key: 'contributor_organization' or 'contributor_employer'
    """
    counter = Counter()
    page = 1
    while True:
        url = (
            f"{endpoint}?api_key={config.FEC_API_KEY}"
            f"&candidate_id={fec_id}&cycle={cycle}"
            f"&per_page={config.FEC_PAGE_SIZE}&page={page}"
        )
        data = utils.load_json_from_url(url)
        results = data.get("results", [])
        if not results:
            break
        for item in results:
            name = item.get(key) or "Unknown"
            amount = item.get("amount", 0)
            counter[name] += amount
        page += 1
    return counter


def build_breakdown(counter: Counter, top_n: int = 10) -> list:
    """
    Convert a Counter to a sorted list of top_n dicts.
    """
    return [{"name": name, "amount": amt} for name, amt in counter.most_common(top_n)]


def main():
    # Map bioguide_id -> internal id
    leg_map = utils.fetch_legislator_map()

    # Fetch all FEC candidates
    with utils.get_cursor(commit=False) as (_, cur):
        cur.execute(FETCH_CANDIDATES_SQL)
        candidates = cur.fetchall()

    rows = []
    for fec_id, bioguide, cycle in candidates:
        leg_id = leg_map.get(bioguide)
        if not leg_id:
            logger.warning("Legislator not found in map", extra={"bioguide": bioguide})
            continue

        # Fetch summary totals
        totals = fetch_totals(fec_id, cycle)
        if not totals:
            continue

        # Itemized breakdowns
        donors_counter = fetch_itemized(SCHEDULE_A_ENDPOINT, fec_id, cycle, "contributor_organization")
        employer_counter = fetch_itemized(SCHEDULE_A_ENDPOINT, fec_id, cycle, "contributor_employer")

        top_donors = build_breakdown(donors_counter)
        industry_breakdown = build_breakdown(employer_counter)

        rows.append(
            (
                leg_id,
                cycle,
                totals.get("total_raised"),
                totals.get("total_spent"),
                totals.get("other_federal_receipts"),
                json.dumps(top_donors),
                json.dumps(industry_breakdown)
            )
        )

    # Upsert into campaign_finance
    if rows:
        with utils.get_cursor() as (_, cur):
            utils.bulk_upsert(
                cur,
                TABLE,
                rows,
                COLUMNS,
                CONFLICT_COLS
            )
        logger.info("FEC finance ETL completed", extra={"rows": len(rows)})
    else:
        logger.info("No rows to upsert for FEC finance ETL")


if __name__ == "__main__":
    main()
