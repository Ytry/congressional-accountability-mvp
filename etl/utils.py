import os
import sys
import time
import json
import yaml
import requests
from pathlib import Path
from contextlib import contextmanager
import psycopg2
import psycopg2.extras
from psycopg2.pool import ThreadedConnectionPool
import config
from logger import setup_logger

# Initialize module-level logger
logger = setup_logger("etl_utils")

# ── Database Connection Pool ───────────────────────────────────────────────────
# Create a shared connection pool for all ETL scripts
try:
    if config.DATABASE_URL:
        _pool = ThreadedConnectionPool(
            minconn=config.DB_POOL_MIN,
            maxconn=config.DB_POOL_MAX,
            dsn=config.DATABASE_URL
        )
    else:
        _pool = ThreadedConnectionPool(
            minconn=config.DB_POOL_MIN,
            maxconn=config.DB_POOL_MAX,
            database=config.DB_NAME,
            user=config.DB_USER,
            password=config.DB_PASSWORD,
            host=config.DB_HOST,
            port=config.DB_PORT
        )
    logger.info("Initialized DB connection pool", extra={
        "minconn": config.DB_POOL_MIN,
        "maxconn": config.DB_POOL_MAX
    })
except Exception:
    logger.exception("Failed to initialize DB connection pool")
    sys.exit(1)

@contextmanager
def get_conn():
    """
    Context manager to get a connection from the pool and return it when done.
    """
    conn = _pool.getconn()
    try:
        yield conn
    finally:
        _pool.putconn(conn)

@contextmanager
def get_cursor(commit: bool = True):
    """
    Context manager yielding (conn, cursor). Commits on exit if commit=True.
    """
    with get_conn() as conn:
        cur = conn.cursor()
        try:
            yield conn, cur
            if commit:
                conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            cur.close()

# ── HTTP Utilities ────────────────────────────────────────────────────────────

def fetch_with_retry(
    url: str,
    timeout: float = None,
    max_retries: int = None,
    retry_delay: float = None
) -> requests.Response:
    """
    GET request with exponential backoff and basic 404 handling.
    Returns Response for status 200, None for 404, retries others.
    """
    timeout = timeout or config.HTTP_TIMEOUT
    max_retries = max_retries or config.HTTP_MAX_RETRIES
    retry_delay = retry_delay or config.HTTP_RETRY_DELAY

    for attempt in range(1, max_retries + 1):
        try:
            resp = requests.get(url, timeout=timeout)
            if resp.status_code == 200:
                return resp
            if resp.status_code == 404:
                return None
            logger.warning("Unexpected status code", extra={
                "url": url,
                "status": resp.status_code,
                "attempt": attempt
            })
        except Exception as e:
            logger.debug("Fetch attempt exception", extra={
                "url": url,
                "attempt": attempt,
                "error": str(e)
            })
        time.sleep(retry_delay * (2 ** (attempt - 1)))

    logger.error("Failed to fetch URL after retries", extra={"url": url})
    return None

def load_json_from_url(url: str) -> dict:
    """
    Fetch JSON from a URL with retries, return parsed dict.
    """
    resp = fetch_with_retry(url)
    if not resp:
        raise IOError(f"Failed to fetch JSON from {url}")
    try:
        return resp.json()
    except Exception:
        logger.exception("Invalid JSON response", extra={"url": url})
        raise

def load_yaml_from_url(url: str) -> list:
    """
    Fetch YAML from a URL with retries, return parsed list/dict.
    """
    resp = fetch_with_retry(url)
    if not resp:
        raise IOError(f"Failed to fetch YAML from {url}")
    try:
        return yaml.safe_load(resp.text)
    except Exception:
        logger.exception("Invalid YAML response", extra={"url": url})
        raise

# ── File Utilities ────────────────────────────────────────────────────────────

def write_json(path: Path, data, indent: int = 2):
    """
    Write data as JSON to the specified file path, creating parent dirs.
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    try:
        with open(path, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=indent)
        logger.info("Wrote JSON file", extra={"path": str(path), "entries": len(data) if hasattr(data, '__len__') else None})
    except Exception:
        logger.exception("Failed to write JSON file", extra={"path": str(path)})
        raise

# ── Database Helpers ─────────────────────────────────────────────────────────

def fetch_legislator_map(query: str = "SELECT id, bioguide_id FROM legislators") -> dict:
    """
    Return a dict mapping bioguide_id -> internal id.
    """
    with get_cursor(commit=False) as (_, cur):
        cur.execute(query)
        rows = cur.fetchall()
    mapping = {bioguide: id for id, bioguide in rows}
    logger.info("Fetched legislator map", extra={"entries": len(mapping)})
    return mapping

# ── Bulk Upsert Helper ────────────────────────────────────────────────────────

def bulk_upsert(
    cur,
    table: str,
    rows: list,
    columns: list,
    conflict_cols: list,
    update_cols: list = None
):
    """
    Perform bulk upsert into the specified table via execute_values.
    - cur: psycopg2 cursor
    - rows: list of tuples
    - columns: list of column names for insert
    - conflict_cols: columns to conflict on
    - update_cols: columns to update on conflict (defaults to all except conflict)
    """
    if not rows:
        return
    update_cols = update_cols or [c for c in columns if c not in conflict_cols]
    col_list = ','.join(columns)
    val_placeholders = ','.join(['%s'] * len(columns))
    conflict_list = ','.join(conflict_cols)
    updates = ', '.join([f"{col}=EXCLUDED.{col}" for col in update_cols])

    sql = f"""
        INSERT INTO {table} ({col_list})
        VALUES %s
        ON CONFLICT ({conflict_list}) DO UPDATE SET {updates}
    """
    psycopg2.extras.execute_values(cur, sql, rows, template=None, page_size=100)
    logger.info("Bulk upsert executed", extra={"table": table, "rows": len(rows)})
