#!/usr/bin/env python3
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

# Module-level logger with detailed debug
logger = setup_logger("etl_utils")

# ── Database Connection Pool ───────────────────────────────────────────────────
# Create a shared connection pool for all ETL scripts
try:
    pool_params = {
        "DB_POOL_MIN": config.DB_POOL_MIN,
        "DB_POOL_MAX": config.DB_POOL_MAX,
        "DATABASE_URL": bool(config.DATABASE_URL)
    }
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
    logger.info("Initialized DB connection pool", extra=pool_params)
    logger.debug("DB connection pool details", extra=pool_params)
except Exception:
    logger.exception("Failed to initialize DB connection pool")
    sys.exit(1)

@contextmanager
def get_conn():
    """
    Yield a DB connection from the pool and return it when done.
    """
    logger.debug("Acquiring DB connection from pool")
    conn = _pool.getconn()
    try:
        yield conn
    finally:
        _pool.putconn(conn)
        logger.debug("Returned DB connection to pool")

@contextmanager
def get_cursor(commit: bool = True):
    """
    Yield (conn, cursor); commits on exit if commit=True, otherwise rolls back on error.
    """
    with get_conn() as conn:
        cur = conn.cursor()
        logger.debug("Opened new cursor", extra={"commit": commit})
        start_time = time.monotonic()
        try:
            yield conn, cur
            if commit:
                conn.commit()
                duration_ms = int((time.monotonic() - start_time) * 1000)
                logger.debug("Transaction committed", extra={"duration_ms": duration_ms})
        except Exception as e:
            conn.rollback()
            logger.debug("Transaction rolled back due to exception", extra={"error": str(e)})
            raise
        finally:
            cur.close()
            logger.debug("Cursor closed")

# ── HTTP Utilities ────────────────────────────────────────────────────────────

def fetch_with_retry(
    url: str,
    timeout: float = None,
    max_retries: int = None,
    retry_delay: float = None
) -> requests.Response:
    """
    GET with exponential backoff and basic 404 handling.
    Logs detailed debug for each attempt and total duration.
    """
    timeout = timeout or config.HTTP_TIMEOUT
    max_retries = max_retries or config.HTTP_MAX_RETRIES
    retry_delay = retry_delay or config.HTTP_RETRY_DELAY

    logger.debug("Starting fetch_with_retry", extra={
        "url": url,
        "timeout": timeout,
        "max_retries": max_retries,
        "retry_delay": retry_delay
    })
    start_time = time.monotonic()
    for attempt in range(1, max_retries + 1):
        logger.debug("Fetch attempt", extra={"url": url, "attempt": attempt})
        try:
            resp = requests.get(url, timeout=timeout)
            logger.debug("Received response", extra={"url": url, "status_code": resp.status_code})
            if resp.status_code == 200:
                total_ms = int((time.monotonic() - start_time) * 1000)
                logger.debug("Fetch succeeded", extra={"url": url, "total_ms": total_ms})
                return resp
            if resp.status_code == 404:
                logger.warning("Resource not found (404)", extra={"url": url})
                return None
            logger.warning("Unexpected status code", extra={
                "url": url,
                "status": resp.status_code,
                "attempt": attempt
            })
        except Exception as e:
            logger.debug("Fetch exception", extra={
                "url": url,
                "attempt": attempt,
                "error": str(e)
            })
        time.sleep(retry_delay * (2 ** (attempt - 1)))

    total_ms = int((time.monotonic() - start_time) * 1000)
    logger.error("Failed to fetch URL after retries", extra={"url": url, "total_ms": total_ms})
    return None


def load_json_from_url(url: str) -> dict:
    """
    Fetch JSON with retries, return parsed dict, with debug timing.
    """
    start_time = time.monotonic()
    resp = fetch_with_retry(url)
    duration_ms = int((time.monotonic() - start_time) * 1000)
    logger.debug("load_json_from_url duration", extra={"url": url, "duration_ms": duration_ms})
    if not resp:
        logger.error("Failed to fetch JSON from URL", extra={"url": url})
        raise IOError(f"Failed to fetch JSON from {url}")
    try:
        data = resp.json()
        logger.debug("JSON parsed successfully", extra={"url": url, "type": type(data).__name__})
        return data
    except Exception:
        logger.exception("Invalid JSON response", extra={"url": url})
        raise


def load_yaml_from_url(url: str) -> list:
    """
    Fetch YAML with retries, return parsed data, with debug timing.
    """
    start_time = time.monotonic()
    resp = fetch_with_retry(url)
    duration_ms = int((time.monotonic() - start_time) * 1000)
    logger.debug("load_yaml_from_url duration", extra={"url": url, "duration_ms": duration_ms})
    if not resp:
        logger.error("Failed to fetch YAML from URL", extra={"url": url})
        raise IOError(f"Failed to fetch YAML from {url}")
    try:
        data = yaml.safe_load(resp.text)
        logger.debug("YAML parsed successfully", extra={"url": url, "records": len(data) if hasattr(data, '__len__') else None})
        return data
    except Exception:
        logger.exception("Invalid YAML response", extra={"url": url})
        raise

# ── File Utilities ────────────────────────────────────────────────────────────

def write_json(path: Path, data, indent: int = 2):
    """
    Write JSON to file, create parent dirs, log debug timing and size.
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    start_time = time.monotonic()
    try:
        with open(path, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=indent)
        duration_ms = int((time.monotonic() - start_time) * 1000)
        logger.info("Wrote JSON file", extra={
            "path": str(path),
            "entries": len(data) if hasattr(data, '__len__') else None,
            "duration_ms": duration_ms
        })
    except Exception:
        logger.exception("Failed to write JSON file", extra={"path": str(path)})
        raise

# ── Database Helpers ─────────────────────────────────────────────────────────

def fetch_legislator_map(query: str = "SELECT id, bioguide_id FROM legislators") -> dict:
    """
    Return a dict mapping bioguide_id -> internal id, with debug logs.
    """
    logger.debug("Fetching legislator map", extra={"query": query})
    start_time = time.monotonic()
    with get_cursor(commit=False) as (_, cur):
        cur.execute(query)
        rows = cur.fetchall()
    mapping = {bioguide: id for id, bioguide in rows}
    duration_ms = int((time.monotonic() - start_time) * 1000)
    logger.info("Fetched legislator map", extra={"entries": len(mapping), "duration_ms": duration_ms})
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
    Perform bulk upsert via execute_values, with debug logs.
    """
    if not rows:
        logger.debug("No rows to upsert", extra={"table": table})
        return
    update_cols = update_cols or [c for c in columns if c not in conflict_cols]
    logger.debug("Preparing bulk upsert", extra={
        "table": table,
        "columns": columns,
        "conflict_cols": conflict_cols,
        "update_cols": update_cols,
        "rows": len(rows)
    })
    start_time = time.monotonic()
    col_list = ','.join(columns)
    conflict_list = ','.join(conflict_cols)
    updates = ', '.join([f"{col}=EXCLUDED.{col}" for col in update_cols])

    sql = f"""
        INSERT INTO {table} ({col_list})
        VALUES %s
        ON CONFLICT ({conflict_list}) DO UPDATE SET {updates}
    """
    psycopg2.extras.execute_values(cur, sql, rows, page_size=100)
    duration_ms = int((time.monotonic() - start_time) * 1000)
    logger.info("Bulk upsert executed", extra={"table": table, "rows": len(rows), "duration_ms": duration_ms})
