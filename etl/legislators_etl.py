import requests
import psycopg2
import json
import yaml
import os
import logging
from typing import Optional

# --- Logging setup ---
logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")

# --- Environment config ---
DB_NAME = os.getenv("dbname")
DB_USER = os.getenv("user")
DB_PASSWORD = os.getenv("password")
DB_HOST = os.getenv("host")
DB_PORT = os.getenv("port")

DATA_SOURCE_URL = "https://raw.githubusercontent.com/unitedstates/congress-legislators/main/legislators-current.yaml"

# --- Database Connection ---
def connect():
    try:
        return psycopg2.connect(
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD,
            host=DB_HOST,
            port=DB_PORT
        )
    except Exception as e:
        logging.critical(f"Database connection failed: {e}")
        raise

# --- Extract YAML data ---
def extract_legislators():
    try:
        logging.info(f"Fetching data from {DATA_SOURCE_URL}")
        response = requests.get(DATA_SOURCE_URL)
        response.raise_for_s
