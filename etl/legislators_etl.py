import os
import requests
import logging
from sqlalchemy import create_engine, Column, String, Integer
from sqlalchemy.orm import declarative_base, sessionmaker
from dotenv import load_dotenv
import argparse

# Load environment variables
load_dotenv()

DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://localhost/congress")
LEGISLATORS_API = os.getenv("LEGISLATORS_API_URL", "https://theunitedstates.io/congress-legislators/legislators-current.json")

# Setup logging
logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("etl_legislators.log"),
        logging.StreamHandler()
    ]
)

# SQLAlchemy setup
Base = declarative_base()

class Legislator(Base):
    __tablename__ = "legislators"

    bioguide_id = Column(String, primary_key=True)
    full_name = Column(String)
    party = Column(String)
    state = Column(String)
    chamber = Column(String)
    district = Column(Integer, nullable=True)

def fetch_legislator_data():
    try:
        logging.info("Fetching data from API...")
        response = requests.get(LEGISLATORS_API)
        response.raise_for_status()
        data = response.json()
        if not data:
            logging.warning("Fetched zero records from API.")
        logging.info(f"Fetched {len(data)} legislators.")
        return data
    except requests.RequestException as e:
        logging.error(f"Error fetching data: {e}")
        raise

def transform_legislator_entry(entry):
    try:
        name = entry["name"]
        bio = entry["id"]
        terms = entry["terms"]
        current_term = terms[-1]

        full_name = f"{name.get('first', '')} {name.get('last', '')}".strip()
        bioguide_id = bio.get("bioguide")
        party = current_term.get("party")
        state = current_term.get("state")
        chamber = current_term.get("type")
        district = current_term.get("district") if chamber == "rep" else None

        if not bioguide_id:
            raise ValueError("Missing BioGuide ID")

        return Legislator(
            bioguide_id=bioguide_id,
            full_name=full_name,
            party=party,
            state=state,
            chamber="House" if chamber == "rep" else "Senate",
            district=district
        )
    except Exception as e:
        logging.error(f"Error transforming legislator entry: {e}")
        return None

def insert_legislators_to_db(data, dry_run=False):
    engine = create_engine(DATABASE_URL)
    Session = sessionmaker(bind=engine)
    session = Session()

    try:
        logging.info("Beginning insert process...")

        for entry in data:
            legislator = transform_legislator_entry(entry)
            if legislator:
                existing = session.query(Legislator).filter_by(bioguide_id=legislator.bioguide_id).first()
                if existing:
                    logging.debug(f"Skipping existing legislator {legislator.bioguide_id}")
                    continue
                session.add(legislator)
        
        if dry_run:
            logging.info("Dry run mode enabled. Rolling back instead of committing.")
            session.rollback()
        else:
            session.commit()
            logging.info("Insert committed successfully.")

    except Exception as e:
        logging.error(f"Database insert error: {e}")
        session.rollback()
    finally:
        session.close()

def main(dry_run=False):
    logging.info("Starting Legislators ETL process...")
    try:
        raw_data = fetch_legislator_data()
        insert_legislators_to_db(raw_data, dry_run=dry_run)
    except Exception as e:
        logging.critical(f"ETL process failed: {e}")
    logging.info("ETL process completed.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run the Legislators ETL script.")
    parser.add_argument("--dry-run", action="store_true", help="Run ETL without committing to DB")
    args = parser.parse_args()

    main(dry_run=args.dry_run)
