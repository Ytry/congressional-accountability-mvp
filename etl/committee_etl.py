import os
from dotenv import load_dotenv
load_dotenv()

DB_CONFIG = {
    "dbname": os.getenv("DB_NAME"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
    "host": os.getenv("DB_HOST"),
    "port": os.getenv("DB_PORT"),
}

API_KEY = os.getenv("CONGRESS_API_KEY")

import requests
import psycopg2
from bs4 import BeautifulSoup


def get_committee_data():
    house_url = "https://clerk.house.gov/committees"
    senate_url = "https://www.senate.gov/committees/committees_home.htm"

    committees = []

    # House scraping
    response = requests.get(house_url)
    if response.status_code == 200:
        soup = BeautifulSoup(response.text, 'html.parser')
        for committee_div in soup.select(".committee-item"):
            name = committee_div.find("h3").get_text(strip=True)
            members = [li.get_text(strip=True) for li in committee_div.select("li")]
            for member in members:
                committees.append({
                    "chamber": "House",
                    "committee_name": name,
                    "member": member
                })

    # Senate scraping
    response = requests.get(senate_url)
    if response.status_code == 200:
        soup = BeautifulSoup(response.text, 'html.parser')
        for link in soup.select("a.committee-link"):
            name = link.get_text(strip=True)
            committees.append({
                "chamber": "Senate",
                "committee_name": name,
                "member": None  # Member data not directly listed
            })

    return committees

def insert_committee_data(data):
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()

    for entry in data:
        cur.execute("""
            INSERT INTO committee_assignments (chamber, committee_name, member_name)
            VALUES (%s, %s, %s)
            ON CONFLICT DO NOTHING;
        """, (entry["chamber"], entry["committee_name"], entry["member"]))

    conn.commit()
    cur.close()
    conn.close()

if __name__ == "__main__":
    committee_data = get_committee_data()
    insert_committee_data(committee_data)
