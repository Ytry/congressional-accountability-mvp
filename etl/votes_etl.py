
import os
import requests
import psycopg2
import xml.etree.ElementTree as ET
import csv
import json
import logging
from datetime import datetime
from dotenv import load_dotenv
from typing import List, Dict

# --- Load environment variables ---
load_dotenv()

# --- Logging setup ---
logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")

# --- DB Config ---
DB_CONFIG = {
    "dbname": os.getenv("dbname"),
    "user": os.getenv("user"),
    "password": os.getenv("password"),
    "host": os.getenv("host"),
    "port": os.getenv("port"),
}

# --- URL Templates ---
HOUSE_BASE_URL = "https://clerk.house.gov/evs/{year}/roll{roll}.xml"
SENATE_BASE_URL = "https://www.senate.gov/legislative/LIS/roll_call_votes/vote{congress}{session}/vote_{congress}_{session}_{roll}.csv"

# --- ICPSR to BioGuide mapping (partial sample; extend this dictionary or load from file if needed) ---
ICPSR_TO_BIOGUIDE = {
  "39310": "C000127",
  "40700": "K000367",
  "29147": "S000033",
  "40704": "W000802",
  "40707": "B001261",
  "29534": "W000437",
  "49703": "C001035",
  "40305": "C001056",
  "15021": "D000563",
  "29566": "G000359",
  "14921": "M000355",
  "40908": "M001176",
  "29142": "R000122",
  "40902": "R000584",
  "40906": "S001181",
  "40909": "W000805",
  "20735": "G000555",
  "40916": "C001088",
  "29701": "A000055",
  "29940": "B001230",
  "40910": "B001267",
  "20758": "B001257",
  "29339": "B000490",
  "20351": "B001243",
  "41101": "B001277",
  "20101": "B001236",
  "20709": "B001260",
  "29323": "C000059",
  "20146": "C001047",
  "20757": "C001072",
  "20356": "C001051",
  "20919": "C001075",
  "20708": "C001066",
  "20955": "C001080",
  "20733": "C001067",
  "20517": "C001061",
  "39301": "C000537",
  "20748": "C001068",
  "20344": "C001053",
  "20501": "C001059",
  "20706": "C001069",
  "29345": "C000880",
  "21106": "C001087",
  "20533": "C001063",
  "29717": "D000096",
  "29710": "D000197",
  "29109": "D000216",
  "21179": "D000616",
  "20316": "D000600",
  "29571": "D000399",
  "21178": "F000459",
  "20521": "F000450",
  "20958": "G000559",
  "21103": "G000565",
  "14226": "G000386",
  "20124": "G000546",
  "20529": "G000553",
  "21191": "G000568",
  "20916": "G000558",
  "21139": "H001052",
  "20930": "H001046",
  "20907": "H001047",
  "20713": "H001042",
  "41107": "H001061",
  "14873": "H000874",
  "21142": "H001058",
  "20712": "J000288",
  "41111": "J000293",
  "20738": "J000289",
  "15029": "K000009",
  "21140": "K000375",
  "21167": "K000376",
  "21166": "L000575",
  "20145": "L000560",
  "29908": "L000557",
  "20755": "L000566",
  "41110": "L000577",
  "29504": "L000397",
  "29393": "L000491",
  "20932": "L000570",
  "20119": "L000562",
  "14435": "M000133",
  "20538": "M001163",
  "20530": "M001157",
  "20903": "M001177",
  "20122": "M001143",
  "29729": "M000312",
  "29776": "M001137",
  "20537": "M001160",
  "29722": "M000934",
  "40300": "M001153",
  "20707": "M001169",
  "49308": "M001111",
  "29377": "N000002",
  "15616": "N000015",
  "15454": "P000034",
  "41104": "P000603",
  "15448": "P000197",
  "20923": "P000595",
  "20920": "P000597",
  "20954": "Q000023",
  "14854": "R000395",
  "20301": "R000575",
  "20759": "S001176",
  "29911": "S001145",
  "20104": "S001150",
  "14858": "S000148",
  "21105": "S001183",
  "21123": "S001189",
  "20321": "S001157",
  "39307": "S000185",
  "21173": "S001184",
  "21102": "S001185",
  "29707": "S000344",
  "29910": "S001148",
  "29768": "S000510",
  "20729": "S001172",
  "14863": "S000522",
  "20310": "S001156",
  "29368": "T000193",
  "29901": "T000460",
  "20946": "T000467",
  "29754": "T000250",
  "20934": "T000469",
  "20342": "T000463",
  "20330": "V000128",
  "29378": "V000081",
  "20725": "W000798",
  "20504": "W000797",
  "29106": "W000187",
  "21116": "W000806",
  "20750": "W000800",
  "20138": "W000795",
  "21118": "W000808",
  "20756": "W000804",
  "21108": "W000809",
  "14871": "W000779",
  "21133": "Y000064",
  "21196": "A000369",
  "21198": "B001278",
  "31101": "D000617",
  "31102": "M001184",
  "41112": "S001194",
  "20749": "F000454",
  "20927": "T000468",
  "21301": "C001095",
  "21302": "L000578",
  "21303": "H001068",
  "21304": "B001287",
  "21306": "S001193",
  "21308": "B001285",
  "21311": "R000599",
  "21312": "T000472",
  "21314": "V000130",
  "21315": "P000608",
  "21321": "F000462",
  "21325": "D000622",
  "21333": "B001282",
  "41301": "W000817",
  "41300": "K000383",
  "21337": "W000812",
  "21338": "D000618",
  "21346": "H001067",
  "21350": "C001096",
  "41302": "F000463",
  "21342": "M001188",
  "21343": "J000294",
  "21352": "B001281",
  "21353": "J000295",
  "21355": "M001190",
  "21356": "P000605",
  "41304": "C001098",
  "21360": "W000814",
  "21362": "C001091",
  "21364": "W000816",
  "21365": "V000131",
  "41305": "K000384",
  "21370": "P000607",
  "21371": "K000385",
  "21373": "S001195",
  "41308": "B001288",
  "21375": "C001101",
  "21536": "N000188",
  "21545": "A000370",
  "21500": "P000609",
  "21503": "H001072",
  "21563": "W000821",
  "21502": "G000574",
  "21504": "D000623",
  "21506": "A000371",
  "21507": "L000582",
  "21508": "T000474",
  "21513": "C001103",
  "21515": "L000583",
  "21516": "A000372",
  "21519": "B001295",
  "21525": "M001196",
  "21526": "M001194",
  "21529": "D000624",
  "21531": "E000294",
  "21544": "R000603",
  "21538": "W000822",
  "21541": "S001196",
  "21548": "B001296",
  "21551": "B001291",
  "21554": "B001292",
  "21556": "N000189",
  "21559": "G000576",
  "41500": "S001198",
  "41502": "E000295",
  "41504": "T000476",
  "41505": "R000605",
  "21561": "K000388",
  "21562": "L000585",
  "21564": "D000626",
  "21565": "C001108",
  "21566": "E000296",
  "41703": "K000393",
  "41702": "H001076",
  "41700": "C001113",
  "21326": "S001190",
  "21705": "B001302",
  "21728": "K000389",
  "21740": "P000613",
  "21709": "C001112",
  "21703": "B001300",
  "21711": "C001110",
  "21706": "B001303",
  "21714": "D000628",
  "21744": "R000609",
  "21746": "S001200",
  "21735": "M001199",
  "21730": "K000391",
  "21702": "B001299",
  "21734": "M001198",
  "21724": "H001077",
  "21727": "J000299",
  "21741": "R000606",
  "21704": "B001301",
  "21708": "B001305",
  "21701": "B001298",
  "21723": "G000583",
  "21743": "R000608",
  "21715": "E000297",
  "21718": "F000466",
  "21745": "S001199",
  "21731": "K000392",
  "21722": "G000581",
  "21700": "A000375",
  "21726": "J000298",
  "21750": "E000298",
  "21753": "N000190",
  "21754": "G000585",
  "21755": "C001114",
  "41706": "S001203",
  "41707": "H001079",
  "21758": "C001115",
  "21759": "B001306",
  "21760": "H001082",
  "21761": "M001206",
  "21762": "S001205",
  "20322": "C001055",
  "21339": "H001066",
  "21968": "S001211",
  "21930": "H001090",
  "21939": "L000593",
  "21948": "N000191",
  "21912": "C001121",
  "21931": "H001081",
  "21971": "S001214",
  "21944": "M001208",
  "21920": "F000469",
  "21921": "G000586",
  "21906": "C001117",
  "21979": "U000040",
  "21903": "B001307",
  "21914": "D000629",
  "21977": "T000482",
  "21955": "P000617",
  "21965": "S001208",
  "21972": "S001215",
  "21975": "T000481",
  "21910": "C001119",
  "21950": "O000173",
  "21969": "S001212",
  "21927": "G000591",
  "21951": "P000614",
  "21980": "V000133",
  "21937": "K000394",
  "21964": "S001207",
  "21938": "L000590",
  "21949": "O000172",
  "21915": "D000631",
  "21934": "H001085",
  "21945": "M001204",
  "21936": "J000302",
  "21956": "R000610",
  "21974": "T000480",
  "21935": "J000301",
  "21905": "B001309",
  "21959": "R000612",
  "21926": "G000590",
  "21911": "C001120",
  "21925": "G000589",
  "21919": "F000468",
  "21917": "E000299",
  "21961": "R000614",
  "21922": "G000587",
  "21908": "C001118",
  "21962": "S001216",
  "21970": "S001213",
  "21946": "M001205",
  "41903": "S001217",
  "41901": "H001089",
  "21923": "G000592",
  "21987": "M001210",
  "15433": "M000687",
  "20953": "L000571",
  "20107": "I000056",
  "29759": "S000250",
  "21307": "V000129",
  "21749": "T000478",
  "21747": "S001201",
  "29354": "F000110",
  "21100": "S001188",
  "21907": "C001123"
}

def db_connection():
    return psycopg2.connect(**DB_CONFIG)

def parse_house_vote(congress: int, session: int, roll: int) -> List[Dict]:
    year = 2023
    url = HOUSE_BASE_URL.format(year=year, roll=str(roll).zfill(3))
    logging.info(f"📥 Fetching House vote from {url}")
    resp = requests.get(url)
    if resp.status_code != 200 or not resp.content.strip().startswith(b'<?xml'):
        logging.warning(f"⚠️ Skipping invalid or non-XML content at {url}")
        return []

    try:
        root = ET.fromstring(resp.content)
    except Exception as e:
        logging.error(f"❌ Failed to parse XML from {url}: {e}")
        return []

    vote_data = []
    try:
        bill_number = root.findtext(".//legis-num")
        vote_desc = root.findtext(".//vote-desc")
        vote_result = root.findtext(".//vote-result")
        date = root.findtext(".//action-date")
        question = root.findtext(".//question-text")
        parsed_date = datetime.strptime(date, "%d-%b-%Y")
    except Exception as e:
        logging.error(f"❌ Failed to parse vote metadata: {e}")
        return []

    tally = {"Yea": 0, "Nay": 0, "Present": 0, "Not Voting": 0}
    for record in root.findall(".//recorded-vote"):
        bioguide_id = record.findtext("legislator")
        position = record.findtext("vote")
        if position not in tally:
            continue
        tally[position] += 1

        vote_data.append({
            "vote_id": f"house-{congress}-{session}-{roll}",
            "chamber": "House",
            "congress": congress,
            "session": session,
            "roll": roll,
            "bioguide_id": bioguide_id,
            "bill_number": bill_number,
            "question": question,
            "vote_description": vote_desc,
            "vote_result": vote_result,
            "position": position,
            "date": parsed_date,
            "tally_yea": tally["Yea"],
            "tally_nay": tally["Nay"],
            "tally_present": tally["Present"],
            "tally_not_voting": tally["Not Voting"],
            "is_key_vote": False
        })

    return vote_data

def parse_senate_vote(congress: int, session: int, roll: int) -> List[Dict]:
    url = SENATE_BASE_URL.format(congress=congress, session=session, roll=str(roll).zfill(5))
    logging.info(f"📥 Fetching Senate vote from {url}")
    resp = requests.get(url)
    if resp.status_code != 200 or "<!DOCTYPE" in resp.text:
        logging.warning(f"⚠️ Skipping invalid or HTML content at {url}")
        return []

    try:
        lines = resp.content.decode("utf-8").splitlines()
        reader = csv.DictReader(lines)
    except Exception as e:
        logging.error(f"❌ Failed to parse CSV from {url}: {e}")
        return []

    vote_data = []
    tally = {"Yea": 0, "Nay": 0, "Present": 0, "Not Voting": 0}
    for row in reader:
        position = row.get("Vote") or row.get("Vote Cast")
        if not position or position not in tally:
            logging.warning(f"⚠️ Skipping row with invalid or missing vote: {row}")
            continue
        tally[position] += 1
        try:
            parsed_date = datetime.strptime(row["Vote Date"], "%m/%d/%Y")
        except Exception:
            logging.warning(f"⚠️ Skipping invalid date: {row.get('Vote Date', '')}")
            continue

        icpsr = row.get("ICPSR", "")
        bioguide_id = ICPSR_TO_BIOGUIDE.get(icpsr, None)
        if not bioguide_id:
            logging.warning(f"⚠️ No BioGuide match found for ICPSR ID {icpsr}")
            continue

        vote_data.append({
            "vote_id": f"senate-{congress}-{session}-{roll}",
            "chamber": "Senate",
            "congress": congress,
            "session": session,
            "roll": roll,
            "bioguide_id": bioguide_id,
            "bill_number": row.get("Measure Number"),
            "question": row.get("Vote Question"),
            "vote_description": row.get("Vote Title"),
            "vote_result": row.get("Result"),
            "position": position,
            "date": parsed_date,
            "tally_yea": tally["Yea"],
            "tally_nay": tally["Nay"],
            "tally_present": tally["Present"],
            "tally_not_voting": tally["Not Voting"],
            "is_key_vote": False
        })

    return vote_data

def insert_votes(vote_records: List[Dict]):
    conn = db_connection()
    cur = conn.cursor()
    success = skipped = 0

    for v in vote_records:
        try:
            cur.execute("SELECT id FROM legislators WHERE bioguide_id = %s", (v["bioguide_id"],))
            legislator = cur.fetchone()
            if not legislator:
                logging.warning(f"⏭️ No match for BioGuide ID {v['bioguide_id']}, skipping.")
                skipped += 1
                continue

            legislator_id = legislator[0]
            cur.execute("""
                INSERT INTO votes (
                    legislator_id, vote_id, bill_number, question_text,
                    vote_description, vote_result, position, date,
                    tally_yea, tally_nay, tally_present, tally_not_voting, is_key_vote
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT DO NOTHING;
            """, (
                legislator_id, v["vote_id"], v["bill_number"], v["question"],
                v["vote_description"], v["vote_result"], v["position"], v["date"],
                v["tally_yea"], v["tally_nay"], v["tally_present"],
                v["tally_not_voting"], v["is_key_vote"]
            ))
            success += 1
        except Exception as e:
            logging.error(f"❌ Insert failed for vote {v['vote_id']}: {e}")
            conn.rollback()

    conn.commit()
    cur.close()
    conn.close()
    logging.info("📊 Vote ETL Summary")
    logging.info(f"✅ Inserted: {success}")
    logging.info(f"⏭️ Skipped: {skipped}")

def run():
    logging.info("🚀 Starting Vote ETL process...")
    all_votes = []
    rolls_to_fetch = [(118, 1, 1), (118, 1, 2)]

    for congress, session, roll in rolls_to_fetch:
        all_votes.extend(parse_house_vote(congress, session, roll))
        all_votes.extend(parse_senate_vote(congress, session, roll))

    insert_votes(all_votes)
    logging.info("🏁 Vote ETL process complete.")

if __name__ == "__main__":
    run()
