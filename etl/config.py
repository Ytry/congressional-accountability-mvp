import os
from pathlib import Path
from dotenv import load_dotenv

# Load environment variables from project root
ROOT_DIR = Path(__file__).parent.parent.resolve()
load_dotenv(ROOT_DIR / ".env")

# ── Environment & Logging ─────────────────────────────────────────────────────
LOG_LEVEL         = os.getenv("LOG_LEVEL", "INFO").upper()
CORRELATION_ID    = os.getenv("CORRELATION_ID")

# ── Paths & Directories ────────────────────────────────────────────────────────
ETL_DIR           = Path(__file__).parent.resolve()
LOGS_DIR          = ETL_DIR / "logs"
PORTRAITS_DIR     = ROOT_DIR / "portraits"
DEBUG_DIR         = ETL_DIR

# Ensure directories exist
LOGS_DIR.mkdir(parents=True, exist_ok=True)
PORTRAITS_DIR.mkdir(parents=True, exist_ok=True)

# ── Database Configuration ────────────────────────────────────────────────────
# Support DATABASE_URL or individual credentials
DATABASE_URL   = os.getenv("DATABASE_URL")
DB_NAME        = os.getenv("DB_NAME")
DB_USER        = os.getenv("DB_USER")
DB_PASSWORD    = os.getenv("DB_PASSWORD")
DB_HOST        = os.getenv("DB_HOST")
DB_PORT        = os.getenv("DB_PORT")
DB_POOL_MIN    = int(os.getenv("DB_POOL_MIN", 1))
DB_POOL_MAX    = int(os.getenv("DB_POOL_MAX", 5))

# ── HTTP / API Settings ───────────────────────────────────────────────────────
HTTP_TIMEOUT      = float(os.getenv("HTTP_TIMEOUT", 15.0))
HTTP_MAX_RETRIES  = int(os.getenv("HTTP_MAX_RETRIES", 3))
HTTP_RETRY_DELAY  = float(os.getenv("HTTP_RETRY_DELAY", 0.5))

# ETL Defaults
CONGRESS          = int(os.getenv("CONGRESS", 118))
SESSION           = int(os.getenv("SESSION", 1))
HOUSE_YEAR        = int(os.getenv("HOUSE_YEAR", 2023))
THREAD_WORKERS    = int(os.getenv("THREAD_WORKERS", 2))
MAX_CONSECUTIVE_MISSES = int(os.getenv("MAX_CONSECUTIVE_MISSES", 10))

# ── Source & Endpoint URLs ────────────────────────────────────────────────────
LEGIS_JSON_URL    = (
    "https://unitedstates.github.io/"
    "congress-legislators/legislators-current.json"
)
LEGIS_YAML_URL    = (
    "https://raw.githubusercontent.com/"
    "unitedstates/congress-legislators/main/legislators-current.yaml"
)
GPO_API_URL       = f"https://pictorialapi.gpo.gov/api/GuideMember/GetMembers/{CONGRESS}"
HOUSE_ROLL_URL    = "https://clerk.house.gov/evs/{year}/roll{roll:03d}.xml"
SENATE_ROLL_URL   = (
    "https://www.senate.gov/legislative/LIS/roll_call_votes/"
    "vote{congress}{session}/vote_{congress}_{session}_{roll:05d}.xml"
)

# ── File Names ───────────────────────────────────────────────────────────────
NAME_TO_BIO_MAP   = ETL_DIR / "name_to_bioguide.json"
PICT_DEBUG_JSON   = DEBUG_DIR / "pictorial_etl_debug.json"

# ── OpenSecrets API Configuration ─────────────────────────────────────────────
OPENSECRETS_API_KEY = os.getenv("OPENSECRETS_API_KEY")

# ── OpenFEC (FEC) API Configuration ───────────────────────────────────────────
# Fetch your FEC key via OPENFEC_API_KEY in .env
FEC_API_KEY    = os.getenv("OPENFEC_API_KEY")
FEC_BASE_URL   = "https://api.open.fec.gov/v1"
FEC_PAGE_SIZE  = int(os.getenv("FEC_PAGE_SIZE", 100))
