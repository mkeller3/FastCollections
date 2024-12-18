import os

from dotenv import load_dotenv

load_dotenv()

DB_HOST = os.getenv("DB_HOST")
DB_DATABASE = os.getenv("DB_DATABASE")
DB_USERNAME = os.getenv("DB_USERNAME")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_PORT = os.getenv("DB_PORT")
CACHE_AGE_IN_SECONDS = int(os.getenv("CACHE_AGE_IN_SECONDS"))
MAX_FEATURES_PER_TILE = int(os.getenv("MAX_FEATURES_PER_TILE"))
NUMERIC_FIELDS = [
    "bigint",
    "bigserial",
    "double precision",
    "integer",
    "smallint",
    "real",
    "smallserial",
    "serial",
    "numeric",
    "money",
]
