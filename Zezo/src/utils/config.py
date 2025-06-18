from os import getenv

from dotenv.main import load_dotenv

load_dotenv()

PROJECT_NAME = getenv("PROJECT_NAME", "Phonetics")
DESCRIPTION = getenv("DESCRIPTION")

# ---------- Flask Config ----------
HOST = getenv("HOST", "0.0.0.0")
PORT = getenv("PORT", "8888")
DEBUG = bool(getenv("DEBUG", False))
VERSION = getenv("VERSION")

# ---------- DataBase Config ----------

DB_HOST = getenv("DB_HOST")
DB_PORT = getenv("DB_PORT")
DB_PASSWORD = getenv("DB_PASSWORD")
DB_NAME = getenv("DB_NAME")
DB_USER = getenv("DB_USER")
DATABASE_URL = "sqlite:///./test.db"

# --------- CutOut Config ----------
API_BASE_URL = getenv("API_BASE_URL")
API_KEY = getenv("API_KEY")
NUM_Enhance = int(getenv("NUM_Enhance", 2))


def get_database_url():
    """It will Generate Database URL for PostgreSQL To connect with

    Returns:
        string: Database Url to open connection with it
    """
    return f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
