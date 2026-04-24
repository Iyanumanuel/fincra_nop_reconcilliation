import duckdb
from utils.config import DB_PATH

def get_connection():
    return duckdb.connect(DB_PATH)