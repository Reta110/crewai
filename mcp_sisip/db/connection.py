import pymysql
from contextlib import contextmanager
from mcp_sisip.config import get_config

def get_connection():
    """Create MySQL connection with environment config."""
    cfg = get_config()["db"]
    if not cfg["database"]:
        raise ValueError("DB_DATABASE is not defined in .env")
    
    return pymysql.connect(
        host=cfg["host"],
        port=cfg["port"],
        database=cfg["database"],
        user=cfg["user"],
        password=cfg["password"],
        charset=cfg["charset"],
    )

@contextmanager
def get_cursor(dict_cursor: bool = True):
    """Context manager to yield a db cursor."""
    conn = get_connection()
    try:
        cur = conn.cursor(
            pymysql.cursors.DictCursor if dict_cursor else pymysql.cursors.Cursor
        )
        yield cur
        conn.commit()
    finally:
        cur.close()
        conn.close()

def query(sql: str, params: tuple = None) -> list:
    """Helper to run generic DB queries safely"""
    with get_cursor() as cur:
        cur.execute(sql, params)
        return cur.fetchall()
