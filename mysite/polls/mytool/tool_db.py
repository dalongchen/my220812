import sqlite3
from pathlib import Path


def get_conn_cur():
    db_path = Path(__file__).resolve().parent.parent / "polls_db.sqlite3"
    conn = sqlite3.connect(db_path)
    return conn, conn.cursor()
