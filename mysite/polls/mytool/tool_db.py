import sqlite3
from pathlib import Path

db_path = Path(__file__).resolve().parent.parent / "polls_db.sqlite3"

def get_conn_cur():
    conn=sqlite3.connect(db_path)
    return conn,conn.cursor()



