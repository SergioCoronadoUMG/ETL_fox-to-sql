from __future__ import annotations
from typing import List, Dict, Any
import pyodbc

def connect_sqlserver(cfg: Dict[str, Any]):
    d = cfg['sqlserver']
    con_str = (
        f"DRIVER={{{{{ {d['driver']} }}}}};"
        f"SERVER={d['server']};DATABASE={d['database']};"
        f"UID={d['user']};PWD={d['password']};"
        f"TrustServerCertificate={'Yes' if d.get('trust_server_certificate', True) else 'No'};"
    )
    return pyodbc.connect(con_str, autocommit=False)

def ensure_table(cursor, create_sql: str, table_fqn: str, truncate: bool = False):
    cursor.execute(f"IF OBJECT_ID('{table_fqn}', 'U') IS NULL BEGIN {create_sql} END")
    if truncate:
        cursor.execute(f"TRUNCATE TABLE {table_fqn}")

def bulk_insert(cursor, table_fqn: str, columns: List[str], rows: List[List[Any]]):
    placeholders = ",".join(["?"] * len(columns))
    cols = ",".join(f"[{c}]" for c in columns)
    sql = f"INSERT INTO {table_fqn} ({cols}) VALUES ({placeholders})"
    cursor.fast_executemany = True
    cursor.executemany(sql, rows)
