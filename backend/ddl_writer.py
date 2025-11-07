from __future__ import annotations
from typing import Dict, Any, List

def create_table_sql(table: str, meta: Dict[str, Any], schema: str) -> str:
    cols = meta["columns_sql"]  # {col: sqltype}
    pk = meta.get("pk") or []
    cols_def = [f"[{c}] {t}" for c, t in cols.items()]
    pk_sql = f", CONSTRAINT [PK_{table}] PRIMARY KEY ({', '.join('['+c+']' for c in pk)})" if pk else ""
    return f"CREATE TABLE [{schema}].[{table}] (\n  " + ",\n  ".join(cols_def) + pk_sql + "\n);"

def fk_sql(table: str, fkmeta: Dict[str, Any], schema: str, idx: int) -> str:
    col = fkmeta["column"]
    rt = fkmeta["ref_table"]
    rc = fkmeta["ref_columns"][0]
    return (
        f"ALTER TABLE [{schema}].[{table}] ADD CONSTRAINT [FK_{table}_{col}_{idx}]\n"        f"  FOREIGN KEY ([{col}]) REFERENCES [{schema}].[{rt}]([{rc}]);"
    )

def index_sql(table: str, col: str, schema: str) -> str:
    return f"CREATE INDEX [IX_{table}_{col}] ON [{schema}].[{table}]([{col}]);"
