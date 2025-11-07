from __future__ import annotations
from typing import Dict, Any, List
import datetime as dt
import decimal

# Mapeo base de tipos FoxPro/DBF -> SQL Server
# Ajustamos tamaños en tiempo de inferencia

def infer_sql_type(sample_values: List[Any]) -> str:
    """Dada una muestra de valores por columna, sugiere el tipo SQL óptimo."""
    non_null = [v for v in sample_values if v is not None]
    if not non_null:
        return "NVARCHAR(255)"  # conservador

    # Si todos son bool
    if all(isinstance(v, bool) for v in non_null):
        return "BIT"

    # Fechas/DateTime
    if all(isinstance(v, (dt.date, dt.datetime)) for v in non_null):
        if any(isinstance(v, dt.datetime) for v in non_null):
            return "DATETIME2"
        return "DATE"

    # Numéricos (int/float/Decimal)
    if all(isinstance(v, (int, float, decimal.Decimal)) for v in non_null):
        # Busca precisión/escala
        max_digits = 0
        max_scale = 0
        for v in non_null:
            s = str(v)
            if "." in s:
                i, f = s.split(".", 1)
                max_digits = max(max_digits, len(i) + len(f))
                max_scale = max(max_scale, len(f))
            else:
                max_digits = max(max_digits, len(s))
        precision = min(max(10, max_digits), 38)
        scale = min(max_scale, 18)
        if scale == 0:
            return "BIGINT" if precision > 9 else "INT"
        return f"DECIMAL({precision},{scale})"

    # Strings
    # Calcula long máx; si supera 4000 -> NVARCHAR(MAX)
    max_len = max(len(str(v)) for v in non_null)
    if max_len > 4000:
        return "NVARCHAR(MAX)"
    sizes = [50, 100, 255, 500, 1000, 2000, 4000]
    target = next((s for s in sizes if max_len <= s), 4000)
    return f"NVARCHAR({target})"

def build_create_table_sql(table: str, schema: str, columns_samples: Dict[str, List[Any]],
                           pk: List[str] | None, overrides: Dict[str, str] | None) -> str:
    cols_sql = []
    for col, sample in columns_samples.items():
        if overrides and col in overrides:
            sqlt = overrides[col]
        else:
            sqlt = infer_sql_type(sample)
        cols_sql.append(f"[{col}] {sqlt}")
    pk_sql = f", CONSTRAINT [PK_{table}] PRIMARY KEY({', '.join(f'[{c}]' for c in (pk or []))})" if pk else ""
    cols_blob = ",\n  ".join(cols_sql)
    return f"CREATE TABLE [{schema}].[{table}] (\n  {cols_blob}{pk_sql}\n);"
