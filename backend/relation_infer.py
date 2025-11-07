from __future__ import annotations
from typing import Dict, Any, List, Tuple
from collections import defaultdict
from discovery_utils import fk_name_hints

def build_lookup_pk_values(table_pk_values: Dict[str, set], schema_data: Dict[str, Any]):
    """table_pk_values: {table: set(pk_values)} para pks de 1 columna. Compuesto no implementado aquí por simplicidad."""
    for t, meta in schema_data.items():
        pk = meta.get("pk") or []
        if len(pk) == 1:
            col = pk[0]
            values = set(meta.get("samples", {}).get(col, []))
            table_pk_values[t] = values

def infer_fks(schema_data: Dict[str, Any], fk_match_threshold: float = 0.95) -> Dict[str, List[Dict[str, Any]]]:
    """Intenta descubrir FKs tabla/col -> ref tabla(col) por nombre + cobertura de valores (1 columna)."""
    table_pk_values: Dict[str, set] = {}
    build_lookup_pk_values(table_pk_values, schema_data)
    fks = defaultdict(list)

    tables = list(schema_data.keys())
    for t in tables:
        meta = schema_data[t]
        cols = meta["columns"]
        pk = meta.get("pk") or []
        for c in cols:
            if pk and c in pk:
                continue
            # Sólo columnas no totalmente vacías
            col_samples = meta.get("samples", {}).get(c, [])
            if not col_samples:
                continue
            # Probar referencia contra todas las tablas con PK simple
            best = (0.0, None)
            for rt, pkvals in table_pk_values.items():
                if rt == t or not pkvals:
                    continue
                matches = sum(1 for v in col_samples if v in pkvals)
                coverage = matches / max(1, len(col_samples))
                score = coverage + (0.05 * fk_name_hints(c, rt))  # +5% por nombre sugerente
                if score > best[0]:
                    best = (score, (rt, [schema_data[rt]["pk"][0]]))
            if best[1] and best[0] >= fk_match_threshold:
                ref_table, ref_cols = best[1]
                fks[t].append({"column": c, "ref_table": ref_table, "ref_columns": ref_cols, "score": round(best[0],3)})
    return fks
