from __future__ import annotations
import os, json
from typing import Dict, Any, List
import yaml
from utils.log import setup_logger
from utils.dbf_utils import normalize_column_name
from extract import list_dbf_files, read_dbf
from transform import apply_transforms, rename_columns
from schema_infer import infer_sql_type
from profile import profile_columns
from key_infer import infer_pk_for_table
from relation_infer import infer_fks
from ddl_writer import create_table_sql, fk_sql, index_sql

log = setup_logger()

def load_config(path: str) -> Dict[str, Any]:
    with open(path, 'r', encoding='utf-8') as f:
        return yaml.safe_load(f)

def sample_values_for_col(rows: List[Dict[str, Any]], col: str, limit: int = 20000):
    out = []
    for r in rows[:limit]:
        v = r.get(col)
        if v is not None and v != "":
            out.append(v)
    return out

def main():
    cfg = load_config('config.yaml')
    files = list_dbf_files(cfg['source_dir'], cfg.get('file_pattern', '*.dbf'))
    if not files:
        log.warning("No se encontraron archivos .dbf")
        return

    discovery_cfg = (cfg.get('discovery') or {})
    sample_rows = int(discovery_cfg.get('sample_rows', 20000))
    pk_thr = float(discovery_cfg.get('pk_uniqueness_threshold', 1.0))
    fk_thr = float(discovery_cfg.get('fk_match_threshold', 0.95))
    max_pk_combo = int(discovery_cfg.get('max_pk_combo', 2))

    schema_meta: Dict[str, Any] = {}  # table -> meta

    # 1) Perfiles y tipos
    for path in files:
        table_src = os.path.splitext(os.path.basename(path))[0].upper()
        tov = (cfg.get('table_overrides') or {}).get(table_src, {})
        table_dst = tov.get('table_name', table_src)
        renames = tov.get('column_renames', {})
        renames_norm = {normalize_column_name(k): normalize_column_name(v) for k, v in renames.items()}
        rules = cfg.get('transform_rules', {})

        # Leer filas y aplicar transformaciones
        columns, rows_iter = read_dbf(path, cfg.get('encoding', 'auto'))
        columns = [normalize_column_name(c) for c in columns]
        rows_cache = []
        for i, r in enumerate(rows_iter):
            row = {normalize_column_name(k): r[k] for k in r}
            row = rename_columns(row, renames_norm)
            row = apply_transforms(row, rules)
            rows_cache.append(row)
            if i >= sample_rows:
                break

        if not rows_cache:
            log.warning(f"{table_src}: vacío (muestreo), omitido")
            continue

        # Perfil
        stats, nrows = profile_columns(rows_cache, columns, max_rows=sample_rows)
        # Tipos SQL por muestra de valores por columna
        col_samples = {c: [r.get(c) for r in rows_cache[:min(len(rows_cache), 1000)]] for c in columns}
        columns_sql = {}
        for c in columns:
            columns_sql[c] = infer_sql_type(col_samples[c])

        schema_meta[table_dst] = {
            "source_table": table_src,
            "columns": columns,
            "columns_sql": columns_sql,
            "stats": stats,
            "samples": {c: sample_values_for_col(rows_cache, c, 5000) for c in columns},  # para FK matching
        }

    # 2) PKs
    for t, meta in schema_meta.items():
        pk = infer_pk_for_table(t, meta["columns"], meta["stats"], uniqueness_threshold=pk_thr, max_combo=max_pk_combo)
        if pk:
            meta["pk"] = pk

    # 3) FKs
    fks = infer_fks(schema_meta, fk_match_threshold=fk_thr)
    for t, lst in fks.items():
        schema_meta[t].setdefault("fks", []).extend(lst)

    # 4) Generar schema.sql
    schema = cfg.get('schema', 'dbo')
    lines: List[str] = []
    for t, meta in schema_meta.items():
        lines.append(create_table_sql(t, meta, schema))
        lines.append("")  # espacio

    # FKs al final
    for t, meta in schema_meta.items():
        for idx, fk in enumerate(meta.get("fks", []), start=1):
            lines.append(fk_sql(t, fk, schema, idx))
    lines.append("")
    # Índices por FKs
    for t, meta in schema_meta.items():
        for fk in meta.get("fks", []):
            lines.append(index_sql(t, fk["column"], schema))

    with open("schema.json", "w", encoding="utf-8") as f:
        f.write(json.dumps(schema_meta, ensure_ascii=False, indent=2))

    with open("schema.sql", "w", encoding="utf-8") as f:
        f.write("\n".join(lines))

    log.info("Esquema descubierto → archivos generados: schema.json, schema.sql")


if __name__ == '__main__':
    main()
