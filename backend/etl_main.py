from __future__ import annotations
import os
import yaml
from typing import Dict, Any, List
from utils.log import setup_logger
from utils.dbf_utils import normalize_column_name
from extract import list_dbf_files, read_dbf
from transform import apply_transforms, rename_columns
from schema_infer import build_create_table_sql
from load import connect_sqlserver, ensure_table, bulk_insert
import validators

log = setup_logger()

def load_config(path: str) -> Dict[str, Any]:
    with open(path, 'r', encoding='utf-8') as f:
        return yaml.safe_load(f)

def process_table(cfg: Dict[str, Any], dbf_path: str, cn):
    encoding = cfg.get('encoding', 'auto')
    schema = cfg.get('schema', 'dbo')
    batch_size = int(cfg.get('batch_size', 5000))

    table_name_src = os.path.splitext(os.path.basename(dbf_path))[0].upper()
    overrides_all = cfg.get('table_overrides', {})
    tov = overrides_all.get(table_name_src, {})

    # Lectura
    columns, rows_iter = read_dbf(dbf_path, encoding)
    columns = [normalize_column_name(c) for c in columns]

    # Renombrados
    renames = tov.get('column_renames', {})
    renames_norm = {normalize_column_name(k): normalize_column_name(v) for k, v in renames.items()}

    rules = cfg.get('transform_rules', {})

    # Cosechamos una muestra para inferencia
    sample_rows = []
    rows_cache = []
    for i, r in enumerate(rows_iter):
        row = {normalize_column_name(k): r[k] for k in r}
        row = rename_columns(row, renames_norm)
        row = apply_transforms(row, rules)
        rows_cache.append(row)
        if i < 1000:  # muestra
            sample_rows.append(row)

    if not rows_cache:
        log.warning(f"{table_name_src}: archivo vacío, omitido")
        return

    # Muestras por columna
    col_samples: Dict[str, List[Any]] = {c: [] for c in rows_cache[0].keys()}
    for r in sample_rows:
        for c, v in r.items():
            col_samples[c].append(v)

    # Nombre destino
    table_name_dst = tov.get('table_name', table_name_src)
    pk = tov.get('primary_keys')
    col_overrides = tov.get('column_overrides', {})

    create_sql = build_create_table_sql(table_name_dst, schema, col_samples, pk, col_overrides)

    cursor = cn.cursor()
    ensure_table(cursor, create_sql, f"[{schema}].[{table_name_dst}]", truncate=cfg.get('truncate_before_load', False))

    # Inserción por lotes
    cols_order = list(rows_cache[0].keys())
    buf = []
    src_count = 0
    for r in rows_cache:
        buf.append([r.get(c) for c in cols_order])
        src_count += 1
        if len(buf) >= batch_size:
            bulk_insert(cursor, f"[{schema}].[{table_name_dst}]", cols_order, buf)
            buf.clear()
    if buf:
        bulk_insert(cursor, f"[{schema}].[{table_name_dst}]", cols_order, buf)

    # Validación básica
    cursor.execute(f"SELECT COUNT(1) FROM [{schema}].[{table_name_dst}]")
    dst_count = cursor.fetchone()[0]
    ok = validators.simple_rowcount_validation(src_count, dst_count)
    log.info(f"{table_name_src} → {table_name_dst}: {src_count} filas origen / {dst_count} destino | Validación: {'OK' if ok else 'MISMATCH'}")

    cn.commit()
    cursor.close()

def main():
    cfg = load_config('config.yaml')
    files = list_dbf_files(cfg['source_dir'], cfg.get('file_pattern', '*.dbf'))
    if not files:
        log.warning("No se encontraron archivos .dbf")
        return

    cn = connect_sqlserver(cfg)
    try:
        for path in files:
            log.info(f"Procesando {path} …")
            process_table(cfg, path, cn)
    finally:
        cn.close()

if __name__ == '__main__':
    main()
