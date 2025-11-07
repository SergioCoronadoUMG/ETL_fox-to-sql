# ETL FoxPro → SQL Server (Python)

## Requisitos
- Python 3.10+
- SQL Server (local o remoto) con ODBC Driver 17/18 instalado
- Paquetes Python: `pip install -r requirements.txt`

## Configura
1. Copia tus `.DBF` (y `.FPT` si hay memo) en `source_dir` (config.yaml).
2. Edita `config.yaml` (conexión, reglas, overrides por tabla).

## Ejecuta
```bash
python etl_main.py
```

## Notas
- Se infiere el tipo SQL a partir de muestras (hasta 1000 filas) y se puede forzar por columna.
- Carga por lotes (`fast_executemany`) para alto rendimiento.
- Validación básica de conteo; puedes expandir en `validators.py`.

## Descubrimiento automático de PK/FK (nuevo)
Genera `schema.json` y `schema.sql` infiriendo **claves primarias** (unicidad) y **claves foráneas** (cobertura de valores + heurística de nombre):

```bash
python discover_schema.py
```
Ajusta parámetros en `config.yaml → discovery`:
- `sample_rows`: filas máximas a muestrear por tabla
- `pk_uniqueness_threshold`: 1.0 para 100% único; usa 0.999 si hay pequeños duplicados
- `fk_match_threshold`: 0.95 cobertura mínima de coincidencias para declarar FK
- `max_pk_combo`: hasta 2 columnas para PK compuesta

Salida:
- `schema.json`: metadatos por tabla (columnas, tipos sugeridos, PK, FKs)
- `schema.sql`: DDL SQL Server con `CREATE TABLE`, `ALTER TABLE ... ADD CONSTRAINT` y `CREATE INDEX` para FKs
```
