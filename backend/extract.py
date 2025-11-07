from __future__ import annotations
import os
from glob import glob
from typing import Dict, Any, Iterable, Tuple, List

from chardet.universaldetector import UniversalDetector

# Intentamos primero con dbfread; si falla, usamos `dbf` (Ethan Furman)
try:
    from dbfread import DBF  # soporta .FPT
    DBFREADER = 'dbfread'
except Exception:  # pragma: no cover
    DBFREADER = 'dbf'
    import dbf

def detect_encoding(path: str) -> str:
    det = UniversalDetector()
    with open(path, 'rb') as f:
        for chunk in iter(lambda: f.read(4096), b''):
            det.feed(chunk)
            if det.done:
                break
    det.close()
    return det.result.get('encoding') or 'cp1252'

def list_dbf_files(source_dir: str, pattern: str) -> List[str]:
    return sorted(glob(os.path.join(source_dir, pattern)))

def read_dbf(path: str, encoding: str = 'auto') -> Tuple[List[str], Iterable[Dict[str, Any]]]:
    # Determinamos encoding si es auto
    codec = detect_encoding(path) if encoding == 'auto' else encoding

    if DBFREADER == 'dbfread':
        table = DBF(path, char_decode_errors='ignore', encoding=codec, load=True)
        columns = [f.name for f in table.fields]
        rows = (dict(r) for r in table)
        return columns, rows
    else:  # librería dbf
        t = dbf.Table(path)
        t.open()
        columns = [f.name for f in t.structure()]
        def gen():
            for rec in t:
                yield {k: rec[k] for k in columns}
        return columns, gen()
