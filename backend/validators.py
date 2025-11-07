from __future__ import annotations
from typing import List, Any
import hashlib

def checksum_rows(rows: List[List[Any]]) -> str:
    h = hashlib.sha256()
    for r in rows:
        h.update(("|".join("" if v is None else str(v) for v in r)).encode('utf-8', 'ignore'))
    return h.hexdigest()

def simple_rowcount_validation(src_count: int, dst_count: int) -> bool:
    return src_count == dst_count
