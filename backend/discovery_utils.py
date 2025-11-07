from __future__ import annotations
from typing import Dict, Any, Iterable, List, Tuple, Set
import itertools

def is_boolish_name(name: str) -> bool:
    n = name.upper()
    return n.startswith("ES_") or n.startswith("IS_") or n.endswith("_OK") or n.endswith("_FLAG")

def likely_pk_names() -> List[str]:
    return ["ID", "ID_", "CODIGO", "COD", "CLAVE"]

def column_name_hints(col: str, table: str) -> int:
    """Pequeño puntaje por heurística de nombre: ID, <tabla>_ID, etc."""
    c = col.upper()
    t = table.upper()
    score = 0
    if c == "ID" or c.endswith("_ID"):
        score += 2
    if c == f"{t}_ID":
        score += 2
    for hint in likely_pk_names():
        if c == hint or c.startswith(hint + "_"):
            score += 1
    return score

def fk_name_hints(col: str, ref_table: str) -> int:
    c = col.upper()
    rt = ref_table.upper()
    score = 0
    if c == "ID_" + rt or c == rt + "_ID" or c.endswith("_ID"):
        score += 2
    if rt[:-1] + "_ID" in c or rt + "_ID" in c:
        score += 1
    return score
