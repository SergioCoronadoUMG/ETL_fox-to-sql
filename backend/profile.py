from __future__ import annotations
from typing import Dict, Any, List, Tuple
import datetime as dt
import decimal

def profile_columns(rows_iter, columns: List[str], max_rows: int = 20000):
    """Devuelve perfil simple por columna: n, nulls, distinct_count, tipos vistos, max_len."""
    stats = {
        c: {
            "count": 0,
            "nulls": 0,
            "distinct": set(),
            "types": set(),
            "max_len": 0
        } for c in columns
    }
    total = 0
    for r in rows_iter:
        total += 1
        if total > max_rows:
            break
        for c in columns:
            v = r.get(c)
            s = stats[c]
            s["count"] += 1
            if v is None or v == "":
                s["nulls"] += 1
            else:
                s["distinct"].add(v)
                s["types"].add(type(v).__name__)
                if isinstance(v, (str,)):
                    s["max_len"] = max(s["max_len"], len(v))
                else:
                    s["max_len"] = max(s["max_len"], len(str(v)))
    # Convert sets to sizes/strings
    for c in columns:
        s = stats[c]
        s["distinct_count"] = len(s["distinct"])
        s["types"] = sorted(list(s["types"]))
        del s["distinct"]
    return stats, total
