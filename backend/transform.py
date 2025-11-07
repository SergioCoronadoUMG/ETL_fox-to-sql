from __future__ import annotations
from typing import Dict, Any
import datetime as dt

def normalize_value(v, rules) -> Any:
    if v is None:
        return None
    if isinstance(v, str):
        s = v.strip() if rules.get('trim_strings', True) else v
        if rules.get('empty_string_as_null', True) and s == '':
            return None
        return s
    if isinstance(v, (dt.date, dt.datetime)):
        return v  # pyodbc maneja directamente
    # Booleanos "FoxPro": T/F, Y/N, 1/0
    tvals = set(rules.get('boolean_true_values', ['T','Y','1',1,True]))
    fvals = set(rules.get('boolean_false_values', ['F','N','0',0,False,'']))
    if v in tvals:
        return True
    if v in fvals:
        return False
    return v

def apply_transforms(row: Dict[str, Any], rules: Dict[str, Any]) -> Dict[str, Any]:
    return {k: normalize_value(v, rules) for k, v in row.items()}

def rename_columns(row: Dict[str, Any], renames: Dict[str, str]) -> Dict[str, Any]:
    if not renames:
        return row
    out = {}
    for k, v in row.items():
        out[renames.get(k, k)] = v
    return out
