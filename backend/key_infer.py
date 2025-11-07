from __future__ import annotations
from typing import Dict, Any, List, Tuple
import itertools
from discovery_utils import column_name_hints

def infer_pk_for_table(table: str, columns: List[str], stats: Dict[str, Any],
                       uniqueness_threshold: float = 1.0, max_combo: int = 2) -> List[str] | None:
    """Busca PK por unicidad y nulls bajas, usando heurísticas de nombres y combos pequeñas."""
    n = None
    for s in stats.values():
        n = s["count"]
        break
    if not n:
        return None

    # Candidatos de 1 columna
    scored = []
    for c in columns:
        s = stats[c]
        if s["nulls"] == 0 and s["distinct_count"] / max(1, n) >= uniqueness_threshold:
            scored.append((column_name_hints(c, table), [c]))
    scored.sort(reverse=True, key=lambda x: (x[0], len(x[1])))
    if scored:
        return scored[0][1]

    # Combos de 2 columnas
    if max_combo >= 2 and len(columns) >= 2:
        best = ( -1, None )
        for a, b in itertools.combinations(columns, 2):
            s1 = stats[a]; s2 = stats[b]
            # Heurística rápida: si la suma de distincts >= n, posible unicidad
            approx = min(n, s1["distinct_count"] * s2["distinct_count"])
            if approx / max(1, n) >= uniqueness_threshold and s1["nulls"] == 0 and s2["nulls"] == 0:
                score = column_name_hints(a, table) + column_name_hints(b, table)
                if score > best[0]:
                    best = (score, [a, b])
        if best[1]:
            return best[1]
    return None
