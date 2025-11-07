from __future__ import annotations

def normalize_column_name(name: str) -> str:
    # FoxPro suele usar MAYÚSCULAS; conservamos pero evitamos espacios/símbolos
    clean = ''.join(ch if ch.isalnum() or ch=='_' else '_' for ch in name)
    return clean.upper()
