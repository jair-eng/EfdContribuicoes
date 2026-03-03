from __future__ import annotations
from decimal import Decimal, InvalidOperation
from typing import Optional, Tuple, Union

Number = Union[int, float, Decimal]

def norm_codigo(c: Optional[str]) -> str:
    return (str(c).strip() if c is not None else "").strip()

def prioridade_por_impacto(impacto) -> Optional[str]:
    if impacto is None:
        return None
    try:
        val = Decimal(str(impacto))
    except (InvalidOperation, ValueError, TypeError):
        return None
    if val <= 0:
        return None
    if val >= Decimal("5000"):
        return "ALTA"
    if val >= Decimal("1000"):
        return "MEDIA"
    return "BAIXA"

def norm_prioridade(p) -> Optional[str]:
    if p is None:
        return None
    p = str(p).strip().upper()
    if p == "MÉDIA":
        p = "MEDIA"
    return p if p in ("ALTA", "MEDIA", "BAIXA") else None

def safe_float(x) -> Optional[float]:
    if x is None:
        return None
    try:
        return float(x)
    except Exception:
        try:
            return float(str(x).replace(",", "."))
        except Exception:
            return None

def key_apontamento(registro_id: Optional[int], tipo: str, codigo: Optional[str]) -> Tuple[int, str, str]:
    rid = int(registro_id) if registro_id is not None else 0
    return (rid, str(tipo), norm_codigo(codigo))