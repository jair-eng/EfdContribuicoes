
from __future__ import annotations

from decimal import Decimal, ROUND_HALF_UP
from typing import Any, Dict, List, Tuple


def _key_chave_cod_item_norm(row: Dict[str, Any]) -> Tuple[str, str]:
    chave = _digits(row.get("chave"))
    cod_item_norm = _s(row.get("cod_item_norm"))
    return chave, cod_item_norm


def _key_chave_num_item(row: Dict[str, Any]) -> Tuple[str, str]:
    chave = _digits(row.get("chave"))
    num_item = _s(row.get("num_item"))
    return chave, num_item

def _s(v: Any) -> str:
    return str(v or "").strip()


def _digits(v: Any) -> str:
    return "".join(ch for ch in str(v or "") if ch.isdigit())


def _dec(v: Any) -> Decimal:
    if isinstance(v, Decimal):
        return v
    if v in (None, "", False):
        return Decimal("0")
    try:
        return Decimal(str(v))
    except Exception:
        return Decimal("0")


def _q2(v: Decimal) -> Decimal:
    return v.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)


def _key_chave_cod_item(row: Dict[str, Any]) -> Tuple[str, str]:
    chave = _digits(row.get("chave"))
    cod_item = _s(row.get("cod_item"))
    return chave, cod_item


def _key_chave(row: Dict[str, Any]) -> str:
    return _digits(row.get("chave"))


def _participante(row: Dict[str, Any]) -> str:
    return (
        _s(row.get("participante_cnpj"))
        or _s(row.get("participante_cpf"))
        or _s(row.get("participante"))
    )
