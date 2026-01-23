# app/sped/c170_utils.py
from __future__ import annotations
import re
from typing import Any, Optional

from app.sped.layouts.c170 import LAYOUT_C170

_RE_CFOP = re.compile(r"^\d{4}$")
_RE_CST = re.compile(r"^\d{2}$")


def _ensure_len(campos: list[Any], idx: int, label: str) -> None:
    if idx < 0 or idx >= len(campos):
        raise ValueError(
            f"C170 inválido: índice {label}={idx} fora do range (len={len(campos)}). "
            "Ajuste os índices em app/sped/layouts/c170.py."
        )


def _norm_str(v: Any) -> str:
    return "" if v is None else str(v).strip()


def validar_cfop(cfop: str) -> str:
    c = _norm_str(cfop)
    if not _RE_CFOP.match(c):
        raise ValueError("CFOP inválido: esperado 4 dígitos (ex: 1102).")
    return c


def validar_cst(cst: str) -> str:
    c = _norm_str(cst)
    if not _RE_CST.match(c):
        raise ValueError("CST inválido: esperado 2 dígitos (ex: 06, 50).")
    return c


def patch_c170_campos(
    campos: list[Any],
    *,
    cfop: Optional[str] = None,
    cst_pis: Optional[str] = None,
    cst_cofins: Optional[str] = None,
) -> list[str]:
    """
    Retorna nova lista de campos (strings) com patch aplicado.
    Não muda ordem nem tamanho.
    """
    if not isinstance(campos, list):
        raise ValueError("C170 inválido: esperado lista em conteudo_json['dados'].")

    novos = ["" if c is None else str(c) for c in campos]

    # valida índices
    _ensure_len(novos, LAYOUT_C170.idx_cfop, "cfop")
    _ensure_len(novos, LAYOUT_C170.idx_cst_pis, "cst_pis")
    _ensure_len(novos, LAYOUT_C170.idx_cst_cofins, "cst_cofins")

    if cfop is not None:
        novos[LAYOUT_C170.idx_cfop] = validar_cfop(cfop)

    if cst_pis is not None:
        novos[LAYOUT_C170.idx_cst_pis] = validar_cst(cst_pis)

    if cst_cofins is not None:
        novos[LAYOUT_C170.idx_cst_cofins] = validar_cst(cst_cofins)

    return novos
