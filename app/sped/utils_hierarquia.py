from __future__ import annotations

from typing import Any, Iterable, Dict, Optional
from sqlalchemy.orm import Session

from app.db.models import EfdRegistro
from app.sped.logic.consolidador import obter_conteudo_final


def _ind_oper_pai_c100_db(db: Session, rid: int) -> str:
    try:
        r170 = db.get(EfdRegistro, int(rid))
        if not r170 or not getattr(r170, "pai_id", None):
            return ""

        r100 = db.get(EfdRegistro, int(r170.pai_id))
        if not r100 or getattr(r100, "reg", "") != "C100":
            return ""

        cj = getattr(r100, "conteudo_json", None) or {}
        dados100 = cj.get("dados") if isinstance(cj, dict) else None
        if not isinstance(dados100, list) or len(dados100) < 1:
            return ""

        return str(dados100[0] or "").strip()
    except Exception:
        return ""


def _build_mapa_linhas_por_id(linhas: Iterable[Any]) -> Dict[int, Any]:
    mapa: Dict[int, Any] = {}
    for ln in linhas or []:
        lid = getattr(ln, "registro_id", None) or getattr(ln, "id", None)
        if lid:
            try:
                mapa[int(lid)] = ln
            except Exception:
                pass
    return mapa


def _reg_da_linha(ln: Any) -> str:
    reg = str(getattr(ln, "reg", "") or "").strip()
    if reg:
        return reg

    conteudo = obter_conteudo_final(ln) or ""
    if conteudo.startswith("|"):
        partes = conteudo.split("|")
        if len(partes) > 1:
            return str(partes[1] or "").strip()

    return ""


def _dados_da_linha(ln: Any) -> list:
    cj = getattr(ln, "conteudo_json", None) or {}
    if isinstance(cj, dict):
        dados = cj.get("dados")
        if isinstance(dados, list):
            return dados

    conteudo = obter_conteudo_final(ln) or ""
    if conteudo.startswith("|"):
        partes = conteudo.split("|")
        if len(partes) >= 3:
            return partes[2:-1] if conteudo.endswith("|") else partes[2:]

    return []


def _ind_oper_pai_c100_linha(linha: Any, linhas: Iterable[Any]) -> str:
    try:
        mapa = _build_mapa_linhas_por_id(linhas)
        atual = linha
        saltos = 0
        max_saltos = 10

        while atual and saltos < max_saltos:
            pai_id = getattr(atual, "pai_id", None)
            if not pai_id:
                return ""

            pai = mapa.get(int(pai_id))
            if not pai:
                return ""

            if _reg_da_linha(pai) == "C100":
                dados100 = _dados_da_linha(pai)
                if not isinstance(dados100, list) or len(dados100) < 1:
                    return ""
                return str(dados100[0] or "").strip()

            atual = pai
            saltos += 1

        return ""
    except Exception:
        return ""


def resolver_ind_oper_c100_com_fallback(
    *,
    db: Session,
    linha: Any,
    rid: int,
    linhas: Iterable[Any],
) -> str:
    ind_oper = ""

    if rid:
        ind_oper = _ind_oper_pai_c100_db(db, rid)
        if ind_oper:
            return ind_oper

    return _ind_oper_pai_c100_linha(linha, linhas)