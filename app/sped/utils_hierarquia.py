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
        linhas_list = list(linhas or [])
        if not linhas_list:
            return ""

        # -------------------------------------------------
        # 1) tenta primeiro pela cadeia pai_id, se existir
        # -------------------------------------------------
        mapa = _build_mapa_linhas_por_id(linhas_list)
        atual = linha
        saltos = 0
        max_saltos = 10

        while atual and saltos < max_saltos:
            pai_id = getattr(atual, "pai_id", None)
            if not pai_id:
                break

            pai = mapa.get(int(pai_id))
            if not pai:
                break

            if _reg_da_linha(pai) == "C100":
                dados100 = _dados_da_linha(pai)
                if not isinstance(dados100, list) or len(dados100) < 1:
                    return ""
                return str(dados100[0] or "").strip()

            atual = pai
            saltos += 1

        # -------------------------------------------------
        # 2) fallback real: procura o C100 anterior na lista
        # -------------------------------------------------
        idx_atual = None

        # tenta por identidade do objeto
        for i, ln in enumerate(linhas_list):
            if ln is linha:
                idx_atual = i
                break

        # fallback por atributos, se identidade não bater
        if idx_atual is None:
            linha_num = getattr(linha, "linha", None)
            reg = str(getattr(linha, "reg", "") or "").upper()
            revisao_id = getattr(linha, "revisao_id", None)

            for i, ln in enumerate(linhas_list):
                if (
                    getattr(ln, "linha", None) == linha_num
                    and str(getattr(ln, "reg", "") or "").upper() == reg
                    and getattr(ln, "revisao_id", None) == revisao_id
                ):
                    idx_atual = i
                    break

        if idx_atual is None:
            return ""

        for j in range(idx_atual - 1, -1, -1):
            ln = linhas_list[j]
            reg_ln = _reg_da_linha(ln)

            if reg_ln == "C100":
                dados100 = _dados_da_linha(ln)
                print("[DBG C100 DADOS PAI]", dados100, flush=True)
                if not isinstance(dados100, list) or len(dados100) < 1:
                    return ""
                return str(dados100[0] or "").strip()

            # se chegou no começo do bloco/fim do bloco, para
            if reg_ln in {"C001", "C990"}:
                break

        return ""

    except Exception as e:
        print(f"[DBG _ind_oper_pai_c100_linha ERRO] {e}", flush=True)
        return ""


def resolver_ind_oper_c100_com_fallback(
    *,
    db: Session,
    linha: Any,
    rid: int,
    linhas: Iterable[Any],
) -> str:
    ind_oper = _ind_oper_pai_c100_linha(linha, linhas)
    if ind_oper:
        return ind_oper

    if rid:
        ind_oper = _ind_oper_pai_c100_db(db, rid)
        if ind_oper:
            return ind_oper

    return ""