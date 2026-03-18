from __future__ import annotations

from typing import Optional
from sqlalchemy.orm import Session
from app.db.models import EfdRevisao, EfdRegistro
from app.services.versao_overlay_service import carregar_linhas_logicas_com_revisoes_e_insert
from app.sped.revisao_overlay import LinhaLogica



def _norm(s: Optional[str]) -> str:
    return (s or "").strip()


def _split_sped_line(linha: str) -> list[str]:
    return (linha or "").strip().strip("|").split("|")

def _norm_upper(s: Optional[str]) -> str:
    return _norm(s).upper()


def _carregar_bloco0_logico(
    db: Session,
    *,
    versao_origem_id: int,
) -> list[LinhaLogica]:
    linhas = carregar_linhas_logicas_com_revisoes_e_insert(
        db,
        versao_origem_id=int(versao_origem_id),
        versao_final_id=None,
    )

    return [
        l
        for l in linhas
        if str(getattr(l, "reg", "")).upper() in {
            "0000","0001","0100","0110","0140","0150","0190","0200","0205","0206","0220","0990"
        }
    ]


def _texto_linha_logica(l: LinhaLogica) -> str:
    conteudo = getattr(l, "conteudo", None)
    if conteudo:
        return conteudo

    rj = getattr(l, "revisao_json", None) or {}
    if isinstance(rj, dict):
        linha_nova = rj.get("linha_nova")
        if linha_nova:
            return linha_nova

    texto = getattr(l, "texto", None)
    if texto:
        return texto

    linha_original = getattr(l, "linha_original", None)
    if linha_original:
        return linha_original

    return ""


def buscar_linha_0190_icms_por_unid(linhas_icms: list[str], unid: str) -> str | None:
    alvo = (unid or "").strip().upper()
    for ln in linhas_icms:
        if not (ln or "").startswith("|0190|"):
            continue
        campos = ln.strip().strip("|").split("|")
        if len(campos) >= 2 and (campos[1] or "").strip().upper() == alvo:
            return ln
    return None


def buscar_linha_0200_icms_por_cod_item(linhas_icms: list[str], cod_item: str) -> str | None:
    alvo = (cod_item or "").strip()
    for ln in linhas_icms:
        if not (ln or "").startswith("|0200|"):
            continue
        campos = ln.strip().strip("|").split("|")
        if len(campos) >= 2 and (campos[1] or "").strip() == alvo:
            return ln
    return None

def _existe_0190_na_versao(
    db: Session,
    *,
    versao_origem_id: int,
    unid: str,
) -> bool:
    unid_final = _norm_upper(unid)
    if not unid_final:
        return False

    # 1) overlay lógico
    linhas = carregar_linhas_logicas_com_revisoes_e_insert(
        db,
        versao_origem_id=int(versao_origem_id),
        versao_final_id=None,
    )

    for l in linhas:
        if str(getattr(l, "reg", "")).upper() != "0190":
            continue

        texto = _texto_linha_logica(l)
        if texto:
            campos = _split_sped_line(texto)
            if len(campos) >= 2 and _norm_upper(campos[1]) == unid_final:
                return True

    # 2) revisões pendentes
    revisoes = (
        db.query(EfdRevisao)
        .filter(
            EfdRevisao.versao_origem_id == int(versao_origem_id),
            EfdRevisao.reg == "0190",
            EfdRevisao.acao.in_(["INSERT_AFTER", "INSERT_BEFORE"]),
        )
        .all()
    )

    for rv in revisoes:
        texto = ((rv.revisao_json or {}).get("linha_nova") or "").strip()
        if texto.startswith("|0190|"):
            campos = _split_sped_line(texto)
            if len(campos) >= 2 and _norm_upper(campos[1]) == unid_final:
                return True

    # 3) fallback direto no registro base
    regs = (
        db.query(EfdRegistro)
        .filter(
            EfdRegistro.versao_id == int(versao_origem_id),
            EfdRegistro.reg == "0190",
        )
        .all()
    )

    for r in regs:
        # 3a) tenta por conteudo texto
        texto = getattr(r, "conteudo", None)
        if isinstance(texto, str):
            texto = texto.strip()
        else:
            texto = ""

        if texto:
            campos = _split_sped_line(texto)
            if len(campos) >= 2 and _norm_upper(campos[1]) == unid_final:
                return True

        # 3b) fallback por conteudo_json / dados
        cj = getattr(r, "conteudo_json", None) or {}
        dados = cj.get("dados") if isinstance(cj, dict) else None
        if isinstance(dados, list) and len(dados) >= 1:
            unid_reg = _norm_upper(dados[0])
            if unid_reg == unid_final:
                return True

    return False
def _existe_0200_na_versao(
    db: Session,
    *,
    versao_origem_id: int,
    cod_item: str,
) -> bool:
    cod_item_final = _norm(cod_item)
    if not cod_item_final:
        return False

    # 1) overlay lógico
    linhas = carregar_linhas_logicas_com_revisoes_e_insert(
        db,
        versao_origem_id=int(versao_origem_id),
        versao_final_id=None,
    )

    for l in linhas:
        if str(getattr(l, "reg", "")).upper() != "0200":
            continue

        texto = _texto_linha_logica(l)
        if texto:
            campos = _split_sped_line(texto)
            if len(campos) >= 2 and _norm(campos[1]) == cod_item_final:
                return True

    # 2) revisões pendentes
    revisoes = (
        db.query(EfdRevisao)
        .filter(
            EfdRevisao.versao_origem_id == int(versao_origem_id),
            EfdRevisao.reg == "0200",
            EfdRevisao.acao.in_(["INSERT_AFTER", "INSERT_BEFORE"]),
        )
        .all()
    )

    for rv in revisoes:
        texto = ((rv.revisao_json or {}).get("linha_nova") or "").strip()
        if texto.startswith("|0200|"):
            campos = _split_sped_line(texto)
            if len(campos) >= 2 and _norm(campos[1]) == cod_item_final:
                return True

    # 3) fallback direto no registro base
    regs = (
        db.query(EfdRegistro)
        .filter(
            EfdRegistro.versao_id == int(versao_origem_id),
            EfdRegistro.reg == "0200",
        )
        .all()
    )

    for r in regs:
        texto = getattr(r, "conteudo", None)
        if isinstance(texto, str):
            texto = texto.strip()
        else:
            texto = ""

        if texto:
            campos = _split_sped_line(texto)
            if len(campos) >= 2 and _norm(campos[1]) == cod_item_final:
                return True

        cj = getattr(r, "conteudo_json", None) or {}
        dados = cj.get("dados") if isinstance(cj, dict) else None
        if isinstance(dados, list) and len(dados) >= 1:
            cod_item_reg = _norm(dados[0])
            if cod_item_reg == cod_item_final:
                return True

    return False