from __future__ import annotations

import json
from typing import Optional, Any
from sqlalchemy.orm import Session
from app.db.models.efd_registro import EfdRegistro
from app.db.models.efd_revisao import EfdRevisao


def _somente_digitos(txt: str | None) -> str:
    return "".join(ch for ch in str(txt or "") if ch.isdigit())


def _fmt_campo(txt: Any) -> str:
    return str(txt or "").strip()


def montar_linha_0150_de_nf(nf) -> str:
    campos = [
        "0150",
        _fmt_campo(getattr(nf, "cod_part", None)),
        _fmt_campo(getattr(nf, "participante_nome", None)),
        _fmt_campo(getattr(nf, "participante_cod_pais", None) or "1058"),
        _somente_digitos(getattr(nf, "participante_cnpj", None)),
        "",  # CPF -> não usamos no fluxo
        _fmt_campo(getattr(nf, "participante_ie", None)),
        _fmt_campo(getattr(nf, "participante_cod_mun", None)),
        _fmt_campo(getattr(nf, "participante_suframa", None)),
        _fmt_campo(getattr(nf, "participante_end", None)),
        _fmt_campo(getattr(nf, "participante_num", None)),
        _fmt_campo(getattr(nf, "participante_compl", None)),
        _fmt_campo(getattr(nf, "participante_bairro", None)),
    ]
    return "|" + "|".join(campos) + "|"


def _extrair_dados_0150(reg: EfdRegistro) -> list[str]:
    """
    efd_registro.conteudo_json esperado:
    {"dados": ["COD_PART", "NOME", "1058", "CNPJ", "", "IE", ...]}
    """
    conteudo = getattr(reg, "conteudo_json", None)
    if not conteudo:
        return []

    if isinstance(conteudo, dict):
        return list(conteudo.get("dados") or [])

    if isinstance(conteudo, str):
        try:
            obj = json.loads(conteudo)
            return list(obj.get("dados") or [])
        except Exception:
            return []

    return []


def _buscar_0150_existente_por_cod_part(
    db: Session,
    *,
    versao_id: int,
    cod_part: str,
) -> Optional[EfdRegistro]:
    if not cod_part:
        return None

    regs = (
        db.query(EfdRegistro)
        .filter(
            EfdRegistro.versao_id == versao_id,
            EfdRegistro.reg == "0150",
        )
        .order_by(EfdRegistro.linha.asc())
        .all()
    )

    cod_part = _fmt_campo(cod_part)
    for reg in regs:
        dados = _extrair_dados_0150(reg)
        if dados and _fmt_campo(dados[0] if len(dados) > 0 else "") == cod_part:
            return reg

    return None


def _buscar_0150_existente_por_cnpj(
    db: Session,
    *,
    versao_id: int,
    cnpj: str,
) -> Optional[EfdRegistro]:
    cnpj = _somente_digitos(cnpj)
    if not cnpj:
        return None

    regs = (
        db.query(EfdRegistro)
        .filter(
            EfdRegistro.versao_id == versao_id,
            EfdRegistro.reg == "0150",
        )
        .order_by(EfdRegistro.linha.asc())
        .all()
    )

    for reg in regs:
        dados = _extrair_dados_0150(reg)
        cnpj_reg = _somente_digitos(dados[3] if len(dados) > 3 else "")
        if cnpj_reg == cnpj:
            return reg

    return None


def _achar_linha_insercao_0150(
    db: Session,
    *,
    versao_id: int,
) -> int:
    """
    Insere novo 0150 logo após o último 0150 existente.
    Se não houver 0150, usa a última linha do bloco 0 conhecida.
    """
    regs_0150 = (
        db.query(EfdRegistro)
        .filter(
            EfdRegistro.versao_id == versao_id,
            EfdRegistro.reg == "0150",
        )
        .order_by(EfdRegistro.linha.desc())
        .all()
    )
    if regs_0150:
        return int(regs_0150[0].linha or 0)

    regs_bloco0 = (
        db.query(EfdRegistro)
        .filter(EfdRegistro.versao_id == versao_id)
        .order_by(EfdRegistro.linha.desc())
        .all()
    )
    if regs_bloco0:
        return int(regs_bloco0[0].linha or 0)

    return 0


def criar_revisao_insert_0150(
    db: Session,
    *,
    versao_origem_id: int,
    linha_ref: int,
    nf,
) -> EfdRevisao:
    linha_nova = montar_linha_0150_de_nf(nf)

    rv = EfdRevisao(
        versao_origem_id=int(versao_origem_id),
        versao_revisada_id=None,
        registro_id=None,
        reg="0150",
        acao="INSERT_AFTER",
        revisao_json={
            "linha_nova": linha_nova,
            "linha_referencia": int(linha_ref or 0),
            "origem": "ICMS_IPI",
            "motivo": "Participante da nota não existe no 0150 da versão",
            "chave_nfe": getattr(nf, "chave_nfe", None),
            "cod_part": getattr(nf, "cod_part", None),
            "participante_cnpj": getattr(nf, "participante_cnpj", None),
        },
        motivo_codigo="CONTRIB_PART_0150_V1",
        apontamento_id=None,
    )
    db.add(rv)
    db.flush()
    return rv


def resolver_ou_criar_0150_por_cnpj(
    db: Session,
    *,
    versao_id: int,
    nf,
) -> str:
    cod_part_nf = _fmt_campo(getattr(nf, "cod_part", None))
    cnpj_nf = _somente_digitos(getattr(nf, "participante_cnpj", None))

    print(
        "[DBG 0150 START]",
        {
            "versao_id": versao_id,
            "chave_nfe": getattr(nf, "chave_nfe", None),
            "cod_part_nf": cod_part_nf,
            "cnpj_nf": cnpj_nf,
            "participante_nome": getattr(nf, "participante_nome", None),
        },
        flush=True,
    )

    if not cnpj_nf:
        raise ValueError(
            f"NF {getattr(nf, 'chave_nfe', None)} sem participante_cnpj para resolver/criar 0150"
        )

    # 1) tenta por cod_part vindo do ICMS/IPI
    if cod_part_nf:
        reg = _buscar_0150_existente_por_cod_part(
            db,
            versao_id=versao_id,
            cod_part=cod_part_nf,
        )
        if reg:
            print(
                "[DBG 0150 MATCH COD_PART]",
                {"cod_part": cod_part_nf, "linha": getattr(reg, "linha", None)},
                flush=True,
            )
            return cod_part_nf

    # 2) tenta por CNPJ
    reg = _buscar_0150_existente_por_cnpj(
        db,
        versao_id=versao_id,
        cnpj=cnpj_nf,
    )
    if reg:
        dados = _extrair_dados_0150(reg)
        cod_part_match = _fmt_campo(dados[0] if len(dados) > 0 else "")
        if cod_part_match:
            print(
                "[DBG 0150 MATCH CNPJ]",
                {
                    "cnpj": cnpj_nf,
                    "cod_part_encontrado": cod_part_match,
                    "linha": getattr(reg, "linha", None),
                },
                flush=True,
            )
            return cod_part_match

    # 3) se não achou, cria novo 0150
    if not cod_part_nf:
        # fallback padronizado só para PJ
        cod_part_nf = f"F{cnpj_nf}"

    linha_ref = _achar_linha_insercao_0150(
        db,
        versao_id=versao_id,
    )

    rv = criar_revisao_insert_0150(
        db,
        versao_origem_id=versao_id,
        linha_ref=linha_ref,
        nf=nf,
    )

    print(
        "[DBG 0150 CRIADO]",
        {
            "rv_id": rv.id,
            "linha_ref": linha_ref,
            "cod_part": cod_part_nf,
            "cnpj": cnpj_nf,
        },
        flush=True,
    )

    return cod_part_nf