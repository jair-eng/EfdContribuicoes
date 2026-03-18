from __future__ import annotations


from typing import Any, Dict, Optional, List
from app.config.settings import ALIQUOTA_PIS_PCT, ALIQUOTA_COFINS_PCT
from decimal import Decimal, ROUND_HALF_UP
from sqlalchemy.orm import Session
from app.db.models import NfIcmsItem, EfdRevisao, NfIcmsBase
from typing import TYPE_CHECKING
from app.icms_ipi.icms_helpers import fmt_sped_num,q2
from app.services.versao_overlay_service import carregar_linhas_logicas_com_revisoes_e_insert
from app.sped.revisao_overlay import LinhaLogica
import logging

log = logging.getLogger(__name__)

if TYPE_CHECKING:
    from app.icms_ipi.icms_ipi_cruzamento_service import DocCtx

def montar_linha_c170_de_icms(item):
    vl_item = q2(item.vl_item)
    vl_desc = q2(item.vl_desc)
    vl_icms = q2(item.vl_icms)

    base = q2(vl_item - vl_desc)

    vl_pis = q2(base * (ALIQUOTA_PIS_PCT / Decimal("100")))
    vl_cofins = q2(base * (ALIQUOTA_COFINS_PCT / Decimal("100")))

    qtd = getattr(item, "qtd", None)
    unid = getattr(item, "unid", None)
    cst_icms = getattr(item, "cst_icms", None)
    aliq_icms = getattr(item, "aliq_icms", None)
    cod_nat = getattr(item, "cod_nat", None)
    cod_cta = getattr(item, "cod_cta", None)


    campos = [
        "C170",
        item.num_item or "1",
        item.cod_item,
        item.descricao or "",
        fmt_sped_num(qtd, casas=4) if qtd not in (None, "") else "0,0000",
        unid or "",
        fmt_sped_num(vl_item),
        fmt_sped_num(vl_desc),
        "0",
        str(cst_icms or "").strip(),
        item.cfop or "",
        str(cod_nat or "").strip(),
        fmt_sped_num(vl_item),
        fmt_sped_num(aliq_icms) if aliq_icms not in (None, "") else "0,00",
        fmt_sped_num(vl_icms),
        "0,00",
        "0,00",
        "0,00",
        "",
        "",
        "",
        "0,00",
        "0,00",
        "0,00",
        "51",
        fmt_sped_num(base),
        fmt_sped_num(ALIQUOTA_PIS_PCT),
        "",
        "",
        fmt_sped_num(vl_pis),
        "51",
        fmt_sped_num(base),
        fmt_sped_num(ALIQUOTA_COFINS_PCT),
        "",
        "",
        fmt_sped_num(vl_cofins),
        (str(cod_cta).strip() if cod_cta else ""),
    ]

    # logs úteis de inconsistência
    if not item.cod_item:
        log.warning(
            "C170 sem cod_item item_id=%s num_item=%s cfop=%s descricao=%s",
            item.id,
            item.num_item,
            item.cfop,
            item.descricao,
        )

    if base < 0:
        log.warning(
            "C170 com base negativa item_id=%s cod_item=%s vl_item=%s vl_desc=%s base=%s",
            item.id,
            item.cod_item,
            vl_item,
            vl_desc,
            base,
        )

    while len(campos) < 37:
        campos.append("")
    if len(campos) != 37:
        log.error(
            "C170 inválido antes de montar linha item_id=%s cod_item=%s len_campos=%s",
            item.id,
            item.cod_item,
            len(campos),
        )
        raise ValueError(f"C170 inválido: esperado 37 campos, veio {len(campos)}")

    linha = "|" + "|".join(campos) + "|"

    # debug curto e útil
    log.debug(
        "C170 montado item_id=%s cod_item=%s cfop=%s cst_pis=%s cst_cofins=%s base=%s vl_pis=%s vl_cofins=%s",
        item.id,
        item.cod_item,
        item.cfop,
        campos[24],
        campos[30],
        campos[25],
        campos[29],
        campos[35],
    )

    return linha


def _registro_insercao_alvo(
    doc_ctx: DocCtx,
    itens_c170: list[Dict[str, Any]],
) -> tuple[int | None, int]:
    """
    Se houver C170, insere após o último C170.
    Senão, insere após o C100.
    Retorna: (registro_id_alvo, linha_ref)
    """
    if itens_c170:
        ultimo = itens_c170[-1]
        return (
            int(ultimo.get("registro_id") or 0) or None,
            int(ultimo.get("linha_num") or 0) or doc_ctx.linha_c100,
        )

    return (
        doc_ctx.registro_id_c100,
        doc_ctx.linha_c100,
    )


def _criar_revisao_insert_c170_faltante(
    db: Session,
    *,
    versao_origem_id: int,
    registro_id_alvo: int | None,
    linha_ref: int,
    item_icms: NfIcmsItem,
    motivo_codigo: str = "CONTRIB_SEM_C170_V1",
    apontamento_id: int | None = None,
) -> EfdRevisao:
    linha_nova = montar_linha_c170_de_icms(item_icms)


    rv = EfdRevisao(
        versao_origem_id=int(versao_origem_id),
        versao_revisada_id=None,
        registro_id=registro_id_alvo,
        reg="C170",
        acao="INSERT_AFTER",
        revisao_json={
            "linha_nova": linha_nova,
            "linha_referencia": int(linha_ref or 0),
            "nf_icms_item_id": int(item_icms.id),
            "origem": "ICMS_IPI",
            "motivo": "Item presente no ICMS/IPI e ausente no C170 da EFD Contribuições",
        },
        motivo_codigo=motivo_codigo,
        apontamento_id=apontamento_id,
    )
    db.add(rv)

    return rv


def _ja_existe_revisao_insert_para_item(
    db: Session,
    *,
    versao_origem_id: int,
    nf_icms_item_id: int,
    motivo_codigo: str = "CONTRIB_SEM_C170_V1",
) -> bool:
    qs = (
        db.query(EfdRevisao.id)
        .filter(EfdRevisao.versao_origem_id == int(versao_origem_id))
        .filter(EfdRevisao.acao == "INSERT_AFTER")
        .filter(EfdRevisao.motivo_codigo == motivo_codigo)
    )

    for rid, in qs.all():
        rv = db.query(EfdRevisao).filter(EfdRevisao.id == rid).first()
        j = getattr(rv, "revisao_json", None) or {}
        if int(j.get("nf_icms_item_id") or 0) == int(nf_icms_item_id):
            return True
    return False



def inserir_c170s_da_nf_encadeados(
    db: Session,
    *,
    versao_origem_id: int,
    nf: NfIcmsBase,
    itens: List[NfIcmsItem],
    linha_c100: LinhaLogica,
) -> Dict[str, Any]:
    total_inseridos = 0

    registro_id_alvo = getattr(linha_c100, "registro_id", None)
    linha_ref_alvo = int(getattr(linha_c100, "linha", 0) or 0)
    acao = "INSERT_AFTER"

    log.info(
        "C170 bloco start nf_id=%s versao_origem_id=%s linha_c100=%s registro_id_c100=%s total_itens=%s",
        getattr(nf, "id", None),
        versao_origem_id,
        linha_ref_alvo,
        registro_id_alvo,
        len(itens),
    )

    log.debug(
        "C170 itens origem nf_id=%s itens=%s",
        getattr(nf, "id", None),
        [
            {
                "item_id": int(getattr(it, "id", 0) or 0),
                "num_item": getattr(it, "num_item", None),
                "cod_item": getattr(it, "cod_item", None),
                "descricao": getattr(it, "descricao", None),
                "cfop": getattr(it, "cfop", None),
                "cst_icms": getattr(it, "cst_icms", None),
                "aliq_icms": str(getattr(it, "aliq_icms", None)),
                "vl_item": str(getattr(it, "vl_item", None)),
                "vl_desc": str(getattr(it, "vl_desc", None)),
                "vl_icms": str(getattr(it, "vl_icms", None)),
                "qtd": str(getattr(it, "qtd", None)),
                "unid": getattr(it, "unid", None),
                "cod_nat": getattr(it, "cod_nat", None),
                "cod_cta": getattr(it, "cod_cta", None),
            }
            for it in itens
        ],
    )

    for idx, it in enumerate(itens, start=1):
        log.debug(
            "C170 item loop idx=%s nf_id=%s item_id=%s num_item=%s cod_item=%s cfop=%s",
            idx,
            getattr(nf, "id", None),
            int(getattr(it, "id", 0) or 0),
            getattr(it, "num_item", None),
            getattr(it, "cod_item", None),
            getattr(it, "cfop", None),
        )

        linha_nova = montar_linha_c170_de_icms(it)

        log.debug(
            "C170 linha nova nf_id=%s item_id=%s linha_nova=%s",
            getattr(nf, "id", None),
            int(getattr(it, "id", 0) or 0),
            linha_nova,
        )

        rv = EfdRevisao(
            versao_origem_id=int(versao_origem_id),
            versao_revisada_id=None,
            registro_id=registro_id_alvo,
            reg="C170",
            acao=acao,
            revisao_json={
                "linha_nova": linha_nova,
                "linha_referencia": int(linha_ref_alvo or 0),
                "nf_icms_item_id": int(it.id),
                "nf_icms_base_id": int(nf.id),
                "origem": "ICMS_IPI",
            },
            motivo_codigo="CONTRIB_SEM_C170_V1",
        )

        db.add(rv)
        db.flush()

        log.info(
            "C170 revisão gravada rv_id=%s nf_id=%s item_id=%s registro_id=%s linha_referencia=%s",
            rv.id,
            getattr(nf, "id", None),
            int(getattr(it, "id", 0) or 0),
            rv.registro_id,
            rv.revisao_json.get("linha_referencia"),
        )

        total_inseridos += 1

        linhas = carregar_linhas_logicas_com_revisoes_e_insert(
            db,
            versao_origem_id=int(versao_origem_id),
            versao_final_id=None,
        )

        linha_c170_inserido = None
        for l in linhas:
            if (
                str(getattr(l, "reg", "")).upper() == "C170"
                and getattr(l, "revisao_id", None) == rv.id
            ):
                linha_c170_inserido = l
                break

        if linha_c170_inserido:
            log.debug(
                "C170 pos-overlay ok rv_id=%s nf_id=%s item_id=%s linha=%s registro_id=%s revisao_id=%s",
                rv.id,
                getattr(nf, "id", None),
                int(getattr(it, "id", 0) or 0),
                getattr(linha_c170_inserido, "linha", None),
                getattr(linha_c170_inserido, "registro_id", None),
                getattr(linha_c170_inserido, "revisao_id", None),
            )

            registro_id_alvo = getattr(linha_c170_inserido, "registro_id", None)
            linha_ref_alvo = int(getattr(linha_c170_inserido, "linha", 0) or 0)
            acao = "INSERT_AFTER"
        else:
            log.warning(
                "C170 pos-overlay miss rv_id=%s nf_id=%s item_id=%s linha_ref_alvo_anterior=%s registro_id_alvo_anterior=%s",
                rv.id,
                getattr(nf, "id", None),
                int(getattr(it, "id", 0) or 0),
                linha_ref_alvo,
                registro_id_alvo,
            )

    log.info(
        "C170 bloco final nf_id=%s total_inseridos=%s linha_fim_bloco=%s registro_id_fim_bloco=%s",
        getattr(nf, "id", None),
        total_inseridos,
        linha_ref_alvo,
        registro_id_alvo,
    )

    log.debug(
        "C170 bloco final snapshot nf_id=%s snapshot=%s",
        getattr(nf, "id", None),
        [
            {
                "linha": getattr(l, "linha", None),
                "reg": getattr(l, "reg", None),
                "registro_id": getattr(l, "registro_id", None),
                "revisao_id": getattr(l, "revisao_id", None),
                "conteudo": getattr(l, "conteudo", None),
            }
            for l in carregar_linhas_logicas_com_revisoes_e_insert(
                db,
                versao_origem_id=int(versao_origem_id),
                versao_final_id=None,
            )
            if str(getattr(l, "reg", "")).upper() in ("C100", "C170")
        ],
    )

    return {
        "total_inseridos": total_inseridos,
        "registro_id_fim_bloco": registro_id_alvo,
        "linha_fim_bloco": linha_ref_alvo,
    }