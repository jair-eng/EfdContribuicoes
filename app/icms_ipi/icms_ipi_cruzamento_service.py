from __future__ import annotations

from dataclasses import dataclass, asdict
from decimal import Decimal
from typing import Any, Dict, List, Optional, Tuple

from sqlalchemy.orm import Session

from app.db.models import EfdRegistro
from app.db.models.nf_icms_item import NfIcmsItem
from app.icms_ipi.icms_c170_utils import _registro_insercao_alvo, _criar_revisao_insert_c170_faltante
from app.icms_ipi.icms_helpers import _campo, _campo_dec, _norm_cod_item, _as_decimal, _s, _only_digits
from app.icms_ipi.icms_ipi_funcoes import _eh_c170, _eh_c100
from app.icms_ipi.icms_ipi_vinculo_service import vincular_documento_icms_ipi
from app.services.versao_overlay_service import carregar_linhas_logicas_com_revisoes
from app.sped.blocoC.c100_utils import salvar_revisao_c100_automatica, patch_c100_totais_imposto
from app.sped.logic.consolidador import _get_dados, calcular_totais_filhos, calcular_totais_filhos_overlay
from app.sped.revisao_overlay import LinhaLogica


# ============================================================
# DTOs
# ============================================================

@dataclass
class DocCtx:
    c100: Any
    chave_nfe: str
    numero_nf: str
    serie_nf: str
    dt_doc: str | None
    vl_doc: Decimal
    linha_c100: int
    registro_id_c100: int | None


@dataclass
class ItemMatchResult:
    match_encontrado: bool
    tipo_match: str = ""
    score: float = 0.0

    nf_icms_item_id: int | None = None
    c170_registro_id: int | None = None
    c170_linha: int | None = None

    chave_nfe: str = ""
    num_item: str = ""
    cod_item: str = ""
    cod_item_norm: str = ""
    cfop: str = ""
    cst_pis: str = ""
    cst_cofins: str = ""
    ncm: str = ""
    descricao: str = ""

    valor_item_efd: Decimal = Decimal("0")
    valor_item_icms: Decimal = Decimal("0")
    valor_icms_icms: Decimal = Decimal("0")
    valor_ipi_icms: Decimal = Decimal("0")

    status: str = "SEM_MATCH"  # MATCH | NAO_ESCRITURADO | FORA_DOMINIO | SEM_MATCH
    observacao: str = ""

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


# ============================================================
# Leitura de C100/C170 da versão lógica
# ============================================================

def _parse_c100_ctx(linha: Any) -> DocCtx:
    dados = list(getattr(linha, "dados", []) or [])

    # Layout esperado no EFD Contribuições:
    # |C100|IND_OPER|IND_EMIT|COD_PART|COD_MOD|COD_SIT|SER|NUM_DOC|CHV_NFE|DT_DOC|...|VL_DOC|...
    numero_nf = _campo(dados, 6)
    serie_nf = _campo(dados, 5)
    chave_nfe = _only_digits(_campo(dados, 7))
    dt_doc = _campo(dados, 8)
    vl_doc = _campo_dec(dados, 10)

    return DocCtx(
        c100=linha,
        chave_nfe=chave_nfe,
        numero_nf=numero_nf,
        serie_nf=serie_nf,
        dt_doc=dt_doc or None,
        vl_doc=vl_doc,
        linha_c100=int(getattr(linha, "linha", 0) or 0),
        registro_id_c100=int(getattr(linha, "registro_id", 0) or 0) or None,
    )

def _parse_c170(linha: Any) -> Dict[str, Any]:
    dados = list(getattr(linha, "dados", []) or [])

    # Layout esperado:
    # |C170|NUM_ITEM|COD_ITEM|DESCR_COMPL|QTD|UNID|VL_ITEM|VL_DESC|IND_MOV|
    # CST_ICMS|CFOP|COD_NAT|VL_BC_ICMS|ALIQ_ICMS|VL_ICMS|VL_BC_ICMS_ST|
    # ALIQ_ST|VL_ICMS_ST|IND_APUR|CST_IPI|COD_ENQ|VL_BC_IPI|ALIQ_IPI|VL_IPI|
    # CST_PIS|VL_BC_PIS|ALIQ_PIS|QUANT_BC_PIS|ALIQ_PIS_R|VL_PIS|
    # CST_COFINS|VL_BC_COFINS|ALIQ_COFINS|QUANT_BC_COFINS|ALIQ_COFINS_R|VL_COFINS|COD_CTA|

    return {
        "linha_obj": linha,
        "registro_id": int(getattr(linha, "registro_id", 0) or 0) or None,
        "linha_num": int(getattr(linha, "linha", 0) or 0) or None,

        "num_item": _campo(dados, 0),
        "cod_item": _campo(dados, 1),
        "cod_item_norm": _norm_cod_item(_campo(dados, 1)),
        "descricao": _campo(dados, 2),

        "cfop": _campo(dados, 9),

        "valor_item": _campo_dec(dados, 5),
        "valor_desc": _campo_dec(dados, 6),

        "vl_bc_icms": _campo_dec(dados, 11),
        "aliq_icms": _campo_dec(dados, 12),
        "vl_icms": _campo_dec(dados, 13),

        "vl_ipi": _campo_dec(dados, 17),

        "cst_pis": _campo(dados, 23),
        "vl_bc_pis": _campo_dec(dados, 24),
        "aliq_pis": _campo_dec(dados, 25),
        "vl_pis": _campo_dec(dados, 27),

        "cst_cofins": _campo(dados, 29),
        "vl_bc_cofins": _campo_dec(dados, 30),
        "aliq_cofins": _campo_dec(dados, 31),
        "vl_cofins": _campo_dec(dados, 33),
    }


def _agrupar_documentos_com_itens(linhas_logicas: list[Any]) -> list[tuple[DocCtx, list[Dict[str, Any]]]]:
    docs: list[tuple[DocCtx, list[Dict[str, Any]]]] = []
    doc_atual: DocCtx | None = None
    itens_atuais: list[Dict[str, Any]] = []

    for linha in linhas_logicas:
        if _eh_c100(linha):
            if doc_atual is not None:
                docs.append((doc_atual, itens_atuais))
            doc_atual = _parse_c100_ctx(linha)
            itens_atuais = []
            continue

        if _eh_c170(linha) and doc_atual is not None:
            itens_atuais.append(_parse_c170(linha))

    if doc_atual is not None:
        docs.append((doc_atual, itens_atuais))

    return docs


# ============================================================
# Busca de itens auxiliares
# ============================================================

def _buscar_itens_nf_icms(
    db: Session,
    *,
    empresa_id: int,
    nf_id: int,
) -> list[NfIcmsItem]:
    return (
        db.query(NfIcmsItem)
        .filter(
            NfIcmsItem.empresa_id == empresa_id,
            NfIcmsItem.nf_icms_base_id == nf_id,
        )
        .order_by(NfIcmsItem.id.asc())
        .all()
    )


def _indexar_itens_icms(itens: list[NfIcmsItem]) -> Dict[str, Any]:
    by_cod: Dict[str, list[NfIcmsItem]] = {}
    by_cod_norm: Dict[str, list[NfIcmsItem]] = {}
    by_num_item: Dict[str, list[NfIcmsItem]] = {}
    by_cfop: Dict[str, list[NfIcmsItem]] = {}

    for it in itens:
        cod = _s(it.cod_item)
        cod_norm = _s(it.cod_item_norm)
        num_item = _s(it.num_item)
        cfop = _s(it.cfop)

        if cod:
            by_cod.setdefault(cod, []).append(it)
        if cod_norm:
            by_cod_norm.setdefault(cod_norm, []).append(it)
        if num_item:
            by_num_item.setdefault(num_item, []).append(it)
        if cfop:
            by_cfop.setdefault(cfop, []).append(it)

    return {
        "by_cod": by_cod,
        "by_cod_norm": by_cod_norm,
        "by_num_item": by_num_item,
        "by_cfop": by_cfop,
        "all": itens,
    }


# ============================================================
# Match item a item
# ============================================================

def _match_item_c170_com_icms(
    c170: Dict[str, Any],
    idx: Dict[str, Any],
    *,
    chave_nfe: str,
) -> ItemMatchResult:
    cod_item = _s(c170.get("cod_item"))
    cod_item_norm = _s(c170.get("cod_item_norm"))
    num_item = _s(c170.get("num_item"))
    cfop = _s(c170.get("cfop"))

    cand: NfIcmsItem | None = None
    tipo = ""
    score = 0.0

    if cod_item and idx["by_cod"].get(cod_item):
        cand = idx["by_cod"][cod_item][0]
        tipo = "COD_ITEM"
        score = 1.00

    elif cod_item_norm and idx["by_cod_norm"].get(cod_item_norm):
        cand = idx["by_cod_norm"][cod_item_norm][0]
        tipo = "COD_ITEM_NORM"
        score = 0.97

    elif num_item and idx["by_num_item"].get(num_item):
        cand = idx["by_num_item"][num_item][0]
        tipo = "NUM_ITEM"
        score = 0.95

    elif cfop and idx["by_cfop"].get(cfop):
        cand = idx["by_cfop"][cfop][0]
        tipo = "CFOP"
        score = 0.70

    if cand is None:
        return ItemMatchResult(
            match_encontrado=False,
            tipo_match="SEM_MATCH",
            score=0.0,
            c170_registro_id=c170.get("registro_id"),
            c170_linha=c170.get("linha_num"),
            chave_nfe=chave_nfe,
            num_item=num_item,
            cod_item=cod_item,
            cod_item_norm=cod_item_norm,
            cfop=cfop,
            descricao=_s(c170.get("descricao")),
            valor_item_efd=_as_decimal(c170.get("valor_item")),
            cst_pis=_s(c170.get("cst_pis")),
            cst_cofins=_s(c170.get("cst_cofins")),
            status="SEM_MATCH",
            observacao="item do C170 não localizado em nf_icms_item",
        )

    return ItemMatchResult(
        match_encontrado=True,
        tipo_match=tipo,
        score=score,
        nf_icms_item_id=int(cand.id),
        c170_registro_id=c170.get("registro_id"),
        c170_linha=c170.get("linha_num"),
        chave_nfe=chave_nfe,
        num_item=_s(cand.num_item) or num_item,
        cod_item=_s(cand.cod_item) or cod_item,
        cod_item_norm=_s(cand.cod_item_norm) or cod_item_norm,
        cfop=_s(cand.cfop) or cfop,
        ncm=_s(cand.ncm),
        descricao=_s(cand.descricao) or _s(c170.get("descricao")),
        valor_item_efd=_as_decimal(c170.get("valor_item")),
        valor_item_icms=_as_decimal(cand.vl_item),
        valor_icms_icms=_as_decimal(cand.vl_icms),
        valor_ipi_icms=_as_decimal(cand.vl_ipi),
        cst_pis=_s(c170.get("cst_pis")),
        cst_cofins=_s(c170.get("cst_cofins")),
        status="MATCH",
        observacao=f"match por {tipo}",
    )


# ============================================================
# Serviço principal
# ============================================================

def cruzar_versao_com_icms_ipi(
    db: Session,
    *,
    versao_origem_id: int,
    empresa_id: int,
    periodo: str | None = None,
    usar_overlay: bool = False,
) -> Dict[str, Any]:
    """
    Primeira versão:
      - carrega C100/C170 da versão
      - vincula documento no nf_icms_base
      - cruza cada C170 com nf_icms_item
      - retorna diagnóstico de match

    Nesta fase:
      - NÃO gera revisão ainda
      - NÃO insere C170 novo ainda
      - foco em preparar MATCH -> REPLACE_LINE
    """
    print("[DBG CRUZ]", "usar_overlay=", usar_overlay, flush=True)
    if usar_overlay:
        print("[DBG CRUZ] carregando COM overlay", flush=True)
        linhas_logicas = carregar_linhas_logicas_com_revisoes(
            db,
            versao_origem_id=versao_origem_id,
        )
    else:
        print("[DBG CRUZ] carregando SEM overlay", flush=True)
        regs = (
            db.query(EfdRegistro)
            .filter(EfdRegistro.versao_id == int(versao_origem_id))
            .order_by(EfdRegistro.linha.asc())
            .all()
        )
        linhas_logicas = [LinhaLogica.from_efd_registro(r) for r in regs]


    docs = _agrupar_documentos_com_itens(linhas_logicas)

    resultados_docs: list[Dict[str, Any]] = []
    resultados_itens: list[Dict[str, Any]] = []

    total_docs = 0
    total_docs_vinculados = 0
    total_itens_c170 = 0
    total_match_item = 0
    total_sem_match_item = 0
    c100_afetados_insert: set[int] = set()

    for doc_ctx, itens_c170 in docs:
        total_docs += 1
        total_itens_c170 += len(itens_c170)

        vinc = vincular_documento_icms_ipi(
            db,
            empresa_id=empresa_id,
            chave_nfe=doc_ctx.chave_nfe or None,
            numero_nf=doc_ctx.numero_nf or None,
            serie_nf=doc_ctx.serie_nf or None,
            dt_doc=doc_ctx.dt_doc,
            vl_doc=doc_ctx.vl_doc,
            periodo=periodo,
        )

        doc_out = {
            "linha_c100": doc_ctx.linha_c100,
            "registro_id_c100": doc_ctx.registro_id_c100,
            "chave_nfe": doc_ctx.chave_nfe,
            "numero_nf": doc_ctx.numero_nf,
            "serie_nf": doc_ctx.serie_nf,
            "dt_doc": doc_ctx.dt_doc,
            "vl_doc": str(doc_ctx.vl_doc),
            "vinculo_icms": vinc,
            "total_itens_c170": len(itens_c170),
        }
        resultados_docs.append(doc_out)

        if not vinc.get("vinculado"):
            for c170 in itens_c170:
                resultados_itens.append(
                    ItemMatchResult(
                        match_encontrado=False,
                        tipo_match="SEM_NF_AUX",
                        score=0.0,
                        c170_registro_id=c170.get("registro_id"),
                        c170_linha=c170.get("linha_num"),
                        chave_nfe=doc_ctx.chave_nfe,
                        num_item=_s(c170.get("num_item")),
                        cod_item=_s(c170.get("cod_item")),
                        cod_item_norm=_s(c170.get("cod_item_norm")),
                        cfop=_s(c170.get("cfop")),
                        descricao=_s(c170.get("descricao")),
                        valor_item_efd=_as_decimal(c170.get("valor_item")),
                        status="SEM_MATCH",
                        observacao="documento não localizado em nf_icms_base",
                    ).to_dict()
                )
                total_sem_match_item += 1
            continue

        total_docs_vinculados += 1
        nf_id = int(vinc.get("nf_id") or 0)

        itens_aux = _buscar_itens_nf_icms(
            db,
            empresa_id=empresa_id,
            nf_id=nf_id,
        )


        idx = _indexar_itens_icms(itens_aux)

        icms_usados: set[int] = set()

        for c170 in itens_c170:
            r = _match_item_c170_com_icms(
                c170,
                idx,
                chave_nfe=doc_ctx.chave_nfe,
            )
            resultados_itens.append(r.to_dict())

            if r.match_encontrado:
                total_match_item += 1
                if r.nf_icms_item_id:
                    icms_usados.add(int(r.nf_icms_item_id))
            else:
                total_sem_match_item += 1

        # ============================================================
        # ICMS/IPI -> C170: detectar item ausente no Contribuições
        # ============================================================

        for item_icms in itens_aux:
            if int(item_icms.id) in icms_usados:
                continue


            resultados_itens.append(
                ItemMatchResult(
                    match_encontrado=False,
                    tipo_match="CONTRIB_SEM_C170",
                    score=0.0,
                    nf_icms_item_id=int(item_icms.id),
                    chave_nfe=doc_ctx.chave_nfe,
                    num_item=_s(item_icms.num_item),
                    cod_item=_s(item_icms.cod_item),
                    cod_item_norm=_s(item_icms.cod_item_norm),
                    cfop=_s(item_icms.cfop),
                    ncm=_s(item_icms.ncm),
                    descricao=_s(item_icms.descricao),
                    valor_item_icms=_as_decimal(item_icms.vl_item),
                    valor_icms_icms=_as_decimal(item_icms.vl_icms),
                    valor_ipi_icms=_as_decimal(item_icms.vl_ipi),
                    status="NAO_ESCRITURADO",
                    observacao="item presente no ICMS/IPI mas ausente no C170 da EFD Contribuições",
                ).to_dict()
            )

            registro_id_alvo, linha_ref = _registro_insercao_alvo(doc_ctx, itens_c170)

            _criar_revisao_insert_c170_faltante(
                db,
                versao_origem_id=versao_origem_id,
                registro_id_alvo=registro_id_alvo,
                linha_ref=linha_ref,
                item_icms=item_icms,
                motivo_codigo="CONTRIB_SEM_C170_V1",
            )

            if registro_id_alvo:
                c100_afetados_insert.add(int(registro_id_alvo))

            print(
                "[DBG INSERT_CRIADO]",
                "nf_item_id=", item_icms.id,
                "chave=", doc_ctx.chave_nfe,
                "registro_id_alvo=", registro_id_alvo,
                "linha_ref=", linha_ref,
                flush=True,
            )

    db.flush()
    print("[DBG INSERTS FLUSHADOS]", sorted(list(c100_afetados_insert)), flush=True)

    # ============================================================
    # CONSOLIDAÇÃO DOS C100 AFETADOS PELOS INSERTS
    # ============================================================

    for c100_id in sorted(c100_afetados_insert):
        try:
            total_pis, total_cofins = calcular_totais_filhos_overlay(
                db,
                versao_origem_id=int(versao_origem_id),
                versao_final_id=None,  # revisões ainda pendentes sobre a origem
                c100_id=int(c100_id),
            )

            reg_c100 = db.get(EfdRegistro, int(c100_id))
            if not reg_c100:
                continue

            dados_c100 = _get_dados(reg_c100)
            campos_atualizados = patch_c100_totais_imposto(
                dados_c100,
                float(total_pis),
                float(total_cofins),
            )

            salvar_revisao_c100_automatica(
                db,
                versao_origem_id=int(versao_origem_id),
                motivo_codigo="CONTRIB_SEM_C170_V1_AUTO_SUM",
                reg_c100=reg_c100,
                novos_dados=campos_atualizados,
                apontamento_id=None,
            )

            print(
                "[DBG C100_INSERT_SUM]",
                "c100_id=", c100_id,
                "pis=", total_pis,
                "cofins=", total_cofins,
                flush=True,
            )

        except Exception as e:
            print("[DBG C100_INSERT_SUM ERRO]", c100_id, e, flush=True)
    db.flush()

    return {
        "ok": True,
        "versao_origem_id": versao_origem_id,
        "empresa_id": empresa_id,
        "periodo": periodo,
        "total_docs": total_docs,
        "total_docs_vinculados": total_docs_vinculados,
        "total_itens_c170": total_itens_c170,
        "total_match_item": total_match_item,
        "total_sem_match_item": total_sem_match_item,
        "documentos": resultados_docs,
        "itens": resultados_itens,
    }