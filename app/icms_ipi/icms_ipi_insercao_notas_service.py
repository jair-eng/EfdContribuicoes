from __future__ import annotations

from decimal import Decimal
from typing import Any, Dict, List, Optional, Set, Tuple
from sqlalchemy.orm import Session
from app.config.settings import ALIQUOTA_PIS_PCT, ALIQUOTA_COFINS_PCT
from app.db.models import EfdRegistro, EfdRevisao, NfIcmsItem
from app.db.models.nf_icms_base import NfIcmsBase
from app.icms_ipi.icms_0150_agregador import resolver_ou_criar_0150_por_cnpj
from app.icms_ipi.icms_c170_utils import  inserir_c170s_da_nf_encadeados
from app.icms_ipi.icms_helpers import (
    _campo,
    _only_digits,
    _s,
    fmt_sped_num,
)
from app.services.versao_overlay_service import carregar_linhas_logicas_com_revisoes_e_insert
from app.sped.bloco_0.bloco_0_0190_0200_agregador import _garantir_mestres_para_notas_elegiveis
from app.sped.logic.consolidador import _get_dados
from app.sped.revisao_overlay import LinhaLogica
from app.sped.utils_geral import q2
import logging

log = logging.getLogger(__name__)


CFOPS_VALIDOS_RECUP = {"1101", "1102", "2101", "2102", "3101", "3102"}

def _nota_tem_cfop_valido(itens: list[NfIcmsItem]) -> bool:
    for it in itens:
        cfop = str(getattr(it, "cfop", "") or "").strip()
        if cfop in CFOPS_VALIDOS_RECUP:
            return True
    return False

def _listar_chaves_c100_existentes(
    db: Session,
    *,
    versao_origem_id: int,
) -> tuple[Set[str], List[EfdRegistro]]:
    regs = (
        db.query(EfdRegistro)
        .filter(
            EfdRegistro.versao_id == int(versao_origem_id),
            EfdRegistro.reg == "C100",
        )
        .order_by(EfdRegistro.linha.asc())
        .all()
    )

    chaves: Set[str] = set()

    for r in regs:
        dados = _get_dados(r)
        chave = _only_digits(_campo(dados, 7))
        if chave:
            chaves.add(chave)

    return chaves, regs

def _resolver_ancora_bloco_c_fim(
    db: Session,
    *,
    versao_origem_id: int,
) -> tuple[Optional[int], int, str]:
    reg_c990 = (
        db.query(EfdRegistro)
        .filter(
            EfdRegistro.versao_id == int(versao_origem_id),
            EfdRegistro.reg == "C990",
        )
        .order_by(EfdRegistro.linha.asc())
        .first()
    )

    if reg_c990:
        return int(reg_c990.id), int(getattr(reg_c990, "linha", 0) or 0), "INSERT_BEFORE"

    # fallback antigo, se não houver C990
    regs_c100 = (
        db.query(EfdRegistro)
        .filter(
            EfdRegistro.versao_id == int(versao_origem_id),
            EfdRegistro.reg == "C100",
        )
        .order_by(EfdRegistro.linha.asc())
        .all()
    )

    if regs_c100:
        ultimo = regs_c100[-1]
        return int(ultimo.id), int(getattr(ultimo, "linha", 0) or 0), "INSERT_AFTER"

    return None, 0, "INSERT_AFTER"

def _buscar_notas_icms_com_itens(
    db: Session,
    *,
    empresa_id: int,
    periodo: str | None = None,
) -> List[tuple[NfIcmsBase, List[NfIcmsItem]]]:
    q = db.query(NfIcmsBase).filter(NfIcmsBase.empresa_id == int(empresa_id))

    if periodo:
        q = q.filter(NfIcmsBase.periodo == str(periodo))

    notas = q.order_by(NfIcmsBase.dt_doc.asc(), NfIcmsBase.id.asc()).all()

    saida: List[tuple[NfIcmsBase, List[NfIcmsItem]]] = []

    for nf in notas:
        itens = (
            db.query(NfIcmsItem)
            .filter(
                NfIcmsItem.empresa_id == int(empresa_id),
                NfIcmsItem.nf_icms_base_id == int(nf.id),
            )
            .order_by(NfIcmsItem.id.asc())
            .all()
        )
        if itens:
            saida.append((nf, itens))

    return saida

def _resolver_ancora_insert_c100(
    regs_c100: List[EfdRegistro],
) -> tuple[Optional[int], int]:
    if not regs_c100:
        return None, 0

    ultimo = regs_c100[-1]
    return int(ultimo.id), int(getattr(ultimo, "linha", 0) or 0)

def montar_linha_c100_de_icms(nf: NfIcmsBase) -> str:
    dt_doc_txt = nf.dt_doc.strftime("%d%m%Y") if getattr(nf, "dt_doc", None) else ""

    vl_doc = q2(getattr(nf, "vl_doc", Decimal("0")) or Decimal("0"))
    vl_icms = q2(getattr(nf, "vl_icms", Decimal("0")) or Decimal("0"))

    # no teu cenário atual, base = valor da nota
    vl_pis = q2(vl_doc * (ALIQUOTA_PIS_PCT / Decimal("100")))
    vl_cofins = q2(vl_doc * (ALIQUOTA_COFINS_PCT / Decimal("100")))

    campos = [
        "C100",                                     # 1
        "0",                                        # 2 IND_OPER
        "1",                                        # 3 IND_EMIT
        str(getattr(nf, "cod_part", "") or ""),     # 4 COD_PART
        str(getattr(nf, "cod_mod", "") or "55"),    # 5 COD_MOD
        str(getattr(nf, "cod_sit", "") or "00"),    # 6 COD_SIT
        str(getattr(nf, "serie", "") or ""),        # 7 SER
        str(getattr(nf, "num_doc", "") or ""),      # 8 NUM_DOC
        str(getattr(nf, "chave_nfe", "") or ""),    # 9 CHV_NFE
        dt_doc_txt,                                 # 10 DT_DOC
        dt_doc_txt,                                 # 11 DT_E_S
        fmt_sped_num(vl_doc),                       # 12 VL_DOC
        "0",                                        # 13 IND_PGTO
        "0",                                        # 14 VL_DESC
        "0",                                        # 15 ABAT_NT
        fmt_sped_num(vl_doc),                       # 16 VL_MERC
        "0",                                        # 17 IND_FRT
        "0",                                        # 18 VL_FRT
        "0",                                        # 19 VL_SEG
        "0",                                        # 20 VL_OUT_DA
        fmt_sped_num(vl_doc),                       # 21 VL_BC_ICMS
        fmt_sped_num(vl_icms),                      # 22 VL_ICMS
        "0",                                        # 23 VL_BC_ICMS_ST
        "0",                                        # 24 VL_ICMS_ST
        "0",                                        # 25 VL_PIS
        fmt_sped_num(vl_pis),                       # 26
        fmt_sped_num(vl_cofins),                    # 27
        "0",                                        # 28
        "0",                                        # 29
    ]

    if len(campos) != 29:
        raise ValueError(f"C100 inválido: esperado 29 campos, veio {len(campos)}")

    linha = "|" + "|".join(campos) + "|"

    return linha

def _ja_existe_revisao_insert_para_nf(
    db: Session,
    *,
    versao_origem_id: int,
    nf_icms_base_id: int,
    motivo_codigo: str = "CONTRIB_SEM_C100_V1",
) -> bool:
    qs = (
        db.query(EfdRevisao.id)
        .filter(EfdRevisao.versao_origem_id == int(versao_origem_id))
        .filter(EfdRevisao.acao.in_(["INSERT_AFTER", "INSERT_BEFORE"]))
        .filter(EfdRevisao.reg == "C100")
        .filter(EfdRevisao.motivo_codigo == motivo_codigo)
        .all()
    )

    for rid, in qs:
        rv = db.query(EfdRevisao).filter(EfdRevisao.id == rid).first()
        j = getattr(rv, "revisao_json", None) or {}
        if int(j.get("nf_icms_base_id") or 0) == int(nf_icms_base_id):
            return True

    return False

def _criar_revisao_insert_c100_faltante(
    db: Session,
    *,
    versao_origem_id: int,
    registro_id_alvo: int | None,
    linha_ref: int,
    nf: NfIcmsBase,
    motivo_codigo: str = "CONTRIB_SEM_C100_V1",
    apontamento_id: int | None = None,
    acao: str = "INSERT_AFTER",
) -> EfdRevisao:

    # 1️⃣ garante participante
    cod_part_final = resolver_ou_criar_0150_por_cnpj(
        db,
        versao_id=versao_origem_id,
        nf=nf,
    )

    # atualiza o objeto da nota
    nf.cod_part = cod_part_final

    print(
        "[DBG C100 COD_PART RESOLVIDO]",
        {
            "nf_id": nf.id,
            "chave_nfe": nf.chave_nfe,
            "cod_part_final": cod_part_final,
        },
        flush=True,
    )

    # 2️⃣ monta linha C100
    linha_nova = montar_linha_c100_de_icms(nf)

    rv = EfdRevisao(
        versao_origem_id=int(versao_origem_id),
        versao_revisada_id=None,
        registro_id=registro_id_alvo,
        reg="C100",
        acao=str(acao).upper(),
        revisao_json={
            "linha_nova": linha_nova,
            "linha_referencia": int(linha_ref or 0),
            "nf_icms_base_id": int(nf.id),
            "origem": "ICMS_IPI",
            "motivo": "Nota presente no ICMS/IPI e ausente no C100 da EFD Contribuições",
        },
        motivo_codigo=motivo_codigo,
        apontamento_id=apontamento_id,
    )
    db.add(rv)
    db.flush()
    return rv

def _achar_linha_c100_por_chave(
    linhas: List[LinhaLogica],
    *,
    chave: str,
) -> Optional[LinhaLogica]:
    chave = _only_digits(chave)
    if not chave:
        return None

    for linha in linhas:
        if str(getattr(linha, "reg", "")).upper() != "C100":
            continue

        dados = list(getattr(linha, "dados", []) or [])
        chave_linha = _only_digits(_campo(dados, 7))
        if chave_linha == chave:
            return linha

    return None



def _achar_proximo_c100_do_mapa(
    linhas: List[LinhaLogica],
    mapa: List[Dict[str, Any]],
    idx_atual: int,
) -> Optional[LinhaLogica]:
    if idx_atual >= len(mapa) - 1:
        return None

    prox_chave = mapa[idx_atual + 1]["chave"]
    return _achar_linha_c100_por_chave(linhas, chave=prox_chave)

def _inserir_bloco_nf_icms_na_efd(
    db: Session,
    *,
    versao_origem_id: int,
    nf: NfIcmsBase,
    itens: List[NfIcmsItem],
    registro_id_alvo: int | None,
    linha_ref_alvo: int,
    acao_inicial: str,
    apontamento_id: int | None = None,
) -> Dict[str, Any]:
    chave = _only_digits(getattr(nf, "chave_nfe", None))

    # 1) insere C100
    rv_c100 = _criar_revisao_insert_c100_faltante(
        db,
        versao_origem_id=versao_origem_id,
        registro_id_alvo=registro_id_alvo,
        linha_ref=linha_ref_alvo,
        nf=nf,
        motivo_codigo="CONTRIB_SEM_C100_V1",
        apontamento_id=apontamento_id,
        acao=acao_inicial,
    )
    db.flush()

    log.info(
        "Bloco NF C100 criado nf_id=%s chave=%s revisao_id=%s registro_id_alvo=%s linha_ref_alvo=%s acao=%s",
        nf.id,
        chave,
        rv_c100.id,
        registro_id_alvo,
        linha_ref_alvo,
        acao_inicial,
    )

    # 2) recarrega e acha o C100 inserido
    linhas = carregar_linhas_logicas_com_revisoes_e_insert(
        db,
        versao_origem_id=int(versao_origem_id),
        versao_final_id=None,
    )

    linha_c100 = None
    for l in linhas:
        if (
            str(getattr(l, "reg", "")).upper() == "C100"
            and getattr(l, "revisao_id", None) == rv_c100.id
        ):
            linha_c100 = l
            break

    if not linha_c100:
        log.warning(
            "Bloco NF C100 não localizado no overlay nf_id=%s chave=%s revisao_id=%s",
            nf.id,
            chave,
            rv_c100.id,
        )
        return {
            "c100_inserido": 1,
            "c170_inseridos": 0,
            "revisao_c100_id": int(rv_c100.id),
            "registro_id_fim_bloco": registro_id_alvo,
            "linha_fim_bloco": linha_ref_alvo,
        }

    log.debug(
        "Bloco NF C100 localizado nf_id=%s chave=%s revisao_id=%s linha_c100=%s registro_id_c100=%s",
        nf.id,
        chave,
        rv_c100.id,
        getattr(linha_c100, "linha", None),
        getattr(linha_c100, "registro_id", None),
    )

    # 3) insere C170 da nota encadeados após o próprio bloco
    res_c170 = inserir_c170s_da_nf_encadeados(
        db,
        versao_origem_id=versao_origem_id,
        nf=nf,
        itens=itens,
        linha_c100=linha_c100,
    )

    log.info(
        "Bloco NF finalizado nf_id=%s chave=%s revisao_c100_id=%s c170_inseridos=%s linha_fim_bloco=%s registro_id_fim_bloco=%s",
        nf.id,
        chave,
        rv_c100.id,
        int(res_c170["total_inseridos"]),
        res_c170["linha_fim_bloco"],
        res_c170["registro_id_fim_bloco"],
    )

    return {
        "c100_inserido": 1,
        "c170_inseridos": int(res_c170["total_inseridos"]),
        "revisao_c100_id": int(rv_c100.id),
        "registro_id_fim_bloco": res_c170["registro_id_fim_bloco"],
        "linha_fim_bloco": res_c170["linha_fim_bloco"],
    }


def inserir_notas_icms_ausentes_na_efd(
    db: Session,
    *,
    versao_origem_id: int,
    empresa_id: int,
    periodo: str | None = None,
    apontamento_id: int | None = None,
) -> Dict[str, Any]:
    chaves_existentes, _regs_c100 = _listar_chaves_c100_existentes(
        db,
        versao_origem_id=versao_origem_id,
    )

    registro_id_alvo, linha_ref_alvo, acao_inicial = _resolver_ancora_bloco_c_fim(
        db,
        versao_origem_id=versao_origem_id,
    )

    notas_com_itens = _buscar_notas_icms_com_itens(
        db,
        empresa_id=empresa_id,
        periodo=periodo,
    )

    total_notas_icms = len(notas_com_itens)
    total_notas_ausentes = 0
    total_c100_insert = 0
    total_c170_insert = 0
    detalhes: List[Dict[str, Any]] = []
    mensagens: List[str] = []

    log.info(
        "Inserção ICMS/EFD iniciada versao_origem_id=%s empresa_id=%s periodo=%s total_notas_icms=%s",
        versao_origem_id,
        empresa_id,
        periodo,
        total_notas_icms,
    )

    # ------------------------------------------------------------
    # FASE 1: filtra só as NFs elegíveis
    # ------------------------------------------------------------
    notas_elegiveis: List[tuple[NfIcmsBase, List[NfIcmsItem], str]] = []

    for nf, itens in notas_com_itens:
        chave = _only_digits(getattr(nf, "chave_nfe", None))
        cfops = [str(getattr(it, "cfop", "") or "").strip() for it in itens]

        log.debug(
            "C100 loop nf_id=%s chave=%s num_doc=%s serie=%s itens=%s cfops=%s",
            nf.id,
            chave,
            getattr(nf, "num_doc", None),
            getattr(nf, "serie", None),
            len(itens),
            cfops,
        )

        if not chave:
            log.debug("C100 skip chave vazia nf_id=%s", nf.id)
            continue

        if chave in chaves_existentes:
            log.debug("C100 skip já existe nf_id=%s chave=%s", nf.id, chave)
            continue

        if not _nota_tem_cfop_valido(itens):
            log.info(
                "C100 ignorado por CFOP fora de escopo nf_id=%s chave=%s cfops=%s",
                nf.id,
                chave,
                cfops,
            )
            continue

        if _ja_existe_revisao_insert_para_nf(
            db,
            versao_origem_id=versao_origem_id,
            nf_icms_base_id=int(nf.id),
            motivo_codigo="CONTRIB_SEM_C100_V1",
        ):
            log.info(
                "C100 skip revisão já existe nf_id=%s chave=%s",
                nf.id,
                chave,
            )
            continue

        notas_elegiveis.append((nf, itens, chave))

    total_notas_ausentes = len(notas_elegiveis)

    log.info(
        "NFs elegíveis preparadas versao_origem_id=%s total_notas_ausentes=%s",
        versao_origem_id,
        total_notas_ausentes,
    )

    log.debug(
        "NFs elegíveis detalhes=%s",
        [
            {
                "nf_id": int(getattr(nf, "id", 0) or 0),
                "chave": chave,
                "itens": len(itens),
            }
            for nf, itens, chave in notas_elegiveis
        ],
    )

    # ------------------------------------------------------------
    # FASE 2: garante mestres SOMENTE das NFs elegíveis
    # ------------------------------------------------------------
    res_mestres = _garantir_mestres_para_notas_elegiveis(
        db,
        versao_origem_id=versao_origem_id,
        notas_elegiveis=notas_elegiveis,
    )

    log.info(
        "Mestres preparados versao_origem_id=%s total_0150=%s total_0190=%s total_0200=%s",
        versao_origem_id,
        res_mestres.get("total_0150", 0),
        res_mestres.get("total_0190", 0),
        res_mestres.get("total_0200", 0),
    )

    # ------------------------------------------------------------
    # FASE 3: insere C100/C170
    # ------------------------------------------------------------
    for nf, itens, chave in notas_elegiveis:
        res_bloco = _inserir_bloco_nf_icms_na_efd(
            db,
            versao_origem_id=versao_origem_id,
            nf=nf,
            itens=itens,
            registro_id_alvo=registro_id_alvo,
            linha_ref_alvo=linha_ref_alvo,
            acao_inicial=acao_inicial,
            apontamento_id=apontamento_id,
        )

        total_c100_insert += int(res_bloco["c100_inserido"])
        total_c170_insert += int(res_bloco["c170_inseridos"])

        chaves_existentes.add(chave)

        # próxima NF ancora no fim do bloco anterior
        registro_id_alvo = res_bloco["registro_id_fim_bloco"]
        linha_ref_alvo = res_bloco["linha_fim_bloco"]
        acao_inicial = "INSERT_AFTER"

        detalhes.append({
            "nf_icms_base_id": int(nf.id),
            "chave_nfe": chave,
            "num_doc": str(getattr(nf, "num_doc", "") or ""),
            "serie": str(getattr(nf, "serie", "") or ""),
            "itens": len(itens),
            "revisao_c100_id": res_bloco["revisao_c100_id"],
            "c170_inseridos": res_bloco["c170_inseridos"],
            "linha_ref_final": linha_ref_alvo,
        })

    db.flush()

    mensagens.append(f"{total_notas_icms} notas ICMS localizadas")
    mensagens.append(f"{total_notas_ausentes} notas elegíveis para inserção")
    mensagens.append(f"{res_mestres.get('total_0150', 0)} registros 0150 criados")
    mensagens.append(f"{res_mestres.get('total_0190', 0)} registros 0190 criados")
    mensagens.append(f"{res_mestres.get('total_0200', 0)} registros 0200 criados")
    mensagens.append(f"{total_c100_insert} registros C100 inseridos")
    mensagens.append(f"{total_c170_insert} registros C170 inseridos")

    log.info(
        "Resumo ICMS/EFD versao_origem_id=%s total_notas_icms=%s total_notas_ausentes=%s total_c100_insert=%s total_c170_insert=%s",
        versao_origem_id,
        total_notas_icms,
        total_notas_ausentes,
        total_c100_insert,
        total_c170_insert,
    )

    return {
        "ok": True,
        "versao_origem_id": int(versao_origem_id),
        "empresa_id": int(empresa_id),
        "periodo": periodo,
        "total_notas_icms": total_notas_icms,
        "total_notas_ausentes": total_notas_ausentes,
        "total_c100_insert": total_c100_insert,
        "total_c170_insert": total_c170_insert,
        "total_0150_criados": int(res_mestres.get("total_0150", 0)),
        "total_0190_criados": int(res_mestres.get("total_0190", 0)),
        "total_0200_criados": int(res_mestres.get("total_0200", 0)),
        "detalhes": detalhes,
        "mensagens": mensagens,
    }