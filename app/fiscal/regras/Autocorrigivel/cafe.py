from __future__ import annotations
from typing import Dict, Any, List, Optional

from sqlalchemy.orm import Session, aliased
from sqlalchemy import func, or_, and_

from app.fiscal.constants import DOM_CAFE
from app.fiscal.settings_fiscais import CSTS_TRIB_NCUM
from app.db.models.efd_registro import EfdRegistro
from app.db.models import EfdVersao, NfIcmsItem
from app.icms_ipi.icms_c170_utils import _ja_existe_revisao_insert_para_item, _criar_revisao_insert_c170_faltante
from app.icms_ipi.icms_ipi_cruzamento_service import cruzar_versao_com_icms_ipi
from app.services.c170_service import revisar_c170_lote
from app.services.dominio_service import resolver_dominio_por_versao
from app.sped.logic.consolidador import popular_pai_id, eh_pf_por_c100, _eh_item_cafe_match, _s


def _resolver_empresa_e_periodo_da_versao(
    db: Session,
    *,
    versao_origem_id: int,
) -> tuple[Optional[int], Optional[str]]:
    versao = db.get(EfdVersao, int(versao_origem_id))
    if not versao:
        return None, None

    arquivo = getattr(versao, "arquivo", None)
    if not arquivo:
        return None, None

    empresa_id = int(getattr(arquivo, "empresa_id", 0) or 0) or None
    periodo = str(getattr(arquivo, "periodo", "") or "").strip() or None
    return empresa_id, periodo


def aplicar_correcao_ind_cafe_cst51_legado(
    db: Session,
    *,
    versao_origem_id: int,
    incluir_revenda: bool = True,
    csts_origem: Optional[List[str]] = None,
    apontamento_id: Optional[int] = None,
    ignorar_registro_ids: Optional[List[int]] = None,
) -> Dict[str, Any]:
    versao_origem_id = int(versao_origem_id)

    dom = (resolver_dominio_por_versao(db, versao_origem_id) or "").strip().upper()
    if dom != DOM_CAFE:
        return {
            "status": "skip",
            "msg": f"dominio={dom} não permite IND_CAFE_V1",
            "candidatos": 0,
            "total_alterado": 0,
            "modo_usado": "LEGADO_EFD",
        }

    if "51" not in set(CSTS_TRIB_NCUM or set()):
        raise ValueError("settings_fiscais.CSTS_TRIB_NCUM não contém '51'.")

    IDX_CFOP = 9
    IDX_CST_PIS = 23
    IDX_CST_COFINS = 29
    IDX_COD_ITEM = 1

    cfops = ["1101", "1102", "2101", "2102", "3101", "3102"]
    if not incluir_revenda:
        cfops = [c for c in cfops if c not in ("1102", "2102", "3102")]

    if not cfops:
        return {
            "status": "vazio",
            "msg": "Sem CFOPs após filtros.",
            "candidatos": 0,
            "modo_usado": "LEGADO_EFD",
        }

    if not csts_origem:
        csts_origem = ["70", "73", "74", "75", "98", "99", "06", "07", "08"]
    csts_origem = [str(x).strip() for x in csts_origem if str(x).strip()]

    popular_pai_id(db, versao_origem_id)

    cfop_expr = func.json_unquote(func.json_extract(EfdRegistro.conteudo_json, f"$.dados[{IDX_CFOP}]"))
    cst_pis_expr = func.json_unquote(func.json_extract(EfdRegistro.conteudo_json, f"$.dados[{IDX_CST_PIS}]"))
    cst_cof_expr = func.json_unquote(func.json_extract(EfdRegistro.conteudo_json, f"$.dados[{IDX_CST_COFINS}]"))
    cod_item_expr = func.json_unquote(func.json_extract(EfdRegistro.conteudo_json, f"$.dados[{IDX_COD_ITEM}]"))

    r0200 = aliased(EfdRegistro)
    ncm_expr_0200 = func.json_unquote(func.json_extract(r0200.conteudo_json, "$.dados[6]"))
    ncm_like_cafe = ncm_expr_0200.like("0901%")

    cfop_filters = [cfop_expr == c for c in cfops]
    r100 = aliased(EfdRegistro)
    cod_sit_c100_expr = func.json_unquote(func.json_extract(r100.conteudo_json, "$.dados[4]"))

    q = (
        db.query(EfdRegistro.id)
        .join(
            r100,
            and_(
                r100.id == EfdRegistro.pai_id,
                r100.reg == "C100",
            ),
        )
        .join(
            r0200,
            and_(
                r0200.versao_id == EfdRegistro.versao_id,
                r0200.reg == "0200",
                func.json_unquote(func.json_extract(r0200.conteudo_json, "$.dados[0]")) == cod_item_expr,
            ),
        )
        .filter(
            EfdRegistro.versao_id == versao_origem_id,
            EfdRegistro.reg == "C170",
            or_(*cfop_filters),
            ncm_like_cafe,
            cod_sit_c100_expr.notin_(["06", "07"]),
        )
        .filter(
            (cst_pis_expr.in_(csts_origem)) & (cst_cof_expr.in_(csts_origem))
        )
    )

    if ignorar_registro_ids:
        ignorar_ids = [int(x) for x in ignorar_registro_ids if int(x or 0) > 0]
        if ignorar_ids:
            q = q.filter(~EfdRegistro.id.in_(ignorar_ids))

    ids = [int(x[0]) for x in q.all()]
    if not ids:
        return {
            "status": "vazio",
            "candidatos": 0,
            "total_alterado": 0,
            "modo_usado": "LEGADO_EFD",
        }

    dbg_pf = {"FOR000000004": 0, "FOR000000005": 0, "FOR000000006": 0}
    dbg_pf_passou = {"FOR000000004": 0, "FOR000000005": 0, "FOR000000006": 0}
    dbg_pf_sem_pai = 0

    for rid in ids:
        try:
            r170 = db.get(EfdRegistro, int(rid))
            pai_id = int(getattr(r170, "pai_id", 0) or 0)
            if not pai_id:
                dbg_pf_sem_pai += 1
                continue

            r100 = db.get(EfdRegistro, pai_id)
            if not r100 or getattr(r100, "reg", "") != "C100":
                dbg_pf_sem_pai += 1
                continue

            dados100 = (getattr(r100, "conteudo_json", None) or {}).get("dados", []) or []
            cod_part = str(dados100[2] or "").strip() if len(dados100) > 2 else ""

            if cod_part not in dbg_pf:
                continue

            is_pf = eh_pf_por_c100(db, versao_origem_id, int(rid))
            if is_pf:
                dbg_pf[cod_part] += 1
            else:
                dbg_pf_passou[cod_part] += 1
                print(f"⚠️ PF_VAZOU_NO_LOTE> cod_part={cod_part} rid={rid} pai_id={pai_id}")
        except Exception as _e:
            print("⚠️ DBG_PF erro rid=", rid, repr(_e))

    print("✅ DBG_PF_LOTE contagem_pf=", dbg_pf, "passou=", dbg_pf_passou, "sem_pai=", dbg_pf_sem_pai)

    lote = [{"registro_id": rid, "cfop": None, "cst_pis": "51", "cst_cofins": "51"} for rid in ids]

    res = revisar_c170_lote(
        db,
        versao_origem_id=versao_origem_id,
        alteracoes=lote,
        motivo_codigo="IND_CAFE_V1",
        apontamento_id=apontamento_id,
    )

    return {
        "status": "ok",
        "candidatos": len(ids),
        "total_alterado": int(res.get("total_alterado") or 0),
        "total_ignorado_pf": int(res.get("total_ignorado_pf") or 0),
        "total_erros": int(res.get("total_erros") or 0),
        "erros_detalhe": res.get("erros_detalhe") or [],
        "modo_usado": "LEGADO_EFD",
    }


def aplicar_correcao_ind_cafe_icms_match_cst51(
    db: Session,
    *,
    versao_origem_id: int,
    empresa_id: int,
    periodo: Optional[str] = None,
    incluir_revenda: bool = True,
    csts_origem: Optional[List[str]] = None,
    apontamento_id: Optional[int] = None,
) -> Dict[str, Any]:
    versao_origem_id = int(versao_origem_id)
    empresa_id = int(empresa_id)

    dom = (resolver_dominio_por_versao(db, versao_origem_id) or "").strip().upper()
    if dom != DOM_CAFE:
        return {
            "status": "skip",
            "msg": f"dominio={dom} não permite IND_CAFE_ICMS_MATCH_V1",
            "candidatos_match": 0,
            "candidatos_corrigiveis": 0,
            "total_alterado": 0,
            "modo_usado": "ICMS_MATCH",
        }

    if "51" not in set(CSTS_TRIB_NCUM or set()):
        raise ValueError("settings_fiscais.CSTS_TRIB_NCUM não contém '51'.")

    cfops_validos = ["1101", "1102", "2101", "2102", "3101", "3102"]
    if not incluir_revenda:
        cfops_validos = [c for c in cfops_validos if c not in ("1102", "2102", "3102")]

    if not cfops_validos:
        return {
            "status": "vazio",
            "msg": "Sem CFOPs após filtros.",
            "candidatos_match": 0,
            "modo_usado": "ICMS_MATCH",
        }

    if not csts_origem:
        csts_origem = ["70", "73", "74", "75", "98", "99", "06", "07", "08"]
    csts_origem = [str(x).strip() for x in csts_origem if str(x).strip()]

    cruz = cruzar_versao_com_icms_ipi(
        db,
        versao_origem_id=versao_origem_id,
        empresa_id=empresa_id,
        periodo=periodo,
    )

    itens = cruz.get("itens") or []
    if not itens:
        return {
            "status": "vazio",
            "msg": "Cruzamento sem itens.",
            "candidatos_match": 0,
            "candidatos_corrigiveis": 0,
            "suspeitos_cst_fora_lista": 0,
            "total_alterado": 0,
            "modo_usado": "ICMS_MATCH",
        }

    lote: List[Dict[str, Any]] = []
    suspeitos: List[Dict[str, Any]] = []

    total_inserts = 0
    candidatos_insert = 0

    candidatos_match = 0
    candidatos_corrigiveis = 0
    descartados_fora_cfop = 0
    descartados_fora_dominio = 0
    descartados_sem_registro = 0

    for item in itens:
        status_item = _s(item.get("status"))
        tipo_match = _s(item.get("tipo_match"))

        # =========================================================
        # CASO 1: MATCH normal -> revisar C170 existente
        # =========================================================
        if status_item == "MATCH":
            candidatos_match += 1

            registro_id = item.get("c170_registro_id")
            if not registro_id:
                descartados_sem_registro += 1
                continue

            cfop = _s(item.get("cfop"))
            if cfop not in cfops_validos:
                descartados_fora_cfop += 1
                continue

            if not _eh_item_cafe_match(item):
                descartados_fora_dominio += 1
                continue

            cst_pis_atual = _s(item.get("cst_pis"))
            cst_cof_atual = _s(item.get("cst_cofins"))

            if cst_pis_atual in csts_origem and cst_cof_atual in csts_origem:
                lote.append({
                    "registro_id": int(registro_id),
                    "cfop": None,
                    "cst_pis": "51",
                    "cst_cofins": "51",
                })
                candidatos_corrigiveis += 1
                continue

            if cst_pis_atual == "51" and cst_cof_atual == "51":
                continue

            suspeitos.append({
                "registro_id": int(registro_id),
                "cfop": cfop,
                "cst_pis_atual": cst_pis_atual,
                "cst_cofins_atual": cst_cof_atual,
                "tipo_match": tipo_match,
                "score": item.get("score"),
                "ncm": _s(item.get("ncm")),
                "descricao": _s(item.get("descricao")),
                "observacao": "match forte com ICMS/IPI, porém CST fora da lista segura",
            })
            continue

        # =========================================================
        # CASO 2: ICMS item existe e C170 está faltando
        # =========================================================
        if tipo_match == "CONTRIB_SEM_C170" and status_item != "MATCH":
            cfop = _s(item.get("cfop"))
            if cfop not in cfops_validos:
                descartados_fora_cfop += 1
                continue

            if not _eh_item_cafe_match(item):
                descartados_fora_dominio += 1
                continue

            nf_icms_item_id = int(item.get("nf_icms_item_id") or 0)
            if not nf_icms_item_id:
                descartados_sem_registro += 1
                continue

            if _ja_existe_revisao_insert_para_item(
                    db,
                    versao_origem_id=versao_origem_id,
                    nf_icms_item_id=nf_icms_item_id,
                    motivo_codigo="CONTRIB_SEM_C170_V1",
            ):
                candidatos_insert += 1
                total_inserts += 1
                print(
                    "[DBG INSERT JA_EXISTE]",
                    "nf_icms_item_id=", nf_icms_item_id,
                    flush=True,
                )
                continue

            item_icms = db.query(NfIcmsItem).filter_by(id=nf_icms_item_id).first()

            if not item_icms:
                descartados_sem_registro += 1
                continue

            docs = cruz.get("documentos") or []
            chave_item = _s(item.get("chave_nfe")) or _s(item.get("chave"))

            doc_vinc = next(
                (
                    d for d in docs
                    if _s(d.get("chave_nfe")) == chave_item
                ),
                None,
            )

            registro_id_c100 = int(doc_vinc.get("registro_id_c100") or 0) if doc_vinc else 0
            linha_c100 = int(doc_vinc.get("linha_c100") or 0) if doc_vinc else 0

            if not registro_id_c100:
                descartados_sem_registro += 1
                continue

            _criar_revisao_insert_c170_faltante(
                db,
                versao_origem_id=versao_origem_id,
                registro_id_alvo=registro_id_c100,
                linha_ref=linha_c100,
                item_icms=item_icms,
                motivo_codigo="CONTRIB_SEM_C170_V1",
                apontamento_id=apontamento_id,
            )
            total_inserts += 1
            candidatos_insert += 1
            continue

    if not lote and total_inserts == 0:
        return {
            "status": "vazio",
            "msg": "Sem candidatos corrigíveis após filtros.",
            "candidatos_match": candidatos_match,
            "candidatos_corrigiveis": 0,
            "candidatos_insert": 0,
            "suspeitos_cst_fora_lista": len(suspeitos),
            "descartados_fora_cfop": descartados_fora_cfop,
            "descartados_fora_dominio": descartados_fora_dominio,
            "descartados_sem_registro": descartados_sem_registro,
            "suspeitos_detalhe": suspeitos[:50],
            "total_alterado": 0,
            "total_inserts": 0,
            "modo_usado": "ICMS_MATCH",
        }

    res = {
        "total_alterado": 0,
        "total_ignorado_pf": 0,
        "total_ignorado_sit": 0,
        "total_erros": 0,
        "erros_detalhe": [],
    }

    if lote:
        res = revisar_c170_lote(
            db,
            versao_origem_id=versao_origem_id,
            alteracoes=lote,
            motivo_codigo="IND_CAFE_V1",
            apontamento_id=apontamento_id,
        )
    print(
        "[DBG ICMS_MATCH FINAL]",
        "lote=", len(lote),
        "total_inserts=", total_inserts,
        "candidatos_insert=", candidatos_insert,
        flush=True,
    )

    registro_ids_alterados = [int(x["registro_id"]) for x in lote if int(x.get("registro_id") or 0) > 0]

    return {
        "status": "ok",
        "candidatos_match": candidatos_match,
        "candidatos_corrigiveis": candidatos_corrigiveis,
        "candidatos_insert": candidatos_insert,
        "suspeitos_cst_fora_lista": len(suspeitos),
        "descartados_fora_cfop": descartados_fora_cfop,
        "descartados_fora_dominio": descartados_fora_dominio,
        "descartados_sem_registro": descartados_sem_registro,
        "suspeitos_detalhe": suspeitos[:50],
        "total_alterado": int(res.get("total_alterado") or 0),
        "total_inserts": int(total_inserts or 0),
        "total_ignorado_pf": int(res.get("total_ignorado_pf") or 0),
        "total_ignorado_sit": int(res.get("total_ignorado_sit") or 0),
        "total_erros": int(res.get("total_erros") or 0),
        "erros_detalhe": res.get("erros_detalhe") or [],
        "modo_usado": "ICMS_MATCH",
        "registro_ids_alterados": registro_ids_alterados,
    }

def aplicar_correcao_ind_cafe_cst51(
    db: Session,
    *,
    versao_origem_id: int,
    incluir_revenda: bool = True,
    csts_origem: Optional[List[str]] = None,
    apontamento_id: Optional[int] = None,
) -> Dict[str, Any]:
    """
    Wrapper oficial da autocorreção do café.

    Estratégia nova:
      1) roda ICMS/IPI + EFD (preferencial)
      2) depois roda o legado como complemento
      3) retorna resultado combinado
    """
    versao_origem_id = int(versao_origem_id)

    empresa_id, periodo = _resolver_empresa_e_periodo_da_versao(
        db,
        versao_origem_id=versao_origem_id,
    )

    res_icms: Dict[str, Any] = {
        "status": "skip",
        "total_alterado": 0,
        "total_inserts": 0,
        "modo_usado": "ICMS_MATCH",
    }

    if empresa_id:
        res_icms = aplicar_correcao_ind_cafe_icms_match_cst51(
            db,
            versao_origem_id=versao_origem_id,
            empresa_id=empresa_id,
            periodo=periodo,
            incluir_revenda=incluir_revenda,
            csts_origem=csts_origem,
            apontamento_id=apontamento_id,
        )

        status_icms = str(res_icms.get("status") or "").strip().lower()
        alterados_icms = int(res_icms.get("total_alterado") or 0)
        inserts_icms = int(res_icms.get("total_inserts") or 0)

        print(
            "[IND_CAFE_V1][ICMS_MATCH]",
            "status=", status_icms,
            "alterados=", alterados_icms,
            "inserts=", inserts_icms,
            "res=", res_icms,
        )

        if status_icms == "erro":
            return res_icms

    ids_ja_tratados_icms = [int(x) for x in (res_icms.get("registro_ids_alterados") or []) if int(x or 0) > 0]

    # legado sempre roda como complemento
    res_legado = aplicar_correcao_ind_cafe_cst51_legado(
        db,
        versao_origem_id=versao_origem_id,
        incluir_revenda=incluir_revenda,
        csts_origem=csts_origem,
        apontamento_id=apontamento_id,
        ignorar_registro_ids=ids_ja_tratados_icms,
    )

    print(
        "[IND_CAFE_V1][LEGADO_EFD]",
        "status=", res_legado.get("status"),
        "alterados=", res_legado.get("total_alterado"),
        "res=", res_legado,
    )

    alterados_icms = int(res_icms.get("total_alterado") or 0)
    inserts_icms = int(res_icms.get("total_inserts") or 0)

    alterados_legado = int(res_legado.get("total_alterado") or 0)
    ignorado_pf_legado = int(res_legado.get("total_ignorado_pf") or 0)
    erros_legado = int(res_legado.get("total_erros") or 0)

    status_final = "vazio"
    if alterados_icms > 0 or inserts_icms > 0 or alterados_legado > 0:
        status_final = "ok"

    return {
        "status": status_final,

        # bloco ICMS_MATCH
        "icms_match": res_icms,
        "legado_efd": res_legado,

        # consolidados
        "total_alterado": alterados_icms + alterados_legado,
        "total_alterado_icms_match": alterados_icms,
        "total_alterado_legado": alterados_legado,
        "total_inserts": inserts_icms,

        "total_ignorado_pf": int(res_icms.get("total_ignorado_pf") or 0) + ignorado_pf_legado,
        "total_ignorado_sit": int(res_icms.get("total_ignorado_sit") or 0),
        "total_erros": int(res_icms.get("total_erros") or 0) + erros_legado,

        "erros_detalhe": (res_icms.get("erros_detalhe") or []) + (res_legado.get("erros_detalhe") or []),

        "modo_usado": "ICMS_MATCH+LEGADO",
        "modo_preferencial": True,
        "modo_complementar": True,
    }