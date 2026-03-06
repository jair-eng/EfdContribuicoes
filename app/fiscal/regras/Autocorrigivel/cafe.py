from __future__ import annotations
from typing import Dict, Any, List, Optional

from sqlalchemy.orm import Session, aliased
from sqlalchemy import func, or_, and_

from app.fiscal.constants import DOM_CAFE
from app.fiscal.settings_fiscais import CSTS_TRIB_NCUM
from app.db.models.efd_registro import EfdRegistro
from app.services.c170_service import revisar_c170_lote
from app.services.dominio_service import resolver_dominio_por_versao
from app.sped.logic.consolidador import popular_pai_id, eh_pf_por_c100


def aplicar_correcao_ind_cafe_cst51(
    db: Session,
    *,
    versao_origem_id: int,
    incluir_revenda: bool = True,
    csts_origem: Optional[List[str]] = None,
    apontamento_id: Optional[int] = None,
) -> Dict[str, Any]:
    versao_origem_id = int(versao_origem_id)

    # ✅ Guard-rail por domínio
    dom = (resolver_dominio_por_versao(db, versao_origem_id) or "").strip().upper()
    if dom != DOM_CAFE:
        return {"status": "skip", "msg": f"dominio={dom} não permite IND_CAFE_V1", "candidatos": 0, "total_alterado": 0}

    # Guard-rail: garante que 51 é permitido como CST de crédito no settings
    if "51" not in set(CSTS_TRIB_NCUM or set()):
        raise ValueError("settings_fiscais.CSTS_TRIB_NCUM não contém '51'.")

    IDX_CFOP = 9
    IDX_CST_PIS = 23
    IDX_CST_COFINS = 29
    IDX_COD_ITEM = 1  # mantém como está (você disse que funciona)

    cfops = ["1101", "1102", "2101", "2102", "3101", "3102"]
    if not incluir_revenda:
        cfops = [c for c in cfops if c not in ("1102", "2102", "3102")]

    if not cfops:
        return {"status": "vazio", "msg": "Sem CFOPs após filtros.", "candidatos": 0}

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
    cod_sit_c100_expr = func.json_unquote(func.json_extract(r100.conteudo_json, "$.dados[4]"))  # COD_SIT no C100

    q = (
        db.query(EfdRegistro.id)
        .join(  # C170 -> C100 pai
            r100,
            and_(
                r100.id == EfdRegistro.pai_id,
                r100.reg == "C100",
            ),
        )
        .join(  # C170 -> 0200
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
            # ✅ BLOQUEIO: não pegar itens de C100 complementar/cancelado
            cod_sit_c100_expr.notin_(["06", "07"]),
        )
        .filter(
            (cst_pis_expr.in_(csts_origem)) & (cst_cof_expr.in_(csts_origem))
        )
    )

    ids = [int(x[0]) for x in q.all()]
    if not ids:
        return {"status": "vazio", "candidatos": 0}

    # DEBUG PF (mantém)
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
    }