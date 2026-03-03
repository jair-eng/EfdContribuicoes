from __future__ import annotations
from typing import Dict, Any, List, Optional
from sqlalchemy.orm import Session, aliased
from app.fiscal.settings_fiscais import CSTS_TRIB_NCUM
from app.db.models.efd_registro import EfdRegistro
from sqlalchemy import func, or_, and_
from app.services.c170_service import revisar_c170_lote
from app.sped.logic.consolidador import  popular_pai_id



def aplicar_correcao_ind_agro_cst51(
    db: Session,
    *,
    versao_origem_id: int,
    # ✅ por padrão NÃO corrige revenda (1102/2102/3102)
    incluir_revenda: bool = False,
    # CSTs de origem (sem crédito hoje)
    csts_origem: Optional[List[str]] = None,
    apontamento_id: Optional[int] = None,
    motivo_codigo: str = "IND_AGRO_V1",
) -> Dict[str, Any]:
    """
    Correção automática (determinística) para tese IND_AGRO (grãos/commodities):
      - Seleciona C170 por CFOP de ENTRADA
        * industrialização: 1101/2101/3101 (sempre)
        * revenda:         1102/2102/3102 (opcional, incluir_revenda=True)
      - Filtra itens cujo NCM (0200) pertence às famílias 10% ou 12%
      - Filtra CSTs de origem (sem crédito)
      - Aplica CST_PIS=51 e CST_COFINS=51 via revisar_c170_lote
      - C100 é consolidado automaticamente no revisar_c170_lote
    """

    versao_origem_id = int(versao_origem_id)

    # Guard-rail: garante que 51 é permitido como CST de crédito no settings
    if "51" not in set(CSTS_TRIB_NCUM or set()):
        raise ValueError("settings_fiscais.CSTS_TRIB_NCUM não contém '51'.")

    # Índices do seu parser (mesmo padrão do café)
    IDX_CFOP = 9
    IDX_CST_PIS = 23
    IDX_CST_COFINS = 29
    IDX_COD_ITEM = 1  # ⚠️ ajuste se no seu parser COD_ITEM não for [1]

    # CFOPs entrada (industrialização + opcional revenda)
    cfops_ind = ["1101", "2101", "3101"]
    cfops_rev = ["1102", "2102", "3102"]
    cfops = list(cfops_ind) + (list(cfops_rev) if incluir_revenda else [])

    if not cfops:
        return {"status": "vazio", "msg": "Sem CFOPs após filtros.", "candidatos": 0}

    # CSTs de origem (conservador/agressivo conforme já vem sendo usado)
    if not csts_origem:
        csts_origem = ["70", "73","74", "75", "98", "99", "06", "07", "08"]
    csts_origem = [str(x).strip() for x in csts_origem if str(x).strip()]
    if not csts_origem:
        return {"status": "vazio", "msg": "Sem CSTs de origem após normalização.", "candidatos": 0}

    # Garante pai_id (trava PF no lote)
    popular_pai_id(db, versao_origem_id)

    # Expressões JSON (C170)
    cfop_expr = func.json_unquote(func.json_extract(EfdRegistro.conteudo_json, f"$.dados[{IDX_CFOP}]"))
    cst_pis_expr = func.json_unquote(func.json_extract(EfdRegistro.conteudo_json, f"$.dados[{IDX_CST_PIS}]"))
    cst_cof_expr = func.json_unquote(func.json_extract(EfdRegistro.conteudo_json, f"$.dados[{IDX_CST_COFINS}]"))
    cod_item_expr = func.json_unquote(func.json_extract(EfdRegistro.conteudo_json, f"$.dados[{IDX_COD_ITEM}]"))

    # --- join C170 -> 0200 (mesma versao) ---
    r0200 = aliased(EfdRegistro)

    # NCM no 0200: índice 6 (padrão do scanner)
    ncm_expr_0200 = func.json_unquote(func.json_extract(r0200.conteudo_json, "$.dados[6]"))

    # Match famílias 10/12
    ncm_like_10 = ncm_expr_0200.like("10%")
    ncm_like_12 = ncm_expr_0200.like("12%")
    ncm_like_agro = or_(ncm_like_10, ncm_like_12)

    # filtros CFOP
    cfop_filters = [cfop_expr == c for c in cfops]

    # Query candidatos:
    # - C170 da versão
    # - CFOP entrada (grupo)
    # - CSTs origem sem crédito (pis ou cofins)
    # - COD_ITEM linka com 0200 e NCM começa com 10 ou 12
    q = (
        db.query(EfdRegistro.id)
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
            ncm_like_agro,
        )
        .filter(
            (cst_pis_expr.in_(csts_origem)) | (cst_cof_expr.in_(csts_origem))
        )
    )

    try:
        ids = [int(x[0]) for x in q.all()]
    except Exception as e:
        print("❌ [IND_AGRO_CORR] ERRO query candidatos:", repr(e))
        return {"status": "erro", "msg": f"Erro ao consultar candidatos: {repr(e)}", "candidatos": 0}

    if not ids:
        return {"status": "vazio", "candidatos": 0}

    # Monta lote: só CSTs (não mexe no CFOP)
    lote = [{"registro_id": rid, "cfop": None, "cst_pis": "51", "cst_cofins": "51"} for rid in ids]

    # Aplica via seu service robusto (com trava PF + consolidação C100)
    try:
        res = revisar_c170_lote(
            db,
            versao_origem_id=versao_origem_id,
            alteracoes=lote,
            motivo_codigo=motivo_codigo,  # ✅ vínculo com a regra/código
            apontamento_id=apontamento_id,
        )
    except Exception as e:
        print("❌ [IND_AGRO_CORR] ERRO revisar_c170_lote:", repr(e))
        return {"status": "erro", "msg": f"Erro ao aplicar revisões: {repr(e)}", "candidatos": len(ids)}

    return {
        "status": "ok",
        "versao_origem_id": int(versao_origem_id),
        "motivo_codigo": str(motivo_codigo),
        "incluiu_revenda": bool(incluir_revenda),
        "cfops_usados": cfops,
        "familias_ncm_0200": ["10%", "12%"],
        "csts_origem": csts_origem,
        "candidatos": len(ids),
        "total_alterado": int(res.get("total_alterado") or 0),
        "total_ignorado_pf": int(res.get("total_ignorado_pf") or 0),
        "total_erros": int(res.get("total_erros") or 0),
        "erros_detalhe": res.get("erros_detalhe") or [],
    }

