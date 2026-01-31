from __future__ import annotations
from sqlalchemy.orm import aliased
from sqlalchemy import func
import traceback
from typing import Any, Dict, List, Optional
from fastapi import APIRouter, Depends, HTTPException, Query, status
from sqlalchemy.orm import Session
from app.db.session import get_db
from app.db.models import EfdRegistro
from app.fiscal.scanner import FiscalScanner
from app.schemas.c170 import C170PatchPayload, C170BatchPayload
from app.schemas.workflow import RevisaoGlobalSchema
from app.services import c170_service
from app.services.c170_service import revisar_c170, revisar_c170_lote
from app.sped.logic.consolidador import aplicar_overlay_revisoes_c170
from sqlalchemy import or_

router = APIRouter(prefix="/workflow", tags=["Workflow"])


@router.get("/versao/{versao_id}/c170", status_code=status.HTTP_200_OK)
def listar_c170(
    versao_id: int,
    db: Session = Depends(get_db),
    limit: int = Query(default=200, ge=1, le=1000),
    offset: int = Query(default=0, ge=0),
    somente_alterados: bool = Query(default=False),
    ocultar_pf: bool = Query(default=True),

    # ✅ filtros do front
    cfop: Optional[str] = Query(default=None),
    cst_pis: Optional[str] = Query(default=None),
) -> Dict[str, Any]:

    c170 = aliased(EfdRegistro)
    c100 = aliased(EfdRegistro)
    p0150 = aliased(EfdRegistro)

    q = (
        db.query(c170)
        .filter(c170.versao_id == int(versao_id))
        .filter(c170.reg == "C170")
    )

    # ✅ filtros no SQL antes do limit/count
    cfop_f = (cfop or "").strip()
    cst_f = (cst_pis or "").strip()

    if cfop_f:
        cfop_json = func.trim(func.JSON_UNQUOTE(func.JSON_EXTRACT(c170.conteudo_json, "$.dados[9]")))
        q = q.filter(cfop_json == cfop_f)

    if cst_f:
        cst_json = func.trim(func.JSON_UNQUOTE(func.JSON_EXTRACT(c170.conteudo_json, "$.dados[23]")))
        q = q.filter(cst_json == cst_f)

    if somente_alterados:
        q = q.filter(c170.alterado.is_(True))

    # ✅ join para classificar PF/PJ via 0150
    q = q.join(
        c100,
        (c100.id == c170.pai_id)
        & (c100.versao_id == c170.versao_id)
        & (c100.reg == "C100"),
    )

    cod_part = func.trim(func.JSON_UNQUOTE(func.JSON_EXTRACT(c100.conteudo_json, "$.dados[2]")))
    cod_0150 = func.trim(func.JSON_UNQUOTE(func.JSON_EXTRACT(p0150.conteudo_json, "$.dados[0]")))
    cpf_0150 = func.trim(func.JSON_UNQUOTE(func.JSON_EXTRACT(p0150.conteudo_json, "$.dados[4]")))

    q = q.join(
        p0150,
        (p0150.versao_id == c100.versao_id)
        & (p0150.reg == "0150")
        & (cod_0150 == cod_part),
    )

    # ✅ contadores PF/PJ (para UI)
    total_pf = (
        q.filter(cpf_0150.isnot(None))
         .filter(cpf_0150 != "")
         .count()
    )
    total_pj = (
        q.filter(or_(cpf_0150.is_(None), cpf_0150 == ""))
         .count()
    )

    # ✅ aplicar ocultar_pf (default True)
    if ocultar_pf:
        q = q.filter(or_(cpf_0150.is_(None), cpf_0150 == ""))

    total = q.count()

    regs: List[EfdRegistro] = (
        q.order_by(c170.linha.asc())
        .offset(int(offset))
        .limit(int(limit))
        .all()
    )

    items: List[Dict[str, Any]] = []
    for r in regs:
        cj = getattr(r, "conteudo_json", None) or {}
        dados_raw = cj.get("dados") or []

        if isinstance(dados_raw, list):
            if (
                len(dados_raw) == 2
                and isinstance(dados_raw[0], str)
                and isinstance(dados_raw[1], list)
                and (dados_raw[0] or "").strip().upper() == "C170"
            ):
                dados = dados_raw[1]
            else:
                dados = dados_raw
        else:
            dados = []

        items.append({
            "registro_id": int(r.id),
            "linha": int(getattr(r, "linha", 0)),
            "reg": "C170",
            "alterado": bool(getattr(r, "alterado", False)),
            "dados": dados,
        })

    try:
        items, aplicadas = aplicar_overlay_revisoes_c170(db, versao_id=int(versao_id), items=items)
    except Exception as e:
        print("ERRO overlay C170:", repr(e))
        aplicadas = 0

    return {
        "items": items,
        "total": int(total),
        "limit": int(limit),
        "offset": int(offset),
        "overlay_revisoes_aplicadas": int(aplicadas),

        # ✅ extra para UI
        "totais_pf_pj": {
            "pf_cpf": int(total_pf),
            "pj_cnpj": int(total_pj),
            "total_bruto": int(total_pf + total_pj),
        }
    }


@router.post("/registro/{registro_id}/revisar-c170", status_code=status.HTTP_200_OK)
def revisar_c170_endpoint(
    registro_id: int,
    payload: C170PatchPayload,
    db: Session = Depends(get_db),
) -> Dict[str, Any]:
    """
    Aplica patch em C170 e cria/atualiza EfdRevisao(REPLACE_LINE) pendente (UPSERT).
    """
    try:
        res = revisar_c170(
            db,
            registro_id=int(registro_id),
            versao_origem_id=int(payload.versao_origem_id),
            cfop=payload.cfop,
            cst_pis=payload.cst_pis,
            cst_cofins=payload.cst_cofins,
            motivo_codigo=str(payload.motivo_codigo or "MANUAL_C170"),
            apontamento_id=int(payload.apontamento_id) if payload.apontamento_id else None,
        )
        db.commit()
        return {"status": "OK", **res}

    except HTTPException:
        db.rollback()
        raise
    except ValueError as e:
        db.rollback()
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=str(e))



@router.post("/versao/{versao_id}/c170/revisar-lote", status_code=status.HTTP_200_OK)
def revisar_c170_lote_endpoint(
    versao_id: int,
    payload: C170BatchPayload,
    db: Session = Depends(get_db),
) -> Dict[str, Any]:
    """
    Aplica patches em lote no C170 e cria/atualiza EfdRevisao(REPLACE_LINE) pendentes.
    Depois roda o scanner automaticamente para reaplicar regras sobre o SPED lógico.
    """
    try:
        if int(payload.versao_origem_id) != int(versao_id):
            raise HTTPException(
                status_code=400,
                detail="versao_origem_id do payload deve bater com o versao_id da URL.",
            )

        alteracoes = [
            (a.model_dump() if hasattr(a, "model_dump") else dict(a))
            for a in (payload.alteracoes or [])
        ]

        res = revisar_c170_lote(
            db,
            versao_origem_id=int(payload.versao_origem_id),
            alteracoes=alteracoes,
            motivo_codigo=str(payload.motivo_codigo or "MANUAL_TABELA_C170"),
            apontamento_id=int(payload.apontamento_id) if payload.apontamento_id else None,
        )

        ok = int((res or {}).get("ok", 0) or 0)
        erros = int((res or {}).get("erros", 0) or 0)
        warnings = (res or {}).get("warnings") or []

        # 1) commit das revisões antes do scan (o scan lê EfdRevisao)
        db.commit()

        # 2) auto-scan — lê revisões pendentes e reaplica regras
        scan = FiscalScanner.scan_versao(
            db,
            versao_id=int(versao_id),
            preservar_resolvidos=True,
            aplicar_revisoes=True,
        )

        # 3) commit do scan (ele escreve em efd_apontamento)
        db.commit()

        return {
            "status": "OK",
            "ok": ok,
            "erros": erros,
            "warnings": warnings,
            "scan": scan,
        }

    except HTTPException:
        db.rollback()
        raise
    except ValueError as e:
        db.rollback()
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        db.rollback()
        # se tiver logger, prefira logger.exception aqui
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/versao/{versao_id}/c170/revisar-global")
def post_revisar_c170_global(
        versao_id: int,
        payload: RevisaoGlobalSchema,
        db: Session = Depends(get_db)
):
    """
    Endpoint que recebe o comando do Streamlit para alterar todos
    os registros de uma vez no banco de dados.
    """
    try:
        # Chamada para a função inteligente que criamos no service
        resultado = c170_service.revisar_c170_global(
            db,
            versao_origem_id=payload.versao_origem_id,
            filtros_origem=payload.filtros_origem,
            valores_novos=payload.valores_novos,
            motivo_codigo=payload.motivo_codigo,
            apontamento_id=payload.apontamento_id
        )

        if resultado.get("status") == "vazio":
            msg = resultado.get("mensagem") or resultado.get(
                "message") or "Nenhum registro encontrado para os filtros informados."
            raise HTTPException(status_code=404, detail=msg)

        return resultado


    except Exception as e:

        print("❌ ERRO post_revisar_c170_global")

        print("Erro:", repr(e))

        print(traceback.format_exc())  # 🔥 ISSO É O QUE FALTAVA

        raise HTTPException(

            status_code=500,

            detail=str(e)

        )