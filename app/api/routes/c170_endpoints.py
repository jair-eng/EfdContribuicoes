from __future__ import annotations
from typing import Any, Dict, List, Optional
from fastapi import APIRouter, Depends, HTTPException, Query, status
from sqlalchemy.orm import Session
from app.db.session import get_db
from app.db.models import EfdRegistro
from app.schemas.c170 import C170PatchPayload
from app.services.c170_service import revisar_c170

router = APIRouter(prefix="/workflow", tags=["Workflow"])


@router.get("/versao/{versao_id}/c170", status_code=status.HTTP_200_OK)
def listar_c170(
    versao_id: int,
    db: Session = Depends(get_db),
    limit: int = Query(default=200, ge=1, le=1000),
    offset: int = Query(default=0, ge=0),
    somente_alterados: bool = Query(default=False),
) -> Dict[str, Any]:
    """
    Lista C170 em formato tabular (para editor manual).
    """
    q = (
        db.query(EfdRegistro)
        .filter(EfdRegistro.versao_id == int(versao_id))
        .filter(EfdRegistro.reg == "C170")
    )

    if somente_alterados:
        q = q.filter(EfdRegistro.alterado.is_(True))

    total = q.count()

    regs: List[EfdRegistro] = (
        q.order_by(EfdRegistro.linha.asc())
        .offset(int(offset))
        .limit(int(limit))
        .all()
    )

    items = []
    for r in regs:
        cj = getattr(r, "conteudo_json", None) or {}
        dados = cj.get("dados") or []
        if not isinstance(dados, list):
            dados = []

        # MVP: devolve dados crus + alguns “atalhos”
        items.append({
            "registro_id": int(r.id),
            "linha": int(getattr(r, "linha", 0)),
            "reg": "C170",
            "alterado": bool(getattr(r, "alterado", False)),
            "dados": dados,  # útil no MVP; depois você pode devolver colunas específicas
        })

    return {
        "items": items,
        "total": int(total),
        "limit": int(limit),
        "offset": int(offset),
    }


@router.post("/registro/{registro_id}/revisar-c170", status_code=status.HTTP_200_OK)
def revisar_c170_endpoint(
    registro_id: int,
    payload: C170PatchPayload,
    db: Session = Depends(get_db),
) -> Dict[str, Any]:
    """
    Aplica patch em C170 e cria EfdRevisao(REPLACE_LINE).
    """
    try:
        res = revisar_c170(
            db,
            registro_id=int(registro_id),
            versao_origem_id=int(payload.versao_origem_id),
            cfop=payload.cfop,
            cst_pis=payload.cst_pis,
            cst_cofins=payload.cst_cofins,
            motivo_codigo=payload.motivo_codigo,
            apontamento_id=payload.apontamento_id,
        )
        db.commit()
        return {"status": "OK", **res}
    except ValueError as e:
        db.rollback()
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=str(e))
