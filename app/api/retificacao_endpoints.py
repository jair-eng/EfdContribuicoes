from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.orm import Session

from app.db.session import get_db
from app.services.retificacao_service import RetificacaoService

router = APIRouter(prefix="/workflow", tags=["Retificação"])


@router.post("/versao/{versao_id}/retificar", status_code=status.HTTP_201_CREATED)
def retificar_versao(versao_id: int, db: Session = Depends(get_db)):
    try:
        res = RetificacaoService.criar_retificacao(db, versao_id=versao_id)
        db.commit()
        return res
    except ValueError as e:
        db.rollback()
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=400, detail=str(e))
