from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.orm import Session
from app.db.session import get_db
from app.services.empresa_resumo_service import EmpresaResumoService

router = APIRouter(prefix="/empresa", tags=["Resumo da Empresa"])


@router.get(
    "/{empresa_id}/resumo",
    status_code=status.HTTP_200_OK,
)
def resumo_empresa(empresa_id: int, db: Session = Depends(get_db)):
    try:
        return EmpresaResumoService.gerar_resumo(db, empresa_id=empresa_id)
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))
