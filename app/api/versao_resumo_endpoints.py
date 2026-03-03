from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.orm import Session
import traceback
from app.db.session import get_db
from app.services.versao_resumo_service import VersaoResumoService

router = APIRouter(prefix="/workflow", tags=["Resumo da Versão"])


@router.get(
    "/versao/{versao_id}/resumo",
    status_code=status.HTTP_200_OK,
)
def resumo_versao(versao_id: int, db: Session = Depends(get_db)):
    try:
        return VersaoResumoService.gerar_resumo(db, versao_id=versao_id)
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        traceback.print_exc()  # <-- imprime o erro real no terminal
        raise HTTPException(status_code=400, detail=repr(e))  # <-- detail mais informativo
