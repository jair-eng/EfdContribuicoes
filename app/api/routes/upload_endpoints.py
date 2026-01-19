from __future__ import annotations
import traceback
from fastapi import APIRouter, Depends, File, HTTPException, UploadFile, status
from pydantic import BaseModel, Field
from sqlalchemy.orm import Session
from app.db.session import get_db
from app.services.upload_confirm_service import UploadConfirmService
from app.services.upload_service import UploadService
from typing import List , Dict , Any
from app.services.upload_preview_service import UploadPreviewService





router = APIRouter(prefix="/sped", tags=["SPED Upload"])

class UploadPreviewResponse(BaseModel):
    temp_id: str
    cnpj: str
    razao_social: str | None = None
    periodo: str
    total_linhas: int
    nome_arquivo: str | None = None
    line_ending: str = "LF"  # "LF" ou "CRLF"


class UploadConfirmPayload(BaseModel):
    temp_id: str = Field(..., min_length=8)
    # opcionais, se você quiser retornar/registrar o nome que veio do upload (não obrigatório)
    nome_arquivo: str | None = None


class UploadConfirmResponse(BaseModel):
    empresa_id: int
    arquivo_id: int
    versao_id: int
    total_registros: int
    line_ending: str | None = None
    apontamentos_gerados: int = 0
    erros_regras: list[str] = []


# ---------- Endpoints ----------

@router.post(
    "/upload/preview",
    status_code=status.HTTP_201_CREATED,
    response_model=UploadPreviewResponse,
)
def upload_preview(file: UploadFile = File(...)):
    """
    Recebe SPED, salva temporariamente e devolve dados pro front confirmar.
    """
    try:
        return UploadService.preview(file)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))


@router.post(
    "/upload/confirm",
    status_code=status.HTTP_201_CREATED,
    response_model=UploadConfirmResponse,
)
def upload_confirm(payload: UploadConfirmPayload, db: Session = Depends(get_db)):
    """
    Confirma o upload:
    - persiste Empresa/Arquivo/Versão/Registros
    - (opcional) executa scanner e salva apontamentos
    """
    try:
        return UploadService.confirm(
            db,
            payload.model_dump()

        )

    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post(
    "/upload/preview-batch",
    status_code=status.HTTP_201_CREATED,
)
def upload_preview_batch(
    files: List[UploadFile] = File(...),
) -> Dict[str, Any]:
    items: List[UploadPreviewResponse] = []
    errors: List[dict] = []

    for f in files:
        try:
            res = UploadPreviewService.processar_preview(f)
            items.append(res)

        except Exception as e:
            errors.append({
                "filename": getattr(f, "filename", None),
                "error": str(e),
            })

    if not items and errors:
        raise HTTPException(
            status_code=400,
            detail={
                "message": "Nenhum arquivo pôde ser processado no preview",
                "errors": errors,
            },
        )

    return {
        "items": items,
        "errors": errors,
        "total_sucesso": len(items),
        "total_erro": len(errors),
    }


@router.post("/upload/confirm-batch", status_code=status.HTTP_201_CREATED)
def upload_confirm_batch(payloads: List[UploadConfirmPayload], db: Session = Depends(get_db)):
    results = []
    errors = []

    for p in payloads:
        try:
            # ✅ SAVEPOINT por arquivo (isolamento real sem conflito)
            with db.begin_nested():
                res = UploadConfirmService.confirmar_upload(
                    db,
                    temp_id=p.temp_id,
                    nome_arquivo=p.nome_arquivo,
                )
            results.append(res)

        except Exception as e:
            # garante que o savepoint foi revertido
            db.rollback()
            errors.append({"temp_id": p.temp_id, "error": str(e)})

    # ✅ commit final do lote (salva todos os que passaram)
    db.commit()

    if not results and errors:
        raise HTTPException(status_code=400, detail={"message": "Nenhum arquivo foi confirmado", "errors": errors})

    return {
        "items": results,
        "errors": errors,
        "total_sucesso": len(results),
        "total_erro": len(errors),
    }
