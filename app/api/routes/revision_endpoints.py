from __future__ import annotations
from typing import Any, Dict, Optional
from fastapi import APIRouter, Depends, HTTPException, status
from pydantic import BaseModel, Field
from sqlalchemy.orm import Session

from app.db.models import EfdVersao
from app.db.session import get_db
from app.schemas.workflow import AplicarRevisaoPayload, AplicarRevisoesEmLotePayload
from app.services.revision_service import RevisionService


router = APIRouter(prefix="/revisao", tags=["Revisao"])


class RevisaoReplaceLineIn(BaseModel):
    apontamento_id: int
    linha_nova: str = Field(min_length=3)
    motivo_codigo: Optional[str] = None

@router.post("/versao/{versao_id}/aplicar-revisao", status_code=status.HTTP_201_CREATED)
def aplicar_revisao(
    versao_id: int,
    payload: AplicarRevisaoPayload,
    db: Session = Depends(get_db),
):
    try:
        # ✅ usa versao_id explicitamente (valida existência)
        versao = db.get(EfdVersao, int(versao_id))
        if not versao:
            raise HTTPException(status_code=404, detail="Versão não encontrada")

        rev = RevisionService.criar_revisao_replace_line(
            db,
            versao_id=int(versao_id),  # ✅ uso explícito (isso já resolve o cinza)
            apontamento_id=int(payload.apontamento_id),
            linha_nova=str(payload.linha_nova),
            motivo_codigo=payload.motivo_codigo,
        )
        db.commit()
        return {
            "revisao_id": int(rev.id),
            "versao_origem_id": int(rev.versao_origem_id),
            "versao_revisada_id": int(rev.versao_revisada_id),
            "registro_id": int(rev.registro_id),
            "acao": str(rev.acao),
        }
    except HTTPException:
        db.rollback()
        raise
    except ValueError as e:
        db.rollback()
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=str(e))


@router.post(
    "/versao/{versao_id}/aplicar-revisoes-em-lote",
    status_code=status.HTTP_201_CREATED,
)
def aplicar_revisoes_em_lote(
    versao_id: int,
    payload: AplicarRevisoesEmLotePayload,
    db: Session = Depends(get_db),
):
    if not payload.itens:
        raise HTTPException(status_code=400, detail="Lista 'itens' vazia.")

    criadas = []
    erros = []

    try:
        # valida existência da versão (tira cinza e dá erro bonito)
        versao = db.get(EfdVersao, int(versao_id))
        if not versao:
            raise HTTPException(status_code=404, detail="Versão não encontrada")

        for it in payload.itens:
            try:
                rev = RevisionService.criar_revisao_replace_line(
                    db,
                    versao_id=int(versao_id),
                    apontamento_id=int(it.apontamento_id),
                    linha_nova=str(it.linha_nova),
                    motivo_codigo=it.motivo_codigo,
                )
                criadas.append({
                    "revisao_id": int(rev.id),
                    "apontamento_id": int(it.apontamento_id),
                    "registro_id": int(rev.registro_id),
                    "versao_revisada_id": int(rev.versao_revisada_id),
                })
            except Exception as e:
                erros.append({
                    "apontamento_id": int(it.apontamento_id),
                    "erro": str(e),
                })

        # ✅ commit único
        db.commit()

        return {
            "versao_id": int(versao_id),
            "total_solicitado": len(payload.itens),
            "total_criadas": len(criadas),
            "total_erros": len(erros),
            "criadas": criadas,
            "erros": erros,
        }

    except HTTPException:
        db.rollback()
        raise
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=str(e))