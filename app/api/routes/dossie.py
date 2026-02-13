# app/api/routes/dossie.py
from __future__ import annotations

from pathlib import Path
from typing import Optional
from app.services.dossie_exportacao_service import gerar_dossie_exportacao_docx_da_pasta
from fastapi import APIRouter, Depends, Query
from fastapi.responses import FileResponse
from sqlalchemy.orm import Session

from app.db.session import get_db  # ajuste se seu get_db estiver em outro lugar
from fastapi.responses import FileResponse
from app.services.dossie_exportacao_service import gerar_dossie_exportacao_docx


router = APIRouter(prefix="/dossie", tags=["Dossiê"])


@router.get("/exportacao/{versao_id}")
def dossie_exportacao(
    versao_id: int,
    db: Session = Depends(get_db),
    empresa_nome: Optional[str] = Query(default=None, description="Opcional: override do nome da empresa no PDF"),
):
    docx_path = gerar_dossie_exportacao_docx(
        db=db,
        versao_id=int(versao_id),
        empresa_nome_override=empresa_nome,
        output_dir=Path.home() / "Downloads" / "Dossies",
    )
    return FileResponse(
        path=str(docx_path),
        media_type="application/vnd.openxmlformats-officedocument.wordprocessingml.document",
        filename=docx_path.name,
    )


@router.get("/exportacao_txt")
def dossie_exportacao_por_txt(
    empresa_nome: Optional[str] = Query(default=None),
    prefer_name_contains: Optional[str] = Query(default=None, description="Opcional: filtra pelo nome do TXT"),
):
    pasta = Path.home() / "Downloads" / "Dossies"  # <<< sua pasta dossie
    docx_path = gerar_dossie_exportacao_docx_da_pasta(
        pasta_txt=pasta,
        output_dir=pasta,  # salva na mesma pasta
        empresa_nome_override=empresa_nome,
        prefer_name_contains=prefer_name_contains,
    )
    return FileResponse(
        path=str(docx_path),
        media_type="application/vnd.openxmlformats-officedocument.wordprocessingml.document",
        filename=docx_path.name,
    )


