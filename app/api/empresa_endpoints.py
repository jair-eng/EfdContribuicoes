from __future__ import annotations

import re
from fastapi import APIRouter, Depends, HTTPException, Query, status
from sqlalchemy.orm import Session

from app.db.session import get_db
from app.db.models import Empresa

router = APIRouter(prefix="/empresa", tags=["Empresa"])


def _somente_digitos(v: str) -> str:
    return re.sub(r"\D+", "", v or "")


@router.get("/buscar", status_code=status.HTTP_200_OK)
def buscar_empresa_por_cnpj(
    cnpj: str = Query(..., min_length=11),
    db: Session = Depends(get_db),
):
    cnpj_limpo = _somente_digitos(cnpj)
    if len(cnpj_limpo) != 14:
        raise HTTPException(status_code=400, detail="CNPJ inválido. Esperado 14 dígitos.")

    empresa = db.query(Empresa).filter(Empresa.cnpj == cnpj_limpo).first()
    if not empresa:
        raise HTTPException(status_code=404, detail="Empresa não encontrada")

    return {
        "empresa_id": int(empresa.id),
        "cnpj": empresa.cnpj,
        "razao_social": empresa.razao_social,
    }
