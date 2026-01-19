from __future__ import annotations
import re
from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.orm import Session
from app.db.session import get_db
from app.services.credito_consolidado_service import CreditoConsolidadoService

router = APIRouter(prefix="/creditos", tags=["Créditos"])

def _somente_digitos(v: str) -> str:
    return re.sub(r"\D+", "", v or "")

def _validar_yyyymm(valor: str, *, nome_campo: str) -> str:
    """
    Normaliza e valida período no formato YYYYMM (6 dígitos).
    """
    p = _somente_digitos(valor)
    if len(p) != 6:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=f"{nome_campo} inválido. Esperado YYYYMM (6 dígitos).",
        )
    ano = int(p[:4])
    mes = int(p[4:6])
    if ano < 2000 or mes < 1 or mes > 12:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=f"{nome_campo} inválido. Esperado YYYYMM com mês 01-12.",
        )
    return p

@router.get("/empresa/{empresa_id}", status_code=status.HTTP_200_OK)
def consolidado_empresa(empresa_id: int, db: Session = Depends(get_db)):
    """
    Consolida créditos da empresa (somente versões VALIDADA/EXPORTADA).
    Retorno pensado para front (lista por tipo).
    """
    if empresa_id <= 0:
        raise HTTPException(status_code=422, detail="empresa_id inválido")

    try:
        dados = CreditoConsolidadoService.consolidar_por_empresa(empresa_id, db)

        # Front-friendly: sempre lista, mesmo vazia
        return [
            {
                "tipo_credito": (getattr(d, "tipo_credito", None) or "NAO_DEFINIDO"),
                "base": float(getattr(d, "base", 0) or 0),
                "credito": float(getattr(d, "credito", 0) or 0),
            }
            for d in (dados or [])
        ]

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

@router.get("/empresa/{empresa_id}/periodo", status_code=status.HTTP_200_OK)
def consolidado_periodo(
    empresa_id: int,
    inicio: str,
    fim: str,
    db: Session = Depends(get_db),
):
    """
    Consolida créditos por período (YYYYMM), somente versões VALIDADA/EXPORTADA.
    Retorno pensado para gráfico no front (serie temporal).
    """
    if empresa_id <= 0:
        raise HTTPException(status_code=422, detail="empresa_id inválido")

    inicio_ok = _validar_yyyymm(inicio, nome_campo="inicio")
    fim_ok = _validar_yyyymm(fim, nome_campo="fim")

    if inicio_ok > fim_ok:
        raise HTTPException(
            status_code=422,
            detail="intervalo inválido: 'inicio' deve ser <= 'fim'",
        )
    try:
        dados = CreditoConsolidadoService.consolidar_por_periodo(
            empresa_id, inicio_ok, fim_ok, db
        )

        return [
            {
                "periodo": str(getattr(d, "periodo", "")),
                "credito": float(getattr(d, "credito", 0) or 0),
            }
            for d in (dados or [])
        ]

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))
