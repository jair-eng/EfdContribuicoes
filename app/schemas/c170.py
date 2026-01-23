# app/schemas/c170.py
from __future__ import annotations

from typing import Optional
from pydantic import BaseModel, Field


class C170PatchPayload(BaseModel):
    versao_origem_id: int = Field(..., ge=1)
    cfop: Optional[str] = None
    cst_pis: Optional[str] = None
    cst_cofins: Optional[str] = None

    motivo_codigo: str = Field(default="MANUAL_C170")
    apontamento_id: Optional[int] = None
