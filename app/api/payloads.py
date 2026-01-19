# app/api/payloads.py
from pydantic import BaseModel, Field

class ReprocessarSelecaoPayload(BaseModel):
    apontamento_ids: list[int] = Field(..., min_items=1)
    motivo: str | None = None
