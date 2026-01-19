from pydantic import BaseModel
from typing import List

class ApontamentoResolucaoIn(BaseModel):
    apontamento_id: int
    resolvido: bool

class ConfirmarRevisaoIn(BaseModel):
    to_resolver: List[int] = []
    to_reabrir: List[int] = []
