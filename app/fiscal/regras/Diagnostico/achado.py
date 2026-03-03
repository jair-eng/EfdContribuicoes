from dataclasses import dataclass , field
from decimal import Decimal
from typing import Optional, Literal, Dict, Any

Tipo = Literal["ERRO", "ALERTA", "OPORTUNIDADE"]
Prioridade = Literal["ALTA", "MEDIA", "BAIXA"]

@dataclass(frozen=True)
class Achado:
    registro_id: int
    tipo: Tipo
    codigo: str
    descricao: str
    regra: Optional[str] = None
    impacto_financeiro: Optional[Decimal] = None
    prioridade: Optional[Prioridade] = None
    meta: Dict[str, Any] = field(default_factory=dict)
