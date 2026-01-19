from __future__ import annotations
from dataclasses import dataclass
from typing import Any, List, Optional

@dataclass(slots=True)
class RegistroFiscalDTO:
    """
    DTO desacoplado do ORM.
    O scanner e as regras só enxergam isso.
    """
    id: int
    reg: str
    linha: int
    dados: List[Any]

    base_credito: float = 0.0
    valor_credito: float = 0.0
    tipo_credito: Optional[str] = None
