from __future__ import annotations
from dataclasses import dataclass
from typing import Any, List, Optional, Dict



@dataclass(slots=True)
class RegistroFiscalDTO:
    id: int
    reg: str
    linha: int
    dados: List[Any]

    # --- NOVOS CAMPOS DE CONTEXTO ---
    is_pf: bool = False  # Indica se o registro pertence a um CPF
    versao_id: Optional[int] = None  # Necessário para consultas de apoio
    empresa_id: Optional[int] = None
    meta: Optional[Dict[str, Any]] = None

    base_credito: float = 0.0
    valor_credito: float = 0.0
    tipo_credito: Optional[str] = None
