from __future__ import annotations

from decimal import Decimal
from contextvars import ContextVar
from typing import Optional, Any, Tuple

_CTX_DB: ContextVar[Any] = ContextVar("_CTX_DB", default=None)
_CTX_EMPRESA_ID: ContextVar[Optional[int]] = ContextVar("_CTX_EMPRESA_ID", default=None)

def _to_bool(v: Any) -> bool:
    if v is None:
        return False
    if isinstance(v, bool):
        return v
    if isinstance(v, (int, float)):
        return v != 0
    if isinstance(v, str):
        s = v.strip().lower()
        return s in ("1", "true", "t", "yes", "y", "sim", "s", "on")
    return False

def _dec_any(v: Any) -> Decimal:
    s = str(v or "").strip()
    if not s:
        return Decimal("0")

    if "," in s and "." in s:
        if s.rfind(",") > s.rfind("."):
            s = s.replace(".", "").replace(",", ".")
        else:
            s = s.replace(",", "")
        return Decimal(s)

    if "," in s:
        s = s.replace(".", "").replace(",", ".")
        return Decimal(s)

    return Decimal(s)

def build_ctx_exportacao(*, linhas_sped: list[str], meta: dict) -> dict:
    credito_total = _dec_any(meta.get("credito_total")) or _dec_any(meta.get("impacto_consolidado"))
    soma_m = _dec_any(meta.get("soma_valores_bloco_m")) or Decimal("0")

    tem_apuracao_m = _to_bool(meta.get("tem_apuracao_m"))
    bloco_m_zerado = _to_bool(meta.get("bloco_m_zerado"))
    if soma_m == Decimal("0") and tem_apuracao_m:
        bloco_m_zerado = True

    return {
        "linhas_sped": linhas_sped,
        "credito_total": credito_total,
        "tem_apuracao_m": tem_apuracao_m,
        "bloco_m_zerado": bloco_m_zerado,
        "soma_valores_bloco_m": soma_m,
        "fonte": meta.get("fonte"),
    }

def set_fiscal_context(db: Any, empresa_id: Optional[int]) -> None:
    _CTX_DB.set(db)
    _CTX_EMPRESA_ID.set(int(empresa_id) if empresa_id is not None else None)

def get_fiscal_context() -> Tuple[Any, Optional[int]]:
    return _CTX_DB.get(), _CTX_EMPRESA_ID.get()

def get_fiscal_db() -> Any:
    return _CTX_DB.get()

def get_fiscal_empresa_id() -> Optional[int]:
    return _CTX_EMPRESA_ID.get()
