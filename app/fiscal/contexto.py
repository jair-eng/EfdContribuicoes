from __future__ import annotations
import re
from decimal import Decimal
from contextvars import ContextVar
from typing import Optional, Any, Tuple

from app.db.models import EfdRegistro

_CTX_DB: ContextVar[Any] = ContextVar("_CTX_DB", default=None)
_CTX_EMPRESA_ID: ContextVar[Optional[int]] = ContextVar("_CTX_EMPRESA_ID", default=None)

def to_bool(v: Any) -> bool:
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

def dec_ptbr(v) -> Decimal:
    if v is None:
        return Decimal("0")
    s = str(v).strip()
    if not s:
        return Decimal("0")
    if "," in s:
        s = s.replace(".", "").replace(",", ".")
    try:
        return Decimal(s)
    except Exception:
        return Decimal("0")

def dec_any(v: Any) -> Decimal:
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
    credito_total = dec_any(meta.get("credito_total")) or dec_any(meta.get("impacto_consolidado"))
    soma_m = dec_any(meta.get("soma_valores_bloco_m")) or Decimal("0")

    tem_apuracao_m = to_bool(meta.get("tem_apuracao_m"))
    bloco_m_zerado = to_bool(meta.get("bloco_m_zerado"))
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


def digits_only(s: str) -> str:
    return "".join(ch for ch in (s or "") if ch.isdigit())


def guess_ncm_from_0200(dados: list) -> str:
    """
    0200 layout EFD (geral):
      |0200|COD_ITEM|DESCR_ITEM|COD_BARRA|COD_ANT_ITEM|UNID_INV|TIPO_ITEM|COD_NCM|...
    Então COD_NCM costuma estar no índice 7 (0-based dentro de dados).
    Mas você tinha 7 e fallback; aqui deixo robusto:
    """
    # posição típica COD_NCM
    if len(dados) > 7:
        n = digits_only(str(dados[7] or ""))
        if len(n) >= 8:
            return n[:8]

    # fallback: procura o primeiro token com 8 dígitos (NCM)
    for v in dados:
        vv = digits_only(str(v or ""))
        if len(vv) >= 8:
            return vv[:8]

    return ""

def match_prefix_star(token: str, value: str) -> bool:
     # token "2710*" casa prefixo numérico
     token = (token or "").strip()
     value = digits_only(value)
     if not token or not value:
          return False
     if token.endswith("*"):
        pref = digits_only(token[:-1])
        return bool(pref) and value.startswith(pref)
     return digits_only(token) == value

def resolver_registro_id(db, versao_id: int, dto) -> int | None:
    # 1) Se o DTO já veio ancorado num EfdRegistro real, usa direto
    rid = int(getattr(dto, "id", 0) or 0)
    if rid > 0:
        # garante que existe e pertence à versão
        ok = (
            db.query(EfdRegistro.id)
            .filter(EfdRegistro.id == rid, EfdRegistro.versao_id == int(versao_id))
            .scalar()
        )
        if ok:
            return rid

    # 2) Fallback: tenta localizar por linha + reg
    reg = str(getattr(dto, "reg", "") or "").strip()
    linha = int(getattr(dto, "linha", 0) or 0)

    # se for *_AGG, tenta também sem _AGG
    regs_tentativa = [reg]
    if reg.endswith("_AGG"):
        regs_tentativa.append(reg[:-4])  # "C190_AGG" -> "C190"

    q = (
        db.query(EfdRegistro.id)
        .filter(EfdRegistro.versao_id == int(versao_id))
    )
    if linha > 0:
        q = q.filter(EfdRegistro.linha == linha)
    q = q.filter(EfdRegistro.reg.in_(regs_tentativa))

    return q.scalar()
