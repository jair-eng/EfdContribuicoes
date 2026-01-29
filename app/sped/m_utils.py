from __future__ import annotations
from decimal import Decimal, ROUND_HALF_UP
from typing import List, Dict, Any, Optional, Tuple
from app.sped.logic.consolidador import _reg_of


def _trunc_2(v: Decimal) -> Decimal:
    return (v * 100).to_integral_value(rounding="ROUND_DOWN") / Decimal("100")

def _key(reg_line: str):
    reg = _reg_of(reg_line)  # "M500"
    # tenta ordenar por número (M100=100, M500=500)
    try:
        n = int(reg[1:])
    except Exception:
        n = 999999
    return (n, reg)  # estável

def _cst_norm(x: str) -> str:
    """'006' -> '06', '6' -> '06', '06' -> '06'."""
    s = _nz_str(x).lstrip("0")
    if not s:
        return ""
    if len(s) == 1:
        s = "0" + s
    return s

def _cst2(v: Any) -> str:
    s = ("" if v is None else str(v)).strip()
    if not s:
        return ""
    s = s.lstrip("0") or "0"
    return s.zfill(2)

def _d(v: Any) -> Decimal:
    if isinstance(v, Decimal):
        return v
    if v is None:
        return Decimal("0")
    s = str(v).strip().replace(".", "").replace(",", ".")  # tolerante
    try:
        return Decimal(s)
    except Exception:
        return Decimal("0")


def _fmt_br(v: Decimal) -> str:
    q = v.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
    return f"{q:.2f}".replace(".", ",")


def _fmt_aliq(v: Decimal) -> str:
    # 4 decimais (ex: 1,6500 / 7,6000)
    q = v.quantize(Decimal("0.0001"), rounding=ROUND_HALF_UP)
    return f"{q:.4f}".replace(".", ",")

def _reg_of_obj(item) -> str:
    reg = getattr(item, "reg", None)
    return (str(reg).strip().upper() if reg else "IGNORAR")

def _reg_of_line(linha: str) -> str:
    if not isinstance(linha, str):
        return ""
    parts = linha.split("|")
    if len(parts) > 1 and parts[1].strip():
        return parts[1].strip().upper()
    return ""

def _nz_str(x: Optional[str]) -> str:
    return (x or "").strip()


def _clean_sped_line(linha: str) -> str:
    s = (linha or "").rstrip("\r\n").strip()
    if not s:
        return ""
    if not s.startswith("|"):
        s = "|" + s
    if not s.endswith("|"):
        s += "|"
    return s

def _rank_m(reg: str) -> int:
    # Ordem mínima segura (estável). Expanda depois.
    ordem = [
        "M001",
        "M100", "M105", "M110", "M115", "M200", "M205", "M210", "M211", "M220", "M230",
        "M300", "M350", "M400", "M410",
        "M500", "M505", "M510", "M515", "M600", "M605", "M610", "M611", "M620", "M630",
        "M700", "M800", "M810",
        "M990",
    ]
    idx = {r: i for i, r in enumerate(ordem)}
    return idx.get(reg, 999)


def _pick_existing_m_lines(linhas_sped: List[str]) -> Tuple[List[str], List[str]]:
    """
    Separa:
      - corpo_m: linhas M* exceto M001/M990/M100/M200/M210/M500/M600/M610 (que vamos reconstruir)
      - outras_m: (não usado aqui)
    Mantém M400/M410/M800/M810 etc.
    """
    manter = []
    for l in linhas_sped:
        if not l or not l.startswith("|M"):
            continue
        if l.startswith("|M001|") or l.startswith("|M990|"):
            continue
        reg = _reg_of(l)
        # estes vamos gerar de novo
        if reg in {"M100", "M200", "M210", "M500", "M600", "M610"}:
            continue
        manter.append(l.strip())
    return manter, []
