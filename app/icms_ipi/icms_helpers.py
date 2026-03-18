from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal, InvalidOperation
import re
from typing import Any, Dict, Optional, List
from decimal import Decimal, ROUND_HALF_UP



# ============================================================
# Helpers
# ============================================================

def fmt_sped_num(v, casas=2) -> str:
    dec = Decimal(str(v or 0))

    if casas == 2:
        dec = dec.quantize(Decimal("0.01"))
    elif casas == 4:
        dec = dec.quantize(Decimal("0.0001"))

    txt = f"{dec:.{casas}f}"

    return txt.replace(".", ",")

def _s(v: Any) -> str:
    return _norm_str(v)

def _as_decimal(v: Any) -> Decimal:
    if isinstance(v, Decimal):
        return v
    if v in (None, "", False):
        return Decimal("0")
    try:
        return Decimal(str(v))
    except Exception:
        return Decimal("0")


def _as_date_str(v: Any) -> str | None:
    if v is None:
        return None
    if isinstance(v, datetime):
        return v.date().isoformat()
    if isinstance(v, date):
        return v.isoformat()
    s = str(v).strip()
    return s or None
def _only_digits(value: Any) -> str:
    if value is None:
        return ""
    return re.sub(r"\D+", "", str(value))


def _norm_str(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip()


def _norm_num_nf(value: Any) -> str:
    s = _only_digits(value)
    if not s:
        return ""
    # remove zeros à esquerda para aumentar chance de match
    s = s.lstrip("0")
    return s or "0"


def _norm_serie(value: Any) -> str:
    s = _only_digits(value)
    if not s:
        s = _norm_str(value)
    s = s.lstrip("0")
    return s or "0"


def _norm_chave(value: Any) -> str:
    s = _only_digits(value)
    return s if len(s) == 44 else ""


def _to_date(value: Any) -> Optional[date]:
    if value is None or value == "":
        return None

    if isinstance(value, date) and not isinstance(value, datetime):
        return value

    if isinstance(value, datetime):
        return value.date()

    s = str(value).strip()

    # formatos comuns
    for fmt in ("%Y-%m-%d", "%d/%m/%Y", "%d%m%Y", "%Y%m%d"):
        try:
            return datetime.strptime(s, fmt).date()
        except ValueError:
            pass

    return None


def _to_decimal(value: Any) -> Decimal:
    if value is None or value == "":
        return Decimal("0")

    if isinstance(value, Decimal):
        return value

    s = str(value).strip().replace(".", "").replace(",", ".")
    try:
        return Decimal(s)
    except Exception:
        return Decimal("0")


def _dec_to_str(value: Any) -> str:
    return f"{_to_decimal(value):f}"

def _norm_cod_item(value: Any) -> str:
    s = _norm_str(value)
    if not s:
        return ""
    return s.lstrip("0") or s


def _campo(dados: list[Any], idx: int) -> str:
    if idx < 0 or idx >= len(dados):
        return ""
    return _norm_str(dados[idx])


def _campo_dec(dados: list[Any], idx: int) -> Decimal:
    if idx < 0 or idx >= len(dados):
        return Decimal("0")
    return _as_decimal(dados[idx])

def q2(v: Decimal) -> Decimal:
    return (v or Decimal("0")).quantize(
        Decimal("0.01"),
        rounding=ROUND_HALF_UP,
    )


def _split_sped_line(line: str) -> tuple[str, list[str]]:
    """
    Converte uma linha SPED como:
    |C100|0|1|...|
    em:
    reg='C100', fields=[...]
    """
    if not line or "|" not in line:
        return "", []
    parts = line.strip().split("|")

    if len(parts) < 3:
        return "", []
    payload = parts[1:-1] if parts[-1] == "" else parts[1:]

    if not payload:
        return "", []
    reg = payload[0].strip()
    fields = [p.strip() for p in payload[1:]]
    return reg, fields


def _parse_date_ddmmyyyy(value: str) -> date | None:
    value = (value or "").strip()
    if not value:
        return None
    return datetime.strptime(value, "%d%m%Y").date()



def _parse_decimal(value: str) -> Decimal:
    value = (value or "").strip()

    if not value:
        return Decimal("0")

    # padrão SPED: 95268,36
    if "," in value:
        value = value.replace(".", "").replace(",", ".")
        try:
            return Decimal(value)
        except InvalidOperation:
            return Decimal("0")

    # números inteiros do SPED já estão corretos
    try:
        return Decimal(value)
    except InvalidOperation:
        return Decimal("0")

