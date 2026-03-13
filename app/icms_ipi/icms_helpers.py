from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal, InvalidOperation
import re
from typing import Any, Dict, Optional
from app.config.settings import ALIQUOTA_PIS_PCT, ALIQUOTA_COFINS_PCT
from decimal import Decimal, ROUND_HALF_UP
from sqlalchemy.orm import Session

from app.db.models import NfIcmsItem, EfdRevisao
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from app.icms_ipi.icms_ipi_cruzamento_service import DocCtx


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


def montar_linha_c170_de_icms(item):
    vl_item = q2(item.vl_item)
    vl_desc = q2(item.vl_desc)
    vl_icms = q2(item.vl_icms)

    base = q2(vl_item - vl_desc)

    vl_pis = q2(base * (ALIQUOTA_PIS_PCT / Decimal("100")))
    vl_cofins = q2(base * (ALIQUOTA_COFINS_PCT / Decimal("100")))

    qtd = getattr(item, "qtd", None)
    unid = getattr(item, "unid", None)
    cst_icms = getattr(item, "cst_icms", None)
    aliq_icms = getattr(item, "aliq_icms", None)
    cod_nat = getattr(item, "cod_nat", None)
    cod_cta = getattr(item, "cod_cta", None)

    campos = [
        "C170",
        item.num_item or "1",
        item.cod_item,
        item.descricao or "",
        fmt_sped_num(qtd, casas=4) if qtd not in (None, "") else "0,0000",
        unid or "",
        fmt_sped_num(vl_item),
        fmt_sped_num(vl_desc),
        "0",
        str(cst_icms or "").strip(),
        item.cfop or "",
        str(cod_nat or "").strip(),
        fmt_sped_num(vl_item),
        fmt_sped_num(aliq_icms) if aliq_icms not in (None, "") else "0,00",
        fmt_sped_num(vl_icms),
        "0,00",
        "0,00",
        "0,00",
        "",
        "",
        "",
        "0,00",
        "0,00",
        "0,00",
        "51",
        fmt_sped_num(base),
        fmt_sped_num(ALIQUOTA_PIS_PCT),
        "",
        "",
        fmt_sped_num(vl_pis),
        "51",
        fmt_sped_num(base),
        fmt_sped_num(ALIQUOTA_COFINS_PCT),
        "",
        "",
        fmt_sped_num(vl_cofins),
        (str(cod_cta).strip() if cod_cta else ""),
    ]
    while len(campos) < 37:
        campos.append("")


    linha = "|" + "|".join(campos) + "|"


    if len(campos) != 37:
        raise ValueError(f"C170 inválido: esperado 37 campos, veio {len(campos)}")
    return linha


def _registro_insercao_alvo(
    doc_ctx: DocCtx,
    itens_c170: list[Dict[str, Any]],
) -> tuple[int | None, int]:
    """
    Se houver C170, insere após o último C170.
    Senão, insere após o C100.
    Retorna: (registro_id_alvo, linha_ref)
    """
    if itens_c170:
        ultimo = itens_c170[-1]
        return (
            int(ultimo.get("registro_id") or 0) or None,
            int(ultimo.get("linha_num") or 0) or doc_ctx.linha_c100,
        )

    return (
        doc_ctx.registro_id_c100,
        doc_ctx.linha_c100,
    )


def _criar_revisao_insert_c170_faltante(
    db: Session,
    *,
    versao_origem_id: int,
    registro_id_alvo: int | None,
    linha_ref: int,
    item_icms: NfIcmsItem,
    motivo_codigo: str = "CONTRIB_SEM_C170_V1",
    apontamento_id: int | None = None,
) -> EfdRevisao:
    linha_nova = montar_linha_c170_de_icms(item_icms)


    rv = EfdRevisao(
        versao_origem_id=int(versao_origem_id),
        versao_revisada_id=None,
        registro_id=registro_id_alvo,
        reg="C170",
        acao="INSERT_AFTER",
        revisao_json={
            "linha_nova": linha_nova,
            "linha_referencia": int(linha_ref or 0),
            "nf_icms_item_id": int(item_icms.id),
            "origem": "ICMS_IPI",
            "motivo": "Item presente no ICMS/IPI e ausente no C170 da EFD Contribuições",
        },
        motivo_codigo=motivo_codigo,
        apontamento_id=apontamento_id,
    )
    db.add(rv)

    return rv


def _ja_existe_revisao_insert_para_item(
    db: Session,
    *,
    versao_origem_id: int,
    nf_icms_item_id: int,
    motivo_codigo: str = "CONTRIB_SEM_C170_V1",
) -> bool:
    qs = (
        db.query(EfdRevisao.id)
        .filter(EfdRevisao.versao_origem_id == int(versao_origem_id))
        .filter(EfdRevisao.acao == "INSERT_AFTER")
        .filter(EfdRevisao.motivo_codigo == motivo_codigo)
    )

    for rid, in qs.all():
        rv = db.query(EfdRevisao).filter(EfdRevisao.id == rid).first()
        j = getattr(rv, "revisao_json", None) or {}
        if int(j.get("nf_icms_item_id") or 0) == int(nf_icms_item_id):
            return True
    return False
