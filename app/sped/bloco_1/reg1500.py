from decimal import Decimal
from app.sped.blocoM.m_utils import _fmt_br, _clean_sped_line


def linha_1500(*, periodo: str, cod_cont: str, valor: Decimal) -> str:
    v = _fmt_br(valor)
    z = _fmt_br(Decimal("0.00"))

    # modelo compatível com o exemplo real e com o PVA (18 campos)
    return _clean_sped_line(
        f"|1500|{periodo}|01||{cod_cont}|{v}||{v}|{z}|||{v}|{z}|||||{v}|"
    )