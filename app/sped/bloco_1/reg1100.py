from decimal import Decimal

from app.sped.blocoM.m_utils import _fmt_br, _clean_sped_line


def linha_1100(*, periodo: str, cod_cont: str, valor: Decimal) -> str:
    """
    Gera registro 1100 no formato aceito pelo PVA,
    espelhando o SPED retificado de referência.
    """
    v = _fmt_br(valor)
    return _clean_sped_line(
        f"|1100|{periodo}|01||{cod_cont}|{v}||{v}|0,00|||{v}|0,00|||||{v}|"
    )
