from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from typing import List, Tuple

from app.sped.blocoM.m_utils import _clean_sped_line, _reg_of_line, _d
from app.sped.bloco_1.reg1100 import linha_1100
from app.sped.bloco_1.reg1500 import linha_1500


def _parse_reg_dados(linha: str) -> Tuple[str, List[str]]:
    ln = _clean_sped_line(linha)
    parts = ln.strip("|").split("|")
    if not parts:
        return "", []
    return parts[0].upper(), parts[1:]


def _extrair_periodo(dados: List[str]) -> str:
    return (dados[0] or "").strip()


def _extrair_saldo(dados: List[str]) -> Decimal:
    for v in reversed(dados):
        s = (v or "").strip()
        if not s:
            continue
        return _d(s)
    return Decimal("0")


def extrair_creditos_mes_bloco_m(linhas_bloco_m: List[str]) -> Tuple[Decimal, Decimal]:
    """
    Extrai o crédito do mês do próprio bloco M calculado (override):
    - M100 -> PIS
    - M500 -> COFINS
    """
    credito_pis = Decimal("0")
    credito_cofins = Decimal("0")

    for ln in linhas_bloco_m or []:
        reg = _reg_of_line(ln)
        if reg not in ("M100", "M500"):
            continue
        reg2, dados = _parse_reg_dados(ln)
        if reg == "M100" and reg2 == "M100":
            credito_pis = _extrair_saldo(dados)
        elif reg == "M500" and reg2 == "M500":
            credito_cofins = _extrair_saldo(dados)

    return credito_pis, credito_cofins


@dataclass(frozen=True)
class Ultimo1100:
    periodo: str
    saldo: Decimal
    linha: str


@dataclass(frozen=True)
class Ultimo1500:
    periodo: str
    saldo: Decimal
    linha: str


def encontrar_1100(linhas_sped: List[str]) -> List[Ultimo1100]:
    out: List[Ultimo1100] = []
    for ln in linhas_sped or []:
        if _reg_of_line(ln) != "1100":
            continue
        reg, dados = _parse_reg_dados(ln)
        if reg != "1100" or len(dados) < 1:
            continue
        periodo = _extrair_periodo(dados)
        if not periodo or len(periodo) != 6:
            continue
        saldo = _extrair_saldo(dados)
        out.append(Ultimo1100(periodo=periodo, saldo=saldo, linha=_clean_sped_line(ln)))
    return out


def encontrar_1500(linhas_sped: List[str]) -> List[Ultimo1500]:
    out: List[Ultimo1500] = []
    for ln in linhas_sped or []:
        if _reg_of_line(ln) != "1500":
            continue
        reg, dados = _parse_reg_dados(ln)
        if reg != "1500" or len(dados) < 1:
            continue
        periodo = _extrair_periodo(dados)
        if not periodo or len(periodo) != 6:
            continue
        saldo = _extrair_saldo(dados)
        out.append(Ultimo1500(periodo=periodo, saldo=saldo, linha=_clean_sped_line(ln)))
    return out


def montar_bloco_1_1100_1500_cumulativo(
    *,
    linhas_sped: List[str],
    periodo_atual: str,  # seu padrão (MMYYYY)
    cod_cont: str,
    credito_pis_mes: Decimal,
    credito_cofins_mes: Decimal,
) -> List[str]:
    encontrados_1100 = encontrar_1100(linhas_sped)
    encontrados_1500 = encontrar_1500(linhas_sped)

    anteriores_1100 = [x for x in encontrados_1100 if x.periodo < periodo_atual]
    anteriores_1500 = [x for x in encontrados_1500 if x.periodo < periodo_atual]

    if anteriores_1100:
        anteriores_1100.sort(key=lambda x: x.periodo)
        saldo_ant_pis = anteriores_1100[-1].saldo
    else:
        saldo_ant_pis = Decimal("0")

    if anteriores_1500:
        anteriores_1500.sort(key=lambda x: x.periodo)
        saldo_ant_cofins = anteriores_1500[-1].saldo
    else:
        saldo_ant_cofins = Decimal("0")

    novo_1100 = linha_1100(periodo=periodo_atual, cod_cont=cod_cont, valor=(saldo_ant_pis + credito_pis_mes))
    novo_1500 = linha_1500(periodo=periodo_atual, cod_cont=cod_cont, valor=(saldo_ant_cofins + credito_cofins_mes))

    bloco = ["|1001|0|"]

    # ✅ 1100: históricos + novo
    for x in sorted(anteriores_1100, key=lambda x: x.periodo):
        bloco.append(x.linha)
    bloco.append(novo_1100)

    # ✅ 1500: históricos + novo
    for x in sorted(anteriores_1500, key=lambda x: x.periodo):
        bloco.append(x.linha)
    bloco.append(novo_1500)

    bloco.append(f"|1990|{len(bloco) + 1}|")
    return bloco

