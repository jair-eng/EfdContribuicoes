from dataclasses import dataclass
from decimal import Decimal
from typing import List
from app.sped.blocoM.m_utils import _clean_sped_line, _d, _reg_of_line
from app.sped.bloco_1.reg1500 import linha_1500


@dataclass(frozen=True)
class Reg1500:
    periodo: str       # YYYYMM
    cod_cont: str
    valor: Decimal
    linha: str

def _parse_1500(linha: str):
    ln = _clean_sped_line(linha)
    parts = ln.strip("|").split("|")
    return parts[1:] if len(parts) > 1 else []

def encontrar_1500(linhas_sped: List[str]) -> List[Reg1500]:
    regs = []

    for ln in linhas_sped or []:
        if _reg_of_line(ln) != "1500":
            continue

        dados = _parse_1500(ln)
        if len(dados) < 5:
            continue

        periodo = (dados[0] or "").strip()
        cod_cont = (dados[3] or "").strip()
        valor = _d(dados[4])

        if not periodo or len(periodo) != 6:
            continue

        regs.append(
            Reg1500(
                periodo=periodo,
                cod_cont=cod_cont,
                valor=valor,
                linha=_clean_sped_line(ln),
            )
        )

    return regs


def montar_bloco_1_1500_cumulativo(
    *,
    linhas_sped: List[str],
    periodo_atual: str,
    cod_cont: str,
    valor_utilizado_mes: Decimal,
) -> List[str]:
    """
    - Mantém 1500 de meses anteriores
    - Remove 1500 do período atual (se existir)
    - Adiciona o 1500 do mês atual (se valor > 0)
    """

    encontrados = [
        x for x in encontrar_1500(linhas_sped)
        if x.cod_cont == cod_cont
    ]

    anteriores = [x for x in encontrados if x.periodo < periodo_atual]

    bloco = []

    # mantém histórico
    for x in sorted(anteriores, key=lambda x: x.periodo):
        bloco.append(x.linha)

    # adiciona o do mês atual, se houver
    if valor_utilizado_mes > 0:
        bloco.append(
            linha_1500(
                periodo=periodo_atual,
                cod_cont=cod_cont,
                valor=valor_utilizado_mes,
            )
        )

    return bloco

def yyyymm_to_mmyyyy(periodo: str) -> str:
    """
    Converte YYYYMM -> MMYYYY
    """
    if not periodo or len(periodo) != 6:
        raise ValueError(f"Período inválido: {periodo}")
    return periodo[4:6] + periodo[0:4]