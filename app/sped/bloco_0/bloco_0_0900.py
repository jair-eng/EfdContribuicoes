from __future__ import annotations
from datetime import date
from decimal import Decimal
from typing import Any, Dict, List, Optional
from app.sped.blocoM.m_utils import _clean_sped_line, _fmt_br
from app.sped.utils_geral import q2, calcular_totais_0900



def gerar_0900_pva_totais_por_bloco(
    *,
    total_a: Decimal = Decimal("0.00"),
    total_c: Decimal = Decimal("0.00"),
    total_d: Decimal = Decimal("0.00"),
    total_f: Decimal = Decimal("0.00"),
    total_i: Decimal = Decimal("0.00"),
    total_1: Decimal = Decimal("0.00"),
    nrb_a: Decimal = Decimal("0.00"),
    nrb_c: Decimal = Decimal("0.00"),
    nrb_d: Decimal = Decimal("0.00"),
    nrb_f: Decimal = Decimal("0.00"),
    nrb_i: Decimal = Decimal("0.00"),
    nrb_1: Decimal = Decimal("0.00"),
    nrb_periodo: Decimal = Decimal("0.00"),
) -> str:
    """
    Layout 0900 (14 campos) exatamente como o PVA mostra:

      1  REC_TOTAL_BLOCO_A
      2  PARC_REC_NRB_BLOCO_A
      3  REC_TOTAL_BLOCO_C
      4  PARC_REC_NRB_BLOCO_C
      5  REC_TOTAL_BLOCO_D
      6  PARC_REC_NRB_BLOCO_D
      7  REC_TOTAL_BLOCO_F
      8  PARC_REC_NRB_BLOCO_F
      9  REC_TOTAL_BLOCO_I
      10 PARC_REC_NRB_BLOCO_I
      11 REC_TOTAL_BLOCO_1
      12 PARC_REC_NRB_BLOCO_1
      13 REC_TOTAL_PERIODO
      14 PARC_REC_NRB_PERIODO
    """
    total_periodo = (total_a + total_c + total_d + total_f + total_i + total_1).quantize(Decimal("0.01"))

    campos = ["0,00"] * 14

    campos[0] = _fmt_br(q2(total_a))
    campos[1] = _fmt_br(q2(nrb_a))
    campos[2] = _fmt_br(q2(total_c))
    campos[3] = _fmt_br(q2(nrb_c))
    campos[4] = _fmt_br(q2(total_d))
    campos[5] = _fmt_br(q2(nrb_d))
    campos[6] = _fmt_br(q2(total_f))
    campos[7] = _fmt_br(q2(nrb_f))
    campos[8] = _fmt_br(q2(total_i))
    campos[9] = _fmt_br(q2(nrb_i))
    campos[10] = _fmt_br(q2(total_1))
    campos[11] = _fmt_br(q2(nrb_1))
    campos[12] = _fmt_br(q2(total_periodo))
    campos[13] = _fmt_br(q2(nrb_periodo))

    return _clean_sped_line("|0900|" + "|".join(campos) + "|")


def _periodo_atrasado(periodo_yyyymm: int) -> bool:
    hoje = date.today()
    ano = periodo_yyyymm // 100
    mes = periodo_yyyymm % 100
    return (ano, mes) < (hoje.year, hoje.month)


def aplicar_0900_se_necessario(
    *,
    linhas_sped: List[str],
    periodo_yyyymm: Optional[int],
) -> List[str]:
    """
    Insere 0900 SOMENTE se:
      - período atrasado
      - não existir 0900
    E garante 0990 consistente (recalculado).
    """

    if not periodo_yyyymm:
        return linhas_sped

    if not _periodo_atrasado(int(periodo_yyyymm)):
        return linhas_sped

    # já tem 0900? não mexe
    if any((l or "").startswith("|0900|") for l in linhas_sped):
        return linhas_sped

    # ---------------------------------------------------------
    tot = calcular_totais_0900(linhas_sped)

    linha_0900 = gerar_0900_pva_totais_por_bloco(
        total_a=tot["total_a"], nrb_a=tot["nrb_a"],
        total_c=tot["total_c"], nrb_c=tot["nrb_c"],
        total_d=tot["total_d"], nrb_d=tot["nrb_d"],
        total_f=tot["total_f"], nrb_f=tot["nrb_f"],
        total_i=tot["total_i"], nrb_i=tot["nrb_i"],
        total_1=tot["total_1"], nrb_1=tot["nrb_1"],
        nrb_periodo=tot["nrb_periodo"],
    )

    print(
        "[0900] totais:",
        "A=", _fmt_br(tot["total_a"]),
        "C=", _fmt_br(tot["total_c"]),
        "D=", _fmt_br(tot["total_d"]),
        "F=", _fmt_br(tot["total_f"]),
        "I=", _fmt_br(tot["total_i"]),
        "1=", _fmt_br(tot["total_1"]),
        "PERIODO=", _fmt_br(tot["total_periodo"]),
    )

    # ---------------------------------------------------------
    # Reconstrói o bloco 0:
    # - copia tudo
    # - ao encontrar 0990, ignora o antigo, insere 0900 e recalcula 0990
    out: List[str] = []
    bloco0: List[str] = []
    em_bloco0 = False
    inseriu_0900 = False
    fechou_bloco0 = False

    for ln in linhas_sped:
        if ln.startswith("|0000|"):
            em_bloco0 = True

        if em_bloco0 and not fechou_bloco0:
            if ln.startswith("|0990|"):
                if not inseriu_0900:
                    bloco0.append(linha_0900)
                    inseriu_0900 = True

                qtd0 = len(bloco0) + 1  # +1 do próprio 0990
                bloco0.append(f"|0990|{qtd0}|")

                out.extend(bloco0)
                fechou_bloco0 = True
                em_bloco0 = False
                continue

            bloco0.append(ln)
            continue

        out.append(ln)

    # fallback: se não achou 0990 no fluxo (arquivo estranho)
    if bloco0 and not fechou_bloco0:
        if not inseriu_0900:
            bloco0.append(linha_0900)
            inseriu_0900 = True
        qtd0 = len(bloco0) + 1
        bloco0.append(f"|0990|{qtd0}|")
        out = bloco0 + out

    # sanity check
    for ln in out:
        if ln.startswith("|0900|"):
            print("[0900] FINAL:", ln)

    return out


def recalcular_0990_bloco0(linhas_sped: List[str]) -> List[str]:
    out = []
    bloco0 = []
    em_bloco0 = False
    fechou = False

    for ln in linhas_sped:
        if ln.startswith("|0000|"):
            em_bloco0 = True

        if em_bloco0 and not fechou:
            if ln.startswith("|0990|"):
                qtd0 = len(bloco0) + 1
                bloco0.append(f"|0990|{qtd0}|")
                out.extend(bloco0)
                fechou = True
                em_bloco0 = False
                continue

            bloco0.append(ln)
            continue

        out.append(ln)

    if bloco0 and not fechou:
        qtd0 = len(bloco0) + 1
        bloco0.append(f"|0990|{qtd0}|")
        out = bloco0 + out

    return out