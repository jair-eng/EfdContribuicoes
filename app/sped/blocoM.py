from __future__ import annotations
from typing import List, Dict, Tuple, Optional,List, Any
from decimal import Decimal, ROUND_HALF_UP
from app.config.settings import ALIQUOTA_PIS_PCT, ALIQUOTA_COFINS_PCT
from app.sped.logic.m_receita import gerar_m_receitas, extrair_receitas_c170, extrair_receitas_c190, \
    extrair_receitas_c170_por_string
from app.sped.m_utils import _reg_of_line, _rank_m, _fmt_br, _fmt_aliq, _pick_existing_m_lines, _key, _clean_sped_line, \
    _trunc_2


def calcular_blocoM(corpo_bloco_m: List[str]) -> List[str]:
    """
    Recebe apenas linhas M* (strings) SEM M001/M990 (ideal),
    e devolve bloco completo: M001 + corpo + M990.
    """

    # 1) normaliza e filtra
    body: List[str] = []
    for l in (corpo_bloco_m or []):
        if not l:
            continue
        s = l.rstrip("\r\n").strip()
        if not s:
            continue
        if not s.startswith("|"):
            s = "|" + s
        if not s.endswith("|"):
            s = s + "|"

        reg = _reg_of_line(s)

        if reg in ("M001", "M990") or not reg:
            continue

        # só aceita M*
        if not reg.startswith("M"):
            continue

        body.append(s)

    # 2) ordena o corpo: M100..M899 antes de M900 etc.
    # OBS: M990 fica FORA dessa ordenação, sempre no final


    body.sort(key=_key)

    # 3) monta bloco completo
    out: List[str] = []
    out.append("|M001|0|")

    out.extend(body)

    # contagem: inclui M001 + M990 + corpo
    qtd = len(out) + 1
    out.append(f"|M990|{qtd}|")

    return out

# Bloco M V 2

def construir_bloco_m_v2(
    *,
    linhas_sped: List[str],
    parsed: List[Dict[str, Any]],
    base_credito: Decimal,
    credito_pis: Decimal,
    credito_cofins: Decimal,
    cod_cont: str = "201",
    nat_bc: str = "01",
    cst_credito: str = "51",
) -> List[str]:

    # base implícita = crédito / (aliquota/100)
    base_pis = base_credito.quantize(Decimal("0.01"))
    base_cof = base_credito.quantize(Decimal("0.01"))

    # 1) Preserva M400/M410/M800/M810 existentes (se tiver no original)
    manter_m_raw, _ = _pick_existing_m_lines(linhas_sped)
    permitidos = {"M400", "M410", "M800", "M810"}

    manter_m: List[str] = []
    seen_local = set()
    for ln in manter_m_raw or []:
        ln_norm = _clean_sped_line(ln)
        if not ln_norm:
            continue
        reg = _reg_of_line(ln_norm)
        if reg not in permitidos:
            continue
        if ln_norm in seen_local:
            continue
        manter_m.append(ln_norm)
        seen_local.add(ln_norm)

    # separa preservadas
    manter_400 = [l for l in manter_m if _reg_of_line(l) in {"M400", "M410"}]
    manter_800 = [l for l in manter_m if _reg_of_line(l) in {"M800", "M810"}]

    # 2) Extrai receitas CST 04/06/07/08/09: C190 -> fallback C170 -> fallback string
    receitas = extrair_receitas_c190(parsed)
    if not receitas:
        receitas = extrair_receitas_c170(parsed)
    if not receitas:
        receitas = extrair_receitas_c170_por_string(linhas_sped)

    m_receitas = [_clean_sped_line(x) for x in (gerar_m_receitas(receitas) or [])]
    m_receitas = [x for x in m_receitas if x]

    # separa calculadas
    m_receitas_400 = [l for l in m_receitas if _reg_of_line(l) in {"M400", "M410"}]
    m_receitas_800 = [l for l in m_receitas if _reg_of_line(l) in {"M800", "M810"}]

    # 3) Monta bloco
    bloco: List[str] = ["|M001|0|"]
    emitted = set(["|M001|0|"])

    def emit(seq: List[str]) -> None:
        for ln in seq:
            ln = _clean_sped_line(ln)
            if ln and ln not in emitted:
                bloco.append(ln)
                emitted.add(ln)

    # ---- PIS: créditos / apuração
    tem_pis = credito_pis > 0 and base_pis > 0
    if tem_pis:
        m100 = _clean_sped_line(
            f"|M100|{cod_cont}|0|{_fmt_br(base_pis)}|{_fmt_aliq(ALIQUOTA_PIS_PCT)}|||{_fmt_br(credito_pis)}|"
            f"0|0|0|{_fmt_br(credito_pis)}|1|0,00|{_fmt_br(credito_pis)}|"
        )
        m105 = _clean_sped_line(
            f"|M105|{nat_bc}|{cst_credito}|{_fmt_br(base_pis)}||{_fmt_br(base_pis)}|{_fmt_br(base_pis)}||||"
        )
        m200 = _clean_sped_line("|M200|0,00|0,00|0,00|0,00|0|0,00|0,00|0|0|0|0|0,00|")

        emit([m100, m105, m200])

    # ---- Receitas PIS (M400/M410): preservadas primeiro, depois calculadas
    emit(manter_400)
    emit(m_receitas_400)

    # ---- COFINS: créditos / apuração
    tem_cof = credito_cofins > 0 and base_cof > 0
    if tem_cof:
        m500 = _clean_sped_line(
            f"|M500|{cod_cont}|0|{_fmt_br(base_cof)}|{_fmt_aliq(ALIQUOTA_COFINS_PCT)}|||{_fmt_br(credito_cofins)}|"
            f"0|0|0|{_fmt_br(credito_cofins)}|1|0,00|{_fmt_br(credito_cofins)}|"
        )
        m505 = _clean_sped_line(
            f"|M505|{nat_bc}|{cst_credito}|{_fmt_br(base_cof)}||{_fmt_br(base_cof)}|{_fmt_br(base_cof)}||||"
        )
        m600 = _clean_sped_line("|M600|0,00|0,00|0,00|0,00|0|0,00|0,00|0|0|0|0|0,00|")

        emit([m500, m505, m600])

    # ---- Receitas COFINS (M800/M810): preservadas primeiro, depois calculadas
    emit(manter_800)
    emit(m_receitas_800)

    bloco.append(f"|M990|{len(bloco) + 1}|")
    return bloco
