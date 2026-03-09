from __future__ import annotations

from typing import Dict, List, Any, Optional, Iterable, Tuple
from decimal import Decimal
from app.config.settings import ALIQUOTA_PIS_PCT, ALIQUOTA_COFINS_PCT
from app.fiscal.contexto import dec_any
from app.sped.blocoM.m_receita import (
    gerar_m_receitas,
    extrair_receitas_c170,
    extrair_receitas_c190,
    extrair_receitas_c170_por_string, _split_m_receitas, _garantir_filhos_m400, _garantir_filhos_m800,
)
from app.sped.blocoM.m_utils import (
    _reg_of_line,
    _fmt_br,
    _fmt_aliq,
    _pick_existing_m_lines,
    _key,
    _clean_sped_line,_cst_norm
)


def calcular_blocoM(corpo_bloco_m: List[str]) -> List[str]:
    """
    Recebe apenas linhas M* (strings) SEM M001/M990 (ideal),
    e devolve bloco completo: M001 + corpo + M990.
    """
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
        if not reg.startswith("M"):
            continue

        body.append(s)

    body.sort(key=_key)

    out: List[str] = ["|M001|0|"]
    out.extend(body)
    out.append(f"|M990|{len(out) + 1}|")
    return out


def construir_bloco_m_v3(
    *,
    linhas_sped: List[str],
    parsed: List[Dict[str, Any]],
    base_por_cst: Dict[str, Decimal],
    cod_cred: str = "201",
    nat_bc: str = "01",
    ajustes_m: Optional[List[Dict[str, Any]]] = None,
) -> List[str]:
    """
    Bloco M "PVA-proof":
      - Sempre materializa M100/M105/M500/M505 quando houver base_total > 0.
      - Sempre emite M200/M600 (zerados, mas presentes).
      - Gera receitas M400/M410 e M800/M810 completas e coerentes.
      - Evita dependência de M* antigos do arquivo.
    """

    ajustes_m = ajustes_m or []
    if ajustes_m:
        # Parte 2: só prova que chegou aqui (não altera cálculo)
        tipos = []
        for a in ajustes_m[:5]:
            t = (a or {}).get("tipo") or (a or {}).get("origem") or "?"
            tipos.append(str(t))
        print(f"✅ BLOCO_M> ajustes_m recebidos={len(ajustes_m)} | tipos_top={tipos}")

    # 0) normaliza bases por CST
    base_por_cst_norm: Dict[str, Decimal] = {}
    for cst, base in (base_por_cst or {}).items():
        c = _cst_norm(cst)
        if not c:
            continue
        b = base if isinstance(base, Decimal) else Decimal(str(base or "0"))
        if b <= 0:
            continue
        base_por_cst_norm[c] = b.quantize(Decimal("0.01"))

    ajustes_m = ajustes_m or []

    base_extra = Decimal("0.00")
    vistos = set()

    for a in ajustes_m:
        if not isinstance(a, dict):
            continue

        # seu loader está te passando "meta" direto? depende de como você montou ajustes_m.
        # Pelo select, no banco está revisao_json={"meta": {...}, "detalhe": {...}}.
        meta = a.get("meta") if isinstance(a.get("meta"), dict) else a  # fallback

        if str(meta.get("tipo") or "").strip().upper() != "EXPORTACAO_RESSARCIMENTO":
            continue

        raw_base = meta.get("base_exportacao")
        if raw_base in (None, "", {}):
            continue

        try:
            v = dec_any(raw_base)
        except Exception:
            continue

        if v <= 0:
            continue

        # dedup bom: tipo + base + origem_regra (evita somar em dobro)
        key = (
            "EXPORTACAO_RESSARCIMENTO",
            str(raw_base),
            str(meta.get("origem_regra") or ""),
            str(meta.get("cod_cont") or ""),
            str(meta.get("nat_bc") or ""),
        )
        if key in vistos:
            continue
        vistos.add(key)

        base_extra += v

    if base_extra > 0:
        print(f"✅ BLOCO_M> AJUSTE_M exportação aplicado: base_extra={_fmt_br(base_extra)} (dedup={len(vistos)})")

    base_total = (sum(base_por_cst_norm.values(), Decimal("0.00")) + base_extra).quantize(Decimal("0.01"))

    # créditos totais
    credito_pis = (base_total * Decimal("0.0165")).quantize(Decimal("0.01"))
    credito_cof = (base_total * Decimal("0.0760")).quantize(Decimal("0.01"))

    # 1) receitas (calculo)
    receitas = extrair_receitas_c190(parsed)
    if not receitas:
        receitas = extrair_receitas_c170(parsed)
    if not receitas:
        receitas = extrair_receitas_c170_por_string(linhas_sped)

    m_receitas = [_clean_sped_line(x) for x in (gerar_m_receitas(receitas) or [])]
    m_receitas = [x for x in m_receitas if x]

    m400_410, m800_810 = _split_m_receitas(m_receitas)
    m400_410 = _garantir_filhos_m400(m400_410)
    m800_810 = _garantir_filhos_m800(m800_810)

    # 2) emissor determinístico
    bloco: List[str] = ["|M001|0|"]
    emitted = set(bloco)

    def emit(seq: Iterable[str]) -> None:
        for ln in seq or []:
            ln = _clean_sped_line(ln)
            if ln and ln not in emitted:
                bloco.append(ln)
                emitted.add(ln)

    print("DBG M> base_por_cst_norm total =", _fmt_br(sum(base_por_cst_norm.values(), Decimal("0.00"))))
    print("DBG M> base_extra             =", _fmt_br(base_extra))
    print("DBG M> base_total             =", _fmt_br(base_total))
    print("DBG M> credito_pis            =", _fmt_br(credito_pis))

    # 3) crédito PIS (M100/M105/M200)
    if base_total > 0:
        m100 = _clean_sped_line(
            f"|M100|{cod_cred}|0|{_fmt_br(base_total)}|{_fmt_aliq(ALIQUOTA_PIS_PCT)}|||{_fmt_br(credito_pis)}|"
            f"0|0|0|{_fmt_br(credito_pis)}|1|0,00|{_fmt_br(credito_pis)}|"
        )
        emit([m100])

        for cst in sorted(base_por_cst_norm.keys()):
            base_cst = base_por_cst_norm[cst]
            m105 = _clean_sped_line(
                f"|M105|{nat_bc}|{cst}|{_fmt_br(base_cst)}||{_fmt_br(base_cst)}|{_fmt_br(base_cst)}||||"
            )
            emit([m105])

        emit(["|M200|0,00|0,00|0,00|0,00|0|0,00|0,00|0|0|0|0|0,00|"])

    # 4) receitas PIS
    emit(m400_410)

    # 5) crédito COFINS (M500/M505/M600)
    if base_total > 0:
        m500 = _clean_sped_line(
            f"|M500|{cod_cred}|0|{_fmt_br(base_total)}|{_fmt_aliq(ALIQUOTA_COFINS_PCT)}|||{_fmt_br(credito_cof)}|"
            f"0|0|0|{_fmt_br(credito_cof)}|1|0,00|{_fmt_br(credito_cof)}|"
        )
        emit([m500])

        for cst in sorted(base_por_cst_norm.keys()):
            base_cst = base_por_cst_norm[cst]
            m505 = _clean_sped_line(
                f"|M505|{nat_bc}|{cst}|{_fmt_br(base_cst)}||{_fmt_br(base_cst)}|{_fmt_br(base_cst)}||||"
            )
            emit([m505])

        emit(["|M600|0,00|0,00|0,00|0,00|0|0,00|0,00|0|0|0|0|0,00|"])

    # 6) receitas COFINS
    emit(m800_810)

    # 7) M990
    bloco.append(f"|M990|{len(bloco) + 1}|")
    return bloco
