from __future__ import annotations
from dataclasses import dataclass
from decimal import Decimal
from typing import Dict, List, Tuple, Any,Optional
from collections import defaultdict
from app.db.models import EfdRevisao
from app.sped.blocoM.m_utils import _fmt_br, _d, _cst2, _clean_sped_line, _reg_of_line, _to_dec
from sqlalchemy.orm import Session
from app.fiscal.settings_fiscais import CSTS_RECEITA_M



@dataclass(frozen=True)
class ReceitaKey:
    cst: str
    conta: str  # pode ser vazia, mas preferimos manter

def extrair_receitas_cst(parsed: List[Dict[str, Any]], *, preferir_c190: bool=True) -> Dict[Tuple[str,str], Decimal]:
    if preferir_c190:
        rec = extrair_receitas_c190(parsed)
        if rec:
            return rec
    return extrair_receitas_c170(parsed)



def extrair_receitas_c190(parsed: List[Dict[str, Any]]) -> Dict[Tuple[str, str], Decimal]:
    """
    Retorna {(cst, conta): valor} a partir de C190.
    Observação: C190 normalmente NÃO traz conta contábil, então conta = "".
    Índices assumidos (dados):
      0=CST, 1=CFOP, 2=ALIQ, 3=VL_OPR, ...
    """
    acc: Dict[Tuple[str, str], Decimal] = defaultdict(lambda: Decimal("0"))
    for row in parsed:
        if (row.get("registro") or "").strip().upper() != "C190":
            continue
        dados = (row.get("conteudo_json") or {}).get("dados") or []
        if len(dados) < 4:
            continue

        cst = _cst2(dados[0])
        if cst not in CSTS_RECEITA_M:
            continue

        vl_opr = _d(dados[3])
        if vl_opr == 0:
            continue

        acc[(cst, "")] += vl_opr

    return dict(acc)



def extrair_receitas_c170(parsed: List[Dict[str, Any]]) -> Dict[Tuple[str, str], Decimal]:
    """
    Fallback: extrai receita a partir de C170.
    Índices (layout comum EFD Contribuições):
      dados[6]  = VL_ITEM
      dados[20] = CST_PIS
      dados[24] = CST_COFINS
      dados[-1] = CONTA (no seu arquivo aparece no final, ex: 4.1.10.100.3)
    """
    acc: Dict[Tuple[str, str], Decimal] = defaultdict(lambda: Decimal("0"))
    for row in parsed:
        if (row.get("registro") or "").strip().upper() != "C170":
            continue
        dados = (row.get("conteudo_json") or {}).get("dados") or []
        if len(dados) < 8:
            continue

        vl_item = _d(dados[6])  # VL_ITEM (ajusta se no seu parser estiver em outra posição)
        if vl_item == 0:
            continue

        conta = (dados[-1] if dados else "") or ""
        conta = str(conta).strip()

        cst_pis = _cst2(dados[20]) if len(dados) > 20 else ""
        cst_cof = _cst2(dados[24]) if len(dados) > 24 else ""

        # soma por CST relevante; evita duplicar se PIS e COFINS vierem iguais
        for cst in {cst_pis, cst_cof}:
            if cst in CSTS_RECEITA_M:
                acc[(cst, conta)] += vl_item

    return dict(acc)

def extrair_receitas_c170_por_string(linhas_sped: list[str]) -> dict[tuple[str, str], Decimal]:
    acc = defaultdict(lambda: Decimal("0"))

    for ln in linhas_sped:
        if "|C170|" not in (ln or ""):
            continue

        parts = (ln or "").strip().strip("|").split("|")
        # parts[0] == 'C170'
        dados = parts[1:]  # sem o REG

        if len(dados) < 8:
            continue

        # VL_ITEM no seu exemplo é 140000,00 (logo após UNID)
        # Estrutura do seu C170: ...|QTD|UNID|VL_ITEM|...
        try:
            vl_item = _d(dados[5])  # QTD(3), UNID(4), VL_ITEM(5) dentro do 'dados'
        except Exception:
            continue
        if vl_item <= 0:
            continue

        conta = (dados[-1] or "").strip()  # no seu arquivo a conta está no final
        cst_pis = (dados[-13] or "").strip().zfill(2) if len(dados) >= 13 else ""   # pega “06” perto do fim
        cst_cof = (dados[-6]  or "").strip().zfill(2) if len(dados) >= 6  else ""   # pega “06” perto do fim

        for cst in {cst_pis, cst_cof}:
            if cst in CSTS_RECEITA_M:
                acc[(cst, conta)] += vl_item

    return dict(acc)


def gerar_m_receitas(receitas: Dict[Tuple[str, str], Decimal]) -> List[str]:
    """
    Gera M400/M410 e M800/M810 no padrão do seu exemplo retificado:
      |M400|06|valor|conta||
      |M410|999|valor|conta||
      |M800|06|valor|conta||
      |M810|999|valor|conta||
    """
    out: List[str] = []
    for (cst, conta), valor in sorted(receitas.items(), key=lambda kv: (kv[0][0], kv[0][1])):
        if valor == 0:
            continue
        conta_str = conta or ""
        out.append(f"|M400|{cst}|{_fmt_br(valor)}|{conta_str}||")
        out.append(f"|M410|999|{_fmt_br(valor)}|{conta_str}||")
        out.append(f"|M800|{cst}|{_fmt_br(valor)}|{conta_str}||")
        out.append(f"|M810|999|{_fmt_br(valor)}|{conta_str}||")
    return out


def _split_m_receitas(m_lines: List[str]) -> Tuple[List[str], List[str]]:
    """Separa linhas M400/M410 e M800/M810."""
    m400 = []
    m800 = []
    for l in m_lines or []:
        reg = _reg_of_line(l)
        if reg in {"M400", "M410"}:
            m400.append(l)
        elif reg in {"M800", "M810"}:
            m800.append(l)
    return m400, m800


def _garantir_filhos_m400(m400_410: List[str]) -> List[str]:
    """
    Garante que todo M400 tenha ao menos um M410 filho
    e que a soma dos VL_REC dos M410 bata com o VL_TOT_REC do M400.
    Estratégia segura:
      - Agrupa por (COD_NAT_REC, CTA_CONT) do M400 (campos 2 e 4 normalmente).
      - Se não houver M410, cria um M410 com COD_CTA="999" e VL_REC=VL_TOT_REC.
      - Se houver M410 mas soma diferente, ajusta o PRIMEIRO M410 para fechar diferença.
    """
    out: List[str] = []
    cur_m400 = None
    filhos: List[str] = []

    def flush():
        nonlocal cur_m400, filhos, out
        if not cur_m400:
            return

        parts400 = cur_m400.split("|")
        # parts: ["", "M400", COD_NAT_REC, VL_TOT_REC, CTA_CONT, "", ""]
        vl_tot = _to_dec(parts400[3]) if len(parts400) > 3 else Decimal("0.00")

        m410_parts = [f.split("|") for f in filhos]
        soma = Decimal("0.00")
        for p in m410_parts:
            # ["", "M410", COD_CTA, VL_REC, CTA_CONT, "", ""]
            if len(p) > 3:
                soma += _to_dec(p[3])
        soma = soma.quantize(Decimal("0.01"))
        vl_tot = vl_tot.quantize(Decimal("0.01"))

        if not filhos:
            # cria filho obrigatório
            # mantem CTA_CONT do M400 no campo 4 do M410
            cta_cont = parts400[4] if len(parts400) > 4 else ""
            m410 = _clean_sped_line(f"|M410|999|{_fmt_br(vl_tot)}|{cta_cont}||")
            filhos = [m410]
        else:
            if soma != vl_tot:
                # ajusta o primeiro M410 para fechar a diferença
                diff = (vl_tot - soma).quantize(Decimal("0.01"))
                p0 = m410_parts[0]
                # campo VL_REC é o 3 (index 3)
                vl0 = _to_dec(p0[3]) if len(p0) > 3 else Decimal("0.00")
                novo = (vl0 + diff).quantize(Decimal("0.01"))
                p0[3] = _fmt_br(novo)
                filhos[0] = _clean_sped_line("|" + "|".join(p0[1:]) + "|")

        out.append(_clean_sped_line(cur_m400))
        for f in filhos:
            out.append(_clean_sped_line(f))

        cur_m400 = None
        filhos = []

    for ln in (m400_410 or []):
        ln = _clean_sped_line(ln)
        if not ln:
            continue
        reg = _reg_of_line(ln)
        if reg == "M400":
            flush()
            cur_m400 = ln
            filhos = []
        elif reg == "M410":
            filhos.append(ln)

    flush()
    return [x for x in out if x]


def _garantir_filhos_m800(m800_810: List[str]) -> List[str]:
    """Mesma lógica de M400/M410, só que para M800/M810."""
    out: List[str] = []
    cur_m800 = None
    filhos: List[str] = []

    def flush():
        nonlocal cur_m800, filhos, out
        if not cur_m800:
            return

        parts800 = cur_m800.split("|")
        # ["", "M800", COD_NAT_REC, VL_TOT_REC, CTA_CONT, "", ""]
        vl_tot = _to_dec(parts800[3]) if len(parts800) > 3 else Decimal("0.00")

        m810_parts = [f.split("|") for f in filhos]
        soma = Decimal("0.00")
        for p in m810_parts:
            # ["", "M810", COD_CTA, VL_REC, CTA_CONT, "", ""]
            if len(p) > 3:
                soma += _to_dec(p[3])
        soma = soma.quantize(Decimal("0.01"))
        vl_tot = vl_tot.quantize(Decimal("0.01"))

        if not filhos:
            cta_cont = parts800[4] if len(parts800) > 4 else ""
            m810 = _clean_sped_line(f"|M810|999|{_fmt_br(vl_tot)}|{cta_cont}||")
            filhos = [m810]
        else:
            if soma != vl_tot:
                diff = (vl_tot - soma).quantize(Decimal("0.01"))
                p0 = m810_parts[0]
                vl0 = _to_dec(p0[3]) if len(p0) > 3 else Decimal("0.00")
                novo = (vl0 + diff).quantize(Decimal("0.01"))
                p0[3] = _fmt_br(novo)
                filhos[0] = _clean_sped_line("|" + "|".join(p0[1:]) + "|")

        out.append(_clean_sped_line(cur_m800))
        for f in filhos:
            out.append(_clean_sped_line(f))

        cur_m800 = None
        filhos = []

    for ln in (m800_810 or []):
        ln = _clean_sped_line(ln)
        if not ln:
            continue
        reg = _reg_of_line(ln)
        if reg == "M800":
            flush()
            cur_m800 = ln
            filhos = []
        elif reg == "M810":
            filhos.append(ln)

    flush()
    return [x for x in out if x]

