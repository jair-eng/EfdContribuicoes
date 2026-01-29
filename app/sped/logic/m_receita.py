from __future__ import annotations
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from typing import Dict, Iterable, List, Optional, Tuple, Any
from collections import defaultdict
from app.sped.m_utils import _fmt_br, _reg_of_line, _cst_norm, _d, _nz_str, _cst2

# CSTs que obrigam M400/M800 (receita CST 04/06/07/08/09)
CSTS_RECEITA_M = {"04", "06", "07", "08", "09"}



@dataclass(frozen=True)
class ReceitaKey:
    cst: str
    conta: str  # pode ser vazia, mas preferimos manter

def extrair_receitas_cst(
    linhas_sped: List[str],
    *,
    preferir_c190: bool = True,
) -> Dict[ReceitaKey, Decimal]:
    """
    Retorna somatório de receita por (CST, conta) para CST 04/06/07/08/09.
    Tenta C190 e cai para C170 quando não encontrar C190 utilizável.
    """

    # 1) tenta C190
    if preferir_c190:
        rec = extrair_receitas_c190(linhas_sped)
        if rec:  # se achou algo útil, usa
            return rec

    # 2) fallback C170
    return extrair_receitas_c170(linhas_sped)


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