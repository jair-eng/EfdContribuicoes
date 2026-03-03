from __future__ import annotations
import re
from typing import Any, Dict, Optional, List, Iterable, Tuple
from app.sped.layouts.c170 import LAYOUT_C170
from dataclasses import dataclass
from app.sped.layouts.c170 import LAYOUT_C170

_RE_CFOP = re.compile(r"^\d{4}$")
_RE_CST = re.compile(r"^\d{2}$")


# --- Funções Auxiliares de Cálculo e Formatação ---



def _get_dados_list(dados: Any) -> List[Any]:
    if dados is None:
        return []
    if isinstance(dados, list):
        return dados
    # se vier dict {"dados":[...]}
    if isinstance(dados, dict) and "dados" in dados:
        return list(dados.get("dados") or [])
    return list(dados) if isinstance(dados, (tuple,)) else []


def get_cfop(dados: List[Any]) -> str:
    d = _get_dados_list(dados)
    return _norm_str(d[LAYOUT_C170.idx_cfop]) if len(d) > LAYOUT_C170.idx_cfop else ""


def get_cst_pis(dados: List[Any]) -> str:
    d = _get_dados_list(dados)
    return _norm_str(d[LAYOUT_C170.idx_cst_pis]).zfill(2) if len(d) > LAYOUT_C170.idx_cst_pis else ""


def get_cst_cofins(dados: List[Any]) -> str:
    d = _get_dados_list(dados)
    return _norm_str(d[LAYOUT_C170.idx_cst_cofins]).zfill(2) if len(d) > LAYOUT_C170.idx_cst_cofins else ""


def get_vl_item(dados: List[Any]) -> float:
    d = _get_dados_list(dados)
    return _parse_sped_float(d[LAYOUT_C170.idx_vl_item]) if len(d) > LAYOUT_C170.idx_vl_item else 0.0


def get_vl_bc_pis(dados: List[Any]) -> float:
    d = _get_dados_list(dados)
    return _parse_sped_float(d[LAYOUT_C170.idx_vl_bc_pis]) if len(d) > LAYOUT_C170.idx_vl_bc_pis else 0.0


def get_vl_pis(dados: List[Any]) -> float:
    d = _get_dados_list(dados)
    return _parse_sped_float(d[LAYOUT_C170.idx_vl_pis]) if len(d) > LAYOUT_C170.idx_vl_pis else 0.0


def get_vl_bc_cofins(dados: List[Any]) -> float:
    d = _get_dados_list(dados)
    return _parse_sped_float(d[LAYOUT_C170.idx_vl_bc_cofins]) if len(d) > LAYOUT_C170.idx_vl_bc_cofins else 0.0


def get_vl_cofins(dados: List[Any]) -> float:
    d = _get_dados_list(dados)
    return _parse_sped_float(d[LAYOUT_C170.idx_vl_cofins]) if len(d) > LAYOUT_C170.idx_vl_cofins else 0.0

def get_aliq_cofins(dados: List[Any]) -> float:
    d = _get_dados_list(dados)
    return _parse_sped_float(d[LAYOUT_C170.idx_aliq_cofins]) if len(d) > LAYOUT_C170.idx_aliq_cofins else 0.0

def get_aliq_pis(dados: List[Any]) -> float:
    d = _get_dados_list(dados)
    idx = getattr(LAYOUT_C170, "idx_aliq_pis", None)
    return _parse_sped_float(d[idx]) if (idx is not None and len(d) > idx) else 0.0


def get_aliq_cofins(dados: List[Any]) -> float:
    d = _get_dados_list(dados)
    idx = getattr(LAYOUT_C170, "idx_aliq_cofins", None)
    return _parse_sped_float(d[idx]) if (idx is not None and len(d) > idx) else 0.0



def is_cst_51(dados: List[Any]) -> bool:
    return (get_cst_pis(dados) == "51") or (get_cst_cofins(dados) == "51")

def _fmt_sped(valor: float) -> str:
    return f"{valor:.2f}".replace('.', ',')


def _parse_sped_float(valor: Any) -> float:
    if not valor: return 0.0
    try:
        s = str(valor).replace('.', '').replace(',', '.')
        return float(s)
    except:
        return 0.0


# --- Suas Defs Originais de Validação ---

def _ensure_len(campos: list[str], idx: int, nome: str) -> None:
    if len(campos) <= idx:
        raise ValueError(f"C170 inválido: faltam campos para {nome} (idx={idx}, len={len(campos)}).")


def _norm_str(v: Any) -> str:
    return "" if v is None else str(v).strip()


def validar_cfop(cfop: str) -> str:
    c = _norm_str(cfop)
    if not _RE_CFOP.match(c):
        raise ValueError("CFOP inválido: esperado 4 dígitos (ex: 1102).")
    return c


def validar_cst(cst: str) -> str:
    c = _norm_str(cst)
    if not _RE_CST.match(c):
        raise ValueError("CST inválido: esperado 2 dígitos (ex: 06, 50).")
    return c


# --- A Função Patch Atualizada com a Lógica de Cálculo ---

def patch_c170_campos(campos: list[Any], *, cfop=None, cst_pis=None, cst_cofins=None) -> list[str]:
    novos = ["" if c is None else str(c).strip() for c in campos]

    # Se a lista enviada já inclui o "C170" no índice 0,
    # precisamos deslocar nossos cálculos em +1
    offset = 1 if novos[0] == "C170" else 0

    try:
        # Valor do Item está no índice 5 (ou 6 se tiver offset)
        valor_item = float(novos[5 + offset].replace(',', '.'))
    except:
        valor_item = 0.0

    if cst_pis:
        novos[23 + offset] = str(cst_pis).zfill(2)  # CST PIS
        if cst_pis == "51":
            novos[24 + offset] = novos[5 + offset]  # BC
            novos[25 + offset] = "1,6500"  # ALIQ
            novos[28 + offset] = f"{(valor_item * 0.0165):.2f}".replace('.', ',')

    if cst_cofins:
        novos[29 + offset] = str(cst_cofins).zfill(2)  # CST COFINS
        if cst_cofins == "51":
            novos[30 + offset] = novos[5 + offset]  # BC
            novos[31 + offset] = "7,6000"  # ALIQ
            novos[34 + offset] = f"{(valor_item * 0.0760):.2f}".replace('.', ',')

    if cfop:
        novos[9 + offset] = str(cfop)

    return novos

def _parse_linha_sped_to_reg_dados(linha: str) -> Tuple[str, List[Any]]:
    s = (linha or "").strip()
    if not s: raise ValueError("Linha SPED vazia")
    s = s.lstrip("\ufeff")
    anchor = "|C170|"
    p = s.find(anchor)
    if p >= 0: s = s[p:]
    if not s.startswith("|"):
        p2 = s.find("|")
        if p2 >= 0: s = s[p2:]
    s = s.strip()
    if not (s.startswith("|") and "|" in s[1:]):
        raise ValueError(f"Linha SPED inválida: {s[:80]}")
    parts = s.strip().strip("|").split("|")
    reg = parts[0].strip().upper()
    dados = parts[1:]
    return reg, dados


@dataclass
class RegistroLike:
    id: int
    registro_id: int
    pai_id: int
    reg: str
    linha: int
    conteudo_json: dict


def linhas_para_rows_like(linhas) -> List[RegistroLike]:
    out: List[RegistroLike] = []
    for l in linhas:
        rid = int(getattr(l, "registro_id", 0) or 0)
        pid = int(getattr(l, "pai_id", 0) or 0)
        out.append(
            RegistroLike(
                id=rid,                         # ✅
                registro_id=rid,                 # ✅ (se existir no RegistroLike)
                pai_id=pid,                      # ✅ (se existir no RegistroLike)
                reg=str(l.reg),
                linha=int(l.linha),
                conteudo_json={"dados": list(l.dados or [])},
            )
        )
    return out



def calcular_credito_item_c170(dados: List[Any]) -> Tuple[float, float, float]:
    d = _get_dados_list(dados)

    cst_pis = get_cst_pis(d)
    cst_cof = get_cst_cofins(d)

    vl_pis = get_vl_pis(d)
    vl_cof = get_vl_cofins(d)

    pis = 0.0
    cof = 0.0

    # 1) se o arquivo já traz VL_PIS/VL_COFINS, é o melhor
    if vl_pis > 0:
        pis = vl_pis
    if vl_cof > 0:
        cof = vl_cof

    # 2) se não tem valor, tenta base*aliq (se layout tiver aliq)
    if pis == 0.0 and cst_pis == "51":
        base = get_vl_bc_pis(d)
        aliq = get_aliq_pis(d)  # pode ser 0 se não existir idx no layout
        if base > 0 and aliq > 0:
            pis = base * (aliq / 100.0)

    if cof == 0.0 and cst_cof == "51":
        base = get_vl_bc_cofins(d)
        aliq = get_aliq_cofins(d)
        if base > 0 and aliq > 0:
            cof = base * (aliq / 100.0)

    # 3) fallback final
    vl_item = get_vl_item(d)
    if pis == 0.0 and cst_pis == "51" and vl_item > 0:
        pis = vl_item * 0.0165
    if cof == 0.0 and cst_cof == "51" and vl_item > 0:
        cof = vl_item * 0.0760

    return pis, cof, pis + cof


def somar_creditos_c170(
    linhas_registros: Iterable[Any],
    *,
    somente_cst_51: bool = True,
    filtro_cfop: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Soma PIS/COFINS a partir de uma lista que pode conter:
      - objetos com .reg e .dados (LinhaLogica / LinhaSpedDinamica etc)
      - strings "|C170|..."
    Retorna dict com totais e contagens.
    """
    total_pis = 0.0
    total_cof = 0.0
    total_itens = 0
    total_itens_cst51 = 0
    total_itens_filtrados_cfop = 0

    cfop_norm = validar_cfop(filtro_cfop) if filtro_cfop else None

    for it in linhas_registros:
        reg = ""
        dados: List[Any] = []

        if isinstance(it, str):
            r, d = _parse_linha_sped_to_reg_dados(it)
            reg = r
            dados = d
        else:
            reg = str(getattr(it, "reg", "") or "").upper()
            dados = list(getattr(it, "dados", None) or [])
            # se por acaso veio "C170" dentro do dados[0]
            if len(dados) > 0 and str(dados[0]).upper() == "C170":
                dados = dados[1:]

        if reg != "C170":
            continue

        total_itens += 1

        if cfop_norm:
            cfop_atual = get_cfop(dados)
            if cfop_atual != cfop_norm:
                continue
            total_itens_filtrados_cfop += 1

        if somente_cst_51 and not is_cst_51(dados):
            continue

        total_itens_cst51 += 1
        pis, cof, _ = calcular_credito_item_c170(dados)
        total_pis += pis
        total_cof += cof

    return {
        "total_pis": total_pis,
        "total_cofins": total_cof,
        "total": total_pis + total_cof,
        "itens_c170": total_itens,
        "itens_c170_cst51": total_itens_cst51,
        "itens_c170_cfop_match": total_itens_filtrados_cfop,
        "filtro_cfop": cfop_norm,
        "somente_cst_51": somente_cst_51,
    }

# -----------------------------
# Helpers
# -----------------------------
def _normalizar_linha_sped(linha: str) -> str:
    s = (linha or "").strip()
    if not s:
        return ""
    if not s.startswith("|"):
        s = "|" + s
    if not s.endswith("|"):
        s = s + "|"
    return s


def _validar_linha_c170(linha: str) -> str:
    """
    Valida e normaliza uma linha SPED C170 no formato pipe.
    Levanta ValueError se inválida.
    """
    s = _normalizar_linha_sped(linha)
    if not s:
        raise ValueError("linha_nova vazia após formatação")

    # precisa conter o token exato do registro
    if "|C170|" not in s:
        raise ValueError("linha_nova inválida: não contém |C170|")

    # validação mínima: deve ter ao menos reg + 1 campo
    partes = s.split("|")
    # Ex: ["", "C170", "campo1", ... , ""]
    if len(partes) < 4 or (partes[1] or "").strip() != "C170":
        raise ValueError("linha_nova inválida: estrutura C170 inesperada")

    return s
