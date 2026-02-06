from decimal import Decimal, ROUND_HALF_UP
from typing import Dict, List
from collections import defaultdict
from typing import Sequence
from app.db.models import EfdRegistro
from app.fiscal.settings_fiscais import CSTS_RECEITA_NCUM, CSTS_RECEITA_M
import re

REGS_RECEITA_BLOCO_C = {"C170", "C175", "C185", "C385", "C485", "C495", "C605", "C870", "C880"}

_RX_CAFE = re.compile(r"\bCAF[ÉE]\b|\bCAFE\b", re.IGNORECASE)
_RX_NUM_BR = re.compile(r"^\d{1,3}(\.\d{3})*,\d+$|^\d+,\d+$|^\d+$")

def get_registro_id(r) -> int:
    # ORM
    if hasattr(r, "id"):
        try:
            return int(getattr(r, "id") or 0)
        except Exception:
            pass

    # Row (SQLAlchemy)
    if hasattr(r, "_mapping"):
        mp = r._mapping
        for k in ("id", "registro_id"):
            try:
                v = mp.get(k)
                if v is not None:
                    return int(v)
            except Exception:
                continue

    # dict
    if isinstance(r, dict):
        for k in ("id", "registro_id"):
            try:
                v = r.get(k)
                if v is not None:
                    return int(v)
            except Exception:
                continue

    return 0


def _parece_cod_item(s: str) -> bool:
    s = (s or "").strip()
    if not s:
        return False
    # evita pegar números puros / valores BR
    if _RX_NUM_BR.match(s):
        return False
    # evita UF comum e CFOP
    if len(s) == 2 and s.isalpha():
        return False
    if len(s) == 4 and s.isdigit():  # CFOP típico
        return False
    return True

def pick_cod_item_c170(dados: list) -> str:
    # candidatos típicos (mas variáveis conforme seu parser)
    for idx in (2, 3, 1, 0):
        if len(dados) > idx:
            cand = str(dados[idx] or "").strip()
            if _parece_cod_item(cand):
                return cand
    return ""

def dec_br(v) -> Decimal:
    s = str(v or "").strip()
    if not s:
        return Decimal("0")

    # se tiver vírgula, é pt-BR: 1.234,56
    if "," in s:
        s = s.replace(".", "").replace(",", ".")
    # se não tiver vírgula, assume padrão: 1234.56 (não remove ponto)

    try:
        return Decimal(s)
    except Exception:
        return Decimal("0")
def _norm_ncm(x) -> str:
    s = str(x or "").strip().replace(".", "")
    return s

def _is_cafe_by_ncm(ncm: str) -> bool:
    # 0901 = café (grão/torrado/descafeinado etc.)
    return bool(ncm) and ncm.startswith("0901")

def _is_cafe_by_desc(desc: str) -> bool:
    return bool(desc) and bool(_RX_CAFE.search(desc))

def _item_parece_cafe(it: dict) -> tuple[bool, bool]:
    ncm = str(it.get("ncm") or "").strip().replace(".", "")
    desc = str(it.get("descricao") or it.get("desc") or "").strip()

    tem_info = bool(ncm) or bool(desc)
    if not tem_info:
        return (False, False)

    if ncm.startswith("0901"):
        return (True, True)

    # ✅ fallback forte: descrição contém CAFÉ/CAFE
    if desc and _RX_CAFE.search(desc):
        return (True, True)

    return (True, False)

def _detectar_indicio_cafe_0200(registros_db: Sequence[EfdRegistro]) -> bool:
    for rr in registros_db:
        if (rr.reg or "").strip() != "0200":
            continue
        d = (rr.conteudo_json or {}).get("dados") or []
        if len(d) < 2:
            continue

        desc = str(d[1] or "").upper()
        if "CAFE" in desc or "CAFÉ" in desc:
            return True

        # se 0200 tiver NCM (muitas vezes índice 6)
        if len(d) > 6:
            ncm = str(d[6] or "").replace(".", "").strip()
            if ncm.startswith("0901"):
                return True

    return False

def q2(v) -> Decimal:
    """Quantiza em 2 casas com segurança (aceita int/float/str/Decimal)."""
    if isinstance(v, Decimal):
        d = v
    else:
        d = Decimal(str(v or "0"))
    return d.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)



def extrair_receita_bloco_c_por_c170(linhas_sped: List[str]) -> Dict[str, Decimal]:
    """
    Retorna um map {cst: soma_vl_item} apenas para C170 de SAÍDA (C100 IND_OPER=1).
    Atribui a receita ao CST (prioriza CST PIS se estiver em receita; senão tenta COFINS).
    """
    acc = defaultdict(lambda: Decimal("0.00"))

    ind_oper_atual = None  # 0=entrada | 1=saída

    c100 = c100_saida = c100_entrada = 0
    c170_total = c170_usados = c170_err = 0
    c170_sem_c100 = 0

    for ln in linhas_sped or []:
        if not ln:
            continue
        s = ln.strip()

        if s.startswith("|C100|"):
            c100 += 1
            dados = s.strip("|").split("|")[1:]
            ind_oper_atual = (dados[0] if len(dados) > 0 else "").strip()
            if ind_oper_atual == "1":
                c100_saida += 1
            elif ind_oper_atual == "0":
                c100_entrada += 1
            continue

        if s.startswith("|C170|"):
            c170_total += 1

            if ind_oper_atual is None:
                c170_sem_c100 += 1
                continue

            # Só SAÍDA
            if ind_oper_atual != "1":
                continue

            try:
                dados = s.strip("|").split("|")[1:]

                # No seu arquivo: VL_ITEM está em dados[5]
                vl_item = dec_br(dados[5]) if len(dados) > 5 else Decimal("0")
                if vl_item <= 0:
                    continue

                # Você vinha pegando CSTs pelo fim; mantive por robustez:
                cst_pis = str(dados[-13] if len(dados) >= 13 else "").strip().zfill(2)
                cst_cof = str(dados[-6] if len(dados) >= 6 else "").strip().zfill(2)

                if not cst_pis.isdigit():
                    cst_pis = ""
                if not cst_cof.isdigit():
                    cst_cof = ""

                # Decide qual CST usar como "classificador" (receita):
                if cst_pis in (CSTS_RECEITA_M | CSTS_RECEITA_NCUM):
                    cst = cst_pis
                elif cst_cof in (CSTS_RECEITA_M | CSTS_RECEITA_NCUM):
                    cst = cst_cof
                else:
                    continue

                acc[cst] += vl_item
                c170_usados += 1

            except Exception:
                c170_err += 1

    print(
        f"[C170MAP] C100={c100} (saida={c100_saida} entrada={c100_entrada}) "
        f"C170_total={c170_total} usados={c170_usados} err={c170_err} "
        f"sem_c100={c170_sem_c100} csts={len(acc)}"
    )

    return {cst: q2(v) for cst, v in acc.items()}

def calcular_totais_0900(linhas_sped: List[str]) -> Dict[str, Decimal]:
    """
    Fonte da verdade para o Registro 0900.
    Calcula totais por bloco + total do período.
    Hoje:
      - Bloco C: implementado de forma robusta (C100 IND_OPER=1 + C170).
      - Demais blocos: placeholder (0.00), prontos para evolução.
    """

    # Bloco A
    total_a = Decimal("0.00")
    nrb_a   = Decimal("0.00")

    # Bloco C (robusto)
    rec_c_map = extrair_receita_bloco_c_por_c170(linhas_sped)
    total_c = q2(sum(rec_c_map.values(), Decimal("0.00")))
    nrb_c   = Decimal("0.00")

    # Blocos futuros (placeholders explícitos)
    total_d = Decimal("0.00"); nrb_d = Decimal("0.00")
    total_f = Decimal("0.00"); nrb_f = Decimal("0.00")
    total_i = Decimal("0.00"); nrb_i = Decimal("0.00")
    total_1 = Decimal("0.00"); nrb_1 = Decimal("0.00")

    total_periodo = q2(total_a + total_c + total_d + total_f + total_i + total_1)
    nrb_periodo   = Decimal("0.00")

    return {
        "total_a": total_a, "nrb_a": nrb_a,
        "total_c": total_c, "nrb_c": nrb_c,
        "total_d": total_d, "nrb_d": nrb_d,
        "total_f": total_f, "nrb_f": nrb_f,
        "total_i": total_i, "nrb_i": nrb_i,
        "total_1": total_1, "nrb_1": nrb_1,
        "total_periodo": total_periodo,
        "nrb_periodo": nrb_periodo,
    }