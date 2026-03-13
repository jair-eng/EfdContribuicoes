from decimal import Decimal, ROUND_HALF_UP
from typing import Dict, List
from collections import defaultdict
from typing import Sequence, Iterable
from app.db.models import EfdRegistro, EfdApontamento
from app.fiscal.contexto import get_fiscal_db
from app.fiscal.settings_fiscais import CSTS_RECEITA_NCUM, CSTS_RECEITA_M
from sqlalchemy.orm import Session

from collections import Counter
import re

REGS_RECEITA_BLOCO_C = {"C170", "C175", "C185", "C385", "C485", "C495", "C605", "C870", "C880"}

_RX_CAFE = re.compile(r"\bCAF[ÉE]\b|\bCAFE\b", re.IGNORECASE)
_RX_GRAOS = re.compile(
    r"\bSOJA\b|\bMILHO\b|\bTRIGO\b|\bSORGO\b|\bGRAO[S]?\b|\bGR[AÃ]O[S]?\b|\bFARELO\b",
    re.IGNORECASE,
)

_RX_NUM_BR = re.compile(r"^\d{1,3}(\.\d{3})*,\d+$|^\d+,\d+$|^\d+$")


CFOPS_DEVOLUCAO_VENDA_ENTRADA = {"1202", "2202", "3202"}  # devolução de venda (mesma lógica da saída, mas NÃO é compra)

def _cfop_gate_entrada_compra(cfop: str) -> bool:
    cfop = (cfop or "").strip()
    if not (cfop and len(cfop) == 4 and cfop.isdigit()):
        return False
    if cfop[0] not in ("1", "2"):
        return False
    # exclui devolução de venda (não é compra para comercialização)
    if cfop in CFOPS_DEVOLUCAO_VENDA_ENTRADA:
        return False
    return True

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

def _safe_json(obj):
    if isinstance(obj, dict):
        return {k: _safe_json(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_safe_json(v) for v in obj]
    if isinstance(obj, Decimal):
        return str(obj)
    return obj



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
    """
    No seu parser atual (EFD Contribuições), COD_ITEM está em dados[1].
    Mantém fallback apenas por segurança.
    """
    try:
        cand = str(dados[1] or "").strip() if len(dados) > 1 else ""
        if cand:
            return cand
    except Exception:
        pass

    # fallback: tenta outros índices (caso algum layout/parse mude)
    for idx in (2, 3, 0):
        if len(dados) > idx:
            cand = str(dados[idx] or "").strip()
            if cand:
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

def _is_cafe_ou_graos(desc: str, ncm: str) -> bool:
    """
    Heurística V1: detecta café ou grãos (commodities agrícolas) por NCM e/ou descrição.
    - Café: NCM 0901 ou descrição com CAFE/CAFÉ
    - Grãos: keywords fortes (SOJA/MILHO/TRIGO/etc.) e NCMs básicos (soja/milho/trigo)
    """
    d = (desc or "").strip()
    n = (ncm or "").replace(".", "").strip()

    # café (mantém o padrão atual)
    if n.startswith("0901"):
        return True
    if d and _RX_CAFE.search(d):
        return True

    # grãos por descrição (forte)
    if d and _RX_GRAOS.search(d):
        return True

    # grãos por NCM (V1 básico, pode ampliar depois)
    # trigo 1001, milho 1005, soja 1201
    if n.startswith(("1001", "1005", "1201")):
        return True

    return False

def _is_graos_by_desc(desc: str) -> bool:
    return bool(desc) and bool(_RX_GRAOS.search(desc))

def _is_commodity_agro_by_ncm(ncm: str) -> bool:
    n = (ncm or "").replace(".", "").strip()
    if not n:
        return False
    # V1: café e alguns grãos “óbvios” (pode ir ampliando depois)
    return n.startswith(("0901", "1001", "1005", "1201"))  # café, trigo, milho, soja

def _is_commodity_agro(desc: str, ncm: str) -> bool:
    return _is_cafe_by_ncm(ncm) or _is_cafe_by_desc(desc) or _is_commodity_agro_by_ncm(ncm) or _is_graos_by_desc(desc)

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

    if _is_cafe_ou_graos(desc, ncm):
        return (True, True)

    # tem info mas não confirmou commodity
    return (True, False)


def _detectar_indicio_cafe_0200(registros_db: Sequence[EfdRegistro]) -> bool:
    for rr in registros_db:
        if (rr.reg or "").strip() != "0200":
            continue

        d = (rr.conteudo_json or {}).get("dados") or []
        if len(d) < 2:
            continue

        desc = str(d[1] or "").upper()

        # ✅ CAFÉ
        if _RX_CAFE.search(desc):
            return True

        # ✅ GRÃOS (novo)
        if _RX_GRAOS.search(desc):
            return True

        # ✅ NCM (resiliente: tenta índices comuns)
        for idx in (6, 7):
            if len(d) > idx:
                ncm = str(d[idx] or "").replace(".", "").strip()
                if ncm.startswith("0901"):  # café
                    return True
                # (NCMs de grãos podem ser adicionados depois, sem pressa)

    return False

def _detectar_indicio_agro_0200(registros_db) -> bool:
    for rr in registros_db:
        if (rr.reg or "").strip() != "0200":
            continue
        d = (rr.conteudo_json or {}).get("dados") or []
        if len(d) < 2:
            continue

        desc = str(d[1] or "").strip()
        if _RX_CAFE.search(desc) or _RX_GRAOS.search(desc):
            return True

        # NCM (no teu 0200 é índice 7)
        if len(d) > 7:
            ncm = str(d[7] or "").replace(".", "").strip()
            if ncm.startswith("0901") or ncm.startswith(("1001","1005","1006","1007","1201")):
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



def consolidar_achados_c170_insumo_v2(result) -> None:
    alvo = "C170_INSUMO_V2"

    achados = [a for a in result.apontamentos
               if str(getattr(a, "codigo", "")).strip() == alvo
               and str(getattr(a, "tipo", "")).strip() == "OPORTUNIDADE"]

    if len(achados) <= 1:
        return

    def prio_rank(p: str) -> int:
        p = (p or "").upper()
        return {"ALTA": 3, "MEDIA": 2, "BAIXA": 1}.get(p, 0)

    def dec_br(x) -> Decimal:
        try:
            if x is None:
                return Decimal("0")
            if isinstance(x, (int, float, Decimal)):
                return Decimal(str(x))
            s = str(x).strip().replace(".", "").replace(",", ".")
            return Decimal(s) if s else Decimal("0")
        except Exception:
            return Decimal("0")

    def get_valores(a) -> dict:
        meta = getattr(a, "meta", None) or {}
        return (meta.get("valores") or {}) if isinstance(meta, dict) else {}

    def get_vl_item(a) -> Decimal:
        valores = get_valores(a)
        return dec_br(valores.get("vl_item"))

    def get_linha(a) -> int:
        meta = getattr(a, "meta", None) or {}
        if isinstance(meta, dict):
            v = meta.get("linha") or meta.get("linha_num") or meta.get("linha_referencia")
            try:
                return int(v)
            except Exception:
                pass
        # fallback: tenta inferir do registro se quiser (mas não depende)
        return 10**18

    # escolhe âncora: maior prioridade, maior vl_item, menor linha, menor registro_id
    achados_sorted = sorted(
        achados,
        key=lambda a: (
            -prio_rank(getattr(a, "prioridade", None)),
            -get_vl_item(a),
            get_linha(a),
            int(getattr(a, "registro_id", 0) or 0),
        )
    )
    anchor = achados_sorted[0]

    # agrega estatísticas + amostras
    soma_vl_item = sum((get_vl_item(a) for a in achados_sorted), Decimal("0"))

    cfops = Counter()
    ncms = Counter()
    csts = Counter()
    amostras = []

    MAX_AMOSTRAS = 30
    for a in achados_sorted:
        valores = get_valores(a)
        cfop = str(valores.get("cfop") or "").strip()
        ncm = str(valores.get("ncm") or "").strip()
        cst_pis = str(valores.get("cst_pis") or "").strip()
        cst_cof = str(valores.get("cst_cof") or "").strip()

        if cfop: cfops[cfop] += 1
        if ncm: ncms[ncm] += 1
        if cst_pis: csts[f"PIS:{cst_pis}"] += 1
        if cst_cof: csts[f"COF:{cst_cof}"] += 1

        if len(amostras) < MAX_AMOSTRAS:
            amostras.append({
                "registro_id": int(getattr(a, "registro_id", 0) or 0),
                "linha": get_linha(a),
                "prioridade": getattr(a, "prioridade", None),
                "cfop": cfop or None,
                "cst_pis": cst_pis or None,
                "cst_cof": cst_cof or None,
                "ncm": ncm or None,
                "vl_item": str(get_vl_item(a)),
            })

    def top(counter: Counter, n: int):
        return [k for k, _ in counter.most_common(n)]

    # injeta meta.sum na âncora
    meta = getattr(anchor, "meta", None) or {}
    meta = dict(meta) if isinstance(meta, dict) else {}
    meta["sum"] = {
        "qtd_itens": int(len(achados_sorted)),
        "soma_vl_item": str(soma_vl_item.quantize(Decimal("0.01"))),
        "top_cfop": top(cfops, 5),
        "top_ncm": top(ncms, 5),
        "top_cst": top(csts, 6),
        "amostras": amostras,
    }
    anchor.meta = meta  # Achado costuma aceitar .meta

    # melhora a descrição do “1 único”
    anchor.descricao = (
        f"C170 (insumos): {len(achados_sorted)} item(ns) com CFOP de entrada e CST sem crédito (catálogo). "
        f"Soma VL_ITEM≈ {soma_vl_item.quantize(Decimal('0.01'))}. "
        f"Top CFOP: {', '.join(top(cfops, 5)) or 'N/D'}. "
        f"Top NCM: {', '.join(top(ncms, 5)) or 'N/D'}."
    )

    # remove duplicados deixando só a âncora
    keep_id = id(anchor)
    nova = []
    removed = 0
    for a in result.apontamentos:
        if str(getattr(a, "codigo", "")).strip() == alvo and str(getattr(a, "tipo", "")).strip() == "OPORTUNIDADE":
            if id(a) == keep_id:
                nova.append(a)
            else:
                removed += 1
            continue
        nova.append(a)

    result.apontamentos = nova
    print(f"[SCAN] CONSOLIDADO {alvo}: removidos={removed} mantido=1", flush=True)



def versao_tem_apontamento_codigo(
    db: Session,
    *,
    versao_id: int,
    codigos: Iterable[str],
    incluir_resolvidos: bool = True,
) -> bool:
    """
    Retorna True se existir apontamento na versão com algum dos códigos informados.

    - incluir_resolvidos=True: considera qualquer apontamento (pendente ou resolvido)
      (bom para "contexto existe", não para "ação pendente")
    - incluir_resolvidos=False: considera apenas pendentes (resolvido=False)
      (bom para decidir auto-fix em batch)
    """
    versao_id = int(versao_id or 0)
    codigos = [str(c).strip() for c in (codigos or []) if str(c).strip()]
    if not versao_id or not codigos:
        return False

    q = (
        db.query(EfdApontamento.id)
        .filter(EfdApontamento.versao_id == versao_id)
        .filter(EfdApontamento.codigo.in_(codigos))
    )

    if not incluir_resolvidos:
        q = q.filter(EfdApontamento.resolvido.is_(False))

    return q.first() is not None


def versao_tem_apontamento_codigo_ctx(
    *,
    versao_id: int,
    codigos: Iterable[str],
    incluir_resolvidos: bool = True,
) -> bool:
    db = get_fiscal_db()
    print("[CTX] db=", db, "db_id=", id(db), "bind=", getattr(db, "bind", None))
    ok = versao_tem_apontamento_codigo(
        db,
        versao_id=versao_id,
        codigos=codigos,
        incluir_resolvidos=incluir_resolvidos,
    )
    print("[CTX] versao_id=", versao_id, "codigos=", list(codigos), "=>", ok)
    return ok



