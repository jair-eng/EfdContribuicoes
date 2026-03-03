from __future__ import annotations

from decimal import Decimal, ROUND_HALF_UP
from typing import List, Dict, Any, Optional, Tuple

from app.db.models import EfdRevisao
from app.sped.logic.consolidador import _reg_of
from pathlib import Path
from typing import List, Dict, Tuple, Optional



def _trunc_2(v: Decimal) -> Decimal:
    return (v * 100).to_integral_value(rounding="ROUND_DOWN") / Decimal("100")

def _key(reg_line: str):
    reg = _reg_of(reg_line)  # "M500"
    # tenta ordenar por número (M100=100, M500=500)
    try:
        n = int(reg[1:])
    except Exception:
        n = 999999
    return (n, reg)  # estável

def _cst_norm(x: str) -> str:
    """'006' -> '06', '6' -> '06', '06' -> '06'."""
    s = _nz_str(x).lstrip("0")
    if not s:
        return ""
    if len(s) == 1:
        s = "0" + s
    return s

def _cst2(v: Any) -> str:
    s = ("" if v is None else str(v)).strip()
    if not s:
        return ""
    s = s.lstrip("0") or "0"
    return s.zfill(2)

def _d(v: Any) -> Decimal:
    if isinstance(v, Decimal):
        return v
    if v is None:
        return Decimal("0")
    s = str(v).strip().replace(".", "").replace(",", ".")  # tolerante
    try:
        return Decimal(s)
    except Exception:
        return Decimal("0")


def _fmt_br(v: Decimal) -> str:
    q = v.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
    return f"{q:.2f}".replace(".", ",")


def _fmt_aliq(v: Decimal) -> str:
    # 4 decimais (ex: 1,6500 / 7,6000)
    q = v.quantize(Decimal("0.0001"), rounding=ROUND_HALF_UP)
    return f"{q:.4f}".replace(".", ",")

def _reg_of_obj(item) -> str:
    reg = getattr(item, "reg", None)
    return (str(reg).strip().upper() if reg else "IGNORAR")

def _reg_of_line(linha: str) -> str:
    if not isinstance(linha, str):
        return ""
    parts = linha.split("|")
    if len(parts) > 1 and parts[1].strip():
        return parts[1].strip().upper()
    return ""

def _nz_str(x: Optional[str]) -> str:
    return (x or "").strip()


def _clean_sped_line(linha: str) -> str:
    s = (linha or "").rstrip("\r\n").strip()
    if not s:
        return ""
    if not s.startswith("|"):
        s = "|" + s
    if not s.endswith("|"):
        s += "|"
    return s

def _rank_m(reg: str) -> int:
    # Ordem mínima segura (estável). Expanda depois.
    ordem = [
        "M001",
        "M100", "M105", "M110", "M115", "M200", "M205", "M210", "M211", "M220", "M230",
        "M300", "M350", "M400", "M410",
        "M500", "M505", "M510", "M515", "M600", "M605", "M610", "M611", "M620", "M630",
        "M700", "M800", "M810",
        "M990",
    ]
    idx = {r: i for i, r in enumerate(ordem)}
    return idx.get(reg, 999)


def _pick_existing_m_lines(linhas_sped: List[str]) -> Tuple[List[str], List[str]]:
    """
    Separa:
      - corpo_m: linhas M* exceto M001/M990/M100/M200/M210/M500/M600/M610 (que vamos reconstruir)
      - outras_m: (não usado aqui)
    Mantém M400/M410/M800/M810 etc.
    """
    manter = []
    for l in linhas_sped:
        if not l or not l.startswith("|M"):
            continue
        if l.startswith("|M001|") or l.startswith("|M990|"):
            continue
        reg = _reg_of(l)
        # estes vamos gerar de novo
        if reg in {"M100", "M200", "M210", "M500", "M600", "M610"}:
            continue
        manter.append(l.strip())
    return manter, []

def _ensure_line(s: str) -> str:
    s = (s or "").rstrip("\r\n").strip()
    if not s:
        return ""
    if not s.startswith("|"):
        s = "|" + s
    if not s.endswith("|"):
        s += "|"
    return s

def ler_linhas_exportado(path: str) -> list[str]:
    with open(path, "r", encoding="iso-8859-1", errors="ignore") as f:
        return [ln.rstrip("\r\n") for ln in f if ln.strip()]

def caminho_sped_corrigido(nome_arquivo: str) -> str:
    downloads = Path.home() / "Downloads"
    pasta = downloads / "Speds Corrigidos"
    pasta.mkdir(parents=True, exist_ok=True)
    return str(pasta / nome_arquivo)

def nome_sped_corrigido(arquivo, versao) -> str:
    return f"EFD_{arquivo.periodo}_VERSAO_{versao.id}_CORRIGIDO.txt"

def _fields(line: str) -> List[str]:
    s = _clean_sped_line(line)
    parts = s.strip("|").split("|")
    # parts[0] = REG
    return parts[1:] if len(parts) > 1 else []

def _key_m(line: str) -> Tuple[str, Tuple[str, ...]]:
    """
    Chave lógica por registro para deduplicar.
    Ajustada pro que mais duplica: M105/M505.
    """
    reg = _reg_of_line(line)

    d = _fields(line)

    if reg == "M105":
        # |M105|NAT_BC|CST|VL_BC|...|
        nat_bc = d[0] if len(d) > 0 else ""
        cst = d[1] if len(d) > 1 else ""
        return (reg, (nat_bc, cst))

    if reg == "M505":
        # |M505|NAT_BC|CST|VL_BC|...|
        nat_bc = d[0] if len(d) > 0 else ""
        cst = d[1] if len(d) > 1 else ""
        return (reg, (nat_bc, cst))

    if reg in ("M400", "M410", "M800", "M810"):
        # dedupe por (CST, COD_CT?, VL_REC?) -> aqui dá pra usar linha inteira (mais seguro)
        return (reg, tuple([_clean_sped_line(line)]))

    # default: dedupe por linha inteira
    return (reg, tuple([_clean_sped_line(line)]))

def sanitizar_bloco_m(bloco_m: List[str]) -> List[str]:
    """
    Mantém somente um bloco M consistente e sem duplicidades.
    Sempre reconta M990.
    """
    if not bloco_m:
        return ["|M001|0|", "|M990|2|"]

    # limpa e filtra só M*
    cleaned = []
    for ln in bloco_m:
        ln = _clean_sped_line(ln)
        if not ln:
            continue
        reg = _reg_of_line(ln)
        if not reg or not reg.startswith("M"):
            continue
        if reg in ("M001", "M990"):
            continue
        cleaned.append(ln)

    # dedupe: última ocorrência vence
    last_by_key: Dict[Tuple[str, Tuple[str, ...]], str] = {}
    order: List[Tuple[str, Tuple[str, ...]]] = []

    for ln in cleaned:
        k = _key_m(ln)
        if k not in last_by_key:
            order.append(k)
        last_by_key[k] = ln

    # remonta na ordem de aparição (mas com o último valor)
    body = [last_by_key[k] for k in order]

    out = ["|M001|0|"]
    out.extend(body)
    out.append(f"|M990|{len(out) + 1}|")
    return out

def _to_dec(v: str) -> Decimal:
    """Converte '1.234,56' ou '1234,56' em Decimal."""
    s = (v or "").strip()
    if not s:
        return Decimal("0.00")
    s = s.replace(".", "").replace(",", ".")
    try:
        return Decimal(s)
    except Exception:
        return Decimal("0.00")

def _json_any(obj) -> Dict[str, Any]:
    """
    Tenta extrair o JSON da revisao de forma tolerante:
    - revisao_json
    - payload_json
    - conteudo_json
    - meta_json
    """
    for attr in ("revisao_json", "payload_json", "conteudo_json", "meta_json", "json"):
        v = getattr(obj, attr, None)
        if isinstance(v, dict) and v:
            return v
    return {}

def carregar_ajustes_m(db, *, versao_id: int) -> List[Dict[str, Any]]:
    rows = (
        db.query(EfdRevisao)
        .filter(
            EfdRevisao.versao_revisada_id == int(versao_id),
            EfdRevisao.acao == "AJUSTE_M",
        )
        .order_by(EfdRevisao.id.asc())
        .all()
    )

    ajustes: List[Dict[str, Any]] = []
    for r in rows:
        j = r.revisao_json or {}
        meta = j.get("meta") if isinstance(j, dict) and isinstance(j.get("meta"), dict) else j
        if not isinstance(meta, dict) or not meta:
            continue

        meta2 = dict(meta)
        meta2.setdefault("acao", "AJUSTE_M")
        meta2.setdefault("versao_revisada_id", int(versao_id))
        meta2.setdefault("revisao_id", int(r.id))

        # ancoragem (opcional)
        meta2.setdefault("registro_id_anchor", int(r.registro_id) if r.registro_id else None)

        ajustes.append(meta2)

    return ajustes