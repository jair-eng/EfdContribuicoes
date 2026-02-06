from __future__ import annotations

from collections import defaultdict
from decimal import Decimal
from typing import Any, Dict, List, Optional, Sequence

from app.db.models import EfdRegistro
from app.fiscal.dto import RegistroFiscalDTO
from app.fiscal.regras.base_regras import RegraBase  # só p/ helpers se quiser, opcional


def montar_c100_entrada_relevante_agg(
    registros_db: Sequence[EfdRegistro],
    *,
    vl_doc_min: Decimal = Decimal("50000"),
    top_n: int = 30,
) -> Optional[RegistroFiscalDTO]:
    c100s = [r for r in registros_db if (r.reg or "").strip() == "C100"]
    if not c100s:
        return None

    itens: List[Dict[str, Any]] = []
    anchor_linha: Optional[int] = None
    anchor_registro_id: Optional[int] = None

    for r in c100s:
        dados = (r.conteudo_json or {}).get("dados") or []
        if len(dados) < 11:
            continue

        ind_oper = str(dados[1] or "").strip()
        if ind_oper != "0":
            continue

        # VL_DOC observado em [10] (fallback [14])
        vl_doc_raw = dados[10] if len(dados) > 10 else None
        if (vl_doc_raw is None or str(vl_doc_raw).strip() == "") and len(dados) > 14:
            vl_doc_raw = dados[14]

        # parse simples (mantém string; regra parseia com dec_br)
        try:
            vl_doc_num = str(vl_doc_raw or "").strip()
        except Exception:
            vl_doc_num = ""

        if not vl_doc_num:
            continue

        # aqui não converte Decimal pra não depender de helper; a regra converte
        # mas dá pra filtrar “min” com um parse rápido:
        try:
            v = Decimal(vl_doc_num.replace(".", "").replace(",", "."))
        except Exception:
            continue

        if v < vl_doc_min:
            continue

        linha_r = int(getattr(r, "linha", 0) or 0)
        rid = int(getattr(r, "id", 0) or 0)

        if linha_r > 0 and (anchor_linha is None or linha_r < anchor_linha):
            anchor_linha = linha_r
        if anchor_registro_id is None and rid > 0:
            anchor_registro_id = rid

        num_doc = str(dados[6] or "").strip() if len(dados) > 6 else ""
        chave = str(dados[7] or "").strip() if len(dados) > 7 else ""
        modelo = str(dados[3] or "").strip() if len(dados) > 3 else ""
        serie = str(dados[5] or "").strip() if len(dados) > 5 else ""

        itens.append(
            {
                "registro_id": rid,
                "linha": linha_r,
                "vl_doc": vl_doc_num,
                "num_doc": num_doc,
                "chave_nfe": chave,
                "modelo": modelo,
                "serie": serie,
            }
        )

    if not itens or not anchor_linha:
        return None

    # ordena por vl_doc desc
    def _v(it: Dict[str, Any]) -> Decimal:
        try:
            return Decimal(str(it.get("vl_doc") or "").replace(".", "").replace(",", "."))
        except Exception:
            return Decimal("0")

    itens_sorted = sorted(itens, key=_v, reverse=True)

    meta = {
        "fonte": "C100",
        "fonte_base": "C100_ENT_AGG",
        "vl_doc_min": str(vl_doc_min),
        "qtd_total": int(len(itens_sorted)),
        "top_n": int(top_n),
        "anchor_reg_base": "C100",
        "anchor_linha": int(anchor_linha),
        "anchor_registro_id": int(anchor_registro_id or 0) or None,
    }

    dados_final: List[Any] = [{"_meta": meta}] + itens_sorted[:top_n]

    return RegistroFiscalDTO(
        id=0,
        reg="C100_ENT_AGG",
        linha=int(anchor_linha),
        dados=dados_final,
    )
