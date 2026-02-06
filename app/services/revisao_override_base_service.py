from __future__ import annotations

from typing import Optional, Dict
from decimal import Decimal
from sqlalchemy.orm import Session

from app.db.models.efd_revisao import EfdRevisao
from app.fiscal.contexto import dec_any
from app.sped.blocoM.m_utils import _cst_norm
from app.fiscal.constants import ACAO_OVERRIDE_BASE_POR_CST


def buscar_override_base_por_cst(
    db: Session,
    *,
    versao_origem_id: int,
    versao_final_id: Optional[int],
) -> Optional[Dict[str, Decimal]]:
    """
    Busca o último OVERRIDE_BASE_POR_CST. Estratégia:
      1) tenta por versao_revisada_id == versao_final_id (quando já materializou/copiou)
      2) fallback: versao_origem_id == versao_origem_id AND versao_revisada_id IS NULL
    """

    def _load(q) -> Optional[Dict[str, Decimal]]:
        rv = q.order_by(EfdRevisao.created_at.desc(), EfdRevisao.id.desc()).first()
        if not rv:
            return None

        j = rv.revisao_json or {}
        base_raw = j.get("base_por_cst") or (j.get("payload") or {}).get("base_por_cst") or {}
        if not isinstance(base_raw, dict) or not base_raw:
            return None

        out: Dict[str, Decimal] = {}
        for cst, v in base_raw.items():
            c = _cst_norm(str(cst))
            if not c:
                continue
            d = dec_any(v)
            if d <= 0:
                continue
            out[c] = out.get(c, Decimal("0.00")) + d

        return out or None

    q = db.query(EfdRevisao).filter(EfdRevisao.acao == ACAO_OVERRIDE_BASE_POR_CST)

    # 1) tenta por revisada (export sempre tem versao_final_id)
    if versao_final_id is not None:
        q1 = q.filter(EfdRevisao.versao_revisada_id == int(versao_final_id))
        got = _load(q1)
        if got:
            return got

    # 2) fallback por origem + NULL (enquanto não copiou p/ revisada)
    q2 = (
        q.filter(EfdRevisao.versao_origem_id == int(versao_origem_id))
        .filter(EfdRevisao.versao_revisada_id.is_(None))
    )
    return _load(q2)
