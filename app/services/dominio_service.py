from typing import Optional
from sqlalchemy.orm import Session

from app.fiscal.constants import DOM_GERAL
from app.db.models.efd_versao import EfdVersao


def resolver_dominio_por_versao(db: Session, versao_id: int) -> str:
    versao = db.get(EfdVersao, int(versao_id))
    if not versao:
        return DOM_GERAL

    dom_v = (getattr(versao, "dominio", None) or "").strip().upper()
    if dom_v:
        return dom_v

    # 🔵 MESMA LÓGICA DO SCANNER
    emp = getattr(getattr(versao, "arquivo", None), "empresa", None)
    dom_e = (getattr(emp, "dominio", None) or "").strip().upper() if emp else ""

    if dom_e:
        return dom_e

    return DOM_GERAL