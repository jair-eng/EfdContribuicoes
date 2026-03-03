from typing import Optional
from sqlalchemy.orm import Session

from app.fiscal.constants import DOM_GERAL
from app.db.models.efd_versao import EfdVersao  # import do seu projeto
from app.db.models.empresa import Empresa       # ajuste o caminho

def resolver_dominio_por_versao(db: Session, versao_id: int) -> str:
    """
    Regra:
      - EfdVersao.dominio (override) se existir
      - senão Empresa.dominio
      - fallback DOM_GERAL
    """
    versao = db.get(EfdVersao, int(versao_id))
    if not versao:
        return DOM_GERAL

    # 1) override na versão
    dom_v = (getattr(versao, "dominio", None) or "").strip()
    if dom_v:
        return dom_v

    # 2) domínio da empresa
    empresa_id = getattr(versao, "empresa_id", None)
    if empresa_id:
        emp = db.get(Empresa, int(empresa_id))
        dom_e = (getattr(emp, "dominio", None) or "").strip() if emp else ""
        if dom_e:
            return dom_e

    return DOM_GERAL