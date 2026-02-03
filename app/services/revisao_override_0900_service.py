from typing import Sequence, List, Dict, Any, Optional, Tuple

from app.db.models import EfdRevisao
from app.sped.blocoM.m_utils import _clean_sped_line
from sqlalchemy.orm import Session

ACAO_OVERRIDE_0900 = "OVERRIDE_0900"

def salvar_override_0900(
    db: Session,
    *,
    versao_origem_id: int,
    linha_0900: str,
    motivo_codigo: str,
    apontamento_id: int | None = None,
    versao_revisada_id: int | None = None,
) -> int:


    linha_0900 = _clean_sped_line(linha_0900)
    if not linha_0900.startswith("|0900|"):
        raise ValueError("Linha 0900 inválida")

    # remove override anterior
    q = db.query(EfdRevisao).filter(EfdRevisao.acao == ACAO_OVERRIDE_0900)
    if versao_revisada_id is not None:
        q = q.filter(EfdRevisao.versao_revisada_id == versao_revisada_id)
    else:
        q = q.filter(EfdRevisao.versao_origem_id == versao_origem_id)
        q = q.filter(EfdRevisao.versao_revisada_id.is_(None))
    q.delete(synchronize_session=False)

    rev = EfdRevisao(
        versao_origem_id=versao_origem_id,
        versao_revisada_id=versao_revisada_id,
        registro_id=None,
        reg="0900",
        acao=ACAO_OVERRIDE_0900,
        revisao_json={
            "linha": linha_0900,
            "detalhe": {"tipo": "BLOCO_0_0900"},
        },
        motivo_codigo=motivo_codigo,
        apontamento_id=apontamento_id,
    )

    db.add(rev)
    db.flush()
    return rev.id


def buscar_override_0900(
    db: Session,
    *,
    versao_origem_id: int,
    versao_final_id: int | None,
) -> str | None:

    q = db.query(EfdRevisao).filter(EfdRevisao.acao == ACAO_OVERRIDE_0900)

    if versao_final_id is not None:
        q = q.filter(EfdRevisao.versao_revisada_id == versao_final_id)
    else:
        q = q.filter(EfdRevisao.versao_origem_id == versao_origem_id)
        q = q.filter(EfdRevisao.versao_revisada_id.is_(None))

    rv = q.order_by(EfdRevisao.created_at.desc()).first()
    if not rv:
        return None

    return _clean_sped_line((rv.revisao_json or {}).get("linha", ""))

