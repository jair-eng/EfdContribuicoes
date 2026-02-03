from __future__ import annotations
from typing import Any, Dict, List, Optional
from sqlalchemy.orm import Session
from app.db.models.efd_revisao import EfdRevisao
from app.db.models.efd_apontamento import EfdApontamento
from app.sped.blocoM.m_utils import _clean_sped_line

def revisar_bloco_m_override(
    db: Session,
    *,
    versao_origem_id: int,
    linhas_bloco_m: List[str],
    motivo_codigo: str,
    apontamento_id: Optional[int] = None,
) -> Dict[str, Any]:
    """
    Salva uma revisão pendente (versao_revisada_id=None) que sobrescreve o Bloco M.
    O export vai ler isso e usar como bloco_m_override.
    """

    # (opcional) marca apontamento como resolvido, se vier
    if apontamento_id is not None:
        ap = db.get(EfdApontamento, int(apontamento_id))
        if ap:
            if int(ap.versao_id) == int(versao_origem_id):
                ap.resolvido = True

    # normaliza/limpa linhas e garante que tem conteúdo
    out: List[str] = []
    for ln in (linhas_bloco_m or []):
        s = _clean_sped_line(str(ln or ""))
        if s:
            out.append(s)

    if not out:
        raise ValueError("Bloco M override vazio")

    # garante presença de M001 e M990 (segurança)
    if not any(x.startswith("|M001|") for x in out):
        out.insert(0, "|M001|0|")
    if not any(x.startswith("|M990|") for x in out):
        out.append(f"|M990|{len(out) + 1}|")

    # apaga overrides pendentes anteriores (somente pendentes)
    db.query(EfdRevisao).filter(
        EfdRevisao.versao_origem_id == int(versao_origem_id),
        EfdRevisao.versao_revisada_id.is_(None),
        EfdRevisao.acao == "OVERRIDE_BLOCK_M",
    ).delete(synchronize_session=False)

    db.add(EfdRevisao(
        versao_origem_id=int(versao_origem_id),
        versao_revisada_id=None,
        registro_id=None,
        reg="M",
        acao="OVERRIDE_BLOCK_M",
        revisao_json={
            "linhas": out,
            "detalhe": {
                "tipo": "BLOCO_M_OVERRIDE",
                "motivo": motivo_codigo,
            },
        },
        motivo_codigo=str(motivo_codigo),
        apontamento_id=apontamento_id,
    ))

    db.flush()
    return {"status": "ok", "linhas": len(out)}
