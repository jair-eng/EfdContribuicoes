from __future__ import annotations

from decimal import Decimal
from typing import Any, Dict, List, Optional
from sqlalchemy.orm import Session
from app.fiscal.scanners.exportacao import _dec_br
from app.db.models.efd_revisao import EfdRevisao
from app.db.models.efd_apontamento import EfdApontamento
from app.sped.blocoM.blocoM import calcular_blocoM
from app.sped.blocoM.m_utils import _clean_sped_line, _reg_of_line

ACAO_OVERRIDE_M = "OVERRIDE_BLOCK_M"


def buscar_override_bloco_m(
    db: Session,
    *,
    versao_origem_id: int,
    versao_final_id: Optional[int],
) -> Optional[List[str]]:

    q = db.query(EfdRevisao).filter(EfdRevisao.acao == ACAO_OVERRIDE_M)
    if versao_final_id is not None:
        q = q.filter(EfdRevisao.versao_revisada_id == int(versao_final_id))
    else:
        q = q.filter(EfdRevisao.versao_origem_id == int(versao_origem_id))
        q = q.filter(EfdRevisao.versao_revisada_id.is_(None))

    rv = q.order_by(EfdRevisao.created_at.desc(), EfdRevisao.id.desc()).first()
    if not rv:
        return None

    linhas = (rv.revisao_json or {}).get("linhas") or []
    corpo = [
        _clean_sped_line(x)
        for x in linhas
        if _reg_of_line(_clean_sped_line(x)).startswith("M")
        and _reg_of_line(_clean_sped_line(x)) not in ("M001", "M990")
    ]

    return calcular_blocoM(corpo)

def extrair_credito_total_do_bloco_m(bloco_m: List[str]) -> Decimal:
    """
    Extrai crédito do Bloco M gerado pelo construir_bloco_m_v3:
      - M100: crédito PIS está em parts[8]
      - M500: crédito COFINS está em parts[8]
    """
    cred_pis = Decimal("0")
    cred_cof = Decimal("0")

    for ln in bloco_m or []:
        s = (ln or "").strip()
        if not s:
            continue

        if s.startswith("|M100|"):
            parts = s.split("|")
            if len(parts) > 8:
                cred_pis += _dec_br(parts[8])

        elif s.startswith("|M500|"):
            parts = s.split("|")
            if len(parts) > 8:
                cred_cof += _dec_br(parts[8])

    return (cred_pis + cred_cof).quantize(Decimal("0.01"))


def salvar_override_bloco_m(
    db: Session,
    *,
    versao_origem_id: int,
    linhas_bloco_m: List[str],
    motivo_codigo: str,
    apontamento_id: Optional[int] = None,
    versao_revisada_id: Optional[int] = None,
) -> int:
    """
    Salva override do Bloco M em EfdRevisao.
    - versao_revisada_id=None: pendente (tela de revisão)
    - versao_revisada_id=int: já materializado
    Retorna id da revisão criada.
    """
    # ✅ normaliza e filtra só M* (sem M001/M990)
    corpo: List[str] = []
    for ln in (linhas_bloco_m or []):
        s = _clean_sped_line(str(ln or ""))
        if not s:
            continue
        if not s.lstrip().startswith("|M"):
            continue
        if s.startswith("|M001|") or s.startswith("|M990|"):
            continue
        corpo.append(s)

    if not corpo:
        raise ValueError("Override Bloco M vazio (sem linhas M*)")

    # ✅ reconstrói bloco completo com contagem correta
    out = calcular_blocoM(corpo)

    # remove override antigo do mesmo escopo
    q = db.query(EfdRevisao).filter(EfdRevisao.acao == ACAO_OVERRIDE_M)
    if versao_revisada_id is not None:
        q = q.filter(EfdRevisao.versao_revisada_id == int(versao_revisada_id))
    else:
        q = q.filter(EfdRevisao.versao_origem_id == int(versao_origem_id))
        q = q.filter(EfdRevisao.versao_revisada_id.is_(None))
    q.delete(synchronize_session=False)

    # marca apontamento resolvido (opcional)
    if apontamento_id is not None:
        ap = db.get(EfdApontamento, int(apontamento_id))
        if ap and int(ap.versao_id) == int(versao_origem_id):
            ap.resolvido = True

    rev = EfdRevisao(
        versao_origem_id=int(versao_origem_id),
        versao_revisada_id=int(versao_revisada_id) if versao_revisada_id is not None else None,
        registro_id=None,
        reg="M",
        acao=ACAO_OVERRIDE_M,
        revisao_json={
            "linhas": out,
            "detalhe": {"tipo": "BLOCO_M_OVERRIDE"},
        },
        motivo_codigo=str(motivo_codigo),
        apontamento_id=int(apontamento_id) if apontamento_id is not None else None,
    )

    db.add(rev)
    db.flush()
    return int(rev.id)