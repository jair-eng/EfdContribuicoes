from __future__ import annotations
from datetime import datetime
from typing import Any, Dict, Optional
from sqlalchemy.orm import Session
from app.db.models import EfdRegistro, EfdRevisao
from app.db.models.ref_models import RefCfop, RefCstPisCofins
from app.sped.c170_utils import patch_c170_campos
from app.sped.formatter import formatar_linha  # ajuste o caminho se necessário


def _get_dados(r: EfdRegistro) -> list[Any]:
    cj = getattr(r, "conteudo_json", None) or {}
    dados = cj.get("dados")
    if not isinstance(dados, list):
        raise ValueError("Registro não possui conteudo_json['dados'] em formato lista.")
    return dados


def _lookup_refs(
    db: Session,
    *,
    cfop: Optional[str],
    cst_pis: Optional[str],
    cst_cofins: Optional[str],
) -> Dict[str, Any]:
    """
    Não bloqueia o MVP: valida se existe e retorna warnings.
    """
    warnings = []

    if cfop:
        found = db.get(RefCfop, cfop)
        if not found:
            warnings.append(f"CFOP {cfop} não cadastrado em ref_cfop (permitido no MVP).")

    for label, cst in (("CST_PIS", cst_pis), ("CST_COFINS", cst_cofins)):
        if cst:
            obj = db.get(RefCstPisCofins, cst)
            if not obj:
                warnings.append(f"{label} {cst} não cadastrado em ref_cst_pis_cofins (permitido no MVP).")
            else:
                if getattr(obj, "gera_credito", False) is False:
                    warnings.append(f"{label} {cst} está marcado como não-gerador de crédito na referência.")

    return {"warnings": warnings}


def revisar_c170(
    db: Session,
    *,
    registro_id: int,
    versao_origem_id: int,
    cfop: Optional[str],
    cst_pis: Optional[str],
    cst_cofins: Optional[str],
    motivo_codigo: str,
    apontamento_id: Optional[int] = None,
) -> Dict[str, Any]:
    r: EfdRegistro | None = db.get(EfdRegistro, int(registro_id))
    if not r:
        raise ValueError("Registro não encontrado.")

    if int(r.versao_id) != int(versao_origem_id):
        raise ValueError("Registro não pertence à versão origem informada.")

    if str(r.reg).strip().upper() != "C170":
        raise ValueError("Apenas registros C170 são suportados neste endpoint.")

    dados = _get_dados(r)

    # aplica patch seguro (valida índices + formatos)
    novos_campos = patch_c170_campos(dados, cfop=cfop, cst_pis=cst_pis, cst_cofins=cst_cofins)

    # render linha nova (contrato central)
    linha_nova = formatar_linha("C170", novos_campos).strip()

    # warnings de referência (sem travar)
    ref_info = _lookup_refs(db, cfop=cfop, cst_pis=cst_pis, cst_cofins=cst_cofins)

    # cria revisão
    rev = EfdRevisao(
        versao_origem_id=int(versao_origem_id),
        versao_revisada_id=None,
        registro_id=int(r.id),
        reg="C170",
        acao="REPLACE_LINE",
        revisao_json={
            "linha_referencia": int(getattr(r, "linha", 0)),
            "linha_nova": linha_nova,
            "detalhe": {
                "tipo": "PATCH_C170",
                "set": {"cfop": cfop, "cst_pis": cst_pis, "cst_cofins": cst_cofins},
                "warnings": ref_info["warnings"],
            },
            "antes": {
                "cfop": str(dados[10]) if len(dados) > 10 else None,  # opcional (debug)
            },
        },
        motivo_codigo=str(motivo_codigo or "MANUAL_C170"),
        apontamento_id=int(apontamento_id) if apontamento_id else None,
        created_at=datetime.utcnow(),
    )
    db.add(rev)
    db.flush()

    return {
        "revisao_id": int(rev.id),
        "registro_id": int(r.id),
        "linha": int(getattr(r, "linha", 0)),
        "linha_nova": linha_nova,
        "warnings": ref_info["warnings"],
    }
