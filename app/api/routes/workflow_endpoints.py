from __future__ import annotations
from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.orm import Session
from app.services.workflow_service import WorkflowService
from app.db.session import get_db
from app.db.models import EfdVersao, EfdRegistro, EfdApontamento
from app.schemas.workflow import ConfirmarRevisaoIn
from typing import Set
from sqlalchemy import or_, func
from typing import Optional
from fastapi import Body


router = APIRouter(prefix="/workflow", tags=["Workflow Fiscal"])

def _exec(db: Session, versao_id: int, acao: str) -> dict:
    try:
        if acao == "revisar":
            WorkflowService.iniciar_revisao(versao_id, db)
            novo_status = "EM_REVISAO"
        elif acao == "validar":
            WorkflowService.validar_versao(versao_id, db)
            novo_status = "VALIDADA"
        else:
            raise HTTPException(status_code=400, detail="Ação inválida")

        db.commit()
        return {"versao_id": versao_id, "acao": acao, "status": novo_status}

    except HTTPException:
        db.rollback()
        raise
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=400, detail=str(e))


@router.post("/versao/{versao_id}/revisar", status_code=status.HTTP_200_OK)
def iniciar_revisao(versao_id: int, db: Session = Depends(get_db)):
    versao = db.get(EfdVersao, versao_id)
    if not versao:
        raise HTTPException(status_code=404, detail="Versão não encontrada")

    if versao.status != "GERADA":
        raise HTTPException(status_code=400, detail="Apenas versões GERADAS podem entrar em revisão")

    versao.status = "EM_REVISAO"
    db.add(versao)
    db.commit()

    return {"versao_id": versao_id, "status": versao.status}



@router.post("/versao/{versao_id}/confirmar-revisao", status_code=status.HTTP_200_OK)
def confirmar_revisao(

        versao_id: int,
        payload: Optional[ConfirmarRevisaoIn] = Body(default=None),
        db: Session = Depends(get_db),
    ):
    try:
        versao = db.query(EfdVersao).filter(EfdVersao.id == versao_id).first()
        if not versao:
            raise HTTPException(404, "Versão não encontrada.")

        # ✅ Regra: só confirma se estiver em revisão
        if versao.status != "EM_REVISAO":
            raise HTTPException(
                status_code=400,
                detail=f"Versão precisa estar EM_REVISAO para confirmar. Status atual: {versao.status}"
            )

        # -----------------------------
        # 1) Normaliza entradas
        # -----------------------------
        to_resolver: Set[int] = set()
        to_reabrir: Set[int] = set()

        if payload is not None:
            to_resolver = set(payload.to_resolver or [])
            to_reabrir = set(payload.to_reabrir or [])

            alteracoes = getattr(payload, "alteracoes", None)
            if alteracoes:
                for a in alteracoes:
                    if bool(a.resolvido) is True:
                        to_resolver.add(int(a.apontamento_id))
                    else:
                        to_reabrir.add(int(a.apontamento_id))

            to_resolver -= to_reabrir

            # aplica em lote (sem commit ainda)
            if to_resolver:
                (db.query(EfdApontamento)
                 .filter(EfdApontamento.versao_id == versao_id,
                         EfdApontamento.id.in_(list(to_resolver)))
                 .update({EfdApontamento.resolvido: True}, synchronize_session=False))

            if to_reabrir:
                (db.query(EfdApontamento)
                 .filter(EfdApontamento.versao_id == versao_id,
                         EfdApontamento.id.in_(list(to_reabrir)))
                 .update({EfdApontamento.resolvido: False}, synchronize_session=False))

        # -----------------------------
        # 2) Aplica em lote (sem commit ainda)
        # -----------------------------
        if to_resolver:
            (db.query(EfdApontamento)
             .filter(
                EfdApontamento.versao_id == versao_id,
                EfdApontamento.id.in_(list(to_resolver)),
            )
             .update({EfdApontamento.resolvido: True}, synchronize_session=False))

        if to_reabrir:
            (db.query(EfdApontamento)
             .filter(
                EfdApontamento.versao_id == versao_id,
                EfdApontamento.id.in_(list(to_reabrir)),
            )
             .update({EfdApontamento.resolvido: False}, synchronize_session=False))

        # -----------------------------
        # 3) Checa pendentes (False ou NULL)
        # -----------------------------
        pendentes = (
                        db.query(func.count(EfdApontamento.id))
                        .filter(EfdApontamento.versao_id == versao_id)
                        .filter(or_(EfdApontamento.resolvido.is_(False), EfdApontamento.resolvido.is_(None)))
                        .scalar()
                    ) or 0

        if int(pendentes) > 0:
            # ✅ desfaz alterações (não deixa banco “meio aplicado”)
            db.rollback()
            raise HTTPException(400, f"Ainda existem {int(pendentes)} apontamentos pendentes.")

        # -----------------------------
        # 4) Status permanece EM_REVISAO (não existe REVISADA no enum)
        # -----------------------------
        # versao.status já é EM_REVISAO; mantemos.
        db.add(versao)

        db.commit()

        return {
            "versao_id": versao_id,
            "status": versao.status,
            "aplicados_resolver": len(to_resolver),
            "aplicados_reabrir": len(to_reabrir),
            "pendentes": int(pendentes),
        }

    except HTTPException:
        db.rollback()
        raise
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/versao/{versao_id}/validar")
def validar_versao(versao_id: int, db: Session = Depends(get_db)):
    versao = db.query(EfdVersao).filter(EfdVersao.id == versao_id).first()
    if not versao:
        raise HTTPException(404, "Versão não encontrada.")

    if versao.status not in ("GERADA", "EM_REVISAO"):
        raise HTTPException(
            400,
            f"Versão precisa estar GERADA ou EM_REVISAO para validar. Status atual: {versao.status}"
        )

    pendentes_erro = (
                         db.query(func.count(EfdApontamento.id))
                         .filter(EfdApontamento.versao_id == versao_id)
                         .filter(EfdApontamento.tipo == "ERRO")
                         .filter(or_(EfdApontamento.resolvido.is_(False), EfdApontamento.resolvido.is_(None)))
                         .scalar()
                     ) or 0

    if int(pendentes_erro) > 0:
        raise HTTPException(400, f"Ainda existem {int(pendentes_erro)} apontamentos de ERRO pendentes.")

    # aqui entram suas regras finais (placeholder)
    # ex: checar registros obrigatórios, coerências, somatórios, etc.

    versao.status = "VALIDADA"
    db.commit()

    return {"versao_id": versao_id, "status": versao.status, "message": "Versão validada com sucesso."}
