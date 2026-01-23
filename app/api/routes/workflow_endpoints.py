from __future__ import annotations
from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.orm import Session
from app.db.models.efd_revisao import EfdRevisao
from app.fiscal.regras.registry import get_regra_por_codigo
from app.schemas.helpers import carregar_linhas_sped, extrair_credito_total
from app.services.revision_service import materializar_versao_revisada
from app.services.workflow_service import WorkflowService
from app.db.session import get_db
from app.db.models import EfdVersao, EfdApontamento
from app.schemas.workflow import ConfirmarRevisaoIn, RevisaoFiscal
from sqlalchemy import or_, func
from typing import Optional
from fastapi import Body
from app.fiscal.contexto import build_ctx_exportacao
import logging
logger = logging.getLogger(__name__)


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

        if str(versao.status) != "EM_REVISAO":
            raise HTTPException(
                status_code=400,
                detail=f"Versão precisa estar EM_REVISAO para confirmar. Status atual: {versao.status}",
            )

        # (compat) aplica alterações se vierem no payload (normalmente payload=None, pois front faz batch)

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

            if to_resolver:
                (
                    db.query(EfdApontamento)
                    .filter(EfdApontamento.versao_id == int(versao_id),
                            EfdApontamento.id.in_(list(to_resolver)))
                    .update({EfdApontamento.resolvido: True}, synchronize_session=False)
                )

            if to_reabrir:
                (
                    db.query(EfdApontamento)
                    .filter(EfdApontamento.versao_id == int(versao_id),
                            EfdApontamento.id.in_(list(to_reabrir)))
                    .update({EfdApontamento.resolvido: False}, synchronize_session=False)
                )

        # ✅ bloqueio: apenas ERRO pendente
        pendentes_erro = (
            db.query(func.count(EfdApontamento.id))
            .filter(EfdApontamento.versao_id == int(versao_id))
            .filter(EfdApontamento.tipo == "ERRO")
            .filter(or_(EfdApontamento.resolvido.is_(False), EfdApontamento.resolvido.is_(None)))
            .scalar()
        ) or 0

        if int(pendentes_erro) > 0:
            db.rollback()
            raise HTTPException(400, f"Ainda existem {int(pendentes_erro)} apontamentos de ERRO pendentes.")

        # Codigo novo

        apontamentos_resolvidos = (
            db.query(EfdApontamento)
            .filter(EfdApontamento.versao_id == int(versao_id))
            .filter(EfdApontamento.resolvido.is_(True))
            .all()
        )

        revisoes: list[RevisaoFiscal] = []
        ja_processadas = set()


        for ap in apontamentos_resolvidos:

            if ap.codigo in ja_processadas:
                continue
            ja_processadas.add(ap.codigo)
            regra = get_regra_por_codigo(ap.codigo)

            if not regra:
                continue

             # ✅ meta sempre existe por apontamento
            meta_ap = ap.meta_json or {}

            ctx = {
                "db": db,
                "versao": versao,
                "apontamento": ap,
            }

            codigo = (ap.codigo or "").strip().upper()

            if codigo in ("EXP_RESSARC_V1", "EXP_M_ZERADO_V1"):

                linhas = carregar_linhas_sped(db=db, versao=versao)
                ctx.update(build_ctx_exportacao(linhas_sped=linhas, meta=meta_ap))

                # ✅ guarda uma referência útil pro “detalhe” (ex.: última regra de export)
                meta_final = meta_ap

            # ✅ Só gera/salva revisão se a regra tem gerar_revisoes
            if hasattr(regra, "gerar_revisoes"):
                # 🔴 LIMPA revisões antigas *ANTES* de criar as novas
                # motivo_codigo deve ser o código da regra (igual r.regra_codigo)
                db.query(EfdRevisao).filter(
                    EfdRevisao.versao_origem_id == int(versao.id),
                    EfdRevisao.motivo_codigo == codigo,
                    # opcional (recomendado): mantém as revisões isoladas por apontamento
                    EfdRevisao.apontamento_id == int(ap.id),
                ).delete(synchronize_session=False)

                novas = regra.gerar_revisoes(ctx) or []
                revisoes.extend(novas)

        detalhe = {
            "impacto_por_cfop": meta_final.get("impacto_por_cfop") or {},
            "impacto_consolidado": meta_final.get("impacto_consolidado"),
            "metodo": meta_final.get("metodo"),
            "fonte": meta_final.get("fonte"),
            "cenario": meta_final.get("cenario"),
            "cfops_detectados": meta_final.get("cfops_detectados"),
            "cfops_top": meta_final.get("cfops_top"),
        }

        for r in revisoes:
            db.add(
                EfdRevisao(
                    versao_origem_id=int(versao.id),
                    versao_revisada_id=None,
                    registro_id=r.registro_id,
                    reg=r.registro,
                    acao=r.operacao,
                    revisao_json={
                        "linha_referencia": r.linha_referencia,  # 1-based
                        "linha_antes": r.linha_antes,  # ✅ novo
                        "linha_hash": r.linha_hash,  # ✅ agora vem preenchido
                        "linha_nova": r.conteudo,
                        "detalhe": detalhe,
                    },
                    motivo_codigo=r.regra_codigo,
                    apontamento_id=int(ap.id),
                )
            )

        # ✅ cria/copia/aplica revisões => devolve a revisada
        versao_revisada_id = materializar_versao_revisada(db=db, versao_origem_id=int(versao_id))

        db.add(versao)
        db.commit()

        return {
            "versao_id": int(versao_id),
            "status": str(versao.status),
            "pendentes_erro": int(pendentes_erro),
            "versao_revisada_id": int(versao_revisada_id),
        }


    except Exception as e:

        db.rollback()

        logger.exception("Erro em confirmar_revisao versao_id=%s", versao_id)

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
