from __future__ import annotations
from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.orm import Session
from app.db.models.efd_revisao import EfdRevisao
from app.fiscal.regras.Diagnostico.registry import get_regra_por_codigo
from app.fiscal.scanner import FiscalScanner
from app.schemas.helpers import carregar_linhas_sped
from app.services.revision_service import materializar_versao_revisada
from app.services.workflow_service import WorkflowService
from app.db.session import get_db
from app.db.models import EfdVersao, EfdApontamento, EfdRegistro
from app.schemas.workflow import ConfirmarRevisaoIn, RevisaoFiscal, ConfirmarRevisaoBody
from sqlalchemy import or_, func
from typing import Optional, Any, Dict, List, Tuple
from sqlalchemy import delete
from fastapi import Body

from app.fiscal.constants import ACAO_OVERRIDE_BASE_POR_CST
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
    body: Optional[ConfirmarRevisaoBody] = Body(default=None),
    db: Session = Depends(get_db),
):
    """
    Confirma revisão:
      - (compat) aplica resolvido/reabrir se vier payload
      - trava: não permite ERRO pendente
      - gera revisões automáticas (ex.: exportação) para apontamentos RESOLVIDOS
      - salva revisões como "pendentes" (versao_revisada_id=None), conforme decisão do projeto
      - materializa versão revisada (cria/copia/aplica REPLACE_LINE/INSERT/DELETE etc.)
      - commit
    """
    payload = body.payload if body else None
    meta_final: Dict[str, Any] = {}

    try:
        versao = db.query(EfdVersao).filter(EfdVersao.id == int(versao_id)).first()
        if not versao:
            raise HTTPException(404, "Versão não encontrada.")

        if str(getattr(versao, "status", "")) != "EM_REVISAO":
            raise HTTPException(
                status_code=400,
                detail=f"Versão precisa estar EM_REVISAO para confirmar. Status atual: {versao.status}",
            )

        # (compat) aplica alterações se vierem no payload (normalmente payload=None; front faz batch)
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
                    .filter(
                        EfdApontamento.versao_id == int(versao_id),
                        EfdApontamento.id.in_(list(to_resolver)),
                    )
                    .update({EfdApontamento.resolvido: True}, synchronize_session=False)
                )

            if to_reabrir:
                (
                    db.query(EfdApontamento)
                    .filter(
                        EfdApontamento.versao_id == int(versao_id),
                        EfdApontamento.id.in_(list(to_reabrir)),
                    )
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

        # ---------- GERAÇÃO DE REVISÕES AUTOMÁTICAS (aqui entra AJUSTE_M) ----------
        apontamentos_resolvidos: List["EfdApontamento"] = (
            db.query(EfdApontamento)
            .filter(EfdApontamento.versao_id == int(versao_id))
            .filter(EfdApontamento.resolvido.is_(True))
            .all()
        )

        # vamos armazenar (ap_id, revisao_fiscal) para não perder o apontamento correto
        revisoes_para_salvar: List[Tuple[int, "RevisaoFiscal"]] = []

        # Se você quer deduplicar por código de regra (como estava), mantenha.
        # (Nota: isso pode pular revisões se houver mais de um apontamento do mesmo código.
        #  Mantive o comportamento original.)
        ja_processadas = set()

        for ap in apontamentos_resolvidos:
            codigo = (ap.codigo or "").strip().upper()
            if not codigo:
                continue

            if codigo in ja_processadas:
                continue
            ja_processadas.add(codigo)

            regra = get_regra_por_codigo(codigo)
            if not regra:
                continue

            meta_ap = ap.meta_json or {}

            ctx: Dict[str, Any] = {
                "db": db,
                "versao": versao,
                "apontamento": ap,
            }

            # Contexto extra para exportação/ressarcimento
            if codigo in ("EXP_RESSARC_V1", "EXP_M_ZERADO_V1"):
                linhas = carregar_linhas_sped(db=db, versao=versao)
                ctx.update(build_ctx_exportacao(linhas_sped=linhas, meta=meta_ap))
                meta_final = meta_ap  # útil pro "detalhe"

                db.query(EfdRevisao).filter(
                    EfdRevisao.versao_origem_id == int(versao.id),
                    EfdRevisao.motivo_codigo == codigo,
                    EfdRevisao.acao.in_(["OVERRIDE_BASE_POR_CST", "AJUSTE_M"]),
                ).delete(synchronize_session=False)

            # ✅ checa o método CERTO (sem confundir com outro nome)
            gerador = getattr(regra, "gerar_revisoes_exp_ressarc_v1", None)
            if not callable(gerador):
                continue

            # 🔴 LIMPA revisões antigas desta regra+apontamento (como você já fazia)
            else:
                db.query(EfdRevisao).filter(
                    EfdRevisao.versao_origem_id == int(versao.id),
                    EfdRevisao.motivo_codigo == codigo,
                    EfdRevisao.apontamento_id == int(ap.id),
                ).delete(synchronize_session=False)

            novas = gerador(ctx) or []
            #tirar
            ops = [str(getattr(x, "operacao", "") or "") for x in novas]
            print(f"[CONFIRMAR] regra={codigo} apontamento={ap.id} novas={len(novas)} ops={ops}")
            for r in novas:
                revisoes_para_salvar.append((int(ap.id), r))

        detalhe = {
            "impacto_por_cfop": (meta_final or {}).get("impacto_por_cfop") or {},
            "impacto_consolidado": (meta_final or {}).get("impacto_consolidado"),
            "metodo": (meta_final or {}).get("metodo"),
            "fonte": (meta_final or {}).get("fonte"),
            "cenario": (meta_final or {}).get("cenario"),
            "cfops_detectados": (meta_final or {}).get("cfops_detectados"),
            "cfops_top": (meta_final or {}).get("cfops_top"),
        }

        # Salva revisões como PENDENTES (versao_revisada_id=None) — conforme decisão
        for ap_id, r in revisoes_para_salvar:
            operacao = str(getattr(r, "operacao", "") or "").strip()

            # 1) OVERRIDE_BASE_POR_CST (já existia)
            if operacao == ACAO_OVERRIDE_BASE_POR_CST:
                payload_r = getattr(r, "payload", None) or {}
                revisao_json = {
                    "base_por_cst": payload_r.get("base_por_cst") or {},
                    "cod_cont": payload_r.get("cod_cont") or "201",
                    "nat_bc": payload_r.get("nat_bc") or "01",
                    "detalhe": detalhe,
                }
                registro_id = None
                reg = "M"

            # 2) AJUSTE_M (novo) — sem linha_nova
            elif operacao == "AJUSTE_M":
                payload_r = getattr(r, "payload", None) or {}
                revisao_json = {
                    "meta": payload_r,   # <- o loader lê isso
                    "detalhe": detalhe,
                }
                # ancoragem opcional (se seu RevisaoFiscal tiver)
                registro_id = getattr(r, "registro_id", None)
                reg = getattr(r, "registro", None) or "M"

            # 3) Padrão: revisões que mexem em linha (REPLACE_LINE/INSERT/DELETE)
            else:
                revisao_json = {
                    "linha_referencia": getattr(r, "linha_referencia", None),
                    "linha_antes": getattr(r, "linha_antes", None),
                    "linha_hash": getattr(r, "linha_hash", None),
                    "linha_nova": getattr(r, "conteudo", None),
                    "detalhe": detalhe,
                }
                registro_id = getattr(r, "registro_id", None)
                reg = getattr(r, "registro", None)

            db.add(
                EfdRevisao(
                    versao_origem_id=int(versao.id),
                    versao_revisada_id=None,  # ✅ pendente até materializar (decisão do projeto)
                    registro_id=int(registro_id) if registro_id else None,
                    reg=str(reg) if reg else None,
                    acao=operacao,
                    revisao_json=revisao_json,
                    motivo_codigo=str(getattr(r, "regra_codigo", None) or codigo),
                    apontamento_id=int(ap_id),
                )
            )
        db.flush()
        # tirar
        q_aj = db.query(func.count(EfdRevisao.id)).filter(
            EfdRevisao.versao_origem_id == int(versao.id),
            EfdRevisao.versao_revisada_id.is_(None),
            EfdRevisao.acao == "AJUSTE_M",
        ).scalar() or 0

        print(f"[CONFIRMAR] AJUSTE_M pendentes antes de materializar = {int(q_aj)}")

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

    except HTTPException:
        # mantém a HTTPException original
        raise
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


@router.post("/versao/{versao_id}/excluir-e-reprocessar", status_code=status.HTTP_200_OK)
def excluir_registro_e_reprocessar(
        apontamento_id: int,
        db: Session = Depends(get_db),
):
    """
    Ação definitiva: Exclui a nota problemática e limpa todos os apontamentos
    para recalcular a apuração do zero (Hard Reset).
    """
    try:
        # 1. Localização e Validação
        ap = db.query(EfdApontamento).filter(EfdApontamento.id == apontamento_id).first()
        if not ap:
            raise HTTPException(404, "Apontamento não encontrado.")

        reg = db.query(EfdRegistro).filter(EfdRegistro.id == ap.registro_id).first()
        if not reg:
            raise HTTPException(404, "Registro original não encontrado.")

        versao = db.query(EfdVersao).filter(EfdVersao.id == ap.versao_id).first()
        if versao.status == "EXPORTADA":
            raise HTTPException(400, "⚠️ Versão bloqueada para alterações (EXPORTADA).")

        # 2. Execução da Exclusão Lógica (Overlay)
        # Removemos revisões de correção anteriores, pois a nota será deletada
        db.query(EfdRevisao).filter(
            EfdRevisao.versao_origem_id == ap.versao_id,
            EfdRevisao.registro_id == ap.registro_id
        ).delete(synchronize_session=False)

        nova_revisao = EfdRevisao(
            versao_origem_id=int(ap.versao_id),
            registro_id=int(ap.registro_id),
            reg=str(reg.reg),
            acao="DELETE",
            motivo_codigo="EXCLUSAO_USUARIO",
            apontamento_id=int(ap.id),
            revisao_json={
                "detalhe": "Usuário optou por excluir o registro para sanear a estrutura do arquivo.",
                "linha_referencia": int(reg.linha)
            }
        )
        db.add(nova_revisao)
        db.flush()

        # 3. O HARD RESET (Igual ao comportamento que você prefere)
        # Limpamos a tabela para que o scanner não trabalhe com lixo de memória
        db.execute(
            delete(EfdApontamento).where(EfdApontamento.versao_id == ap.versao_id)
        )

        # 4. REPROCESSAMENTO TOTAL
        # preservar_resolvidos=False força o sistema a revalidar tudo sob a nova ótica (sem a nota excluída)
        scan_res = FiscalScanner.scan_versao(
            db,
            versao_id=int(ap.versao_id),
            preservar_resolvidos=False,
            aplicar_revisoes=True
        )

        # Atualiza status para garantir que o fluxo de revisão continue
        versao.status = "EM_REVISAO"

        db.commit()

        return {
            "versao_id": ap.versao_id,
            "registro_excluido": reg.id,
            "status": "PROCESSADO",
            "scan_info": scan_res
        }

    except Exception as e:
        db.rollback()
        logger.exception("Falha no reprocessamento pós-exclusão")
        raise HTTPException(status_code=500, detail=f"Erro ao reprocessar: {str(e)}")