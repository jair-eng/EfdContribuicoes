from __future__ import annotations

from app.db.models.efd_revisao import EfdRevisao
from app.fiscal.scanner import FiscalScanner
from typing import Optional, Literal, List, Dict, Any , Set
from fastapi import APIRouter, Depends, HTTPException, Query, status , Body
from app.db.session import get_db
from app.db.models import EfdVersao, EfdArquivo
from app.db.models import EfdApontamento, EfdRegistro
from sqlalchemy.orm import Session
from app.api.payloads import ReprocessarSelecaoPayload
from sqlalchemy import update, or_ , delete, case , Integer, select, func, exists
from pydantic import BaseModel, Field
import time
import traceback
from typing import Any, Dict
from fastapi import HTTPException, status


from app.schemas.workflow import AplicarRevisaoPayload, ApontamentosBatchPayload
from app.services.apontamento_service import ApontamentoService
from app.services.revision_service import RevisionService

router = APIRouter(prefix="/workflow", tags=["Apontamentos"])


class ReprocessarPayload(BaseModel):
    preservar_resolvidos: bool = True
    motivo: Optional[str] = None
    aplicar_revisoes: bool = True

@router.post("/versao/{versao_id}/reprocessar", status_code=status.HTTP_200_OK)
def reprocessar_apontamentos(
    versao_id: int,
    payload: ReprocessarPayload = ReprocessarPayload(),
    db: Session = Depends(get_db),
) -> Dict[str, Any]:

    def dbg(msg: str) -> None:
        print(f"[REPROCESSAR v{versao_id}] {msg}", flush=True)

    t0 = time.time()
    step = "INIT"

    dbg(">>> INICIO")

    origem = db.query(EfdVersao).filter(EfdVersao.id == versao_id).first()
    if not origem:
        dbg("ERRO: Versão não encontrada")
        raise HTTPException(status_code=404, detail="Versão não encontrada")

    dbg(f"STATUS ATUAL = {origem.status}")

    if origem.status == "EXPORTADA":
        dbg("BLOQUEADO: versão EXPORTADA")
        raise HTTPException(
            status_code=400,
            detail="⚠️ Esta versão está EXPORTADA e é congelada. Reprocessar/editar está bloqueado.",
        )

    try:
        # -----------------------------
        # Checkpoint 0 (antes)
        # -----------------------------
        step = "BEFORE_COUNTS"
        before_total = (
            db.query(func.count(EfdApontamento.id))
            .filter(EfdApontamento.versao_id == versao_id)
            .scalar()
        ) or 0

        before_res = (
            db.query(func.count(EfdApontamento.id))
            .filter(
                EfdApontamento.versao_id == versao_id,
                EfdApontamento.resolvido.is_(True),
            )
            .scalar()
        ) or 0

        dbg(f"ANTES: total={int(before_total)} resolvidos={int(before_res)}")

        # -----------------------------
        # 1) Status da versão
        # -----------------------------
        step = "SET_STATUS"
        dbg("STEP SET_STATUS -> EM_REVISAO")
        db.execute(
            update(EfdVersao)
            .where(EfdVersao.id == versao_id)
            .values(status="EM_REVISAO")
            .execution_options(synchronize_session=False)
        )
        db.flush()
        dbg("OK SET_STATUS")

        # -----------------------------
        # 2) Hard reset: apaga tudo
        # -----------------------------
        step = "DELETE_APONTAMENTOS"
        dbg("STEP DELETE_APONTAMENTOS")
        db.execute(delete(EfdApontamento).where(EfdApontamento.versao_id == versao_id))
        db.flush()

        deleted_count = (
            db.query(func.count(EfdApontamento.id))
            .filter(EfdApontamento.versao_id == versao_id)
            .scalar()
        ) or 0
        dbg(f"APOS DELETE: total={int(deleted_count)} (esperado 0)")

        # -----------------------------
        # 3) Scan (recria apontamentos)
        # -----------------------------
        step = "SCAN"
        aplicar_revisoes = bool(getattr(payload, "aplicar_revisoes", True))
        preservar_resolvidos = bool(getattr(payload, "preservar_resolvidos", True))

        dbg(f"STEP SCAN (aplicar_revisoes={aplicar_revisoes}, preservar_resolvidos={preservar_resolvidos})")
        t_scan = time.time()

        versao = db.get(EfdVersao, int(versao_id))
        if not versao:
            raise HTTPException(status_code=404, detail="Versão não encontrada.")

        empresa_id = getattr(versao, "empresa_id", None)
        if empresa_id is None and getattr(versao, "arquivo_id", None):
            arquivo = db.get(EfdArquivo, int(versao.arquivo_id))
            empresa_id = getattr(arquivo, "empresa_id", None)

        if empresa_id is None:
            raise HTTPException(status_code=400, detail="Não foi possível resolver empresa_id para a versão.")

        # ✅ FIX: garantir empresa_id persistido na versão
        if getattr(versao, "empresa_id", None) is None:
            versao.empresa_id = int(empresa_id)
            db.add(versao)
            db.flush()

        # (opcional mas recomendado) gravar domínio da empresa
        if not (getattr(versao, "dominio", None) or "").strip():
            emp = getattr(getattr(versao, "arquivo", None), "empresa", None)
            dom_emp = (getattr(emp, "dominio", None) or "").strip().upper() if emp else ""
            if dom_emp:
                versao.dominio = dom_emp
                db.add(versao)
                db.flush()

        res = FiscalScanner.scan_versao(
            db,
            versao_id=int(versao_id),
            empresa_id=int(empresa_id),
            preservar_resolvidos=preservar_resolvidos,
            aplicar_revisoes=aplicar_revisoes,
        )

        dbg(f"OK SCAN em {time.time() - t_scan:.2f}s | scan_res={res}")

        # -----------------------------
        # 4) Força pendente
        # -----------------------------
        step = "FORCE_PENDENTE"
        dbg("STEP FORCE_PENDENTE (resolvido=False)")
        db.execute(
            update(EfdApontamento)
            .where(EfdApontamento.versao_id == versao_id)
            .values(resolvido=False)
            .execution_options(synchronize_session=False)
        )
        db.flush()
        dbg("OK FORCE_PENDENTE")

        # ✅ persiste tudo
        step = "COMMIT"
        dbg("STEP COMMIT")
        db.commit()
        dbg("OK COMMIT")

        # -----------------------------
        # Checkpoint final
        # -----------------------------
        step = "AFTER_COUNTS"
        after_total = (
            db.query(func.count(EfdApontamento.id))
            .filter(EfdApontamento.versao_id == versao_id)
            .scalar()
        ) or 0

        after_res = (
            db.query(func.count(EfdApontamento.id))
            .filter(
                EfdApontamento.versao_id == versao_id,
                EfdApontamento.resolvido.is_(True),
            )
            .scalar()
        ) or 0

        dbg(f"DEPOIS: total={int(after_total)} resolvidos={int(after_res)}")

        if int(after_res) > 0:
            step = "GUARD_RAIL_RESOLVIDOS"
            ids = [
                r[0]
                for r in (
                    db.query(EfdApontamento.id)
                    .filter(
                        EfdApontamento.versao_id == versao_id,
                        EfdApontamento.resolvido.is_(True),
                    )
                    .limit(20)
                    .all()
                )
            ]
            dbg(f"BUG: sobrou resolvido! ids={ids}")
            raise HTTPException(
                status_code=400,
                detail=f"BUG: ainda existem resolvidos após hard reset. Ex IDs: {ids}",
            )

        dbg(f">>> FIM OK em {time.time() - t0:.2f}s")

        return {
            "versao_id": versao_id,
            "before_total": int(before_total),
            "before_resolvidos": int(before_res),
            "after_total": int(after_total),
            "after_resolvidos": int(after_res),
            "scan_result": res,
            "message": "Reprocessamento TOTAL: apontamentos recriados e forçados para pendente.",
            "aplicar_revisoes": aplicar_revisoes,
            "preservar_resolvidos": preservar_resolvidos,
            "elapsed_s": round(time.time() - t0, 2),
        }

    except HTTPException as e:
        dbg(f"HTTPException step={step} status={e.status_code} detail={e.detail}")
        db.rollback()
        raise
    except Exception as e:
        dbg(f"EXCEPTION step={step} err={repr(e)}")
        dbg("TRACEBACK:\n" + traceback.format_exc())
        db.rollback()
        raise HTTPException(status_code=400, detail=f"BUG step={step}: {str(e)}")


@router.get("/versao/{versao_id}/apontamentos/debug")
def debug_apontamentos(versao_id: int, db: Session = Depends(get_db)):
    rows = db.query(
        EfdApontamento.id,
        EfdApontamento.tipo,
        EfdApontamento.resolvido,
    ).filter(EfdApontamento.versao_id == versao_id).order_by(EfdApontamento.id.desc()).limit(50).all()

    return [{"id": r[0], "tipo": r[1], "resolvido": bool(r[2])} for r in rows]

@router.post("/versao/{versao_id}/reprocessar_selecao")
def reprocessar_selecao(
    versao_id: int,
    payload: ReprocessarSelecaoPayload,
    db: Session = Depends(get_db),
):
    versao = db.query(EfdVersao).filter(EfdVersao.id == versao_id).first()
    if not versao:
        raise HTTPException(status_code=404, detail="Versão não encontrada")

    if versao.status == "EXPORTADA":
        raise HTTPException(status_code=400, detail="Versão EXPORTADA é congelada.")

    ids = list({int(x) for x in payload.apontamento_ids})

    # valida pertencimento
    found_ids = {
        i for (i,) in db.query(EfdApontamento.id)
        .filter(EfdApontamento.versao_id == versao_id, EfdApontamento.id.in_(ids))
        .all()
    }
    missing = [i for i in ids if i not in found_ids]
    if missing:
        raise HTTPException(status_code=400, detail=f"IDs inválidos nesta versão: {missing[:20]}")

    try:
        # reabre selecionados
        (db.query(EfdApontamento)
           .filter(EfdApontamento.versao_id == versao_id, EfdApontamento.id.in_(ids))
           .update({EfdApontamento.resolvido: False}, synchronize_session=False))
        db.flush()

        # reprocessa versão preservando os demais resolvidos
        res = FiscalScanner.scan_versao(db, versao_id=versao_id, preservar_resolvidos=True)

        pendentes = (db.query(func.count(EfdApontamento.id))
                       .filter(EfdApontamento.versao_id == versao_id, EfdApontamento.resolvido == False)  # noqa
                       .scalar()) or 0

        if versao.status == "VALIDADA" and pendentes > 0:
            versao.status = "EM_REVISAO"

        db.commit()
        return {"versao_id": versao_id, "reabertos": len(ids), "pendentes": pendentes, **(res or {})}

    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=400, detail=str(e))


@router.get(
    "/versao/{versao_id}/apontamentos",
    status_code=status.HTTP_200_OK,

)
def listar_apontamentos(
        versao_id: int,
        db: Session = Depends(get_db),
        tipo: Optional[Literal["ERRO", "OPORTUNIDADE"]] = Query(default=None),
        resolvido: Optional[bool] = Query(default=None),
        bucket: Optional[Literal["ALTA_CHANCE", "REVISAR", "BAIXA"]] = Query(default=None),
        cenario: Optional[Literal["SEM_RESSARC", "COM_RESSARC"]] = Query(default=None),
        limit: int = Query(default=50, ge=1, le=500),
        offset: int = Query(default=0, ge=0),
) -> Dict[str, Any]:

    """
    Lista apontamentos da versão.
    Filtros:
      - tipo: ERRO | OPORTUNIDADE
      - resolvido: true | false
    Paginação:
      - limit, offset
    Retorna também dados do registro associado (linha/reg).
    """
    try:
        tem_revisao_sq = exists().where(
            (EfdRevisao.versao_origem_id == versao_id) &
            (EfdRevisao.registro_id == EfdApontamento.registro_id)
        )

        last_revisao_id_sq = (
            select(EfdRevisao.id)
            .where(
                (EfdRevisao.versao_origem_id == versao_id) &
                (EfdRevisao.registro_id == EfdApontamento.registro_id)
            )
            .order_by(EfdRevisao.id.desc())
            .limit(1)
            .scalar_subquery()
        )

        last_versao_revisada_id_sq = (
            select(EfdRevisao.versao_revisada_id)
            .where(
                (EfdRevisao.versao_origem_id == versao_id) &
                (EfdRevisao.registro_id == EfdApontamento.registro_id)
            )
            .order_by(EfdRevisao.id.desc())
            .limit(1)
            .scalar_subquery()
        )

        q = (
            db.query(
                EfdApontamento,
                EfdRegistro,
                tem_revisao_sq.label("tem_revisao"),
                last_revisao_id_sq.label("revisao_id"),
                last_versao_revisada_id_sq.label("versao_revisada_id"),
            )
            .outerjoin(EfdRegistro, EfdRegistro.id == EfdApontamento.registro_id)
            .filter(EfdApontamento.versao_id == versao_id)
        )

        if tipo is not None:
            q = q.filter(EfdApontamento.tipo == tipo)

        if resolvido is not None:
            if resolvido is True:
                q = q.filter(EfdApontamento.resolvido.is_(True))
            else:
                q = q.filter(or_(EfdApontamento.resolvido.is_(False), EfdApontamento.resolvido.is_(None)))

        total = q.count()

        # -------- filtros via meta_json --------
        if bucket is not None:
            q = q.filter(
                func.JSON_UNQUOTE(
                    func.JSON_EXTRACT(EfdApontamento.meta_json, "$.bucket")
                ) == bucket
            )

        if cenario is not None:
            q = q.filter(
                func.JSON_UNQUOTE(
                    func.JSON_EXTRACT(EfdApontamento.meta_json, "$.cenario")
                ) == cenario
            )


        prioridade_ordem = case(
            (EfdApontamento.prioridade == "ALTA", 1),
            (EfdApontamento.prioridade == "MEDIA", 2),
            (EfdApontamento.prioridade == "BAIXA", 3),
            else_=4,
        )

        # substitui NULLS LAST
        impacto_nulls_last = case(
            (EfdApontamento.impacto_financeiro.is_(None), 1),
            else_=0,
        )

        # score no meta_json (NULLs last)
        score_expr = func.JSON_UNQUOTE(
            func.JSON_EXTRACT(EfdApontamento.meta_json, "$.score")
        )
        score_int = func.CAST(score_expr, Integer)

        score_nulls_last = case(
            (score_expr.is_(None), 1),
            else_=0,
        )


        rows = (
            q.order_by(
                EfdApontamento.resolvido.asc(),  # pendentes primeiro
                prioridade_ordem.asc(),  # ALTA -> MEDIA -> BAIXA
                score_nulls_last.asc(),  # score NULL por último
                score_int.desc(),  # score maior primeiro
                impacto_nulls_last.asc(),  # NULL por último
                EfdApontamento.impacto_financeiro.desc(),  # impacto maior primeiro
                EfdRegistro.linha.asc(),  # linha crescente
                EfdApontamento.id.desc(),  # desempate
            )
            .offset(offset)
            .limit(limit)
            .all()
        )

        itens: List[Dict[str, Any]] = []
        for a, r, tem_revisao, revisao_id, versao_revisada_id in rows:
            itens.append(
                {
                    "id": int(a.id),
                    "versao_id": int(a.versao_id),
                    "registro_id": int(a.registro_id),
                    "tipo": a.tipo,
                    "codigo": a.codigo,
                    "descricao": a.descricao,
                    "impacto_financeiro": float(a.impacto_financeiro) if a.impacto_financeiro is not None else None,
                    "prioridade": getattr(a, "prioridade", None),
                    "resolvido": bool(a.resolvido) if a.resolvido is not None else False,
                    # ✅ expose meta completo pro front
                    "meta": dict(a.meta_json or {}),
                    "score": (a.meta_json or {}).get("score") if getattr(a, "meta_json", None) else None,
                    "bucket": (a.meta_json or {}).get("bucket") if getattr(a, "meta_json", None) else None,
                    "cenario": (a.meta_json or {}).get("cenario") if getattr(a, "meta_json", None) else None,
                    "registro": (
                        {"linha": int(r.linha), "reg": str(r.reg)}
                        if r is not None
                        else None
                    ),
                    "tem_revisao": bool(tem_revisao),
                    "revisao_id": int(revisao_id) if revisao_id is not None else None,
                    "versao_revisada_id": int(versao_revisada_id) if versao_revisada_id is not None else None,
                }
            )

        return {
            "versao_id": versao_id,
            "total": int(total),
            "limit": int(limit),
            "offset": int(offset),
            "items": itens,
        }


    except Exception as e:

        raise HTTPException(status_code=500, detail=f"Erro interno ao listar apontamentos: {e}")



@router.patch(
    "/apontamento/{apontamento_id}/resolver",
    status_code=status.HTTP_200_OK,
)
def resolver_apontamento(
    apontamento_id: int,
    db: Session = Depends(get_db),
) -> Dict[str, Any]:
    """
    Marca um apontamento como resolvido.
    """
    ap = (
        db.query(EfdApontamento)
        .filter(EfdApontamento.id == apontamento_id)
        .first()
    )

    if not ap:
        raise HTTPException(status_code=404, detail="Apontamento não encontrado")

    # o context manager cuida de commit/rollback
    with db.begin():
        ap.resolvido = True
        db.add(ap)
        db.flush()

    return {
        "id": int(ap.id),
        "versao_id": int(ap.versao_id),
        "resolvido": True,
        "status": "Resolvido",
    }



@router.patch(
    "/apontamento/{apontamento_id}/reabrir",
    status_code=status.HTTP_200_OK,
)
def reabrir_apontamento(
        apontamento_id: int,
        db: Session = Depends(get_db),
) -> Dict[str, Any]:
    """
    Marca um apontamento como NÃO resolvido (reabre).
    """
    try:
        ap = db.query(EfdApontamento).filter(EfdApontamento.id == apontamento_id).first()
        if not ap:
            raise HTTPException(status_code=404, detail="Apontamento não encontrado")

        ap.resolvido = False
        db.add(ap)
        db.commit()

        return {
            "id": int(ap.id),
            "versao_id": int(ap.versao_id),
            "resolvido": False,
            "status": "Pendente",
        }

    except HTTPException:
        db.rollback()
        raise
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=400, detail=str(e))


@router.patch("/apontamento/batch", status_code=status.HTTP_200_OK)
def aplicar_apontamentos_em_lote(
    payload: ApontamentosBatchPayload,
    db: Session = Depends(get_db),
) -> Dict[str, Any]:
    """
    Aplica resolver/reabrir em lote (performático) e retorna auditoria do que foi aplicado.
    Reabrir ganha se um ID estiver em ambos.
    """
    try:
        versao_id = int(payload.versao_id)

        to_resolver: Set[int] = set(map(int, payload.to_resolver or []))
        to_reabrir: Set[int] = set(map(int, payload.to_reabrir or []))

        # reabrir ganha
        to_resolver -= to_reabrir

        requested = len(to_resolver) + len(to_reabrir)

        updated_resolver = 0
        updated_reabrir = 0

        if to_resolver:
            res = db.execute(
                update(EfdApontamento)
                .where(
                    EfdApontamento.versao_id == versao_id,
                    EfdApontamento.id.in_(to_resolver),
                )
                .values(resolvido=True)
            )
            updated_resolver = int(res.rowcount or 0)

        if to_reabrir:
            res = db.execute(
                update(EfdApontamento)
                .where(
                    EfdApontamento.versao_id == versao_id,
                    EfdApontamento.id.in_(to_reabrir),
                )
                .values(resolvido=False)
            )
            updated_reabrir = int(res.rowcount or 0)

        db.commit()

        pendentes = db.execute(
            select(func.count())
            .select_from(EfdApontamento)
            .where(
                EfdApontamento.versao_id == versao_id,
                (EfdApontamento.resolvido.is_(False) | EfdApontamento.resolvido.is_(None)),
            )
        ).scalar_one()

        updated_total = updated_resolver + updated_reabrir

        return {
            "versao_id": versao_id,
            "requested": requested,
            "to_resolver": len(to_resolver),
            "to_reabrir": len(to_reabrir),
            "updated_resolver": updated_resolver,
            "updated_reabrir": updated_reabrir,
            "updated_total": updated_total,
            "nao_encontrados_ou_outra_versao": int(requested - updated_total),
            "pendentes_restantes": int(pendentes),
            "status": "OK",
        }

    except HTTPException:
        db.rollback()
        raise
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=400, detail=str(e))



@router.post("/versao/{versao_id}/aplicar-revisao",
    status_code=status.HTTP_201_CREATED,
)
def aplicar_revisao_apontamento(
    apontamento_id: int,
    payload: AplicarRevisaoPayload,
    db: Session = Depends(get_db),
):
    """
    Cria uma revisão (REPLACE_LINE) ligada a um apontamento.
    - cria (ou reutiliza) a versão revisada automaticamente
    - não altera a versão original
    """
    try:
        rev = RevisionService.criar_revisao_replace_line(
            db,
            apontamento_id=int(apontamento_id),
            linha_nova=str(payload.linha_nova),
            motivo_codigo=payload.motivo_codigo,
        )
        db.commit()
        return {
            "revisao_id": int(rev.id),
            "versao_origem_id": int(rev.versao_origem_id),
            "versao_revisada_id": int(rev.versao_revisada_id),
            "registro_id": int(rev.registro_id),
            "acao": str(rev.acao),
            "linha_num": rev.revisao_json.get("linha_num"),
            "motivo_codigo": rev.motivo_codigo,
        }
    except ValueError as e:
        db.rollback()
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=str(e))

@router.patch("/versao/{versao_id}/resolver_todos", status_code=200)
def resolver_todos_pendentes(versao_id: int, db: Session = Depends(get_db)):

    with db.begin():
        r = ApontamentoService.resolver_todos_pendentes_por_versao(db, versao_id=int(versao_id))

    return {
        "versao_id": r.versao_id,
        "updated_total": r.updated_total,
        "pendentes_restantes": r.pendentes_restantes,
        "status": "OK",
    }