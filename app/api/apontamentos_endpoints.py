from __future__ import annotations
from app.fiscal.scanner import FiscalScanner
from typing import Optional, Literal, List, Dict, Any , Set
from fastapi import APIRouter, Depends, HTTPException, Query, status , Body
from app.db.session import get_db
from app.db.models import EfdVersao
from app.db.models import EfdApontamento, EfdRegistro
from sqlalchemy.orm import Session
from app.api.payloads import ReprocessarSelecaoPayload
from sqlalchemy import update, or_ , delete, case , Integer, select, func
from pydantic import BaseModel, Field
from sqlalchemy.sql import func



router = APIRouter(prefix="/workflow", tags=["Apontamentos"])


class ReprocessarPayload(BaseModel):
    preservar_resolvidos: bool = True
    motivo: Optional[str] = None

@router.post("/versao/{versao_id}/reprocessar", status_code=status.HTTP_200_OK)
def reprocessar_apontamentos(
    versao_id: int,
    payload: ReprocessarPayload = ReprocessarPayload(),
    db: Session = Depends(get_db),
) -> Dict[str, Any]:

    origem = db.query(EfdVersao).filter(EfdVersao.id == versao_id).first()
    if not origem:
        raise HTTPException(status_code=404, detail="Versão não encontrada")

    if origem.status == "EXPORTADA":
        raise HTTPException(status_code=400, detail="⚠️ Esta versão está EXPORTADA e é congelada. Reprocessar/editar está bloqueado.")

    try:
        # -----------------------------
        # Checkpoint 0 (antes)
        # -----------------------------
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

        # -----------------------------
        # 1) Status da versão
        # -----------------------------
        db.execute(
            update(EfdVersao)
            .where(EfdVersao.id == versao_id)
            .values(status="EM_REVISAO")
            .execution_options(synchronize_session=False)
        )

        # -----------------------------
        # 2) Hard reset: apaga tudo
        # -----------------------------
        db.execute(delete(EfdApontamento).where(EfdApontamento.versao_id == versao_id))

        # -----------------------------
        # 3) Scan (recria apontamentos)
        # -----------------------------
        res = FiscalScanner.scan_versao(
            db,
            versao_id=versao_id,
            preservar_resolvidos=False,
        )

        # -----------------------------
        # 4) Regra de sistema: após reprocessar, NADA fica resolvido
        #    (cinto e suspensório, caso alguma regra/mapper tente setar True)
        # -----------------------------
        db.execute(
            update(EfdApontamento)
            .where(EfdApontamento.versao_id == versao_id)
            .values(resolvido=False)
            .execution_options(synchronize_session=False)
        )

        # ✅ persiste tudo
        db.commit()

        # -----------------------------
        # Checkpoint final (depois do commit)
        # -----------------------------
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

        # Guard rail: não pode sobrar resolvido
        if int(after_res) > 0:
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
            raise HTTPException(
                status_code=400,
                detail=f"BUG: ainda existem resolvidos após hard reset. Ex IDs: {ids}",
            )

        return {
            "versao_id": versao_id,
            "before_total": int(before_total),
            "before_resolvidos": int(before_res),
            "after_total": int(after_total),
            "after_resolvidos": int(after_res),
            "scan_result": res,
            "message": "Reprocessamento TOTAL: apontamentos recriados e forçados para pendente.",
        }

    except HTTPException:
        db.rollback()
        raise
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=400, detail=str(e))



@router.get("/versao/{versao_id}/apontamentos/debug")
def debug_apontamentos(versao_id: int, db: Session = Depends(get_db)):
    rows = db.query(
        EfdApontamento.id,
        EfdApontamento.tipo,
        EfdApontamento.resolvido,
    ).filter(EfdApontamento.versao_id == versao_id).order_by(EfdApontamento.id.desc()).limit(50).all()

    return [{"id": r[0], "tipo": r[1], "resolvido": bool(r[2])} for r in rows]

@router.post("/workflow/versao/{versao_id}/reprocessar_selecao")
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
        q = (
            db.query(EfdApontamento, EfdRegistro)
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
        for a, r in rows:
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

class ApontamentosBatchPayload(BaseModel):
    versao_id: int
    to_resolver: List[int] = Field(default_factory=list)
    to_reabrir: List[int] = Field(default_factory=list)


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
    try:
        ap = db.query(EfdApontamento).filter(EfdApontamento.id == apontamento_id).first()
        if not ap:
            raise HTTPException(status_code=404, detail="Apontamento não encontrado")

        ap.resolvido = True
        db.add(ap)
        db.commit()

        return {
            "id": int(ap.id),
            "versao_id": int(ap.versao_id),
            "resolvido": True,
            "status": "Resolvido",
        }


    except HTTPException:
        db.rollback()
        raise
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=400, detail=str(e))


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