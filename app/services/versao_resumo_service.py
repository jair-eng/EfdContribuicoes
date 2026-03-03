from __future__ import annotations
from sqlalchemy.orm import Session
from sqlalchemy import func, case, or_
from app.db.models import (EfdVersao , EfdRegistro, EfdApontamento, EfdArquivo , Empresa)
from fastapi import Depends
from sqlalchemy.orm import Session
from app.db.session import get_db   # ou onde seu get_db está definido


class VersaoResumoService:
    @staticmethod
    def gerar_resumo(db: Session, *, versao_id: int) -> dict:
        versao = db.query(EfdVersao).filter(EfdVersao.id == versao_id).first()
        if not versao:
            raise ValueError("Versão não encontrada")

        # --- carrega contexto (empresa/arquivo) ---
        arquivo = None
        empresa = None

        if getattr(versao, "arquivo_id", None) is not None:
            arquivo = db.query(EfdArquivo).filter(EfdArquivo.id == versao.arquivo_id).first()

        if arquivo and getattr(arquivo, "empresa_id", None) is not None:
            empresa = db.query(Empresa).filter(Empresa.id == arquivo.empresa_id).first()

        total_registros = (
            db.query(func.count(EfdRegistro.id))
            .filter(EfdRegistro.versao_id == versao_id)
            .scalar()
        )

        pendente_expr = or_(EfdApontamento.resolvido.is_(False), EfdApontamento.resolvido.is_(None))
        resolvido_expr = EfdApontamento.resolvido.is_(True)

        agg = (
            db.query(
                func.count(EfdApontamento.id).label("total"),

                # pendentes = FALSE ou NULL
                func.sum(case((pendente_expr, 1), else_=0)).label("pendentes"),

                # resolvidos = TRUE
                func.sum(case((resolvido_expr, 1), else_=0)).label("resolvidos"),

                # tipos (total por tipo, independente de resolvido)
                func.sum(case((EfdApontamento.tipo == "ERRO", 1), else_=0)).label("erros"),
                func.sum(case((EfdApontamento.tipo == "OPORTUNIDADE", 1), else_=0)).label("oportunidades"),

                # impacto estimado (somente pendentes + não nulo)
                func.sum(
                    case(
                        (
                            pendente_expr & EfdApontamento.impacto_financeiro.isnot(None),
                            EfdApontamento.impacto_financeiro,
                        ),
                        else_=0,
                    )
                ).label("impacto_estimado_total"),

                # pendentes por prioridade
                func.sum(case((pendente_expr & (EfdApontamento.prioridade == "ALTA"), 1), else_=0)).label("pendentes_alta"),
                func.sum(case((pendente_expr & (EfdApontamento.prioridade == "MEDIA"), 1), else_=0)).label("pendentes_media"),
                func.sum(case((pendente_expr & (EfdApontamento.prioridade == "BAIXA"), 1), else_=0)).label("pendentes_baixa"),
            )
            .filter(EfdApontamento.versao_id == versao_id)
            .one_or_none()

        )
        if not agg:
            class _AggZero:
                total = 0
                pendentes = 0
                resolvidos = 0
                erros = 0
                oportunidades = 0
                pendentes_alta = 0
                pendentes_media = 0
                pendentes_baixa = 0
                impacto_estimado_total = 0

            agg = _AggZero()

        print("DEBUG total_registros =", total_registros, flush=True)
        print("DEBUG agg =", agg, type(agg), flush=True)
        return {
            "versao_id": int(versao_id),
            "status": versao.status,

            # ✅ NOVO: contexto no topo
            "empresa": {
                "id": int(empresa.id) if empresa else None,
                "cnpj": empresa.cnpj if empresa else None,
                "razao_social": empresa.razao_social if empresa else None,
            },
            "arquivo": {
                "id": int(arquivo.id) if arquivo else None,
                "nome_arquivo": getattr(arquivo, "nome_arquivo", None) if arquivo else None,
                "periodo": getattr(arquivo, "periodo", None) if arquivo else None,
                "line_ending": getattr(arquivo, "line_ending", None) if arquivo else None,
            },
            "versao": {
                "id": int(versao.id),
                "numero": int(getattr(versao, "numero", 1)),
                "status": versao.status,
            },

            # mantém o que já existia
            "total_registros": int(total_registros or 0),
            "apontamentos": {
                "total": int(agg.total or 0),
                "pendentes": int(agg.pendentes or 0),
                "resolvidos": int(agg.resolvidos or 0),
                "erros": int(agg.erros or 0),
                "oportunidades": int(agg.oportunidades or 0),
                "pendentes_por_prioridade": {
                    "alta": int(agg.pendentes_alta or 0),
                    "media": int(agg.pendentes_media or 0),
                    "baixa": int(agg.pendentes_baixa or 0),
                },
            },
            "impacto_estimado_total": float(agg.impacto_estimado_total or 0),
        }