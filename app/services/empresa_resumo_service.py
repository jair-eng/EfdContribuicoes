from sqlalchemy.orm import Session
from sqlalchemy import func, case, or_
from app.db.models import Empresa, EfdArquivo, EfdVersao, EfdApontamento

class EmpresaResumoService:
    @staticmethod
    def gerar_resumo(db: Session, *, empresa_id: int) -> dict:
        empresa = db.query(Empresa).filter(Empresa.id == empresa_id).first()
        if not empresa:
            raise ValueError("Empresa não encontrada")

        # -----------------------------
        # Arquivos / versões (contadores)
        # -----------------------------
        arquivos_total = (
            db.query(func.count(EfdArquivo.id))
            .filter(EfdArquivo.empresa_id == empresa_id)
            .scalar()
        ) or 0

        versoes = (
            db.query(EfdVersao.id, EfdVersao.status)
            .join(EfdArquivo, EfdArquivo.id == EfdVersao.arquivo_id)
            .filter(EfdArquivo.empresa_id == empresa_id)
            .all()
        )

        status_count = {"VALIDADA": 0, "EM_REVISAO": 0, "GERADA": 0, "EXPORTADA": 0}
        versoes_validas = []

        for v_id, status in versoes:
            if status in status_count:
                status_count[status] += 1
            if status == "VALIDADA":
                versoes_validas.append(int(v_id))

        # -----------------------------
        # Apontamentos agregados (empresa inteira)
        # - considera pendente = False ou NULL (mais robusto)
        # -----------------------------
        pendente_expr_emp = or_(EfdApontamento.resolvido.is_(False), EfdApontamento.resolvido.is_(None))
        resolvido_expr_emp = EfdApontamento.resolvido.is_(True)

        ap = (
            db.query(
                func.count(EfdApontamento.id).label("total"),
                func.sum(case((pendente_expr_emp, 1), else_=0)).label("pendentes"),
                func.sum(case((resolvido_expr_emp, 1), else_=0)).label("resolvidos"),
            )
            .join(EfdVersao, EfdVersao.id == EfdApontamento.versao_id)
            .join(EfdArquivo, EfdArquivo.id == EfdVersao.arquivo_id)
            .filter(EfdArquivo.empresa_id == empresa_id)
            .one()
        )

        # -----------------------------
        # ✅ NOVO: lista robusta de versões com mini-resumo (1 query)
        # -----------------------------
        pendente_expr = or_(EfdApontamento.resolvido.is_(False), EfdApontamento.resolvido.is_(None))

        rows = (
            db.query(
                EfdVersao.id.label("versao_id"),
                EfdVersao.numero.label("numero"),
                EfdVersao.status.label("status"),

                EfdArquivo.id.label("arquivo_id"),
                EfdArquivo.nome_arquivo.label("nome_arquivo"),
                EfdArquivo.periodo.label("periodo"),

                func.count(EfdApontamento.id).label("ap_total"),
                func.sum(case((pendente_expr, 1), else_=0)).label("pendentes"),
                func.sum(case((pendente_expr & (EfdApontamento.prioridade == "ALTA"), 1), else_=0)).label("pend_alta"),
                func.sum(case((pendente_expr & (EfdApontamento.prioridade == "MEDIA"), 1), else_=0)).label("pend_media"),
                func.sum(case((pendente_expr & (EfdApontamento.prioridade == "BAIXA"), 1), else_=0)).label("pend_baixa"),

                func.sum(
                    case(
                        (pendente_expr & EfdApontamento.impacto_financeiro.isnot(None), EfdApontamento.impacto_financeiro),
                        else_=0,
                    )
                ).label("impacto_estimado_total"),
            )
            .join(EfdArquivo, EfdArquivo.id == EfdVersao.arquivo_id)
            .outerjoin(EfdApontamento, EfdApontamento.versao_id == EfdVersao.id)
            .filter(EfdArquivo.empresa_id == empresa_id)
            .group_by(
                EfdVersao.id, EfdVersao.numero, EfdVersao.status,
                EfdArquivo.id, EfdArquivo.nome_arquivo, EfdArquivo.periodo,
            )
            .order_by(EfdArquivo.periodo.desc(), EfdVersao.id.desc())
            .all()
        )

        versoes_items = []
        for r in rows:
            versoes_items.append({
                "versao_id": int(r.versao_id),
                "numero": int(r.numero or 1),
                "status": r.status,

                "arquivo_id": int(r.arquivo_id),
                "nome_arquivo": r.nome_arquivo,
                "periodo": r.periodo,

                "apontamentos_total": int(r.ap_total or 0),
                "pendentes": int(r.pendentes or 0),
                "pendentes_por_prioridade": {
                    "alta": int(r.pend_alta or 0),
                    "media": int(r.pend_media or 0),
                    "baixa": int(r.pend_baixa or 0),
                },
                "impacto_estimado_total": float(r.impacto_estimado_total or 0),
            })

        # -----------------------------
        # Retorno (mantém compatibilidade + adiciona itens)
        # -----------------------------
        return {
            "empresa_id": int(empresa_id),
            "cnpj": empresa.cnpj,
            "razao_social": empresa.razao_social,

            "arquivos": {
                "total": int(arquivos_total or 0),
                "validados": int(status_count.get("VALIDADA", 0)),
                "em_revisao": int(status_count.get("EM_REVISAO", 0)),
                "gerados": int(status_count.get("GERADA", 0)),
                "exportados": int(status_count.get("EXPORTADA", 0)),
            },

            "apontamentos": {
                "total": int(ap.total or 0),
                "pendentes": int(ap.pendentes or 0),
                "resolvidos": int(ap.resolvidos or 0),
            },

            "versoes_validas": versoes_validas,

            # ✅ NOVO
            "versoes_total": int(len(versoes_items)),
            "versoes_items": versoes_items,
        }
