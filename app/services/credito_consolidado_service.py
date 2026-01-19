from sqlalchemy import func
from app.db.models import (
    EfdRegistro,
    EfdVersao,
    EfdArquivo
)

class CreditoConsolidadoService:

    @staticmethod
    def consolidar_por_empresa(empresa_id: int, db):
        """
        Consolida TODOS os créditos válidos da empresa
        (somente versões VALIDADA ou EXPORTADA)
        """

        query = (
            db.query(
                EfdRegistro.tipo_credito,
                func.sum(EfdRegistro.base_credito).label("base"),
                func.sum(EfdRegistro.valor_credito).label("credito")
            )
            .join(EfdVersao, EfdVersao.id == EfdRegistro.versao_id)
            .join(EfdArquivo, EfdArquivo.id == EfdVersao.arquivo_id)
            .filter(EfdArquivo.empresa_id == empresa_id)
            .filter(EfdVersao.status.in_(["VALIDADA", "EXPORTADA"]))
            .group_by(EfdRegistro.tipo_credito)
        )

        return query.all()

    @staticmethod
    def consolidar_por_periodo(empresa_id, inicio, fim, db):
        """
        Consolidação por período (ex: últimos 5 anos)
        """

        return (
            db.query(
                EfdArquivo.periodo,
                func.sum(EfdRegistro.valor_credito).label("credito")
            )
            .join(EfdVersao)
            .join(EfdRegistro)
            .filter(EfdArquivo.empresa_id == empresa_id)
            .filter(EfdArquivo.periodo.between(inicio, fim))
            .filter(EfdVersao.status.in_(["VALIDADA", "EXPORTADA"]))
            .group_by(EfdArquivo.periodo)
            .order_by(EfdArquivo.periodo)
            .all()
        )
