from __future__ import annotations
import csv
from io import StringIO
from sqlalchemy.orm import Session
from app.db.models import EfdApontamento, EfdRegistro
from sqlalchemy import case


class ApontamentosExportService:
    @staticmethod
    def exportar_csv(db: Session, *, versao_id: int) -> str:
        """
        Gera CSV dos apontamentos da versão.
        Retorna CSV como string (o endpoint converte pra bytes com utf-8-sig).
        """

        rows = (
            db.query(EfdApontamento, EfdRegistro)
            .outerjoin(EfdRegistro, EfdRegistro.id == EfdApontamento.registro_id)
            .filter(EfdApontamento.versao_id == versao_id)
            .order_by(
                case((EfdRegistro.linha.is_(None), 1), else_=0).asc(),  # NULL por último
                EfdRegistro.linha.asc(),
                EfdApontamento.id.asc(),
            )
            .all()
        )

        output = StringIO()
        writer = csv.writer(
            output,
            delimiter=";",
            quoting=csv.QUOTE_ALL,   # ✅ Excel-proof (não quebra coluna por ; , aspas etc.)
            lineterminator="\n",
        )

        writer.writerow([
            "linha",
            "registro",
            "tipo",
            "codigo",
            "descricao",
            "impacto_financeiro",
            "resolvido",
        ])

        for a, r in rows:
            linha = r.linha if r else ""
            reg = r.reg if r else ""

            desc = (a.descricao or "")
            desc = desc.replace("\r", " ").replace("\n", " ").strip()

            impacto = ""
            if a.impacto_financeiro is not None:
                # Excel BR: vírgula decimal (se preferir ponto, remova o replace)
                impacto = f"{float(a.impacto_financeiro):.2f}".replace(".", ",")

            writer.writerow([
                linha,
                reg,
                a.tipo or "",
                a.codigo or "",
                desc,
                impacto,
                "SIM" if a.resolvido else "NAO",
            ])

        return output.getvalue()

