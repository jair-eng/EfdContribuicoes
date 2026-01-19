from __future__ import annotations

from sqlalchemy.orm import Session
from sqlalchemy import func

from app.db.models import EfdVersao, EfdRegistro


class RetificacaoService:
    @staticmethod
    def criar_retificacao(db: Session, *, versao_id: int) -> dict:
        origem = db.query(EfdVersao).filter(EfdVersao.id == versao_id).first()
        if not origem:
            raise ValueError("Versão não encontrada")

        if origem.status != "EXPORTADA":
            raise ValueError("Somente versões EXPORTADAS podem ser retificadas")

        prox_numero = (
            db.query(func.max(EfdVersao.numero))
            .filter(EfdVersao.arquivo_id == origem.arquivo_id)
            .scalar()
        ) or 0
        prox_numero = int(prox_numero) + 1

        nova = EfdVersao(
            arquivo_id=int(origem.arquivo_id),
            numero=prox_numero,
            status="GERADA",
            # se você adicionar o FK retifica_de_versao_id, preenche aqui:
            retifica_de_versao_id=int(origem.id),
        )
        db.add(nova)
        db.flush()

        regs_origem = (
            db.query(EfdRegistro)
            .filter(EfdRegistro.versao_id == origem.id)
            .order_by(EfdRegistro.linha.asc())
            .all()
        )

        buffer = []
        for r in regs_origem:
            buffer.append(
                EfdRegistro(
                    versao_id=int(nova.id),
                    reg=r.reg,
                    linha=r.linha,
                    conteudo_json=r.conteudo_json,
                )
            )

        if buffer:
            db.bulk_save_objects(buffer)

        return {
            "versao_origem_id": int(origem.id),
            "nova_versao_id": int(nova.id),
            "novo_numero": int(nova.numero),
            "status": nova.status,
            "total_registros": int(len(buffer)),
        }
