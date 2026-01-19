from fastapi import HTTPException
from sqlalchemy.orm import Session
from app.db.models import EfdVersao, EfdArquivo, EfdApontamento
from sqlalchemy import func, or_

class WorkflowService:

    @staticmethod
    def validar_versao(versao_id: int, db: Session) -> None:
        versao = db.get(EfdVersao, versao_id)
        if not versao:
            raise HTTPException(status_code=404, detail="Versão não encontrada")

        # 🔒 REGRA DE STATUS: só valida se estiver em revisão
        if versao.status != "EM_REVISAO":
            raise HTTPException(
                status_code=400,
                detail=f"Versão precisa estar EM_REVISAO para validar. Status atual: {versao.status}",
            )

        # 🔒 REGRA: não pode validar se houver pendências (False ou NULL)
        pendentes_erro = (
                             db.query(func.count(EfdApontamento.id))
                             .filter(EfdApontamento.versao_id == versao_id)
                             .filter(EfdApontamento.tipo == "ERRO")
                             .filter(or_(EfdApontamento.resolvido.is_(False), EfdApontamento.resolvido.is_(None)))
                             .scalar()
                         ) or 0

        if int(pendentes_erro) > 0:
            raise ValueError(
                f"Não é possível validar a versão. Existem {int(pendentes_erro)} apontamentos de ERRO pendentes."
            )

        versao.status = "VALIDADA"
        db.add(versao)
        db.commit()

    @staticmethod
    def marcar_exportada(versao_id: int, db: Session) -> None:
        versao = db.get(EfdVersao, versao_id)
        if not versao:
            raise HTTPException(status_code=404, detail="Versão não encontrada")

        if versao.status != "VALIDADA":
            raise HTTPException(status_code=400, detail="Somente versões validadas podem ser exportadas")

        versao.status = "EXPORTADA"
        db.add(versao)
