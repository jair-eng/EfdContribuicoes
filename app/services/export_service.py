from __future__ import annotations
from sqlalchemy.orm import Session
from app.db.models import EfdRegistro, EfdVersao, EfdArquivo
from app.db.models.efd_revisao import EfdRevisao
from app.sped.writer import gerar_sped
from app.services.workflow_service import WorkflowService
from app.services.revision_apply import aplicar_revisoes


def exportar_sped(*, versao_id: int, caminho_saida: str, db: Session) -> str:
    versao = db.get(EfdVersao, versao_id)


    if not versao:
        raise ValueError("Versão não encontrada")



    status_atual = getattr(versao, "status", None)

    # regra refinada:
    if status_atual not in ("VALIDADA", "EXPORTADA"):
        raise ValueError(
            f"Somente versões VALIDADAS ou EXPORTADAS podem ser exportadas (status atual: {status_atual})"
        )

    # ✅ se for revisada, exporta base da origem (imutável) + revisões
    retifica_de = getattr(versao, "retifica_de_versao_id", None)
    versao_origem_id = int(retifica_de) if retifica_de else int(versao.id)

    registros = (
        db.query(EfdRegistro)
        .filter(EfdRegistro.versao_id == versao_origem_id)
        .order_by(EfdRegistro.linha.asc())
        .all()
    )

    arquivo = db.get(EfdArquivo, int(versao.arquivo_id))
    line_ending = getattr(arquivo, "line_ending", "LF") if arquivo else "LF"
    newline = "\r\n" if str(line_ending).upper() == "CRLF" else "\n"

    # ✅ aplica revisões (opção 2: por versao_origem_id)
    revisoes = (
        db.query(EfdRevisao)
        .filter(EfdRevisao.versao_origem_id == int(versao_origem_id))
        .order_by(EfdRevisao.id.asc())
        .all()
    )

    if revisoes:
        registros = aplicar_revisoes(registros, revisoes)  # nova função (REPLACE + INSERT_AFTER)

    gerar_sped(registros, caminho_saida, newline=newline)
    print("[EXPORT] versao_id=", versao_id, "caminho_saida=", caminho_saida)
    print("[EXPORT] gerar_sped ref=", gerar_sped, "module=", getattr(gerar_sped, "__module__", "?"))

    if status_atual == "VALIDADA":
        WorkflowService.marcar_exportada(versao_id, db)

    return caminho_saida