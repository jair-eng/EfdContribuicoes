from __future__ import annotations
from sqlalchemy.orm import Session
from app.db.models import EfdRegistro, EfdVersao, EfdArquivo
from app.sped.writer import gerar_sped
from app.services.workflow_service import WorkflowService


def exportar_sped(*, versao_id: int, caminho_saida: str, db: Session) -> str:
    versao = db.get(EfdVersao, versao_id)
    if not versao:
        raise ValueError("Versão não encontrada")

    status_atual = getattr(versao, "status", None)

    # ✅ regra refinada:
    # - VALIDADA: pode exportar (e será marcada EXPORTADA)
    # - EXPORTADA: pode re-gerar o arquivo (re-download / arquivo apagado)
    if status_atual not in ("VALIDADA", "EXPORTADA"):
        raise ValueError(
            f"Somente versões VALIDADAS ou EXPORTADAS podem ser exportadas (status atual: {status_atual})"
        )

    registros = (
        db.query(EfdRegistro)
        .filter(EfdRegistro.versao_id == versao_id)
        .order_by(EfdRegistro.linha.asc())
        .all()
    )
    if not registros:
        raise ValueError("Versão não possui registros para exportação")

    arquivo = db.get(EfdArquivo, int(versao.arquivo_id))
    line_ending = getattr(arquivo, "line_ending", "LF") if arquivo else "LF"
    newline = "\r\n" if str(line_ending).upper() == "CRLF" else "\n"

    # 1) gera arquivo (se falhar, não muda status)
    gerar_sped(registros, caminho_saida, newline=newline)

    # 2) marca exportada (sem commit aqui) — só se era VALIDADA
    if status_atual == "VALIDADA":
        WorkflowService.marcar_exportada(versao_id, db)

    return caminho_saida
