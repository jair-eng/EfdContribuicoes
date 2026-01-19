from __future__ import annotations
from sqlalchemy.orm import Session
from app.fiscal.scanner import FiscalScanner
from app.services.upload_preview_service import UploadPreviewService
from app.services.upload_confirm_service import UploadConfirmService

class UploadService:
    """
    Orquestra upload:
      preview -> confirm

    Transação:
      - UploadConfirmService NÃO comita
      - UploadService comita 1 vez no final
    """

    @staticmethod
    def preview(upload_file) -> dict:
        return UploadPreviewService.processar_preview(upload_file)

    @staticmethod
    def confirm(db: Session, payload: dict) -> dict:
        temp_id = payload.get("temp_id")
        if not temp_id:
            raise ValueError("temp_id é obrigatório")

        nome_arquivo = payload.get("nome_arquivo")

        try:
            # 1) Persistir (gera empresa/arquivo/versao/registros)
            resultado = UploadConfirmService.confirmar_upload(
                db=db,
                temp_id=temp_id,
                nome_arquivo=nome_arquivo,
            )

            versao_id = resultado["versao_id"]

            # 2) Rodar scanner automaticamente (resiliente)
            apontamentos_gerados = 0
            erros_regras: list[str] = []

            try:
                # Sugestão: o scanner retorna a qtd de apontamentos e/ou lista de erros
                scan_result = FiscalScanner.scan_versao(db=db, versao_id=versao_id)

                # suporte a diferentes formatos de retorno
                if isinstance(scan_result, dict):
                    apontamentos_gerados = int(scan_result.get("apontamentos_gerados", 0))
                    erros_regras = list(scan_result.get("erros_regras", []))
                elif isinstance(scan_result, int):
                    apontamentos_gerados = int(scan_result)

            except Exception as e:
                # Não mata o confirm — só informa
                erros_regras.append(f"Scanner falhou: {e}")





            # 3) Response mais “produto”
            scan = FiscalScanner.scan_versao(db, versao_id=resultado["versao_id"])
            resultado["apontamentos_gerados"] = scan["apontamentos_gerados"]
            resultado["erros_regras"] = scan["erros_regras"]

            # 4 commit de tudo
            db.commit()
            return resultado

        except Exception:
            db.rollback()
            raise