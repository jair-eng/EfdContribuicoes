from __future__ import annotations
from pathlib import Path
from fastapi import APIRouter, Depends, HTTPException, status
from fastapi.responses import FileResponse
from sqlalchemy.orm import Session
from starlette.responses import StreamingResponse
from app.db.session import get_db
from app.db.models import EfdVersao, EfdArquivo, Empresa
from app.services.apontamentos_export_service import ApontamentosExportService
from app.services.export_service import exportar_sped
import zipfile
import uuid
from pydantic import BaseModel
from typing import List
from io import BytesIO
import logging
logger = logging.getLogger(__name__)

EXPORT_DIR = Path("exports")
EXPORT_DIR.mkdir(parents=True, exist_ok=True)
router = APIRouter(prefix="/export", tags=["Export"])

@router.get(
    "/versao/{versao_id}/apontamentos.csv",
    status_code=status.HTTP_200_OK,
)
def exportar_apontamentos_csv(
    versao_id: int,
    db: Session = Depends(get_db),
):
    try:
        versao = db.get(EfdVersao, versao_id)
        if not versao:
            raise HTTPException(status_code=404, detail="Versão não encontrada")

        # ✅ CSV é permitido na revisão e depois
        if versao.status not in ("EM_REVISAO", "VALIDADA", "EXPORTADA"):
            raise HTTPException(
                status_code=403,
                detail=f"CSV bloqueado: versão com status '{versao.status}'. Inicie a revisão primeiro."
            )

        csv_content = ApontamentosExportService.exportar_csv(db, versao_id=versao_id)

        # ✅ Excel-friendly: UTF-8 com BOM (acentos OK) e sempre bytes
        if isinstance(csv_content, str):
            data = csv_content.encode("utf-8-sig")
        elif isinstance(csv_content, (bytes, bytearray)):
            data = bytes(csv_content)
        else:
            # fallback defensivo
            data = str(csv_content).encode("utf-8-sig")

        return StreamingResponse(
            BytesIO(data),
            media_type="text/csv; charset=utf-8",
            headers={"Content-Disposition": f'attachment; filename="apontamentos_versao{versao_id}.csv"'},
        )

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.get("/versao/{versao_id}", status_code=status.HTTP_200_OK)
def baixar_sped(versao_id: int, db: Session = Depends(get_db)):
    """
    Gera o arquivo e devolve para download.

    Regras:
      - VALIDADA: gera e marca como EXPORTADA (feito dentro do exportar_sped)
      - EXPORTADA: permite re-download; se arquivo sumiu, pode regerar sem revalidar
      - outros status: bloqueia
    """
    try:
        versao = db.get(EfdVersao, versao_id)
        if not versao:
            raise HTTPException(status_code=404, detail="Versão não encontrada")

        numero = getattr(versao, "numero", None)
        sufixo = f"v{numero}_" if numero is not None else ""
        out_path = EXPORT_DIR / f"sped_corrigido_{sufixo}versao_{versao_id}.txt"

        # ✅ Re-download direto
        if versao.status == "EXPORTADA" and out_path.exists():
            return FileResponse(
                path=str(out_path),
                media_type="text/plain",
                filename=out_path.name,
            )

        # ✅ Só permite gerar se VALIDADA ou EXPORTADA (arquivo pode ter sido limpo)
        if versao.status not in ("VALIDADA", "EXPORTADA"):
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail=f"Export bloqueado: versão com status '{versao.status}'. Valide antes de exportar."
            )

        # Gera arquivo (e marca EXPORTADA internamente se estava VALIDADA)
        exportar_sped(versao_id=versao_id, caminho_saida=str(out_path), db=db)
        db.commit()

        if not out_path.exists():
            raise HTTPException(status_code=500, detail="Falha ao gerar arquivo de exportação")

        return FileResponse(
            path=str(out_path),
            media_type="text/plain",
            filename=out_path.name,
        )

    except HTTPException:
        db.rollback()
        raise
    except ValueError as e:
        db.rollback()
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.exception("Erro export versao_id=%s", versao_id)
        db.rollback()
        raise HTTPException(status_code=500, detail=str(e))

class ExportZipPayload(BaseModel):
    versao_ids: List[int]

@router.post("/versoes-zip")
def exportar_versoes_zip(payload: ExportZipPayload, db: Session = Depends(get_db)):
    try:
        zip_path = _gerar_zip_por_versoes(versao_ids=payload.versao_ids, db=db)
        return FileResponse(path=str(zip_path), media_type="application/zip", filename="SPED_exports.zip")
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))

@router.get("/empresa/{empresa_id}/versoes-zip")
def exportar_versoes_zip_por_empresa_status(
    empresa_id: int,
    status: List[str] = ["VALIDADA", "EXPORTADA"],  # ?status=VALIDADA&status=EXPORTADA
    db: Session = Depends(get_db),
):
    status_norm = [str(s).strip().upper() for s in (status or [])]
    allowed = {"VALIDADA", "EXPORTADA"}
    status_norm = [s for s in status_norm if s in allowed]
    if not status_norm:
        raise HTTPException(status_code=400, detail="Status inválido. Use: VALIDADa / EXPORTADA.")

    # busca IDs das versões pela empresa
    versao_ids = [
        int(v_id) for (v_id,) in (
            db.query(EfdVersao.id)
            .join(EfdArquivo, EfdArquivo.id == EfdVersao.arquivo_id)
            .filter(EfdArquivo.empresa_id == int(empresa_id))
            .filter(EfdVersao.status.in_(status_norm))
            .order_by(EfdVersao.id.desc())
            .all()
        )
    ]

    if not versao_ids:
        raise HTTPException(status_code=404, detail="Nenhuma versão encontrada para os status informados.")

    try:
        zip_path = _gerar_zip_por_versoes(versao_ids=versao_ids, db=db)
        # nome melhor (inclui status)
        filename = f"SPED_empresa_{empresa_id}_{'-'.join(status_norm)}.zip"
        return FileResponse(path=str(zip_path), media_type="application/zip", filename=filename)

    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))


def _gerar_zip_por_versoes(*, versao_ids: list[int], db: Session) -> Path:
    versao_ids = [int(v) for v in versao_ids if v is not None]
    if not versao_ids:
        raise HTTPException(status_code=400, detail="Informe versao_ids (lista de int).")

    zip_path = EXPORT_DIR / f"SPED_export_{uuid.uuid4().hex}.zip"

    with zipfile.ZipFile(zip_path, "w", compression=zipfile.ZIP_DEFLATED) as zf:
        for vid in versao_ids:
            out_path = EXPORT_DIR / f"SPED_versao_{vid}.txt"

            caminho = exportar_sped(
                versao_id=int(vid),
                caminho_saida=str(out_path),
                db=db,
            )

            p = Path(caminho)
            if not p.exists():
                raise HTTPException(status_code=500, detail=f"Falha ao gerar arquivo da versão {vid}")

            zf.write(str(p), arcname=p.name)

    return zip_path