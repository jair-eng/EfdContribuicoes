from __future__ import annotations
from pathlib import Path
from fastapi import APIRouter, Depends, HTTPException, status
from fastapi.responses import FileResponse
from sqlalchemy.orm import Session
from starlette.responses import StreamingResponse
from app.db.session import get_db
from app.db.models import EfdVersao
from app.services.apontamentos_export_service import ApontamentosExportService
from app.services.export_service import exportar_sped
import zipfile
import uuid
from pydantic import BaseModel
from typing import List
from io import BytesIO

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
        db.rollback()
        raise HTTPException(status_code=500, detail=str(e))

class ExportZipPayload(BaseModel):
    versao_ids: List[int]

@router.post("/versoes-zip")
def exportar_versoes_zip(payload: ExportZipPayload, db: Session = Depends(get_db)):
    versao_ids = [int(v) for v in payload.versao_ids if v is not None]
    if not versao_ids:
        raise HTTPException(status_code=400, detail="Informe versao_ids (lista de int).")

    zip_path = EXPORT_DIR / f"SPED_export_{uuid.uuid4().hex}.zip"

    try:
        with zipfile.ZipFile(zip_path, "w", compression=zipfile.ZIP_DEFLATED) as zf:
            for vid in versao_ids:
                # cria um nome de arquivo por versão (evita sobrescrever)
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

        return FileResponse(
            path=str(zip_path),
            media_type="application/zip",
            filename="SPED_exports.zip",
        )

    except ValueError as e:
        # exportar_sped lança ValueError quando status não permite, etc.
        raise HTTPException(status_code=400, detail=str(e))
    except HTTPException:
        raise
    except Exception as e:
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))