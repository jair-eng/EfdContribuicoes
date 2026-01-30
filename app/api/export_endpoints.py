from __future__ import annotations
from pathlib import Path
from fastapi import APIRouter, Depends, HTTPException, status
from fastapi.responses import FileResponse
from sqlalchemy.orm import Session
from starlette.responses import StreamingResponse
from app.db.session import get_db
from app.db.models import EfdVersao, EfdArquivo, Empresa
from app.schemas.workflow import ExportZipPayload
from app.services.apontamentos_export_service import ApontamentosExportService
from app.services.export_service import exportar_sped
import zipfile
import uuid
from fastapi import Query
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
def baixar_sped(versao_id: int,valor_utilizado_mes: float = Query(default=0.0, ge=0.0), db: Session = Depends(get_db)):
    """
    Gera o arquivo e devolve para download.
    Agora com invalidação de cache para garantir que revisões novas apareçam no TXT.
    """
    try:
        versao = db.get(EfdVersao, versao_id)
        if not versao:
            raise HTTPException(status_code=404, detail="Versão não encontrada")

        # 1. Definir o caminho do arquivo
        numero = getattr(versao, "numero", None)
        sufixo = f"v{numero}_" if numero is not None else ""
        out_path = EXPORT_DIR / f"sped_corrigido_{sufixo}versao_{versao_id}.txt"

        # 2. 🧹 LIMPEZA TOTAL (FIM DO FANTASMA)
        # Sempre removemos o arquivo antigo se ele existir para garantir
        # que o 'exportar_sped' escreva dados novos do zero.
        if out_path.exists():
            print(f"♻️ RESET: Removendo arquivo anterior da Versão {versao_id} para garantir integridade.")
            try:
                out_path.unlink()
            except Exception as e:
                print(f"⚠️ Alerta: Não foi possível deletar o arquivo físico: {e}")

        # 3. Bloqueio de status (Apenas para garantir que a revisão foi confirmada)
        if versao.status not in ("VALIDADA", "EXPORTADA"):
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail=f"Export bloqueado: status '{versao.status}'. Confirme as revisões antes."
            )

        # 4) 🚀 GERAÇÃO REAL (service decide o caminho final)
        print(f"⚙️ PROCESSAMENTO: Gerando SPED novo para Versão {versao_id}...")

        caminho_saida = exportar_sped(
            versao_id=versao_id,
            db=db,
            valor_utilizado_mes=valor_utilizado_mes,  # ✅ repassa
        )
        out_path = Path(caminho_saida)

        db.commit()

        if not out_path.exists() or not out_path.is_file():
            raise HTTPException(500, f"Falha crítica: o arquivo não foi gerado em {out_path}")

        return FileResponse(
            path=str(out_path),
            media_type="text/plain",
            filename=out_path.name,
        )

    except Exception as e:

        db.rollback()
        raise HTTPException(status_code=500, detail=str(e))


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
            # ✅ Alinhando o nome do arquivo para garantir que sigamos o padrão da rota individual
            versao = db.get(EfdVersao, vid)
            numero = getattr(versao, "numero", None)
            sufixo = f"v{numero}_" if numero is not None else ""
            out_path = EXPORT_DIR / f"sped_corrigido_{sufixo}versao_{vid}.txt"

            # ✅ Forçar regeração: se o arquivo existe, deletamos antes de chamar o exportar_sped
            # Isso garante que o ZIP sempre contenha o CST 51 mais recente
            if out_path.exists():
                out_path.unlink()

            print(f"📦 ZIP: Gerando arquivo para Versão {vid} dentro do lote...")
            caminho = exportar_sped(
                versao_id=int(vid),
                caminho_saida=str(out_path),
                db=db,
            )

            p = Path(caminho)
            if not p.exists():
                raise HTTPException(status_code=500, detail=f"Falha ao gerar arquivo da versão {vid}")

            zf.write(str(p), arcname=p.name)

        db.commit()  # Commit único após processar todo o lote

    return zip_path