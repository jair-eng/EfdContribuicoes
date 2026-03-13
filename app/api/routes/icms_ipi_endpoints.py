from __future__ import annotations

import os
import tempfile
from pathlib import Path
from typing import Any

from fastapi import APIRouter, Depends, File, Form, HTTPException, UploadFile, status
from sqlalchemy.orm import Session

from app.db.models.empresa import Empresa
from app.db.session import get_db
from app.icms_ipi.parser_sped_icms import parse_sped_icms_ipi_preview
from app.icms_ipi.service_icms_import import (
    gerar_preview_sped_icms,
    importar_sped_icms,
)

router = APIRouter(prefix="/icms-ipi", tags=["ICMS/IPI"])


def _somente_digitos(valor: str | None) -> str:
    if not valor:
        return ""
    return "".join(ch for ch in str(valor) if ch.isdigit())


def _buscar_empresa_por_cnpj(db: Session, cnpj: str) -> Empresa:
    cnpj_limpo = _somente_digitos(cnpj)
    if not cnpj_limpo:
        raise HTTPException(
            status_code=400,
            detail="CNPJ não encontrado no arquivo SPED ICMS/IPI.",
        )

    empresa = (
        db.query(Empresa)
        .filter(Empresa.cnpj == cnpj_limpo)
        .first()
    )

    if not empresa:
        raise HTTPException(
            status_code=400,
            detail=f"Empresa não encontrada para o CNPJ {cnpj_limpo}.",
        )
    return empresa


async def _salvar_upload_temporario(upload: UploadFile) -> str:
    nome = upload.filename or "arquivo_sem_nome.txt"
    sufixo = Path(nome).suffix or ".txt"

    with tempfile.NamedTemporaryFile(delete=False, suffix=sufixo) as tmp:
        conteudo = await upload.read()
        tmp.write(conteudo)
        return tmp.name


@router.post("/preview", status_code=status.HTTP_200_OK)
async def preview_icms_ipi(
    arquivos: list[UploadFile] = File(...),
    db: Session = Depends(get_db),
) -> dict[str, Any]:
    if not arquivos:
        raise HTTPException(status_code=400, detail="Nenhum arquivo enviado.")

    previews: list[dict[str, Any]] = []
    empresa_ref: Empresa | None = None
    periodos: set[str] = set()

    for upload in arquivos:
        if not (upload.filename or "").lower().endswith(".txt"):
            raise HTTPException(
                status_code=400,
                detail=f"Arquivo inválido: {upload.filename}. Envie apenas .txt",
            )

        temp_path = None
        try:
            temp_path = await _salvar_upload_temporario(upload)

            preview_raw = parse_sped_icms_ipi_preview(temp_path)
            empresa_info = preview_raw.get("empresa") or {}
            cnpj_arquivo = empresa_info.get("cnpj")

            try:
                empresa = _buscar_empresa_por_cnpj(db, cnpj_arquivo)
            except HTTPException as e:
                raise HTTPException(
                    status_code=400,
                    detail=f"Arquivo {upload.filename}: {e.detail}",
                )

            if empresa_ref is None:
                empresa_ref = empresa
            elif empresa_ref.id != empresa.id:
                raise HTTPException(
                    status_code=400,
                    detail=(
                        "Os arquivos enviados pertencem a empresas diferentes. "
                        f"Empresa esperada: {empresa_ref.cnpj} | "
                        f"Arquivo atual: {_somente_digitos(cnpj_arquivo)}"
                    ),
                )

            preview = gerar_preview_sped_icms(
                db=db,
                arquivo_path=temp_path,
                empresa_id=empresa.id,
            )

            if preview.get("periodo"):
                periodos.add(preview["periodo"])

            previews.append(preview)

        finally:
            if temp_path and os.path.exists(temp_path):
                os.remove(temp_path)

    if not empresa_ref:
        raise HTTPException(
            status_code=400,
            detail="Não foi possível gerar preview dos arquivos enviados.",
        )

    return {
        "ok": True,
        "empresa_id": empresa_ref.id,
        "empresa_nome": getattr(empresa_ref, "razao_social", None)
        or getattr(empresa_ref, "nome", None)
        or "Empresa sem nome",
        "cnpj": empresa_ref.cnpj,
        "qtd_arquivos": len(previews),
        "periodos": sorted(periodos),
        "arquivos": previews,
        "resumo": {
            "total_notas": sum(int(p.get("total_notas", 0) or 0) for p in previews),
            "total_itens": sum(int(p.get("total_itens", 0) or 0) for p in previews),
            "total_vl_doc": sum((p.get("total_vl_doc", 0) or 0) for p in previews),
            "total_vl_item": sum((p.get("total_vl_item", 0) or 0) for p in previews),
            "total_vl_icms": sum((p.get("total_vl_icms", 0) or 0) for p in previews),
        },
    }



@router.post("/importar", status_code=status.HTTP_200_OK)
async def importar_icms_ipi(
    arquivos: list[UploadFile] = File(...),
    sobrescrever_existentes: bool = Form(False),
    db: Session = Depends(get_db),
) -> dict[str, Any]:
    if not arquivos:
        raise HTTPException(status_code=400, detail="Nenhum arquivo enviado.")

    resultados: list[dict[str, Any]] = []
    empresa_ref: Empresa | None = None
    periodos: set[str] = set()

    for upload in arquivos:
        if not (upload.filename or "").lower().endswith(".txt"):
            raise HTTPException(
                status_code=400,
                detail=f"Arquivo inválido: {upload.filename}. Envie apenas .txt",
            )

        temp_path = None
        try:
            temp_path = await _salvar_upload_temporario(upload)

            preview_raw = parse_sped_icms_ipi_preview(temp_path)
            empresa_info = preview_raw.get("empresa") or {}
            cnpj_arquivo = empresa_info.get("cnpj")

            empresa = _buscar_empresa_por_cnpj(db, cnpj_arquivo)

            if empresa_ref is None:
                empresa_ref = empresa
            elif empresa_ref.id != empresa.id:
                raise HTTPException(
                    status_code=400,
                    detail=(
                        "Os arquivos enviados pertencem a empresas diferentes. "
                        f"Empresa esperada: {empresa_ref.cnpj} | "
                        f"Arquivo atual: {_somente_digitos(cnpj_arquivo)}"
                    ),
                )

            resultado = importar_sped_icms(
                db=db,
                arquivo_path=temp_path,
                empresa_id=empresa.id,
                sobrescrever_existentes=sobrescrever_existentes,
            )

            if resultado.get("periodo"):
                periodos.add(resultado["periodo"])

            resultados.append(resultado)

        finally:
            if temp_path and os.path.exists(temp_path):
                os.remove(temp_path)

    if not empresa_ref:
        raise HTTPException(
            status_code=400,
            detail="Não foi possível importar os arquivos enviados.",
        )

    return {
        "ok": True,
        "empresa_id": empresa_ref.id,
        "empresa_nome": getattr(empresa_ref, "razao_social", None)
        or getattr(empresa_ref, "nome", None)
        or "Empresa sem nome",
        "cnpj": empresa_ref.cnpj,
        "qtd_arquivos": len(resultados),
        "periodos": sorted(periodos),
        "resultados": resultados,
        "resumo": {
            "total_lido_notas": sum(int(r.get("total_lido_notas", 0) or 0) for r in resultados),
            "total_lido_itens": sum(int(r.get("total_lido_itens", 0) or 0) for r in resultados),
            "inseridas": sum(int(r.get("inseridas", 0) or 0) for r in resultados),
            "atualizadas": sum(int(r.get("atualizadas", 0) or 0) for r in resultados),
            "ignoradas": sum(int(r.get("ignoradas", 0) or 0) for r in resultados),
            "itens_inseridos": sum(int(r.get("itens_inseridos", 0) or 0) for r in resultados),
            "itens_removidos": sum(int(r.get("itens_removidos", 0) or 0) for r in resultados),
            "total_vl_doc": sum((r.get("total_vl_doc", 0) or 0) for r in resultados),
            "total_vl_item": sum((r.get("total_vl_item", 0) or 0) for r in resultados),
            "total_vl_icms": sum((r.get("total_vl_icms", 0) or 0) for r in resultados),
        },
    }