from __future__ import annotations

import tempfile
from pathlib import Path

from fastapi import APIRouter, HTTPException
from fastapi.responses import FileResponse

from app.icms_ipi.icms_ipi_funcoes import normalizar_itens_preview_icms_ipi
from app.icms_ipi.parser_sped_icms import parse_sped_icms_ipi_preview
from relatorio_oportunidades.foto_oportunidade.cruzar_icms_contribuicoes import (
    cruzar_icms_ipi_com_efd_contrib,
    resumir_cruzamento,
)
from relatorio_oportunidades.foto_oportunidade.exportar_cruzamento_xlsx import exportar_cruzamento_xlsx
from relatorio_oportunidades.foto_oportunidade.normalizar_efd_contribuicoes import (
    normalizar_efd_contribuicoes_para_cruzamento,
)

router = APIRouter(prefix="/foto-recuperacao", tags=["Foto Recuperação"])

PASTA_ICMS = Path(r"C:\Sped\ICMS_IPI")
PASTA_CONTRIB = Path(r"C:\Sped\CONTRIB")


@router.post("/executar")
def executar_foto_recuperacao():
    if not PASTA_ICMS.exists() or not PASTA_ICMS.is_dir():
        raise HTTPException(
            status_code=400,
            detail=f"Pasta ICMS/IPI não encontrada: {PASTA_ICMS}",
        )

    if not PASTA_CONTRIB.exists() or not PASTA_CONTRIB.is_dir():
        raise HTTPException(
            status_code=400,
            detail=f"Pasta EFD Contribuições não encontrada: {PASTA_CONTRIB}",
        )

    arquivos_icms = sorted(PASTA_ICMS.glob("*.txt"))
    arquivos_contrib = sorted(PASTA_CONTRIB.glob("*.txt"))

    if not arquivos_icms:
        raise HTTPException(
            status_code=400,
            detail=f"Nenhum arquivo .txt encontrado na pasta ICMS/IPI: {PASTA_ICMS}",
        )

    if not arquivos_contrib:
        raise HTTPException(
            status_code=400,
            detail=f"Nenhum arquivo .txt encontrado na pasta Contribuições: {PASTA_CONTRIB}",
        )

    linhas_icms = []
    linhas_contrib = []

    for arq in arquivos_icms:
        parsed = parse_sped_icms_ipi_preview(str(arq))
        linhas = normalizar_itens_preview_icms_ipi(parsed)
        linhas_icms.extend(linhas)

    for arq in arquivos_contrib:
        linhas = normalizar_efd_contribuicoes_para_cruzamento(arq)
        linhas_contrib.extend(linhas)

    linhas_cruzadas = cruzar_icms_ipi_com_efd_contrib(linhas_icms, linhas_contrib)
    resumo = resumir_cruzamento(linhas_cruzadas)

    PASTA_SAIDA = Path(r"C:\Sped\saida")
    PASTA_SAIDA.mkdir(parents=True, exist_ok=True)

    out_xlsx = PASTA_SAIDA / "foto_recuperacao_cruzada.xlsx"

    exportar_cruzamento_xlsx(
        out_xlsx,
        linhas_cruzadas=linhas_cruzadas,
        resumo=resumo,
    )

    return FileResponse(
        path=str(out_xlsx),
        filename="foto_recuperacao_cruzada.xlsx",
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )