from __future__ import annotations
import uuid
from pathlib import Path
from typing import Any, Dict, Optional
import traceback
from fastapi import UploadFile
from app.sped.parser import parse_sped_preview
import re

TEMP_DIR = Path("tmp_uploads")
TEMP_DIR.mkdir(parents=True, exist_ok=True)


def _detect_line_ending(path: Path) -> str:
    # Detecta pelo primeiro chunk: se encontrar \r\n, assume CRLF, senão LF.
    with path.open("rb") as f:
        chunk = f.read(1024 * 1024)  # 1MB
    return "CRLF" if b"\r\n" in chunk else "LF"


def _count_lines_fast(path: Path) -> int:
    # Conta '\n' em binário. Funciona para LF e CRLF.
    total = 0
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            total += chunk.count(b"\n")
    return total


class UploadPreviewService:
    HEX_LONG_RE = re.compile(r"^[0-9A-Fa-f]{20,}$")

    @staticmethod
    def _parece_hash(v: str) -> bool:
        v = (v or "").strip()
        return bool(v) and len(v) >= 20 and bool(UploadPreviewService.HEX_LONG_RE.match(v))

    @staticmethod
    def processar_preview(upload_file: UploadFile) -> Dict[str, Any]:
        """
        Salva arquivo temporário e extrai metadados para o front confirmar.

        Retorna:
          - temp_id
          - cnpj, razao_social, periodo (YYYYMM)
          - total_linhas
          - nome_arquivo
          - line_ending (LF/CRLF)
        """
        temp_id = str(uuid.uuid4())
        temp_path = TEMP_DIR / f"{temp_id}.sped"

        try:
            # 🔒 CRÍTICO: garantir que o cursor do stream está no começo
            # (às vezes o endpoint ou outro código já leu algo do UploadFile)
            try:
                upload_file.file.seek(0)
            except Exception:
                # Nem todo objeto suporta seek (raro), mas geralmente suporta.
                pass

            # Salvar em chunks (mais previsível que copyfileobj quando há cursor estranho)
            with temp_path.open("wb") as out:
                while True:
                    chunk = upload_file.file.read(1024 * 1024)  # 1MB
                    if not chunk:
                        break
                    out.write(chunk)

            # Métricas confiáveis do arquivo realmente salvo
            total_linhas = _count_lines_fast(temp_path)
            line_ending = _detect_line_ending(temp_path)

            # Seu preview parser deve focar em metadados (CNPJ/razão/período).
            # Mesmo que ele calcule total_linhas, vamos confiar no contador acima.
            # limite para evitar varrer arquivo inteiro em casos estranhos (previne travas)
            dados = parse_sped_preview(str(temp_path), max_lines=50000)

            rs = (dados.get("razao_social") or "").strip()
            if UploadPreviewService._parece_hash(rs):
                rs = ""

            return {
                "temp_id": temp_id,
                "cnpj": dados["cnpj"],
                "razao_social": rs,
                "periodo": dados["periodo"],
                "total_linhas": total_linhas,
                "nome_arquivo": getattr(upload_file, "filename", None),
                "line_ending": line_ending,
            }


        except Exception:

            traceback.print_exc()

            # NÃO apaga o temp aqui

            raise

