from __future__ import annotations
import re
from pathlib import Path
from typing import Optional, Dict, Any
from sqlalchemy.orm import Session

from app.fiscal.constants import DOMINIOS_VALIDOS, DOM_GERAL
from app.sped.parser import parse_sped_preview, parse_sped_full
from app.db.models import Empresa, EfdArquivo, EfdVersao, EfdRegistro

TEMP_DIR = Path("tmp_uploads")

# -------------------------------------------------------------------
# Utilitários
# -------------------------------------------------------------------

def _somente_digitos(v: str) -> str:
    return re.sub(r"\D+", "", v or "")


def _validar_periodo(periodo: str) -> str:
    p = _somente_digitos(periodo)
    if len(p) != 6:
        raise ValueError("Período inválido. Esperado YYYYMM (6 dígitos).")
    ano = int(p[:4])
    mes = int(p[4:6])
    if ano < 2000 or mes < 1 or mes > 12:
        raise ValueError("Período inválido. Esperado YYYYMM com mês 01-12.")
    return p


def _validar_line_ending(v: Optional[str]) -> str:
    le = (v or "LF").strip().upper()
    if le not in ("LF", "CRLF"):
        raise ValueError("line_ending inválido. Use 'LF' ou 'CRLF'.")
    return le


# -------------------------------------------------------------------
# Heurísticas de correção de razão social
# -------------------------------------------------------------------

HEX_LONG_RE = re.compile(r"^[0-9A-Fa-f]{20,}$")


def _parece_hash(v: Optional[str]) -> bool:
    s = (v or "").strip()
    return bool(s) and len(s) >= 20 and bool(HEX_LONG_RE.match(s))


def _nome_valido(v: Optional[str]) -> bool:
    s = (v or "").strip()
    # pelo menos 3 letras → evita hash, números, lixo
    return sum(ch.isalpha() for ch in s) >= 3


# -------------------------------------------------------------------
# Metadados do SPED (reaproveita PREVIEW)
# -------------------------------------------------------------------

def _extrair_metadados_sped(file_path: Path) -> dict:
    """
    Usa exatamente o mesmo parser do PREVIEW para garantir consistência:
      - cnpj
      - razao_social (preferencialmente do 0140)
      - periodo (YYYYMM)
      - line_ending
    """
    dados = parse_sped_preview(str(file_path))

    return {
        "cnpj": dados["cnpj"],
        "razao_social": dados.get("razao_social"),
        "periodo": dados["periodo"],
        "line_ending": dados.get("line_ending", "LF"),
        "total_linhas": dados.get("total_linhas", 0),
    }


# -------------------------------------------------------------------
# Service de CONFIRM
# -------------------------------------------------------------------

class UploadConfirmService:
    """
    Persistência do upload confirmado.

    Regras:
      - NÃO faz commit (caller controla)
      - NÃO faz rollback
      - sempre apaga o arquivo temporário no finally
    """

    @staticmethod
    def confirmar_upload(
        db: Session,
        *,
        temp_id: str,
        nome_arquivo: Optional[str] = None,
        dominio: str | None = None,
        batch_size: int = 5000,
    ) -> Dict[str, Any]:

        file_path = TEMP_DIR / f"{temp_id}.sped"
        if not file_path.exists():
            raise ValueError("Arquivo temporário não encontrado")

        meta = _extrair_metadados_sped(file_path)

        cnpj_limpo = _somente_digitos(meta["cnpj"])
        if len(cnpj_limpo) != 14:
            raise ValueError("CNPJ inválido. Esperado 14 dígitos.")

        periodo_ok = _validar_periodo(meta["periodo"])
        le_ok = _validar_line_ending(meta.get("line_ending"))

        razao = (meta.get("razao_social") or "").strip() or None

        try:
            dominio_payload = (dominio or "").strip().upper()

            if dominio_payload and dominio_payload not in DOMINIOS_VALIDOS:
                raise ValueError(f"dominio inválido: {dominio_payload}")
            # ------------------------------------------------------------
            # 1) EMPRESA  (🔥 correção definitiva do hash 🔥)
            # ------------------------------------------------------------
            empresa = db.query(Empresa).filter_by(cnpj=cnpj_limpo).first()

            if not empresa:
                # cria nova
                empresa = Empresa(
                    cnpj=cnpj_limpo,
                    razao_social=razao if _nome_valido(razao) else None,
                    dominio=dominio_payload or DOM_GERAL,
                )
                db.add(empresa)
                db.flush()
            else:
                # autocorreção:
                # se o nome novo é válido e o atual está vazio OU parece hash → atualiza
                if _nome_valido(razao) and (
                    not (empresa.razao_social or "").strip()
                    or _parece_hash(empresa.razao_social)
                ):
                    empresa.razao_social = razao
                    db.add(empresa)
                    db.flush()

                if dominio_payload and (empresa.dominio or DOM_GERAL) == DOM_GERAL:
                    empresa.dominio = dominio_payload
                    db.add(empresa)
                    db.flush()

            # ------------------------------------------------------------
            # 2) ARQUIVO
            # ------------------------------------------------------------
            arquivo = EfdArquivo(
                empresa_id=int(empresa.id),
                nome_arquivo=nome_arquivo,
                periodo=periodo_ok,
                status="ORIGINAL",
                line_ending=le_ok,
            )
            db.add(arquivo)
            db.flush()

            # ------------------------------------------------------------
            # 3) VERSÃO 1
            # ------------------------------------------------------------
            versao = EfdVersao(
                arquivo_id=int(arquivo.id),
                numero=1,
                status="GERADA",
            )
            db.add(versao)
            db.flush()

            # ------------------------------------------------------------
            # 4) REGISTROS (bulk)
            # ------------------------------------------------------------
            buffer: list[EfdRegistro] = []
            total = 0

            for r in parse_sped_full(str(file_path)):
                linha_num = r.get("linha") or r.get("linha_num")
                if linha_num is None:
                    raise KeyError("linha")

                buffer.append(
                    EfdRegistro(
                        versao_id=int(versao.id),
                        reg=r["registro"],
                        linha=int(linha_num),  # <-- usa o campo atual
                        conteudo_json=r["conteudo_json"],
                    )
                )

                if len(buffer) >= batch_size:
                    db.bulk_save_objects(buffer)
                    total += len(buffer)
                    buffer.clear()

            if buffer:
                db.bulk_save_objects(buffer)
                total += len(buffer)


            return {
                "temp_id": temp_id,
                "empresa_id": int(empresa.id),
                "arquivo_id": int(arquivo.id),
                "versao_id": int(versao.id),
                "total_registros": int(total),
                "line_ending": le_ok,
                "periodo": periodo_ok,
                "cnpj": cnpj_limpo,
            }

        except Exception:

            # mantém o temp para retry/debug
            # (se quiser, pode apagar só em erros específicos)
            raise
