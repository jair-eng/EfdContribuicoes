from __future__ import annotations

from dataclasses import dataclass, asdict
from datetime import datetime, date
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any


def _split_sped_line(line: str) -> tuple[str, list[str]]:
    """
    Converte uma linha SPED como:
    |C100|0|1|...|
    em:
    reg='C100', fields=[...]
    """
    parts = line.strip().split("|")
    payload = parts[1:-1] if parts and parts[-1] == "" else parts[1:]
    if not payload:
        return "", []
    reg = payload[0].strip()
    fields = [p.strip() for p in payload[1:]]
    return reg, fields


def _parse_date_ddmmyyyy(value: str) -> date | None:
    value = (value or "").strip()
    if not value:
        return None
    return datetime.strptime(value, "%d%m%Y").date()


def _parse_decimal(value: str) -> Decimal:
    value = (value or "").strip()

    if not value:
        return Decimal("0")

    # caso normal do SPED: 575,05
    if "," in value:
        value = value.replace(".", "").replace(",", ".")
        try:
            return Decimal(value)
        except InvalidOperation:
            return Decimal("0")

    # caso sem separador decimal: 57505 -> 575.05
    if value.isdigit():
        return Decimal(value) / Decimal("100")

    try:
        return Decimal(value)
    except InvalidOperation:
        return Decimal("0")


@dataclass
class NfIcmsPreviewItem:
    chave_nfe: str
    num_doc: str | None
    dt_doc: date | None
    vl_doc: Decimal
    vl_icms: Decimal
    nome_arquivo: str
    fonte: str = "EFD_ICMS_IPI"


def parse_sped_icms_ipi_preview(
    arquivo_path: str,
) -> dict[str, Any]:
    """
    Lê um SPED ICMS/IPI e devolve um preview simples, sem gravar no banco.

    Regras:
    - Usa C100 como cabeçalho da nota
    - Soma VL_ICMS dos C190 subsequentes até o próximo C100
    """
    path = Path(arquivo_path)
    if not path.exists():
        raise FileNotFoundError(f"Arquivo não encontrado: {arquivo_path}")

    periodo: str | None = None
    dt_ini: date | None = None
    dt_fin: date | None = None

    notas: list[NfIcmsPreviewItem] = []

    nota_atual: NfIcmsPreviewItem | None = None

    with path.open("r", encoding="latin1") as f:
        for raw_line in f:
            line = raw_line.strip()
            if not line:
                continue

            reg, fields = _split_sped_line(line)
            if not reg:
                continue

            if reg == "0000":
                # Layout esperado:
                # |0000|COD_VER|COD_FIN|DT_INI|DT_FIN|NOME|CNPJ|...
                dt_ini = _parse_date_ddmmyyyy(fields[2] if len(fields) > 2 else "")
                dt_fin = _parse_date_ddmmyyyy(fields[3] if len(fields) > 3 else "")
                if dt_ini:
                    periodo = dt_ini.strftime("%Y%m")

            elif reg == "C100":
                # Fecha a nota anterior
                if nota_atual:
                    notas.append(nota_atual)

                # Layout esperado:
                # fields[6]  = NUM_DOC
                # fields[7]  = CHV_NFE
                # fields[8]  = DT_DOC
                # fields[10] = VL_DOC
                num_doc = fields[6] if len(fields) > 6 else None
                chave_nfe = fields[7] if len(fields) > 7 else ""
                dt_doc = _parse_date_ddmmyyyy(fields[8] if len(fields) > 8 else "")
                vl_doc = _parse_decimal(fields[10] if len(fields) > 10 else "0")

                nota_atual = NfIcmsPreviewItem(
                    chave_nfe=chave_nfe,
                    num_doc=num_doc,
                    dt_doc=dt_doc,
                    vl_doc=vl_doc,
                    vl_icms=Decimal("0"),
                    nome_arquivo=path.name,
                )

            elif reg == "C190":
                # Layout esperado:
                # fields[5] = VL_ICMS
                if nota_atual:
                    vl_icms = _parse_decimal(fields[5] if len(fields) > 5 else "0")
                    nota_atual.vl_icms += vl_icms

    # Fecha última nota
    if nota_atual:
        notas.append(nota_atual)

    total_notas = len(notas)
    total_vl_doc = sum((n.vl_doc for n in notas), Decimal("0"))
    total_vl_icms = sum((n.vl_icms for n in notas), Decimal("0"))

    return {
        "arquivo": path.name,
        "periodo": periodo,
        "dt_ini": dt_ini,
        "dt_fin": dt_fin,
        "total_notas": total_notas,
        "total_vl_doc": total_vl_doc,
        "total_vl_icms": total_vl_icms,
        "notas_preview": [asdict(n) for n in notas[:20]],
        "notas": notas,  # deixa disponível para o service gravar depois
    }