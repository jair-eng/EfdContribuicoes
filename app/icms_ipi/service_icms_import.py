from __future__ import annotations

from decimal import Decimal
from pathlib import Path
from typing import Any

from sqlalchemy.orm import Session

from app.db.models.nf_icms_base import NfIcmsBase
from app.icms_ipi.parser_sped_icms import parse_sped_icms_ipi_preview


def gerar_preview_sped_icms(
    *,
    db: Session,
    arquivo_path: str,
    empresa_id: int,
) -> dict[str, Any]:
    """
    Lê o SPED ICMS/IPI e devolve preview sem gravar nada no banco.
    """
    preview = parse_sped_icms_ipi_preview(arquivo_path)

    return {
        "empresa_id": empresa_id,
        "arquivo": preview["arquivo"],
        "periodo": preview["periodo"],
        "dt_ini": preview["dt_ini"],
        "dt_fin": preview["dt_fin"],
        "total_notas": preview["total_notas"],
        "total_vl_doc": preview["total_vl_doc"],
        "total_vl_icms": preview["total_vl_icms"],
        "notas_preview": preview["notas_preview"],
    }


def importar_sped_icms(
    *,
    db: Session,
    arquivo_path: str,
    empresa_id: int,
    sobrescrever_existentes: bool = False,
) -> dict[str, Any]:
    """
    Importa SPED ICMS/IPI para nf_icms_base.

    Regras:
    - usa o parser para montar as notas
    - grava por empresa_id + chave_nfe
    - se sobrescrever_existentes=True, atualiza os dados da nota existente
    """
    preview = parse_sped_icms_ipi_preview(arquivo_path)

    periodo = preview["periodo"]
    notas = preview["notas"]
    nome_arquivo = Path(arquivo_path).name

    inseridas = 0
    atualizadas = 0
    ignoradas = 0

    for item in notas:
        existente = (
            db.query(NfIcmsBase)
            .filter(
                NfIcmsBase.empresa_id == empresa_id,
                NfIcmsBase.chave_nfe == item.chave_nfe,
            )
            .first()
        )

        if existente:
            if sobrescrever_existentes:
                existente.periodo = periodo
                existente.dt_doc = item.dt_doc
                existente.vl_doc = item.vl_doc
                existente.vl_icms = item.vl_icms
                existente.fonte = item.fonte
                existente.nome_arquivo = nome_arquivo
                atualizadas += 1
            else:
                ignoradas += 1
            continue

        nova = NfIcmsBase(
            empresa_id=empresa_id,
            periodo=periodo,
            chave_nfe=item.chave_nfe,
            dt_doc=item.dt_doc,
            vl_doc=item.vl_doc,
            vl_icms=item.vl_icms,
            fonte=item.fonte,
            nome_arquivo=nome_arquivo,
        )
        db.add(nova)
        inseridas += 1

    db.commit()

    total_importado = (
        db.query(NfIcmsBase)
        .filter(
            NfIcmsBase.empresa_id == empresa_id,
            NfIcmsBase.periodo == periodo,
        )
        .count()
    )

    return {
        "ok": True,
        "empresa_id": empresa_id,
        "arquivo": nome_arquivo,
        "periodo": periodo,
        "total_lido": len(notas),
        "inseridas": inseridas,
        "atualizadas": atualizadas,
        "ignoradas": ignoradas,
        "total_importado_empresa_periodo": total_importado,
        "total_vl_doc": preview["total_vl_doc"],
        "total_vl_icms": preview["total_vl_icms"],
    }