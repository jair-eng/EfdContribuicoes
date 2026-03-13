from __future__ import annotations

from dataclasses import asdict, dataclass
from decimal import Decimal
from typing import Any, Dict, Optional

from sqlalchemy import text
from sqlalchemy.orm import Session

from app.icms_ipi.icms_helpers import (
    _dec_to_str,
    _norm_chave,
    _norm_num_nf,
    _norm_serie,
    _norm_str,
    _to_date,
    _to_decimal,
)


# ============================================================
# DTO de retorno
# ============================================================

@dataclass
class VinculoIcmsIpiResult:
    vinculado: bool = False
    tipo_vinculo: Optional[str] = None   # CHAVE | DOC_DATA | DOC_SERIE | DOC_VALOR_APROX
    score: float = 0.0

    nf_id: Optional[int] = None
    empresa_id: Optional[int] = None
    periodo: Optional[str] = None

    chave_nfe: str = ""
    numero_nf: str = ""
    serie_nf: str = ""
    dt_doc: Optional[str] = None

    vl_doc: str = "0"
    vl_icms: str = "0"

    origem: str = "nf_icms_base"
    observacao: str = ""

    @classmethod
    def not_found(cls, observacao: str = "") -> "VinculoIcmsIpiResult":
        return cls(vinculado=False, observacao=observacao)

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


# ============================================================
# Mapeamento de linha SQL -> DTO
# ============================================================

def _row_to_result(
    row: Any,
    *,
    tipo_vinculo: str,
    score: float,
    observacao: str = "",
) -> VinculoIcmsIpiResult:
    dt_doc = row.dt_doc.isoformat() if getattr(row, "dt_doc", None) else None

    return VinculoIcmsIpiResult(
        vinculado=True,
        tipo_vinculo=tipo_vinculo,
        score=score,
        nf_id=getattr(row, "id", None),
        empresa_id=getattr(row, "empresa_id", None),
        periodo=_norm_str(getattr(row, "periodo", None)) or None,
        chave_nfe=_norm_chave(getattr(row, "chave_nfe", None)),
        numero_nf=_norm_num_nf(getattr(row, "num_doc", None)),
        serie_nf=_norm_serie(getattr(row, "serie", None)),
        dt_doc=dt_doc,
        vl_doc=_dec_to_str(getattr(row, "vl_doc", None)),
        vl_icms=_dec_to_str(getattr(row, "vl_icms", None)),
        origem="nf_icms_base",
        observacao=observacao,
    )


# ============================================================
# Queries isoladas
# ============================================================

def buscar_nf_por_chave(
    db: Session,
    *,
    empresa_id: int,
    chave_nfe: str,
    periodo: str | None = None,
) -> Optional[VinculoIcmsIpiResult]:
    chave = _norm_chave(chave_nfe)
    if not chave:
        return None

    filtros = [
        "empresa_id = :empresa_id",
        "REPLACE(REPLACE(REPLACE(chave_nfe, '.', ''), '-', ''), ' ', '') = :chave",
    ]
    params: Dict[str, Any] = {
        "empresa_id": empresa_id,
        "chave": chave,
    }

    if periodo:
        filtros.append("periodo = :periodo")
        params["periodo"] = periodo

    sql = text(
        f"""
        SELECT
            id,
            empresa_id,
            periodo,
            chave_nfe,
            num_doc,
            serie,
            dt_doc,
            vl_doc,
            vl_icms
        FROM nf_icms_base
        WHERE {' AND '.join(filtros)}
        ORDER BY id DESC
        LIMIT 1
        """
    )

    row = db.execute(sql, params).mappings().first()
    if not row:
        return None

    return _row_to_result(row, tipo_vinculo="CHAVE", score=1.0)


def buscar_nf_por_doc(
    db: Session,
    *,
    empresa_id: int,
    numero_nf: str,
    serie_nf: str,
    dt_doc: Any = None,
    periodo: str | None = None,
) -> Optional[VinculoIcmsIpiResult]:
    numero = _norm_num_nf(numero_nf)
    serie = _norm_serie(serie_nf)
    dt = _to_date(dt_doc)

    if not numero:
        return None

    filtros_base = ["empresa_id = :empresa_id"]
    params_base: Dict[str, Any] = {"empresa_id": empresa_id}

    if periodo:
        filtros_base.append("periodo = :periodo")
        params_base["periodo"] = periodo

    # 1) match forte: número + série + data
    if numero and serie and dt:
        filtros = list(filtros_base)
        filtros.extend([
            "TRIM(LEADING '0' FROM CAST(num_doc AS CHAR)) = :numero",
            "TRIM(LEADING '0' FROM CAST(serie AS CHAR)) = :serie",
            "dt_doc = :dt_doc",
        ])

        params = dict(params_base)
        params.update({
            "numero": numero,
            "serie": serie,
            "dt_doc": dt,
        })

        sql = text(
            f"""
            SELECT
                id,
                empresa_id,
                periodo,
                chave_nfe,
                num_doc,
                serie,
                dt_doc,
                vl_doc,
                vl_icms
            FROM nf_icms_base
            WHERE {' AND '.join(filtros)}
            ORDER BY id DESC
            LIMIT 1
            """
        )

        row = db.execute(sql, params).mappings().first()
        if row:
            return _row_to_result(row, tipo_vinculo="DOC_DATA", score=0.95)

    # 2) match médio: número + série
    if numero and serie:
        filtros = list(filtros_base)
        filtros.extend([
            "TRIM(LEADING '0' FROM CAST(num_doc AS CHAR)) = :numero",
            "TRIM(LEADING '0' FROM CAST(serie AS CHAR)) = :serie",
        ])

        params = dict(params_base)
        params.update({
            "numero": numero,
            "serie": serie,
        })

        sql = text(
            f"""
            SELECT
                id,
                empresa_id,
                periodo,
                chave_nfe,
                num_doc,
                serie,
                dt_doc,
                vl_doc,
                vl_icms
            FROM nf_icms_base
            WHERE {' AND '.join(filtros)}
            ORDER BY dt_doc DESC, id DESC
            LIMIT 1
            """
        )

        row = db.execute(sql, params).mappings().first()
        if row:
            return _row_to_result(row, tipo_vinculo="DOC_SERIE", score=0.85)

    # 3) fallback fraco: só número
    if numero:
        filtros = list(filtros_base)
        filtros.append("TRIM(LEADING '0' FROM CAST(num_doc AS CHAR)) = :numero")

        params = dict(params_base)
        params["numero"] = numero

        sql = text(
            f"""
            SELECT
                id,
                empresa_id,
                periodo,
                chave_nfe,
                num_doc,
                serie,
                dt_doc,
                vl_doc,
                vl_icms
            FROM nf_icms_base
            WHERE {' AND '.join(filtros)}
            ORDER BY dt_doc DESC, id DESC
            LIMIT 1
            """
        )

        row = db.execute(sql, params).mappings().first()
        if row:
            return _row_to_result(
                row,
                tipo_vinculo="DOC",
                score=0.70,
                observacao="match apenas por número do documento",
            )

    return None


def buscar_nf_por_doc_valor_aprox(
    db: Session,
    *,
    empresa_id: int,
    numero_nf: str,
    dt_doc: Any = None,
    vl_doc: Any = None,
    periodo: str | None = None,
    tolerancia_valor: Decimal = Decimal("0.05"),
) -> Optional[VinculoIcmsIpiResult]:
    numero = _norm_num_nf(numero_nf)
    dt = _to_date(dt_doc)
    valor = _to_decimal(vl_doc)

    if not numero:
        return None

    params: Dict[str, Any] = {
        "empresa_id": empresa_id,
        "numero": numero,
    }

    filtros = [
        "empresa_id = :empresa_id",
        "TRIM(LEADING '0' FROM CAST(num_doc AS CHAR)) = :numero",
    ]

    if periodo:
        filtros.append("periodo = :periodo")
        params["periodo"] = periodo

    if dt:
        filtros.append("dt_doc BETWEEN :dt_ini AND :dt_fim")
        params["dt_ini"] = dt
        params["dt_fim"] = dt

    if valor > 0:
        filtros.append("ABS(COALESCE(vl_doc, 0) - :vl_doc) <= :tol")
        params["vl_doc"] = valor
        params["tol"] = tolerancia_valor

    sql = text(
        f"""
        SELECT
            id,
            empresa_id,
            periodo,
            chave_nfe,
            num_doc,
            serie,
            dt_doc,
            vl_doc,
            vl_icms
        FROM nf_icms_base
        WHERE {' AND '.join(filtros)}
        ORDER BY dt_doc DESC, id DESC
        LIMIT 1
        """
    )

    row = db.execute(sql, params).mappings().first()
    if not row:
        return None

    return _row_to_result(
        row,
        tipo_vinculo="DOC_VALOR_APROX",
        score=0.65,
        observacao="match por número + data/valor aproximado",
    )


# ============================================================
# Função principal de vínculo
# ============================================================

def vincular_documento_icms_ipi(
    db: Session,
    *,
    empresa_id: int,
    chave_nfe: str | None = None,
    numero_nf: str | None = None,
    serie_nf: str | None = None,
    dt_doc: Any = None,
    vl_doc: Any = None,
    periodo: str | None = None,
) -> Dict[str, Any]:
    """
    Tenta vincular um documento do SPED Contribuições com a base auxiliar nf_icms_base.

    Estratégia:
      1) chave_nfe
      2) número + série + data
      3) número + série
      4) número + data/valor aproximado
    """

    # 1) chave
    if chave_nfe:
        res = buscar_nf_por_chave(
            db,
            empresa_id=empresa_id,
            chave_nfe=chave_nfe,
            periodo=periodo,
        )
        if res:
            return res.to_dict()

    # 2/3/4) documento
    if numero_nf:
        res = buscar_nf_por_doc(
            db,
            empresa_id=empresa_id,
            numero_nf=numero_nf,
            serie_nf=serie_nf or "",
            dt_doc=dt_doc,
            periodo=periodo,
        )
        if res:
            return res.to_dict()

        res = buscar_nf_por_doc_valor_aprox(
            db,
            empresa_id=empresa_id,
            numero_nf=numero_nf,
            dt_doc=dt_doc,
            vl_doc=vl_doc,
            periodo=periodo,
        )
        if res:
            return res.to_dict()

    return VinculoIcmsIpiResult.not_found("nota não localizada em nf_icms_base").to_dict()


# ============================================================
# Helper de enriquecimento para usar no scanner/agregador
# ============================================================

def montar_meta_icms_aux(
    db: Session,
    *,
    empresa_id: int,
    chave_nfe: str | None = None,
    numero_nf: str | None = None,
    serie_nf: str | None = None,
    dt_doc: Any = None,
    vl_doc: Any = None,
    periodo: str | None = None,
) -> Dict[str, Any]:
    """
    Helper simples para plugar em dto.meta["icms_aux"].
    """
    return vincular_documento_icms_ipi(
        db,
        empresa_id=empresa_id,
        chave_nfe=chave_nfe,
        numero_nf=numero_nf,
        serie_nf=serie_nf,
        dt_doc=dt_doc,
        vl_doc=vl_doc,
        periodo=periodo,
    )