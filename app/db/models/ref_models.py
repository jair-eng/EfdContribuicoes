from __future__ import annotations
import re
from datetime import datetime
from typing import Optional
from sqlalchemy import (
    String,
    DateTime,
    Boolean,
    Enum,
    Index,
    text,
)
from sqlalchemy.orm import Mapped, mapped_column, validates
from app.db.models.base import Base  # ajuste se seu Base estiver em outro path


_CFOP_RE = re.compile(r"^\d{4}$")
_CST_RE = re.compile(r"^\d{2}$")


class RefCfop(Base):
    """
    Tabela de referência de CFOP (mínimo MVP + expansão incremental).

    Observações:
    - CFOP é 4 dígitos.
    - tipo indica ENTRADA/SAIDA (útil para validações e alertas).
    - grupo é derivável do primeiro dígito, mas armazenar ajuda no filtro.
    """
    __tablename__ = "ref_cfop"

    cfop: Mapped[str] = mapped_column(String(4), primary_key=True)  # ex: "1102"
    descricao: Mapped[str] = mapped_column(String(255), nullable=False)

    tipo: Mapped[str] = mapped_column(
        Enum("ENTRADA", "SAIDA", name="ref_cfop_tipo"),
        nullable=False,
        index=True,
    )

    # ex: "1xxx", "7xxx"
    grupo: Mapped[str] = mapped_column(String(4), nullable=False, index=True)

    # auditoria
    created_at: Mapped[datetime] = mapped_column(
        DateTime, nullable=False, server_default=text("CURRENT_TIMESTAMP")
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime,
        nullable=False,
        server_default=text("CURRENT_TIMESTAMP"),
        onupdate=datetime.utcnow,
    )

    __table_args__ = (
        Index("ix_ref_cfop_tipo_grupo", "tipo", "grupo"),
    )

    @validates("cfop")
    def _validate_cfop(self, key: str, value: str) -> str:
        v = (value or "").strip()
        if not _CFOP_RE.match(v):
            raise ValueError("CFOP inválido: esperado 4 dígitos (ex: '1102').")
        return v

    @validates("grupo")
    def _validate_grupo(self, key: str, value: str) -> str:
        v = (value or "").strip()
        if len(v) != 4 or not v.endswith("xxx"):
            raise ValueError("grupo inválido: esperado formato '1xxx', '7xxx', etc.")
        if v[0] not in "123567":
            # 4xxx existe em alguns contextos, mas na prática fiscal comum é menos usado.
            # você pode liberar se quiser.
            raise ValueError("grupo inválido: primeiro dígito esperado em {1,2,3,5,6,7}.")
        return v

    @classmethod
    def grupo_from_cfop(cls, cfop: str) -> str:
        c = (cfop or "").strip()
        if not _CFOP_RE.match(c):
            raise ValueError("CFOP inválido para derivar grupo.")
        return f"{c[0]}xxx"

    def __repr__(self) -> str:
        return f"<RefCfop cfop={self.cfop} tipo={self.tipo}>"


class RefCstPisCofins(Base):
    """
    Referência de CST PIS/COFINS.
    gera_credito é um 'helper' do sistema para regras/sugestões.
    """
    __tablename__ = "ref_cst_pis_cofins"

    cst: Mapped[str] = mapped_column(String(2), primary_key=True)  # ex: "06"
    descricao: Mapped[str] = mapped_column(String(255), nullable=False)

    # Use Boolean aqui. No MySQL vira TINYINT(1) por baixo,
    # mas evita warning de "display width" no DDL se você usar Alembic.
    gera_credito: Mapped[bool] = mapped_column(Boolean, nullable=False, server_default=text("0"))

    observacao: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)

    created_at: Mapped[datetime] = mapped_column(
        DateTime, nullable=False, server_default=text("CURRENT_TIMESTAMP")
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime,
        nullable=False,
        server_default=text("CURRENT_TIMESTAMP"),
        onupdate=datetime.utcnow,
    )

    __table_args__ = (
        Index("ix_ref_cst_gera_credito", "gera_credito"),
    )

    @validates("cst")
    def _validate_cst(self, key: str, value: str) -> str:
        v = (value or "").strip()
        if not _CST_RE.match(v):
            raise ValueError("CST inválido: esperado 2 dígitos (ex: '06').")
        return v

    def __repr__(self) -> str:
        return f"<RefCstPisCofins cst={self.cst} gera_credito={self.gera_credito}>"
