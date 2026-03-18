from __future__ import annotations

from datetime import datetime

from sqlalchemy import (
    CHAR,
    Column,
    Date,
    DateTime,
    ForeignKey,
    Index,
    BigInteger,
    Integer,
    Numeric,
    String,
    UniqueConstraint,
)
from sqlalchemy.orm import relationship

from app.db.models.base import Base


class NfIcmsBase(Base):
    __tablename__ = "nf_icms_base"

    id = Column(BigInteger, primary_key=True, autoincrement=True)

    empresa_id = Column(Integer, ForeignKey("empresa.id"), nullable=False, index=True)
    periodo = Column(CHAR(6), nullable=False)  # YYYYMM

    chave_nfe = Column(CHAR(44), nullable=False)
    dt_doc = Column(Date, nullable=True)

    num_doc = Column(String(20), nullable=True)
    serie = Column(String(10), nullable=True)

    vl_doc = Column(Numeric(15, 2), nullable=True)
    vl_icms = Column(Numeric(15, 2), nullable=False, default=0)

    fonte = Column(String(20), nullable=False, default="EFD_ICMS_IPI")
    nome_arquivo = Column(String(255), nullable=True)

    cod_part = Column(String(60), nullable=True, index=True)

    participante_nome = Column(String(255), nullable=True)
    participante_cod_pais = Column(String(10), nullable=True)

    participante_cnpj = Column(String(14), nullable=True, index=True)
    participante_cpf = Column(String(11), nullable=True, index=True)
    participante_ie = Column(String(30), nullable=True)

    participante_cod_mun = Column(String(10), nullable=True)
    participante_suframa = Column(String(30), nullable=True)

    participante_end = Column(String(255), nullable=True)
    participante_num = Column(String(20), nullable=True)
    participante_compl = Column(String(255), nullable=True)
    participante_bairro = Column(String(100), nullable=True)

    created_at = Column(DateTime, nullable=False, default=datetime.utcnow)
    updated_at = Column(
        DateTime,
        nullable=False,
        default=datetime.utcnow,
        onupdate=datetime.utcnow,
    )

    empresa = relationship("Empresa")
    items = relationship(
        "NfIcmsItem",
        back_populates="base",
        cascade="all, delete-orphan",
        passive_deletes=True,
    )

    __table_args__ = (
        UniqueConstraint(
            "empresa_id",
            "periodo",
            "chave_nfe",
            name="ux_nf_icms_base_emp_per_chave",
        ),
        Index("idx_nf_icms_base_periodo", "periodo"),
        Index("idx_nf_icms_base_chave", "chave_nfe"),
        Index("idx_nf_icms_base_empresa_periodo", "empresa_id", "periodo"),
        Index("idx_nf_icms_base_empresa_dt", "empresa_id", "dt_doc"),
    )