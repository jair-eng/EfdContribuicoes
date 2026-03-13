from __future__ import annotations

from datetime import datetime
from sqlalchemy.orm import relationship
from app.db.models.base import Base

from sqlalchemy import (
    CHAR,
    Column,
    DateTime,
    ForeignKey,
    Index,
    BigInteger,
    Integer,
    Numeric,
    String,
)



class NfIcmsItem(Base):
    __tablename__ = "nf_icms_item"

    id = Column(BigInteger, primary_key=True, autoincrement=True)

    nf_icms_base_id = Column(
        BigInteger,
        ForeignKey("nf_icms_base.id", ondelete="CASCADE", onupdate="CASCADE"),
        nullable=False,
        index=True,
    )

    empresa_id = Column(Integer, ForeignKey("empresa.id"), nullable=False, index=True)
    periodo = Column(CHAR(6), nullable=False)  # YYYYMM
    chave_nfe = Column(CHAR(44), nullable=False)

    num_item = Column(String(10), nullable=True)
    cod_item = Column(String(60), nullable=True)
    cod_item_norm = Column(String(60), nullable=True)

    descricao = Column(String(255), nullable=True)
    ncm = Column(String(20), nullable=True)
    cfop = Column(String(10), nullable=True)

    qtd = Column(Numeric(15, 4), nullable=True)
    unid = Column(String(20), nullable=True)
    cst_icms = Column(String(10), nullable=True)
    aliq_icms = Column(Numeric(15, 4), nullable=True)

    participante_cnpj = Column(String(14), nullable=True)
    participante_nome = Column(String(255), nullable=True)

    vl_item = Column(Numeric(15, 2), nullable=False, default=0)
    vl_desc = Column(Numeric(15, 2), nullable=False, default=0)
    vl_icms = Column(Numeric(15, 2), nullable=False, default=0)
    vl_ipi = Column(Numeric(15, 2), nullable=False, default=0)
    contabil = Column(Numeric(15, 2), nullable=False, default=0)

    origem_item = Column(String(50), nullable=True)
    nome_arquivo = Column(String(255), nullable=True)

    created_at = Column(DateTime, nullable=False, default=datetime.utcnow)
    updated_at = Column(
        DateTime,
        nullable=False,
        default=datetime.utcnow,
        onupdate=datetime.utcnow,
    )

    base = relationship("NfIcmsBase", back_populates="items")
    empresa = relationship("Empresa")

    __table_args__ = (
        Index("idx_nf_icms_item_emp_per", "empresa_id", "periodo"),
        Index("idx_nf_icms_item_chave", "chave_nfe"),
        Index("idx_nf_icms_item_chave_cod", "chave_nfe", "cod_item"),
        Index("idx_nf_icms_item_chave_codnorm", "chave_nfe", "cod_item_norm"),
        Index("idx_nf_icms_item_chave_numitem", "chave_nfe", "num_item"),
        Index("idx_nf_icms_item_chave_cfop", "chave_nfe", "cfop"),
        Index("idx_nf_icms_item_ncm", "ncm"),
    )