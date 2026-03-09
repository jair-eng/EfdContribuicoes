from __future__ import annotations
from datetime import datetime
from sqlalchemy import (Column,Integer,String,ForeignKey,Date,DateTime,Numeric,UniqueConstraint,Index,CHAR,)
from sqlalchemy.orm import relationship
from app.db.models.base import Base


class NfIcmsBase(Base):
    __tablename__ = "nf_icms_base"

    id = Column(Integer, primary_key=True, autoincrement=True)

    empresa_id = Column(Integer, ForeignKey("empresa.id"), nullable=False)
    periodo = Column(CHAR(6), nullable=False)  # YYYYMM

    chave_nfe = Column(String(44), nullable=False)
    dt_doc = Column(Date, nullable=True)
    
    cnpj_emitente = Column(String(14), nullable=True)
    num_doc = Column(String(20), nullable=True)
    serie = Column(String(10), nullable=True)

    vl_doc = Column(Numeric(15, 2), nullable=True, default=0)
    vl_icms = Column(Numeric(15, 2), nullable=True, default=0)

    fonte = Column(String(30), nullable=True)  # ex.: EFD_ICMS_IPI
    nome_arquivo = Column(String(255), nullable=True)

    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)

    empresa = relationship("Empresa")

    __table_args__ = (
        UniqueConstraint("empresa_id", "chave_nfe", name="uq_empresa_chave"),
        Index("idx_nf_icms_base_periodo", "periodo"),
        Index("idx_nf_icms_base_chave", "chave_nfe"),
    )