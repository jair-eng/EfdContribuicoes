from __future__ import annotations
from datetime import datetime
from sqlalchemy import Column, Integer, String, ForeignKey, DateTime, Enum, CHAR
from sqlalchemy.orm import relationship
from app.db.models.base import Base

class EfdArquivo(Base):
    __tablename__ = "efd_arquivo"

    id = Column(Integer, primary_key=True, autoincrement=True)
    empresa_id = Column(Integer, ForeignKey("empresa.id"), nullable=False)

    nome_arquivo = Column(String(255))
    periodo = Column(CHAR(6), nullable=False)  # YYYYMM
    data_upload = Column(DateTime, default=datetime.utcnow)

    line_ending = Column(Enum("LF", "CRLF"), nullable=False, default="LF")

    status = Column(Enum("ORIGINAL", "EM_REVISAO", "CORRIGIDO"), default="ORIGINAL")

    empresa = relationship("Empresa", back_populates="arquivos")
    versoes = relationship("EfdVersao", back_populates="arquivo")
