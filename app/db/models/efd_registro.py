from sqlalchemy import Column, Integer, BigInteger, ForeignKey, Boolean, CHAR, String, Numeric
from sqlalchemy.dialects.mysql import JSON
from sqlalchemy.orm import relationship
from app.db.models.base import Base

class EfdRegistro(Base):
    __tablename__ = "efd_registro"

    id = Column(BigInteger, primary_key=True, autoincrement=True)
    versao_id = Column(Integer, ForeignKey("efd_versao.id"), nullable=False)

    linha = Column(Integer, nullable=False)
    reg = Column(CHAR(4), nullable=False)
    conteudo_json = Column(JSON, nullable=False)

    alterado = Column(Boolean, default=False)

    # Campos “extras” que você quer usar no consolidado (opcional):
    base_credito = Column(Numeric(15, 2), default=0)
    valor_credito = Column(Numeric(15, 2), default=0)
    tipo_credito = Column(String(50))

    versao = relationship("EfdVersao", back_populates="registros")
