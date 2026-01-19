from sqlalchemy import Column, Integer, String
from sqlalchemy.orm import relationship
from app.db.models.base import Base

class Empresa(Base):
    __tablename__ = "empresa"

    id = Column(Integer, primary_key=True, autoincrement=True)
    razao_social = Column(String(255))
    cnpj = Column(String(14), unique=True)

    arquivos = relationship("EfdArquivo", back_populates="empresa")
