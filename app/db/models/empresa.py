from sqlalchemy import Column, Integer, String
from sqlalchemy.orm import relationship
from app.db.models.base import Base
from sqlalchemy import Enum

from app.fiscal.constants import DOMINIOS_VALIDOS, DOM_GERAL


class Empresa(Base):
    __tablename__ = "empresa"

    id = Column(Integer, primary_key=True, autoincrement=True)
    razao_social = Column(String(255))
    cnpj = Column(String(14), unique=True)

    dominio = Column(
        Enum(*DOMINIOS_VALIDOS, name="empresa_dominio_enum"),
        nullable=False,
        default=DOM_GERAL,
    )

    arquivos = relationship("EfdArquivo", back_populates="empresa")