from datetime import datetime
from sqlalchemy import Column, Integer, ForeignKey, DateTime, Text, Enum
from sqlalchemy.orm import relationship
from sqlalchemy import ForeignKey
from sqlalchemy.orm import Mapped, mapped_column
from app.db.models.base import Base


class EfdVersao(Base):
    __tablename__ = "efd_versao"

    id = Column(Integer, primary_key=True, autoincrement=True)
    arquivo_id = Column(Integer, ForeignKey("efd_arquivo.id"), nullable=False)

    numero = Column(Integer, nullable=False, default=1)
    data_geracao = Column(DateTime, default=datetime.utcnow)
    observacao = Column(Text)

    status = Column(Enum("GERADA", "EM_REVISAO", "VALIDADA", "EXPORTADA"), default="GERADA")
    retifica_de_versao_id: Mapped[int | None] = mapped_column(
        ForeignKey("efd_versao.id"),
        nullable=True,
    )

    arquivo = relationship("EfdArquivo", back_populates="versoes")
    registros = relationship("EfdRegistro", back_populates="versao")
