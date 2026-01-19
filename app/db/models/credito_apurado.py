from datetime import datetime
from sqlalchemy import Column, Integer, ForeignKey, DateTime, DECIMAL, CHAR, String
from app.db.models.base import Base

class CreditoApurado(Base):
    __tablename__ = "credito_apurado"

    id = Column(Integer, primary_key=True, autoincrement=True)
    empresa_id = Column(Integer, ForeignKey("empresa.id"), nullable=False)
    periodo = Column(CHAR(6))
    tipo = Column(String(50))
    valor = Column(DECIMAL(15, 2))
    data_calculo = Column(DateTime, default=datetime.utcnow)
