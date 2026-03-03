from __future__ import annotations
from datetime import datetime
from typing import Optional, Any, Dict
from sqlalchemy import (
    Column, Integer, String, DateTime, ForeignKey, Enum, Index)
from sqlalchemy.dialects.mysql import JSON as MySQLJSON
from sqlalchemy.orm import relationship
from app.db.models.base import Base


class EfdRevisao(Base):
    __tablename__ = "efd_revisao"

    id = Column(Integer, primary_key=True, autoincrement=True)

    # versão original (imutável) e versão revisada (derivada)
    versao_origem_id = Column(Integer, ForeignKey("efd_versao.id"), nullable=False, index=True)
    versao_revisada_id = Column(Integer, ForeignKey("efd_versao.id"), nullable=True, index=True)

    # aponta o registro que será afetado (na prática, do arquivo original)
    registro_id = Column(Integer, ForeignKey("efd_registro.id"), nullable=True, index=True)

    # redundâncias úteis para filtro/relatório (não dependem de JSON)
    reg = Column(String(10), nullable=True)

    # MVP: começamos só com substituir linha inteira
    acao = Column(
        Enum("REPLACE_LINE","INSERT_AFTER","INSERT_BEFORE","DELETE","OVERRIDE_BLOCK_M","OVERRIDE_0900","OVERRIDE_BASE_POR_CST","AJUSTE_M", name="efd_revisao_acao"),
        nullable=False,
        default="REPLACE_LINE",
    )

    # payload da revisão (patch). ex:
    # {"linha_nova": "|C170|...|", "motivo": "...", ...}
    revisao_json = Column(MySQLJSON, nullable=False)

    # rastreabilidade: regra/apontamento que originou a revisão
    motivo_codigo = Column(String(50), nullable=True)
    apontamento_id = Column(Integer, ForeignKey("efd_apontamento.id"), nullable=True, index=True)

    created_at = Column(DateTime, nullable=False, default=datetime.utcnow)

    # Relationships (opcional, mas útil)
    versao_origem = relationship("EfdVersao", foreign_keys=[versao_origem_id])
    versao_revisada = relationship("EfdVersao", foreign_keys=[versao_revisada_id])
    registro = relationship("EfdRegistro", foreign_keys=[registro_id])
    apontamento = relationship("EfdApontamento", foreign_keys=[apontamento_id])

    __table_args__ = (
        Index("ix_efd_revisao_origem_dest", "versao_origem_id", "versao_revisada_id"),
    )
