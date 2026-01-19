from sqlalchemy.orm import Mapped, mapped_column
from sqlalchemy import BigInteger, Integer, String, Text, DECIMAL, Boolean, Enum, ForeignKey, Index
from app.db.models.base import Base
from sqlalchemy.dialects.mysql import JSON

class EfdApontamento(Base):
    __tablename__ = "efd_apontamento"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)

    versao_id: Mapped[int] = mapped_column(Integer, ForeignKey("efd_versao.id"), nullable=False)
    registro_id: Mapped[int] = mapped_column(BigInteger, ForeignKey("efd_registro.id"), nullable=False)

    tipo: Mapped[str] = mapped_column(
        String(20),
        nullable=False,
        index=True,
    )

    codigo: Mapped[str | None] = mapped_column(String(30), nullable=True)
    descricao: Mapped[str | None] = mapped_column(Text, nullable=True)
    impacto_financeiro: Mapped[float | None] = mapped_column(DECIMAL(15, 2), nullable=True)



    prioridade: Mapped[str] = mapped_column(
        Enum("ALTA", "MEDIA", "BAIXA"),
        nullable=False,
        server_default="BAIXA",
    )
    # contexto da regra (JSON)
    meta_json: Mapped[dict | None] = mapped_column(JSON, nullable=True)

    resolvido: Mapped[bool] = mapped_column(
        Boolean,
        nullable=False,
        default=False,
        server_default="0",

    )

    __table_args__ = (
        Index("idx_versao_tipo_resolvido", "versao_id", "tipo", "resolvido"),
        Index("idx_registro", "registro_id"),
    )