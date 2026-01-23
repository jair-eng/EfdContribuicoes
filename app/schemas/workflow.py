from pydantic import BaseModel, Field
from typing import List, Optional, Literal



class ApontamentosBatchPayload(BaseModel):
    versao_id: int
    to_resolver: List[int] = Field(default_factory=list)
    to_reabrir: List[int] = Field(default_factory=list)

class ApontamentoResolucaoIn(BaseModel):
    apontamento_id: int
    resolvido: bool

class ConfirmarRevisaoIn(BaseModel):
    to_resolver: List[int] = []
    to_reabrir: List[int] = []

class AplicarRevisaoPayload(BaseModel):
    apontamento_id: int
    linha_nova: str = Field(min_length=3)
    motivo_codigo: Optional[str] = None


class RevisaoItemPayload(BaseModel):
    apontamento_id: int
    linha_nova: str = Field(min_length=3)
    motivo_codigo: Optional[str] = None

class AplicarRevisoesEmLotePayload(BaseModel):
    itens: List[RevisaoItemPayload] = Field(default_factory=list)

class RevisaoFiscal(BaseModel):
    """
    Representa um ajuste fiscal que será aplicado
    na versão revisada do SPED.
    """
    registro_id: Optional[int] = None
    registro: Optional[str] = None

    operacao: Literal[
        "REPLACE_LINE",
        "INSERT_AFTER",
        "INSERT_BEFORE",
        "DELETE",
    ]

    conteudo: Optional[str] = Field(
        None,
        description="Linha SPED completa (com pipes e sem \\n)"
    )

    # 🔑 linha_num 1-based do SPED (mesmo padrão do EfdRegistro.linha)
    linha_referencia: Optional[int] = None

    # ✅ idempotência/conflito
    linha_antes: Optional[str] = Field(
        None,
        description="Linha atual esperada (antes da revisão). Recomendado em REPLACE/DELETE."
    )
    linha_hash: Optional[str] = None

    regra_codigo: str = Field(..., example="EXP_RESSARC_V1")