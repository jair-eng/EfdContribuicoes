from pydantic import BaseModel, Field
from typing import List, Optional, Literal, Dict, Any



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

class ConfirmarRevisaoBody(BaseModel):
    payload: Optional[ConfirmarRevisaoIn] = None

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

        # ✅ operações "sem linha" (metadados/overrides)
        "OVERRIDE_BASE_POR_CST",
        "OVERRIDE_BLOCK_M",
        "OVERRIDE_0900",
        "AJUSTE_M",
    ]

    conteudo: Optional[str] = Field(
        None,
        description="Linha SPED completa (com pipes e sem \\n). Usado em REPLACE/INSERT/DELETE."
    )

    # 🔑 linha_num 1-based do SPED (mesmo padrão do EfdRegistro.linha)
    linha_referencia: Optional[int] = None

    # ✅ idempotência/conflito
    linha_antes: Optional[str] = Field(
        None,
        description="Linha atual esperada (antes da revisão). Recomendado em REPLACE/DELETE."
    )
    linha_hash: Optional[str] = None

    # ✅ payload para overrides/ajustes (ex.: base_por_cst, ajuste exportação)
    payload: Optional[Dict[str, Any]] = Field(
        None,
        description="Payload JSON para operações que não substituem uma linha diretamente (OVERRIDE_*, AJUSTE_M)."
    )

    regra_codigo: str = Field(..., example="EXP_RESSARC_V1")

# ✅ Esta classe define o formato do JSON que o FastAPI espera receber
class ExportZipPayload(BaseModel):
    versao_ids: List[int]

class RevisaoGlobalSchema(BaseModel):
    versao_origem_id: int
    motivo_codigo: str
    filtros_origem: Dict[str, Optional[str]]
    valores_novos: Dict[str, Optional[str]]
    apontamento_id: Optional[int] = None
    limit_preview: int = 100