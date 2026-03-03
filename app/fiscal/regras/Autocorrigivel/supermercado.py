
from sqlalchemy.orm import Session
from typing import  Optional
from app.fiscal.regras.Autocorrigivel.shared import aplicar_correcao_sup_por_grupos_ncm_cst51_hibrido
def aplicar_correcao_sup_embalagens_cst51_hibrido(
    db: Session, *,
    versao_origem_id: int,
    empresa_id: Optional[int] = None,
    apontamento_id: Optional[int] = None,
):
    return aplicar_correcao_sup_por_grupos_ncm_cst51_hibrido(
        db,
        versao_origem_id=versao_origem_id,
        empresa_id=empresa_id,
        incluir_revenda=True,
        grupos_ncm=[
            "NCM_EMBALAGEM_39",  # plásticos
            "NCM_EMBALAGEM_48",  # papel/papelão
            "NCM_EMBALAGEM_63",  # big bag / sacaria
            "NCM_EMBALAGEM_73",  # ferro/aço
            "NCM_EMBALAGEM_44",  # paletes de madeira
        ],
        apontamento_id=apontamento_id,
        motivo_codigo="EMB_INSUMO_V1",
    )



def aplicar_correcao_sup_limpeza_cst51_hibrido(
    db: Session, *,
    versao_origem_id: int,
    empresa_id: Optional[int] = None,
    apontamento_id: Optional[int] = None,
):
    return aplicar_correcao_sup_por_grupos_ncm_cst51_hibrido(
        db,
        versao_origem_id=versao_origem_id,
        empresa_id=empresa_id,
        incluir_revenda=True,
        grupos_ncm=["SUP_NCM_HIGIENE_LIMPEZA"],
        apontamento_id=apontamento_id,
        motivo_codigo="SUP_LIMPEZA_INSUMO_V1",
    )
