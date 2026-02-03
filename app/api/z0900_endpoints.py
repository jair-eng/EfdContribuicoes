from app.services.revisao_override_0900_service import salvar_override_0900
from app.sped.bloco_0.bloco_0_0900 import gerar_0900, _calcular_receita_bruta
from sqlalchemy.orm import Session
from datetime import date

def resolver_0900(
    db: Session,
    *,
    versao_origem_id: int,
    parsed: list[dict],
    linhas_sped: list[str],
    periodo_yyyymm: int,
    apontamento_id: int,
):
    receita = _calcular_receita_bruta(
        parsed=parsed,
        linhas_sped=linhas_sped,
    )

    linha = gerar_0900(
        data_ref=date.today(),
        receita_bruta=receita,
    )

    salvar_override_0900(
        db,
        versao_origem_id=versao_origem_id,
        linha_0900=linha,
        motivo_codigo="BL0_0900_OBRIGATORIO",
        apontamento_id=apontamento_id,
    )
