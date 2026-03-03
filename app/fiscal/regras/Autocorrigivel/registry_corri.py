from app.fiscal.regras.Autocorrigivel.cafe import aplicar_correcao_ind_cafe_cst51

_CORRECOES = {
    "IND_CAFE_V1": aplicar_correcao_ind_cafe_cst51,
}

def get_correcao_por_codigo(codigo: str):
    return _CORRECOES.get((codigo or "").strip())