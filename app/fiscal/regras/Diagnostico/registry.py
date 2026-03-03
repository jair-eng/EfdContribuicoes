from app.fiscal.regras.Diagnostico.regra_exportacao import RegraExportacaoRessarcimentoV1

# instâncias (simples) ou classes (se preferir lazy)
_RULES = {
    "EXP_RESSARC_V1": RegraExportacaoRessarcimentoV1(),

}

def get_regra_por_codigo(codigo: str):
    return _RULES.get((codigo or "").strip())
