from app.fiscal.regras.base_regras import RegraBase
from decimal import Decimal

def build_ctx_exportacao(*, linhas_sped: list[str], meta: dict) -> dict:
    credito_total = RegraBase.dec_any(meta.get("credito_total")) or RegraBase.dec_any(meta.get("impacto_consolidado"))
    soma_m = RegraBase.dec_any(meta.get("soma_valores_bloco_m")) or Decimal("0")

    tem_apuracao_m = RegraBase.to_bool(meta.get("tem_apuracao_m"))
    bloco_m_zerado = RegraBase.to_bool(meta.get("bloco_m_zerado"))
    if soma_m == Decimal("0") and tem_apuracao_m:
        bloco_m_zerado = True

    return {
        "linhas_sped": linhas_sped,
        "credito_total": credito_total,
        "tem_apuracao_m": tem_apuracao_m,
        "bloco_m_zerado": bloco_m_zerado,
        "soma_valores_bloco_m": soma_m,
        "fonte": meta.get("fonte"),
    }
