from __future__ import annotations

from app.fiscal.regras.Autocorrigivel.cafe import aplicar_correcao_ind_cafe_cst51

from typing import Callable, Optional, Dict, Any
from app.fiscal.constants import DOM_CAFE, DOM_AGRO, DOM_SUP, DOM_POSTO, DOM_GERAL


CorrecaoFn = Callable[..., Dict[str, Any]]

CORRECOES_POR_DOMINIO: dict[str, dict[str, CorrecaoFn]] = {
    DOM_CAFE: {
        "IND_CAFE_V1": aplicar_correcao_ind_cafe_cst51,
    },
    DOM_AGRO: {
        # "IND_AGRO_V1": aplicar_correcao_ind_agro_cst51,
    },
    DOM_SUP: {},
    DOM_POSTO: {},
    DOM_GERAL: {},
}

def get_correcao_por_dominio_e_codigo(dominio: Optional[str], codigo: str) -> Optional[CorrecaoFn]:
    dom = (dominio or "").strip() or DOM_GERAL
    cod = (codigo or "").strip()
    if not cod:
        return None

    # 1) tenta no domínio específico
    fn = (CORRECOES_POR_DOMINIO.get(dom, {}) or {}).get(cod)
    if fn:
        return fn

    # 2) fallback no geral (se você quiser permitir correções globais)
    return (CORRECOES_POR_DOMINIO.get(DOM_GERAL) or {}).get(cod)