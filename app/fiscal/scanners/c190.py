from typing import Sequence, Any, Dict, List
from app.db.models import EfdRegistro
from app.fiscal.dto import RegistroFiscalDTO

def montar_c190_agg(registros_db: Sequence[EfdRegistro]) -> RegistroFiscalDTO | None:
    c190s = [r for r in registros_db if (r.reg or "").strip() == "C190"]
    if not c190s:
        return None

    anchor = min(c190s, key=lambda r: r.id)

    itens = []
    for r in c190s:
        dados = (r.conteudo_json or {}).get("dados") or []
        if len(dados) >= 4:
            itens.append({
                "cst": dados[0],
                "cfop": dados[1],
                "vl_opr": dados[3],
            })

    if not itens:
        return None

    return RegistroFiscalDTO(
        id=int(anchor.id),
        reg="C190_AGG",
        linha=int(anchor.linha),
        dados=itens,
    )
