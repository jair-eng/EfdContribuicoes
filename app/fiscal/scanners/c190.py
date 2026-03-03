from typing import Sequence
from app.db.models import EfdRegistro
from app.fiscal.dto import RegistroFiscalDTO


def montar_c190_agg(registros_db: Sequence[EfdRegistro]) -> RegistroFiscalDTO | None:
    c190s = [r for r in registros_db if (r.reg or "").strip() == "C190"]
    if not c190s:
        return None

    # pega o primeiro C190 por id só pra ancorar linha/id do DTO
    anchor_reg = min(c190s, key=lambda r: int(r.id))

    itens = []
    for r in c190s:
        dados = (r.conteudo_json or {}).get("dados") or []
        if len(dados) >= 6:
            itens.append({
                "cst": dados[0],
                "cfop": dados[1],
                "vl_opr": dados[3],
                "vl_icms": dados[5],
                "linha": int(getattr(r, "linha", 0) or 0),  # opcional, mas ajuda no top3
            })

    if not itens:
        return None

    return RegistroFiscalDTO(
        id=int(anchor_reg.id),
        reg="C190_AGG",
        linha=int(getattr(anchor_reg, "linha", 0) or 0),
        dados=itens,
    )
