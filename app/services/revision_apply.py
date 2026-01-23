from __future__ import annotations
from typing import List, Any, Dict
from copy import deepcopy


def aplicar_revisoes(registros: List[Any], revisoes: List[Any]) -> List[Any]:
    """
    Aplica revisões sobre uma lista de EfdRegistro (origem),
    usando linha_referencia como âncora (opção 2).

    Suporta:
      - REPLACE_LINE
      - INSERT_AFTER

    Retorna NOVA lista (não altera a original).
    """

    # 1) mapa linha -> revisão (última vence)
    replace_map: Dict[int, str] = {}
    inserts_after: Dict[int, List[str]] = {}

    for rv in revisoes:
        acao = str(getattr(rv, "acao", "") or "")
        payload = dict(getattr(rv, "revisao_json", {}) or {})

        linha_ref = payload.get("linha_referencia")
        if not linha_ref:
            continue

        linha_ref = int(linha_ref)
        linha_nova = (payload.get("linha_nova") or "").strip()
        if not linha_nova:
            continue

        if acao == "REPLACE_LINE":
            replace_map[linha_ref] = linha_nova

        elif acao == "INSERT_AFTER":
            inserts_after.setdefault(linha_ref, []).append(linha_nova)

    if not replace_map and not inserts_after:
        return registros

    # 2) aplica em ordem de linhas
    saida: List[Any] = []

    for r in registros:
        r_linha = int(getattr(r, "linha"))

        # REPLACE_LINE
        if r_linha in replace_map:
            r2 = deepcopy(r)
            cj = dict(getattr(r2, "conteudo_json") or {})
            cj["raw"] = replace_map[r_linha]
            r2.conteudo_json = cj
            saida.append(r2)
        else:
            saida.append(r)

        # INSERT_AFTER
        if r_linha in inserts_after:
            for linha_ins in inserts_after[r_linha]:
                saida.append(linha_ins)  # linha crua, writer aceita

    return saida