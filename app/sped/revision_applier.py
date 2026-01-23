from __future__ import annotations
from dataclasses import dataclass
from typing import Iterable, List, Literal, Optional, Tuple

Op = Literal["REPLACE_LINE", "DELETE", "INSERT_BEFORE", "INSERT_AFTER"]


@dataclass(frozen=True)
class _Action:
    # posição 0-based na lista base
    pos: int
    op: Op
    content: Optional[str]
    # tie-breaker estável
    order: int


def apply_revisions(lines: List[str], revisions: Iterable[dict]) -> List[str]:
    """
    Aplica revisões sobre uma lista de linhas (SEM \n no fim).
    `revisions` pode ser lista de RevisaoFiscal.model_dump() ou dict compatível.
    `linha_referencia` é 1-based (padrão humano/SPED).
    """
    n = len(lines)
    actions: List[_Action] = []

    # 1) normaliza ações -> 0-based pos
    for idx, r in enumerate(revisions):
        op: Op = r["operacao"]
        ref_1b = int(r["linha_referencia"])
        pos = ref_1b - 1

        if pos < 0 or pos >= n:
            raise ValueError(f"linha_referencia fora do range: {ref_1b} (1..{n})")

        content = r.get("conteudo", None)

        if op in ("REPLACE_LINE", "INSERT_BEFORE", "INSERT_AFTER") and not content:
            raise ValueError(f"{op} exige conteudo (linha {ref_1b})")

        if op == "DELETE":
            content = None

        actions.append(_Action(pos=pos, op=op, content=content, order=idx))

    if not actions:
        return lines

    # 2) separa por tipo para evitar efeito cascata:
    #    - REPLACE e DELETE não mudam índices "base" se aplicados via mapa
    #    - INSERTs são aplicados no final, com offsets calculados por posição
    replaces = [a for a in actions if a.op == "REPLACE_LINE"]
    deletes  = [a for a in actions if a.op == "DELETE"]
    inserts  = [a for a in actions if a.op in ("INSERT_BEFORE", "INSERT_AFTER")]

    # 3) aplica REPLACE (último ganha se houver conflito na mesma linha)
    #    determinístico: ordem natural (order)
    replaced = list(lines)
    for a in sorted(replaces, key=lambda x: (x.pos, x.order)):
        replaced[a.pos] = a.content or replaced[a.pos]

    # 4) aplica DELETE (se deletar uma linha, ela some)
    #    se houver DELETE e REPLACE no mesmo pos, DELETE vence (mais seguro)
    delete_positions = set(a.pos for a in deletes)

    kept_with_pos: List[Tuple[int, str]] = []
    for i, line in enumerate(replaced):
        if i not in delete_positions:
            kept_with_pos.append((i, line))

    # 5) prepara INSERTs (ancorados no índice BASE, antes das deleções)
    #    Para inserir em linha deletada: ainda usamos a posição base como âncora.
    #    Ordem determinística:
    #      - INSERT_BEFORE vem antes de INSERT_AFTER no mesmo pos
    #      - respeita order
    def insert_sort_key(a: _Action):
        before_first = 0 if a.op == "INSERT_BEFORE" else 1
        return (a.pos, before_first, a.order)

    inserts_sorted = sorted(inserts, key=insert_sort_key)

    # 6) monta um "banco" de inserts por pos
    ins_before: dict[int, List[str]] = {}
    ins_after: dict[int, List[str]] = {}
    for a in inserts_sorted:
        if a.op == "INSERT_BEFORE":
            ins_before.setdefault(a.pos, []).append(a.content or "")
        else:
            ins_after.setdefault(a.pos, []).append(a.content or "")

    # 7) reconstrói lista final caminhando pelas linhas mantidas
    #    usando a pos BASE original como chave de inserção.
    out: List[str] = []
    for base_pos, line in kept_with_pos:
        # inserts BEFORE do base_pos
        if base_pos in ins_before:
            out.extend(ins_before[base_pos])

        out.append(line)

        # inserts AFTER do base_pos
        if base_pos in ins_after:
            out.extend(ins_after[base_pos])

    return out
