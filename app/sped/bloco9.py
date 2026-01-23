from __future__ import annotations
from collections import Counter
from typing import Iterable, List
from app.db.models import EfdRegistro

BLOCO9_REGS = {"9001", "9900", "9990", "9999"}

def calcular_bloco9(registros: Iterable) -> List[str]:

    def _reg_from_line(line: str) -> str:
        if not line.startswith("|"):
            return ""
        parts = line.split("|")
        return (parts[1] or "").strip() if len(parts) > 2 else ""

    regs_base = []
    for r in registros:
        if isinstance(r, str):
            regs_base.append(_reg_from_line(r))
        else:
            regs_base.append((getattr(r, "reg", "") or "").strip())

    # base sem bloco 9 (recalcula sempre)
    regs_sem_bloco9 = [reg for reg in regs_base if reg and reg not in BLOCO9_REGS]

    contador = Counter(regs_sem_bloco9)
    tipos = sorted(contador.keys())

    linhas_9900: List[str] = []
    for reg in tipos:
        linhas_9900.append(f"|9900|{reg}|{contador[reg]}|")

    # 9900 precisa conter também os registros do próprio bloco 9
    qtd_linhas_9900 = len(linhas_9900) + 4  # 9001, 9900, 9990, 9999

    linhas_9900.append("|9900|9001|1|")
    linhas_9900.append(f"|9900|9900|{qtd_linhas_9900}|")
    linhas_9900.append("|9900|9990|1|")
    linhas_9900.append("|9900|9999|1|")

    # total de linhas do bloco 9 = 9001 + 9900s + 9990 + 9999
    total_bloco9 = 1 + len(linhas_9900) + 2
    linha_9990 = f"|9990|{total_bloco9}|"

    # total geral = base + bloco9 (inclui 9999)
    total_geral = len(regs_sem_bloco9) + total_bloco9
    linha_9999 = f"|9999|{total_geral}|"

    return ["|9001|0|"] + linhas_9900 + [linha_9990, linha_9999]