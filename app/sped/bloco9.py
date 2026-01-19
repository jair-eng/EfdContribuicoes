from __future__ import annotations
from collections import Counter
from typing import Iterable, List
from app.db.models import EfdRegistro

BLOCO9_REGS = {"9900", "9990", "9999"}

def calcular_bloco9(registros: Iterable) -> List[str]:
    """
    Calcula Bloco 9 conforme SPED:
      - 9900 inclui todos os tipos de registro do arquivo (exceto o bloco 9 original, que é recalculado)
      - adiciona sempre 9900/9990/9999
      - 9990 = total de linhas do bloco 9 (inclui 9900/9990/9999)
      - 9999 = total geral do arquivo (inclui 9999)
    """

    regs_base = [(getattr(r, "reg", "") or "").strip() for r in registros]

    # base sem bloco 9 (recalcula sempre)
    regs_sem_bloco9 = [reg for reg in regs_base if reg and reg not in BLOCO9_REGS]

    contador = Counter(regs_sem_bloco9)

    tipos = sorted(contador.keys())

    linhas_9900: List[str] = []
    for reg in tipos:
        linhas_9900.append(f"|9900|{reg}|{contador[reg]}|")

    # 9900 precisa conter também os registros do próprio bloco 9
    # qtd total de linhas 9900 = (linhas dos tipos) + (9900/9990/9999)
    qtd_linhas_9900 = len(linhas_9900) + 3

    linhas_9900.append(f"|9900|9900|{qtd_linhas_9900}|")
    linhas_9900.append("|9900|9990|1|")
    linhas_9900.append("|9900|9999|1|")

    # 9990: total de linhas do bloco 9 = linhas_9900 + 9990 + 9999
    total_bloco9 = len(linhas_9900) + 2
    linha_9990 = f"|9990|{total_bloco9}|"

    # 9999: total geral = linhas base + bloco 9 inteiro (inclui 9999)
    total_geral = len(regs_sem_bloco9) + total_bloco9
    linha_9999 = f"|9999|{total_geral}|"

    return linhas_9900 + [linha_9990, linha_9999]