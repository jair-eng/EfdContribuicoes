from __future__ import annotations

from typing import Optional, List, Dict
from app.sped.bloco_9.bloco9 import calcular_bloco9
from app.sped.blocoM.blocoM import calcular_blocoM
from app.sped.logic.consolidador import obter_conteudo_final
from app.sped.blocoM.m_utils import (
    _reg_of_obj,
    _clean_sped_line,
    _reg_of_line,
    _ensure_line,
)

BLOCO9_REGS = {"9001", "9900", "9990", "9999"}

print("🔥 ARQUIVO WRITER.PY CARREGADO PELO SISTEMA")



def gerar_sped(
    registros,
    destino_arquivo: str,
    *,
    newline: Optional[str] = None,
    bloco_m_override: Optional[List[str]] = None,
    bloco_1_override: Optional[List[str]] = None,
) -> None:
    """
    Writer determinístico:
      - Monta buckets por bloco e garante ordem: 0..I, depois M, depois P, depois 1, depois 9.
      - Se bloco_m_override existir, ignora qualquer M* vindo dos registros e usa override.
      - Se bloco_1_override existir, sobrescreve o bucket "1" inteiro com o override (pode conter 1001/1100/1500/1990 etc).
      - Recalcula sempre Bloco 9 ao final.
    """
    nl = newline if newline in ("\n", "\r\n") else "\r\n"

    # 1) Ordena por linha (quando existir)
    try:
        registros.sort(key=lambda x: getattr(x, "linha", 0))
    except Exception:
        pass

    # 2) Separa linhas em buckets por bloco (ordem determinística)
    #    Isso evita 100% o bug do 1001 aparecer antes do M001.
    ordem_blocos = ["0", "A", "C", "D", "E", "F", "G", "H", "I", "M", "P", "1"]
    buckets: Dict[str, List[str]] = {b: [] for b in ordem_blocos}

    corpo_bloco_m: List[str] = []  # M* vindo do arquivo (fallback) quando não houver override

    for r in registros:
        # --- suporta tanto EfdRegistro quanto str ---
        if isinstance(r, str):
            linha = _clean_sped_line(r)
            if not linha:
                continue
            reg_line = _reg_of_line(linha)
            if not reg_line or reg_line == "IGNORAR":
                continue
            reg_obj = reg_line
        else:
            reg_obj = _reg_of_obj(r)

            # drop bloco 9 e marcadores que sempre recalculamos
            if reg_obj in {"9001", "9900", "9990", "9999", "M001", "M990", "0990", "IGNORAR"}:
                continue

            # filtro PF (segurança)
            if reg_obj == "C170" and getattr(r, "is_pf", False) is True:
                continue

            linha = obter_conteudo_final(r)
            linha = _clean_sped_line(linha)
            if not linha:
                continue

            reg_line = _reg_of_line(linha)
            if not reg_line or reg_line == "IGNORAR":
                continue

        # ✅ Se veio override, ignoramos QUALQUER M* que esteja nos registros
        if bloco_m_override is not None and reg_obj.startswith("M"):
            continue

        # Se for M* e não tem override, vai pro corpo_bloco_m (para calcular_blocoM)
        if reg_obj.startswith("M"):
            corpo_bloco_m.append(linha)
            continue

        bloco = reg_line[0].upper()
        if bloco in buckets:
            buckets[bloco].append(linha)


    # ✅ aplica override do bloco 1 UMA única vez (fora do loop)
    # (permite incluir 1100/1500/etc dentro do mesmo bloco_1_override)
    if bloco_1_override is not None:
        bloco1: List[str] = []
        for l in bloco_1_override:
            ln = _ensure_line(l)
            ln = _clean_sped_line(ln)
            if ln:
                bloco1.append(ln)
        buckets["1"] = bloco1

    # 3) Monta saída: 0..I, depois M, depois P, depois 1, depois 9
    linhas_finais: List[str] = []

    # 3.1) blocos antes do M (0..I)
    for b in ordem_blocos:
        if b == "M":
            break
        linhas_finais.extend(buckets[b])

    # 3.2) bloco M (override > fallback)
    if bloco_m_override is not None:
        bloco_m_completo = [
            _clean_sped_line(x)
            for x in (bloco_m_override or [])
            if _clean_sped_line(x)
        ]
    else:
        bloco_m_completo = calcular_blocoM(corpo_bloco_m) if corpo_bloco_m else []
        bloco_m_completo = [
            _clean_sped_line(x)
            for x in (bloco_m_completo or [])
            if _clean_sped_line(x)
        ]

    linhas_finais.extend(bloco_m_completo)

    # 3.3) bloco P e 1 SEMPRE depois do M
    linhas_finais.extend(buckets.get("P", []))
    linhas_finais.extend(buckets.get("1", []))

    # 3.4) bloco 9 SEMPRE no final (recalculado)
    bloco9 = calcular_bloco9(linhas_finais)
    for l9 in bloco9:
        l9c = _clean_sped_line(l9)
        if l9c:
            linhas_finais.append(l9c)

    # 4) Escreve
    with open(destino_arquivo, "w", encoding="iso-8859-1", errors="replace", newline="") as f:
        for linha in linhas_finais:
            f.write(linha + nl)



