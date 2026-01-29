from __future__ import annotations
from typing import Optional, List

from app.sped.bloco9 import calcular_bloco9
from app.sped.blocoM import calcular_blocoM
from app.sped.logic.consolidador import obter_conteudo_final
from app.sped.m_utils import _reg_of_obj, _clean_sped_line, _reg_of_line

BLOCO9_REGS = {"9001", "9900", "9990", "9999"}
REGS_RECALCULO = BLOCO9_REGS.union({"M001", "M990"})

print("🔥 ARQUIVO WRITER.PY CARREGADO PELO SISTEMA")


def gerar_sped(
    registros,
    destino_arquivo: str,
    *,
    newline: Optional[str] = None,
    bloco_m_override: Optional[List[str]] = None,
) -> None:
    nl = newline if newline in ("\n", "\r\n") else "\r\n"

    # 1) Ordena por linha (quando existir)
    try:
        registros.sort(key=lambda x: getattr(x, "linha", 0))
    except Exception:
        pass

    # 2) Separa linhas em buckets por bloco (ordem determinística)
    #    Isso evita 100% o bug do 1001 aparecer antes do M001.
    ordem_blocos = ["0", "A", "C", "D", "E", "F", "G", "H", "I", "M", "1"]
    buckets: dict[str, List[str]] = {b: [] for b in ordem_blocos}

    corpo_bloco_m: List[str] = []  # M* vindo do arquivo (fallback) quando não houver override

    for r in registros:
        reg_obj = _reg_of_obj(r)

        # drop bloco 9 e marcadores que sempre recalculamos
        if reg_obj in {"9001", "9900", "9990", "9999", "M001", "M990", "IGNORAR"}:
            continue

        # filtro PF (segurança)
        if reg_obj == "C170" and getattr(r, "is_pf", False) is True:
            continue

        linha = obter_conteudo_final(r)
        linha = _clean_sped_line(linha)
        if not linha:
            continue

        # ✅ Se veio override, ignoramos QUALQUER M* que esteja nos registros
        if bloco_m_override is not None and reg_obj.startswith("M"):
            continue

        # Se for M* e não tem override, vai pro corpo_bloco_m (para calcular_blocoM)
        if reg_obj.startswith("M"):
            corpo_bloco_m.append(linha)
            continue

        # Caso geral: bucket pelo bloco baseado na LINHA (mais confiável aqui)
        reg_line = _reg_of_line(linha)  # ex: "C100", "1001"
        if not reg_line or reg_line == "IGNORAR":
            continue

        bloco = reg_line[0].upper()
        if bloco in buckets:
            buckets[bloco].append(linha)
        else:
            # Se aparecer algo fora (raro), joga no bloco "1" pra não quebrar o arquivo
            buckets["1"].append(linha)

    # 3) Monta saída: 0..I, depois M, depois 1, depois 9
    linhas_finais: List[str] = []

    # 3.1) blocos antes do M
    for b in ordem_blocos:
        if b == "M":
            break
        linhas_finais.extend(buckets[b])

    # 3.2) bloco M (override > fallback)
    if bloco_m_override is not None:
        bloco_m_completo = [
            _clean_sped_line(x) for x in (bloco_m_override or []) if _clean_sped_line(x)
        ]
    else:
        bloco_m_completo = calcular_blocoM(corpo_bloco_m) if corpo_bloco_m else []
        bloco_m_completo = [
            _clean_sped_line(x) for x in (bloco_m_completo or []) if _clean_sped_line(x)
        ]

    linhas_finais.extend(bloco_m_completo)

    # 3.3) bloco 1 SEMPRE depois do M
    linhas_finais.extend(buckets["1"])

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
