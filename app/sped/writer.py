from __future__ import annotations
from typing import Optional
from app.sped.bloco9 import calcular_bloco9
from app.sped.formatter import formatar_linha

BLOCO9_REGS = {"9900", "9990", "9999"}

def gerar_sped(registros, destino_arquivo: str, *, newline: Optional[str] = None) -> None:
    """
    Gera arquivo SPED mantendo padrão e recalculando Bloco 9 (sem duplicar).

    Estratégia robusta:
      1) remove qualquer 9900/9990/9999 do conjunto base
      2) formata e escreve somente a base
      3) recalcula e escreve um único Bloco 9 no final

    newline:
      - None -> usa "\\n"
      - "\\n" -> LF
      - "\\r\\n" -> CRLF
    """
    nl = newline if newline in ("\n", "\r\n") else "\n"

    # 1) Base sem Bloco 9 (evita duplicação)
    base = [r for r in registros if str(getattr(r, "reg", "")).strip() not in BLOCO9_REGS]

    # 2) Formata linhas base
    linhas: list[str] = []
    for r in base:
        conteudo = getattr(r, "conteudo_json", None) or {}
        campos = conteudo.get("dados", []) or []
        linha = formatar_linha(str(r.reg), campos)

        # segurança: garante pipe final
        if not linha.endswith("|"):
            linha += "|"

        linhas.append(linha)

    # 3) Recalcula Bloco 9 a partir da BASE (não do original com 9900/9990/9999)
    bloco9 = calcular_bloco9(base)

    # segurança: garante pipe final também no bloco9
    bloco9_ok = []
    for linha in bloco9:
        if not linha.endswith("|"):
            linha += "|"
        bloco9_ok.append(linha)

    linhas.extend(bloco9_ok)

    # 4) Escreve truncando, sem tradução automática de newline
    # Sugestão: usar errors="strict" para detectar problemas cedo.
    # Se você preferir tolerância máxima, use "replace" (mas eu evitaria "ignore").
    with open(destino_arquivo, "w", encoding="latin-1", errors="strict", newline="") as f:
        for linha in linhas:
            f.write(linha + nl)

