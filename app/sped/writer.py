from __future__ import annotations
from typing import Optional
from app.sped.bloco9 import calcular_bloco9
from app.sped.formatter import formatar_linha

BLOCO9_REGS = {"9001", "9900", "9990", "9999"}

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
    def _reg_of(item) -> str:
        if isinstance(item, str):
            parts = item.split("|")
            return parts[1].strip() if len(parts) > 2 else ""
        return str(getattr(item, "reg", "") or "").strip()

    base = [r for r in registros if _reg_of(r) not in BLOCO9_REGS]

    # 2) Formata linhas base
    linhas: list[str] = []
    for r in base:
        # INSERT_* pode inserir linha crua (str)
        if isinstance(r, str):
            linha = r
        else:
            conteudo = getattr(r, "conteudo_json", None) or {}

            # revisão REPLACE_LINE tem prioridade
            raw = conteudo.get("raw")
            if raw:
                linha = str(raw)
            else:
                campos = conteudo.get("dados", []) or []
                linha = formatar_linha(str(r.reg), campos)

        # normaliza e garante pipe final
        linha = (linha or "").rstrip("\r\n")
        if not linha.endswith("|"):
            linha += "|"

        linhas.append(linha)

    # 3) Recalcula Bloco 9 a partir da BASE
    bloco9 = calcular_bloco9(base)

    bloco9_ok = []
    for linha in bloco9:
        linha = (linha or "").rstrip("\r\n")
        if not linha.endswith("|"):
            linha += "|"
        bloco9_ok.append(linha)

    linhas.extend(bloco9_ok)

    # 4) Escrita FINAL — UTF-8 explícito
    # errors="strict" é bom: se quebrar, revela bug real cedo

    with open(destino_arquivo, "w", encoding="utf-8", errors="strict", newline="") as f:
        for linha in linhas:
            f.write(linha)
            f.write(nl)


