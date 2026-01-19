from typing import Iterable

def formatar_linha(registro: str, campos: Iterable[str]) -> str:
    """
    Gera uma linha SPED no formato oficial:
    |REG|campo1|campo2|...|

    - Não remove campos
    - Não altera valores
    - Mantém exatamente a ordem recebida
    """

    # Segurança mínima: evitar None quebrando join
    reg = (registro or "").strip()
    campos_str = ["" if c is None else str(c) for c in campos]

    return "|" + "|".join([reg] + campos_str) + "|"
