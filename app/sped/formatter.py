from typing import Iterable

def formatar_linha(registro: str, campos: Iterable[str]) -> str:
    reg = str(registro or "").strip()
    # Limpa espaços extras que podem deslocar campos
    campos_str = ["" if c is None else str(c).strip() for c in campos]

    # Trava de segurança: não duplica o nome do registro se ele já estiver na lista
    if campos_str and campos_str[0] == reg:
        corpo = campos_str
    else:
        corpo = [reg] + campos_str

    return "|" + "|".join(corpo) + "|"
