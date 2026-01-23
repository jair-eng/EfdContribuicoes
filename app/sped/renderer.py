
from typing import List
from app.db.models import EfdRegistro  # ajuste o import conforme seu projeto



import hashlib

def _sha1(s: str) -> str:
    return hashlib.sha1(s.encode("utf-8")).hexdigest()
def render_sped_line(reg: str, dados: List[str]) -> str:
    """
    Renderiza uma linha SPED a partir de REG + dados.
    Ex:
      reg="M200"
      dados=["3500,00","0,00","0,00","0,00"]
    -> "|M200|3500,00|0,00|0,00|0,00|"
    """
    dados = ["" if d is None else str(d) for d in dados]
    reg = (reg or "").strip()
    return "|" + reg + "|" + "|".join(dados) + "|"


def render_from_registro(registro: EfdRegistro) -> str:
    """
    Renderiza uma linha SPED a partir de um EfdRegistro (DB).
    """
    conteudo = registro.conteudo_json or {}
    dados = conteudo.get("dados") or []
    return render_sped_line(registro.reg, dados)