from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Dict, Set


# -------------------------------------------------
# Utils internos
# -------------------------------------------------

def _digits_only(s: str) -> str:
    return re.sub(r"\D+", "", s or "")


def _norm_code(s: str) -> str:
    s = (s or "").strip().upper()
    if not s:
        return ""
    s = re.sub(r"\s+", "", s)
    return s


def _is_range_token(tok: str) -> bool:
    return "-" in tok and tok.count("-") == 1


def _match_token(token: str, value: str) -> bool:
    """
    token: item do catálogo (ex: "2202*", "22021000", "1001-1008")
    value: valor a testar (ex: "22021000", "2202.10.00")
    """

    token = _norm_code(token)
    if not token:
        return False

    # Prefixo (2202*)
    if token.endswith("*"):
        pref = _digits_only(token[:-1])
        val = _digits_only(value)
        return bool(pref) and val.startswith(pref)

    # Faixa (1001-1008)
    if _is_range_token(token):
        a, b = token.split("-", 1)
        a = _digits_only(a)
        b = _digits_only(b)
        val = _digits_only(value)

        if not (a and b and val):
            return False

        n = max(len(a), len(b), len(val))
        a = a.zfill(n)
        b = b.zfill(n)
        val = val.zfill(n)

        return a <= val <= b

    # Match exato por dígitos
    return _digits_only(token) == _digits_only(value)


# -------------------------------------------------
# Catálogo Fiscal
# -------------------------------------------------

@dataclass(frozen=True)
class CatalogoFiscal:
    """
    slug -> set(codigos)

    Exemplos de códigos:
      2202*
      22021000
      1001-1008
    """
    grupos: Dict[str, Set[str]]

    # Compat dict-like
    def get(self, slug: str, default=None):
        return self.grupos.get(slug, default)

    def keys(self):
        return self.grupos.keys()

    def items(self):
        return self.grupos.items()

    def __contains__(self, slug: str) -> bool:
        return slug in self.grupos

    def __getitem__(self, slug: str) -> Set[str]:
        return self.grupos[slug]

    # API própria
    def codigos(self, slug: str) -> Set[str]:
        return set(self.grupos.get(slug, set()) or set())

    def match(self, slug: str, value: str) -> bool:
        v = (value or "").strip()
        if not v:
            return False

        itens = self.grupos.get(slug) or set()
        for tok in itens:
            if _match_token(tok, v):
                return True
        return False

    # Conveniências
    def ncm_match(self, slug: str, ncm: str) -> bool:
        return self.match(slug, ncm)

    def cfop_match(self, slug: str, cfop: str) -> bool:
        return self.match(slug, cfop)

    def cst_match(self, slug: str, cst: str) -> bool:
        return self.match(slug, cst)

    # Compat com código legado
    def match_codigo(self, slug: str, valor: str) -> bool:
        return self.match(slug, valor)
