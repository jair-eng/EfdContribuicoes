from dataclasses import dataclass
from typing import Dict, Set, Iterable, Any, Optional

@dataclass(frozen=True)
class CatalogoFiscal:
    grupos: Dict[str, Set[str]]  # slug -> set(codigo)

    # --- compat dict-like ---
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

    # --- API própria (já tinha) ---
    def codigos(self, slug: str) -> Set[str]:
        return self.grupos.get(slug, set())

    def match_codigo(self, slug: str, valor: str) -> bool:
        v = (valor or "").strip()
        if not v:
            return False
        cods = self.codigos(slug)
        if not cods:
            return False

        for c in cods:
            c = (c or "").strip()
            if not c:
                continue
            if c.endswith("*") and v.startswith(c[:-1]):
                return True
            if v == c:
                return True
        return False
