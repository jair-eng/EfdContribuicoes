# app/sped/layouts/c170.py
from __future__ import annotations

from dataclasses import dataclass



@dataclass(frozen=True)
class C170Layout:
    idx_cfop: int = 9
    idx_cst_pis: int = 23
    idx_cst_cofins: int = 29

LAYOUT_C170 = C170Layout()
