# app/sped/layouts/c170.py
from __future__ import annotations

from dataclasses import dataclass



@dataclass(frozen=True)
class C170Layout:
    idx_cfop: int = 9
    idx_cst_pis: int = 23
    idx_cst_cofins: int = 29

    idx_vl_item: int = 5
    idx_vl_bc_pis: int = 24
    idx_aliq_pis: int = 25
    idx_vl_pis: int = 28

    idx_vl_bc_cofins: int = 30
    idx_aliq_cofins: int = 31
    idx_vl_cofins: int = 34

LAYOUT_C170 = C170Layout()