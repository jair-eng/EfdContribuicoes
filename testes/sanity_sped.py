from __future__ import annotations
from dataclasses import dataclass
from typing import List, Optional, Tuple, Dict
from decimal import Decimal
import re

# -------- helpers --------
def _clean(line: str) -> str:
    s = (line or "").strip().rstrip("\r\n")
    if not s:
        return ""
    if not s.startswith("|"):
        s = "|" + s
    if not s.endswith("|"):
        s = s + "|"
    return s

def _reg(line: str) -> str:
    s = _clean(line)
    if not s:
        return ""
    parts = s.strip("|").split("|")
    return (parts[0] or "").upper().strip()

def _to_dec_br(x: str) -> Decimal:
    s = (x or "").strip()
    if not s:
        return Decimal("0")
    s = s.replace(".", "").replace(",", ".")
    try:
        return Decimal(s)
    except Exception:
        return Decimal("0")

def _cst_norm(x: str) -> str:
    s = re.sub(r"\s+", "", str(x or ""))
    s = s.lstrip("0")
    if not s:
        return ""
    if len(s) == 1:
        s = "0" + s
    return s

@dataclass
class SanityReport:
    ok: bool
    errors: List[str]
    warnings: List[str]
    meta: Dict[str, object]

# -------- main check --------
def sanity_check_sped_lines(lines: List[str]) -> SanityReport:
    errors: List[str] = []
    warnings: List[str] = []

    L = [_clean(x) for x in (lines or []) if _clean(x)]
    regs = [_reg(x) for x in L]

    def idx_of(r: str) -> Optional[int]:
        try:
            return regs.index(r)
        except ValueError:
            return None

    # ---- basic required regs ----
    for must in ("0000", "9999"):
        if must not in regs:
            errors.append(f"Faltando registro obrigatório {must}")

    # ---- block ordering (high level) ----
    i_m001 = idx_of("M001")
    i_m990 = idx_of("M990")
    i_p001 = idx_of("P001")
    i_p990 = idx_of("P990")
    i_1001 = idx_of("1001")
    i_1990 = idx_of("1990")
    i_9001 = idx_of("9001")
    i_9999 = idx_of("9999")

    if i_m001 is None or i_m990 is None:
        errors.append("Bloco M inválido: faltando M001/M990")
    else:
        if i_m001 > i_m990:
            errors.append("Bloco M inválido: M001 depois de M990")
        # M990 count check
        m990_parts = L[i_m990].strip("|").split("|")
        try:
            declared = int(m990_parts[1])
        except Exception:
            declared = None
        actual = (i_m990 - i_m001 + 1)
        if declared is None:
            warnings.append("M990: não consegui ler a quantidade declarada")
        elif declared != actual:
            errors.append(f"M990 quantidade divergente: declarado={declared} atual={actual}")

    # Order: ... M ... P ... 1 ... 9
    # (não exige existir P ou 1, mas se existirem, ordem tem que ser correta)
    if i_m990 is not None and i_p001 is not None and i_p001 < i_m990:
        errors.append("Ordem inválida: P001 apareceu antes de fechar o Bloco M (M990)")
    if i_p990 is not None and i_1001 is not None and i_1001 < i_p990:
        errors.append("Ordem inválida: 1001 apareceu antes de fechar o Bloco P (P990)")
    if i_1990 is not None and i_9001 is not None and i_9001 < i_1990:
        errors.append("Ordem inválida: Bloco 9 começou antes de fechar o Bloco 1 (1990)")
    if i_9001 is not None and i_9999 is not None and i_9999 < i_9001:
        errors.append("Ordem inválida: 9999 apareceu antes de 9001")

    # ---- block 1 integrity (if present) ----
    if i_1001 is not None:
        if i_1990 is None:
            errors.append("Bloco 1 inválido: tem 1001 mas não tem 1990")
        else:
            if i_1001 > i_1990:
                errors.append("Bloco 1 inválido: 1001 depois de 1990")
            # 1990 count check
            parts = L[i_1990].strip("|").split("|")
            try:
                declared = int(parts[1])
            except Exception:
                declared = None
            actual = (i_1990 - i_1001 + 1)
            if declared is None:
                warnings.append("1990: não consegui ler a quantidade declarada")
            elif declared != actual:
                errors.append(f"1990 quantidade divergente: declarado={declared} atual={actual}")

    # ---- block 9 must be last ----
    if i_9001 is None:
        errors.append("Bloco 9 inválido: faltando 9001")
    else:
        # ensure no non-9 regs after 9001 except 9 regs
        tail_regs = regs[i_9001:]
        for r in tail_regs:
            if not r.startswith("9"):
                errors.append(f"Bloco 9 inválido: registro {r} apareceu após 9001")

    # ---- M100/M500 coherence (optional checks) ----
    # if M100 exists, ensure M105 exists after it (common PVA expectation in your pattern)
    if "M100" in regs:
        i = idx_of("M100")
        if i is not None:
            # look until M200 or next major reg; just ensure a M105 is somewhere after M100 before M200
            found = False
            for j in range(i + 1, min(len(regs), i + 20)):
                if regs[j] == "M200":
                    break
                if regs[j] == "M105":
                    found = True
                    break
            if not found:
                warnings.append("Encontrou M100, mas não achou M105 logo após (verifique padrão do seu bloco M)")

    if "M500" in regs:
        i = idx_of("M500")
        if i is not None:
            found = False
            for j in range(i + 1, min(len(regs), i + 20)):
                if regs[j] == "M600":
                    break
                if regs[j] == "M505":
                    found = True
                    break
            if not found:
                warnings.append("Encontrou M500, mas não achou M505 logo após (verifique padrão do seu bloco M)")

    # ---- meta info ----
    meta = {
        "total_lines": len(L),
        "has_M": i_m001 is not None,
        "has_P": i_p001 is not None,
        "has_1": i_1001 is not None,
        "has_9": i_9001 is not None,
    }

    ok = len(errors) == 0
    return SanityReport(ok=ok, errors=errors, warnings=warnings, meta=meta)
