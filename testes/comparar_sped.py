from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, List, Optional, Set, Tuple


BLOCO9_REGS: Set[str] = {"9001", "9900", "9990", "9999"}


def _rstrip_eol(s: str) -> str:
    return s.rstrip("\r\n")


def _split_reg(linha: str) -> str:
    """
    Extrai o REG de uma linha SPED. Ex: '|M100|...' -> 'M100'
    Retorna '' se não conseguir.
    """
    if not linha:
        return ""
    if not (linha.startswith("|") and "|" in linha[1:]):
        return ""
    parts = linha.split("|")
    return (parts[1] or "").strip() if len(parts) > 1 else ""


def _normalize_line(linha: str) -> str:
    """
    Normaliza:
    - remove EOL
    - garante pipes no começo e fim se for linha SPED válida
    """
    s = _rstrip_eol(linha).strip()
    if not s:
        return ""
    if s.startswith("|") and not s.endswith("|"):
        s += "|"
    return s


def read_sped_lines(path: str | Path, *, encoding: str = "latin-1") -> List[str]:
    p = Path(path)
    with p.open("r", encoding=encoding, errors="ignore", newline="") as f:
        out = []
        for raw in f:
            s = _normalize_line(raw)
            if s:
                out.append(s)
        return out


def strip_bloco9(lines: List[str]) -> List[str]:
    """
    Remove qualquer linha cujo REG seja do bloco 9.
    Útil para comparação original vs revisado, já que bloco 9 é recalculado.
    """
    out = []
    for ln in lines:
        reg = _split_reg(ln)
        if reg and reg in BLOCO9_REGS:
            continue
        out.append(ln)
    return out


@dataclass
class DiffItem:
    linha: int  # 1-based
    reg_orig: str
    reg_rev: str
    original: Optional[str]
    revisado: Optional[str]
    tipo: str  # "MODIFICADA" | "INSERIDA" | "REMOVIDA"


@dataclass
class DiffReport:
    total_linhas_orig: int
    total_linhas_rev: int
    total_diffs: int
    diffs: List[DiffItem]
    diffs_fora_permitido: List[DiffItem]
    inseridas: int
    removidas: int
    modificadas: int


def comparar_sped(
    original_lines: List[str],
    revisado_lines: List[str],
    *,
    allowed_regs_to_change: Set[str] = frozenset({"M100", "M200"}),
    ignore_bloco9: bool = True,
) -> DiffReport:
    """
    Compara por índice (linha a linha). Detecta:
      - modificada: existe nos dois índices e texto diferente
      - inserida: existe no revisado e não existe no original (arquivo maior)
      - removida: existe no original e não existe no revisado

    Por padrão ignora bloco 9 e só permite mudanças em M100/M200.
    """
    orig = strip_bloco9(original_lines) if ignore_bloco9 else original_lines
    rev = strip_bloco9(revisado_lines) if ignore_bloco9 else revisado_lines

    diffs: List[DiffItem] = []

    max_len = max(len(orig), len(rev))
    for i in range(max_len):
        o = orig[i] if i < len(orig) else None
        r = rev[i] if i < len(rev) else None

        if o == r:
            continue

        reg_o = _split_reg(o or "")
        reg_r = _split_reg(r or "")

        if o is None and r is not None:
            diffs.append(DiffItem(
                linha=i + 1,
                reg_orig="",
                reg_rev=reg_r,
                original=None,
                revisado=r,
                tipo="INSERIDA",
            ))
        elif o is not None and r is None:
            diffs.append(DiffItem(
                linha=i + 1,
                reg_orig=reg_o,
                reg_rev="",
                original=o,
                revisado=None,
                tipo="REMOVIDA",
            ))
        else:
            diffs.append(DiffItem(
                linha=i + 1,
                reg_orig=reg_o,
                reg_rev=reg_r,
                original=o,
                revisado=r,
                tipo="MODIFICADA",
            ))

    # contagens
    inseridas = sum(1 for d in diffs if d.tipo == "INSERIDA")
    removidas = sum(1 for d in diffs if d.tipo == "REMOVIDA")
    modificadas = sum(1 for d in diffs if d.tipo == "MODIFICADA")

    # valida diffs fora do permitido
    diffs_fora: List[DiffItem] = []
    for d in diffs:
        # Se for inserida/modificada, considere o reg do revisado
        reg_check = d.reg_rev if d.tipo in ("INSERIDA", "MODIFICADA") else d.reg_orig

        # Linhas não-SPED (reg="") contam como fora do permitido
        if not reg_check or reg_check not in allowed_regs_to_change:
            diffs_fora.append(d)

    return DiffReport(
        total_linhas_orig=len(orig),
        total_linhas_rev=len(rev),
        total_diffs=len(diffs),
        diffs=diffs,
        diffs_fora_permitido=diffs_fora,
        inseridas=inseridas,
        removidas=removidas,
        modificadas=modificadas,
    )


def print_report(report: DiffReport, *, max_show: int = 30) -> None:
    print("=== COMPARAÇÃO ORIGINAL x REVISADO (ignorando bloco 9) ===")
    print(f"Linhas (orig): {report.total_linhas_orig}")
    print(f"Linhas (rev) : {report.total_linhas_rev}")
    print(f"Diffs totais : {report.total_diffs}")
    print(f"  - Modificadas: {report.modificadas}")
    print(f"  - Inseridas  : {report.inseridas}")
    print(f"  - Removidas  : {report.removidas}")
    print()

    if report.diffs_fora_permitido:
        print("⚠️ DIFERENÇAS FORA DO PERMITIDO (apenas M100/M200):")
        for d in report.diffs_fora_permitido[:max_show]:
            print(f"- Linha {d.linha} [{d.tipo}] reg_orig={d.reg_orig} reg_rev={d.reg_rev}")
            print(f"  O: {d.original}")
            print(f"  R: {d.revisado}")
        if len(report.diffs_fora_permitido) > max_show:
            print(f"... +{len(report.diffs_fora_permitido) - max_show} diffs")
        print()
    else:
        print("✅ Nenhuma diferença fora do permitido (somente M100/M200 alterados).")
        print()

    print("Amostra de diferenças:")
    for d in report.diffs[:max_show]:
        print(f"- Linha {d.linha} [{d.tipo}] reg_orig={d.reg_orig} reg_rev={d.reg_rev}")
        print(f"  O: {d.original}")
        print(f"  R: {d.revisado}")
    if len(report.diffs) > max_show:
        print(f"... +{len(report.diffs) - max_show} diffs")


if __name__ == "__main__":
    # ajuste caminhos
    original_path = "original.txt"
    revisado_path = "revisado.txt"

    orig_lines = read_sped_lines(original_path)
    rev_lines = read_sped_lines(revisado_path)

    report = comparar_sped(
        orig_lines,
        rev_lines,
        allowed_regs_to_change={"M100", "M200"},
        ignore_bloco9=True,
    )
    print_report(report, max_show=40)

    # opcional: falhar o processo se algo fora do permitido acontecer
    if report.diffs_fora_permitido:
        raise SystemExit(2)
