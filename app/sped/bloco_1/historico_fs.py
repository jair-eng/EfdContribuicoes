from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Optional, Tuple
import re


@dataclass(frozen=True)
class SpedInfo:
    path: Path
    cnpj: Optional[str]
    periodo: Optional[int]  # YYYYMM
    mtime: float


def ler_linhas_sped(path: Path) -> list[str]:
    with path.open("r", encoding="utf-8-sig", errors="ignore") as f:
        return [ln.rstrip("\n") for ln in f]


def _limpar_cnpj(s: str) -> str:
    return re.sub(r"\D+", "", s or "")


def _parse_0000_cnpj_periodo(path: Path) -> tuple[Optional[str], Optional[int]]:
    """
    Lê poucas linhas do início e tenta extrair CNPJ e período pelo registro 0000.
    Parsing defensivo (layout do 0000 varia entre arquivos).
    """
    try:
        with path.open("r", encoding="utf-8-sig", errors="ignore") as f:
            for _ in range(400):
                line = f.readline()
                if not line:
                    break
                line = line.strip()
                if not line.startswith("|0000|"):
                    continue

                parts = line.strip("|").split("|")
                cnpj = None
                periodo = None

                # 1) CNPJ = primeiro token com 14 dígitos
                for tok in parts:
                    t = _limpar_cnpj(tok)
                    if len(t) == 14:
                        cnpj = t
                        break

                # 2) token AAAAMM
                for tok in parts:
                    t = re.sub(r"\D+", "", tok or "")
                    if len(t) == 6 and t.isdigit():
                        yyyymm = int(t)
                        if 190001 <= yyyymm <= 210012:
                            periodo = yyyymm
                            break

                # 3) derivar de data (YYYYMMDD ou DDMMAAAA)
                if periodo is None:
                    for tok in parts:
                        t = re.sub(r"\D+", "", tok or "")
                        if len(t) == 8 and t.isdigit():
                            # YYYYMMDD
                            yyyymm1 = int(t[:6])
                            if 190001 <= yyyymm1 <= 210012:
                                periodo = yyyymm1
                                break

                            # DDMMAAAA -> AAAAMM
                            dd = int(t[0:2])
                            mm = int(t[2:4])
                            yyyy = int(t[4:8])
                            if 1 <= dd <= 31 and 1 <= mm <= 12 and 1900 <= yyyy <= 2100:
                                periodo = yyyy * 100 + mm
                                break

                return cnpj, periodo
    except Exception:
        return None, None

    return None, None


def buscar_sped_exportado_anterior_por_pasta(
    *,
    pasta_speds_corrigidos: Path,
    cnpj_empresa: str,
    periodo_atual: Optional[int],
    ignorar_path: Optional[Path] = None,
) -> Optional[Path]:
    """
    Busca o SPED exportado anterior na pasta (histórico FS), filtrando por CNPJ e período.
    - Preferência: maior período < periodo_atual
    - Fallback: arquivo mais recente por mtime
    """
    cnpj_empresa = _limpar_cnpj(cnpj_empresa)

    candidatos: list[tuple[Path, Optional[int], float]] = []
    for p in pasta_speds_corrigidos.glob("*.txt"):
        if ignorar_path and p.resolve() == ignorar_path.resolve():
            continue

        cnpj_p, periodo_p = _parse_0000_cnpj_periodo(p)
        if cnpj_p != cnpj_empresa:
            continue

        st = p.stat()
        candidatos.append((p, periodo_p, st.st_mtime))

    if not candidatos:
        return None

    # modo normal: usa período se tiver
    if periodo_atual is not None:
        candidatos_ok = [(p, per, mt) for (p, per, mt) in candidatos if per is not None and per < periodo_atual]
        if candidatos_ok:
            candidatos_ok.sort(key=lambda x: (x[1], x[2]))  # (periodo, mtime)
            return candidatos_ok[-1][0]

    # fallback: mais recente por mtime
    candidatos.sort(key=lambda x: x[2])
    return candidatos[-1][0]


def extrair_cnpj_periodo_do_0000(linhas: list[str]) -> Tuple[Optional[str], Optional[int]]:
    """
    Extrai CNPJ e período (YYYYMM) do 0000 a partir das linhas do arquivo.
    Observação: no seu layout você já tinha DT_INI em DDMMAAAA em parts[5].
    """
    for ln in linhas or []:
        ln = (ln or "").strip()
        if not ln.startswith("|0000|"):
            continue

        parts = ln.strip("|").split("|")

        # CNPJ = primeiro token com 14 dígitos
        cnpj = None
        for tok in parts:
            t = re.sub(r"\D+", "", tok or "")
            if len(t) == 14:
                cnpj = t
                break

        # período por DT_INI (DDMMAAAA) no parts[5]
        periodo = None
        dt_ini = parts[5] if len(parts) > 5 else ""
        dt = re.sub(r"\D+", "", dt_ini or "")
        if len(dt) == 8 and dt.isdigit():
            dd = int(dt[0:2])
            mm = int(dt[2:4])
            yyyy = int(dt[4:8])
            if 1 <= dd <= 31 and 1 <= mm <= 12 and 1900 <= yyyy <= 2100:
                periodo = yyyy * 100 + mm

        return cnpj, periodo

    return None, None
