from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, Iterator, List, Optional , Tuple


# -----------------------------
# Utilitários gerais
# -----------------------------

def detectar_line_ending(file_path: str, *, max_bytes: int = 65536) -> str:
    with open(file_path, "rb") as f:
        chunk = f.read(max_bytes)
    return "CRLF" if b"\r\n" in chunk else "LF"


def _rstrip_eol(raw: str) -> str:
    if raw.endswith("\r\n"):
        return raw[:-2]
    if raw.endswith("\n"):
        return raw[:-1]
    return raw


def _split_sped_line(line: str) -> List[str]:
    return line.split("|")


def _somente_digitos(v: str) -> str:
    return "".join(ch for ch in (v or "") if ch.isdigit())


def _is_data_ddmmaaaa(v: str) -> bool:
    v = (v or "").strip()
    if len(v) != 8 or not v.isdigit():
        return False
    try:
        datetime.strptime(v, "%d%m%Y")
        return True
    except ValueError:
        return False


def _is_data_aaaammdd(v: str) -> bool:
    v = (v or "").strip()
    if len(v) != 8 or not v.isdigit():
        return False
    try:
        datetime.strptime(v, "%Y%m%d")
        return True
    except ValueError:
        return False


def _parse_data_sped(v: str) -> Optional[datetime]:
    """
    SPED costuma usar DDMMAAAA.
    Mas aceitamos AAAAMMDD também (alguns arquivos vêm assim).
    """
    v = (v or "").strip()
    if not v:
        return None
    if _is_data_ddmmaaaa(v):
        return datetime.strptime(v, "%d%m%Y")
    if _is_data_aaaammdd(v):
        return datetime.strptime(v, "%Y%m%d")
    return None


def _periodo_yyyymm(value: str) -> Optional[str]:
    if not value:
        return None
    v = value.strip()
    if not v:
        return None

    # YYYYMM
    if len(v) == 6 and v.isdigit():
        return v

    # YYYY-MM-DD
    if len(v) >= 10 and v[4] == "-" and v[7] == "-":
        try:
            dt = datetime.strptime(v[:10], "%Y-%m-%d")
            return dt.strftime("%Y%m")
        except ValueError:
            return None

    # DDMMAAAA ou AAAAMMDD
    dt = _parse_data_sped(v)
    if dt:
        return dt.strftime("%Y%m")

    return None


def _cnpj_valido_14(d: str) -> bool:
    # Validação simples: 14 dígitos e não tudo igual (evita 000..)
    if len(d) != 14 or not d.isdigit():
        return False
    if d == d[0] * 14:
        return False
    return True


def _find_cnpj_14(partes: List[str]) -> Optional[str]:
    for p in partes:
        d = _somente_digitos((p or "").strip())
        if _cnpj_valido_14(d):
            return d
    return None


# -----------------------------
# Extração robusta do 0000 (EFD Contribuições)
# -----------------------------

def _extract_0000_contrib(partes: List[str]) -> Dict[str, Optional[str]]:
    """
    Extrai campos do 0000 de forma robusta para EFD Contribuições.

    Estratégia:
      - Mantém campos vazios (split já preserva)
      - Remove vazios de borda: ['', '0000', ... , ''] -> ['0000', ...]
      - Encontra a PRIMEIRA data válida (DT_INI) e a próxima (DT_FIN)
      - Assume:
          nome = campo após DT_FIN
          cnpj = campo após nome
          uf   = campo após cnpj
          cod_mun = campo após uf
    """
    # normaliza bordas
    if partes and partes[0] == "":
        partes = partes[1:]
    if partes and partes[-1] == "":
        partes = partes[:-1]

    out = {
        "dt_ini": None,
        "dt_fin": None,
        "periodo": None,
        "razao_social": None,
        "cnpj": None,
        "uf": None,
        "cod_mun": None,
    }

    if len(partes) < 2 or (partes[0] or "").strip() != "0000":
        return out

    # encontra dt_ini e dt_fin (datas reais)
    dt_ini_idx = None
    dt_ini_dt = None
    for i in range(1, len(partes)):
        dt = _parse_data_sped(partes[i])
        if dt:
            dt_ini_idx = i
            dt_ini_dt = dt
            break

    if dt_ini_idx is None or dt_ini_dt is None:
        return out

    # dt_fin logo depois, também precisa ser data real
    dt_fin_dt = None
    dt_fin_idx = dt_ini_idx + 1
    if dt_fin_idx < len(partes):
        dt_fin_dt = _parse_data_sped(partes[dt_fin_idx])

    # se dt_fin não for data, ainda dá pra usar dt_ini para periodo, mas o resto fica suspeito
    out["dt_ini"] = dt_ini_dt.strftime("%d%m%Y")
    out["periodo"] = dt_ini_dt.strftime("%Y%m")

    if dt_fin_dt:
        out["dt_fin"] = dt_fin_dt.strftime("%d%m%Y")

        nome_idx = dt_fin_idx + 1
        cnpj_idx = dt_fin_idx + 2

        if nome_idx < len(partes):
            nome = (partes[nome_idx] or "").strip()
            # evita o bug: hash/recibo virando nome
            # nome precisa ter pelo menos 3 letras ou conter espaço (heurística segura)
            if nome and (sum(ch.isalpha() for ch in nome) >= 3):
                out["razao_social"] = nome

        if cnpj_idx < len(partes):
            cnpj = _somente_digitos(partes[cnpj_idx] or "")
            if _cnpj_valido_14(cnpj):
                out["cnpj"] = cnpj

        uf_idx = dt_fin_idx + 3
        cod_mun_idx = dt_fin_idx + 4

        if uf_idx < len(partes):
            uf = (partes[uf_idx] or "").strip()
            if len(uf) == 2 and uf.isalpha():
                out["uf"] = uf

        if cod_mun_idx < len(partes):
            cod_mun = _somente_digitos(partes[cod_mun_idx] or "")
            # COD_MUN IBGE normalmente 7 dígitos
            if len(cod_mun) == 7:
                out["cod_mun"] = cod_mun

    return out


def _extract_period_from_0000(partes: List[str]) -> Optional[str]:
    """
    Agora usa a extração robusta (datas reais) e cai no fallback antigo só se necessário.
    """
    ext = _extract_0000_contrib(partes)
    if ext.get("periodo"):
        return ext["periodo"]

    # fallback genérico (bem conservador)
    for p in partes:
        dt = _parse_data_sped(_somente_digitos(p or ""))
        if dt:
            return dt.strftime("%Y%m")

    return None


def _guess_razao_social_no_0000(partes: List[str]) -> Optional[str]:
    """
    Mantém apenas como fallback. Preferimos extrair determinístico do 0000.
    Heurística segura: evita hash/recibo (hex longo).
    """
    candidatos = []
    for p in partes:
        pp = (p or "").strip()
        if not pp:
            continue
        # ignora coisas que parecem hash/recibo
        if len(pp) >= 20 and all(ch in "0123456789ABCDEFabcdef" for ch in pp):
            continue
        if any(ch.isalpha() for ch in pp):
            candidatos.append(pp)

    if not candidatos:
        return None
    return max(candidatos, key=len).strip() or None


# -----------------------------
# Parser FULL (persistência)
# -----------------------------

def parse_sped_full(file_path: str) -> Iterator[Dict[str, Any]]:
    with open(file_path, encoding="latin-1", errors="ignore") as f:
        for idx, raw in enumerate(f, start=1):
            line = _rstrip_eol(raw)
            if not line:
                continue

            if not line.startswith("|") or not line.endswith("|"):
                continue

            partes = _split_sped_line(line)
            if len(partes) < 3:
                continue

            reg = (partes[1] or "").strip()
            if not reg:
                continue

            dados = partes[2:-1]

            yield {
                "linha": idx,
                "registro": reg,
                "conteudo_json": {"dados": dados},
            }


# -----------------------------
# Parser PREVIEW (metadados)
# -----------------------------

def parse_sped_preview(file_path: str, max_lines: int = 50000) -> dict:
    dados = {
        "cnpj": None,
        "razao_social": None,
        "periodo": None,
        "line_ending": detectar_line_ending(file_path),
    }

    seen_0140 = False  # <- 1) declara aqui

    with open(file_path, encoding="latin-1", errors="replace") as f:
        for i, raw in enumerate(f, start=1):
            if i > max_lines:
                break

            line = _rstrip_eol(raw)
            if not line or "|" not in line:
                continue

            partes = _split_sped_line(line)
            if len(partes) < 2:
                continue

            reg = (partes[1] or "").strip()
            if not reg:
                continue

            if reg == "0000":
                ext = _extract_0000_contrib(partes)

                if not dados["periodo"] and ext.get("periodo"):
                    dados["periodo"] = ext["periodo"]

                # só pega razão social do 0000 se vier determinística
                if not dados["razao_social"] and ext.get("razao_social"):
                    dados["razao_social"] = ext["razao_social"]

                if not dados["cnpj"] and ext.get("cnpj"):
                    dados["cnpj"] = ext["cnpj"]

                if not dados["cnpj"]:
                    cnpj = _find_cnpj_14(partes)
                    if cnpj:
                        dados["cnpj"] = cnpj

                if not dados["periodo"]:
                    periodo = _extract_period_from_0000(partes)
                    if periodo:
                        dados["periodo"] = periodo

                # EVITA: não usar guess que pega hash como "nome"
                # Deixa a razão social vir do 0140, que é mais confiável.
                # if not dados["razao_social"]:
                #     dados["razao_social"] = _guess_razao_social_no_0000(partes)

            elif reg == "0140":
                seen_0140 = True  # <- 2) marca que passou no 0140

                # 0140 é o melhor lugar para NOME/CNPJ do estabelecimento
                nome, cnpj = _extract_razao_cnpj_from_0140(partes)
                if nome:
                    dados["razao_social"] = nome  # <- pode sobrescrever sem medo
                if cnpj and not dados["cnpj"]:
                    dados["cnpj"] = cnpj

            elif reg == "0150" and not dados["cnpj"]:
                cnpj = _find_cnpj_14(partes)
                if cnpj:
                    dados["cnpj"] = cnpj

            # <- 3) só pode parar se já passou pelo 0140
            if dados["cnpj"] and dados["periodo"] and dados["razao_social"] and seen_0140:
                break

    if not dados["cnpj"]:
        raise ValueError("CNPJ não encontrado no SPED (preview)")

    if not dados["periodo"]:
        raise ValueError("Período (YYYYMM) não encontrado no SPED (preview)")

    if not dados["razao_social"]:
        dados["razao_social"] = ""

    return dados

def _extract_razao_cnpj_from_0140(partes: List[str]) -> Tuple[Optional[str], Optional[str]]:
    """
    0140 (EFD Contribuições):
      |0140|COD_EST|NOME|CNPJ|UF|IE|COD_MUN|IM|SUFRAMA|
    No seu exemplo COD_EST está vazio, então NOME fica em partes[3].
    """
    # remove bordas vazias
    if partes and partes[0] == "":
        partes = partes[1:]
    if partes and partes[-1] == "":
        partes = partes[:-1]

    if len(partes) < 5 or (partes[0] or "").strip() != "0140":
        return None, None

    nome = (partes[2] or "").strip()
    # quando COD_EST vem vazio, os campos deslocam 1 pra frente
    # Ex: |0140||NOME|CNPJ|...| => partes[1]=="" e nome cai em partes[2]
    # No seu split com bordas removidas: ["0140", "", "POSTO...", "418...", ...]
    if not nome and len(partes) > 3:
        nome = (partes[3] or "").strip()

    # tenta achar CNPJ por posição provável e fallback geral
    cnpj = None
    # posição típica: partes[3] quando nome está em partes[2]
    if len(partes) > 3:
        c = _somente_digitos(partes[3] or "")
        if _cnpj_valido_14(c):
            cnpj = c
    if not cnpj and len(partes) > 4:
        c = _somente_digitos(partes[4] or "")
        if _cnpj_valido_14(c):
            cnpj = c
    if not cnpj:
        cnpj = _find_cnpj_14(partes)

    # nome: evita hash e lixo
    if nome and sum(ch.isalpha() for ch in nome) < 3:
        nome = ""

    return (nome or None), cnpj
