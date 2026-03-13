from __future__ import annotations

from dataclasses import dataclass, asdict
from decimal import Decimal
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Any


# ============================================================
# Helpers básicos
# ============================================================

def only_digits(x: object) -> str:
    return "".join(ch for ch in str(x or "") if ch.isdigit())


def dec_br(s: str | Decimal | None) -> Decimal:
    if s is None:
        return Decimal("0")
    if isinstance(s, Decimal):
        return s
    txt = str(s).strip()
    if not txt:
        return Decimal("0")

    # padrão BR: 95.268,36 -> 95268.36
    if "," in txt:
        txt = txt.replace(".", "").replace(",", ".")

    try:
        return Decimal(txt)
    except Exception:
        return Decimal("0")


def clean_line(ln: str) -> str:
    ln = (ln or "").strip().rstrip("\r\n")
    if not ln:
        return ""
    if not ln.startswith("|"):
        ln = "|" + ln
    if not ln.endswith("|"):
        ln = ln + "|"
    return ln


def split_reg_fields(ln: str) -> Tuple[str, List[str]]:
    ln = clean_line(ln)
    if not ln:
        return "", []
    parts = ln.strip("|").split("|")
    return (parts[0].upper().strip(), parts[1:]) if parts else ("", [])


def parse_periodo_from_0000(fields: List[str]) -> Optional[str]:
    for tok in fields:
        t = only_digits(tok)
        if len(t) == 8:
            return f"{t[2:4]}{t[4:8]}"
    return None


def extrair_cnpj_cpf_0150(fields: List[str]) -> Tuple[str, str]:
    """
    Layout comum:
      |0150|COD_PART|NOME|COD_PAIS|CNPJ|CPF|IE|COD_MUN|...

    Mas tenta ser robusto se houver deslocamento.
    """
    cnpj = fields[3] if len(fields) > 3 else ""
    cpf = fields[4] if len(fields) > 4 else ""

    d_cnpj = only_digits(cnpj)
    d_cpf = only_digits(cpf)

    if len(d_cnpj) != 14:
        for tok in fields:
            if len(only_digits(tok)) == 14:
                cnpj = tok
                d_cnpj = only_digits(tok)
                break

    if len(d_cpf) != 11:
        for tok in fields:
            if len(only_digits(tok)) == 11:
                cpf = tok
                d_cpf = only_digits(tok)
                break

    return cnpj, cpf


# ============================================================
# DTOs
# ============================================================

@dataclass
class ContribItemRow:
    periodo: str
    arquivo: str
    empresa: str

    cod_part: str
    participante_nome: str
    participante_cnpj: str
    participante_cpf: str

    num_item: str
    cod_item_norm: str

    dt_doc: str
    num_doc: str
    serie: str
    chave: str

    cod_item: str
    descr_item: str
    ncm: str
    cfop: str

    vl_item: Decimal
    vl_desc: Decimal
    vl_icms: Decimal
    vl_ipi: Decimal

    cst_pis: str
    vl_bc_pis: Decimal
    aliq_pis: Decimal
    vl_pis: Decimal

    cst_cofins: str
    vl_bc_cofins: Decimal
    aliq_cofins: Decimal
    vl_cofins: Decimal

    cod_cta: str
    conta_nome: str
    conta_classif: str

    origem: str = "EFD_CONTRIBUICOES"


# ============================================================
# Parser/normalizador puro do EFD Contribuições
# ============================================================
def normalizar_efd_contribuicoes_para_cruzamento(path: Path) -> List[Dict[str, Any]]:
    """
    Lê um EFD Contribuições e devolve linhas normalizadas no nível de item (C170),
    prontas para cruzamento com ICMS/IPI.

    Não aplica:
      - filtros de tese
      - filtros de café
      - cálculo de oportunidade
      - exclusão de PF
      - bloqueio por participante desconhecido

    Objetivo:
      - normalizar os dados do Contribuições
      - preservar o máximo possível para cruzamento

    Ajustes:
      - inclui num_item
      - inclui cod_item_norm
      - reforça mapas com chave normalizada
      - melhora robustez para match por item/posição
    """
    periodo_mmyyyy = ""
    empresa_cnpj = ""

    # 0150
    part_data_map: Dict[str, Tuple[str, str]] = {}
    part_nome_map: Dict[str, str] = {}

    # 0200
    item_to_ncm: Dict[str, str] = {}
    item_to_desc: Dict[str, str] = {}

    # 0500
    conta_map: Dict[str, Tuple[str, str]] = {}

    # --------------------------------------------------------
    # 1ª passada: cadastros
    # --------------------------------------------------------
    with path.open("r", encoding="utf-8-sig", errors="ignore") as f:
        for raw in f:
            reg, fields = split_reg_fields(raw)

            if reg == "0000":
                if not periodo_mmyyyy:
                    periodo_mmyyyy = parse_periodo_from_0000(fields) or ""
                empresa_cnpj = only_digits(fields[6]) if len(fields) > 6 else ""

            elif reg == "0150":
                cod = (fields[0] if len(fields) > 0 else "").strip()
                nome = (fields[1] if len(fields) > 1 else "").strip()
                cnpj, cpf = extrair_cnpj_cpf_0150(fields)

                if cod:
                    part_data_map[cod] = (cnpj, cpf)
                    part_nome_map[cod] = nome

                    cod_dig = only_digits(cod)
                    if cod_dig:
                        part_data_map[cod_dig] = (cnpj, cpf)
                        part_data_map[cod_dig.lstrip("0") or "0"] = (cnpj, cpf)
                        part_nome_map[cod_dig] = nome
                        part_nome_map[cod_dig.lstrip("0") or "0"] = nome

            elif reg == "0200":
                cod_i = (fields[0] if len(fields) > 0 else "").strip()
                desc_i = (fields[1] if len(fields) > 1 else "").strip()
                ncm_i = only_digits(fields[7]) if len(fields) > 7 else ""

                if cod_i:
                    item_to_ncm[cod_i] = ncm_i
                    item_to_desc[cod_i] = desc_i

                    cod_i_norm = cod_i.lstrip("0") or cod_i
                    item_to_ncm[cod_i_norm] = ncm_i
                    item_to_desc[cod_i_norm] = desc_i

            elif reg == "0500":
                cod_cta = (fields[4] if len(fields) > 4 else "").strip()
                nome_cta = (fields[5] if len(fields) > 5 else "").strip()
                classif = (fields[6] if len(fields) > 6 else "").strip()
                if cod_cta:
                    conta_map[cod_cta] = (nome_cta, classif)

    # --------------------------------------------------------
    # 2ª passada: documentos + itens
    # --------------------------------------------------------
    rows: List[ContribItemRow] = []

    current_cod_part = ""
    current_num_doc = ""
    current_serie = ""
    current_dt_doc = ""
    current_chv = ""
    current_ind_oper = ""
    current_cod_sit = ""

    with path.open("r", encoding="utf-8-sig", errors="ignore") as f:
        for raw in f:
            reg, fields = split_reg_fields(raw)

            if reg == "C100":
                # Layout esperado:
                # |C100|IND_OPER|IND_EMIT|COD_PART|COD_MOD|COD_SIT|SER|NUM_DOC|CHV_NFE|DT_DOC|...
                current_ind_oper = (fields[0] if len(fields) > 0 else "").strip()
                current_cod_part = (fields[2] if len(fields) > 2 else "").strip()
                current_cod_sit = (fields[4] if len(fields) > 4 else "").strip()
                current_serie = (fields[5] if len(fields) > 5 else "").strip()
                current_num_doc = (fields[6] if len(fields) > 6 else "").strip()
                current_chv = (fields[7] if len(fields) > 7 else "").strip()
                current_dt_doc = (fields[8] if len(fields) > 8 else "").strip()
                continue

            if reg != "C170":
                continue

            # Só considera C170 se houver C100 corrente
            if not current_num_doc and not current_chv:
                continue

            # Ignora documentos cancelados/complementares
            if current_cod_sit in {"06", "07"}:
                continue

            # Campos básicos do C170
            num_item = (fields[0] if len(fields) > 0 else "").strip()
            cod_item = (fields[1] if len(fields) > 1 else "").strip()
            cod_item_norm = cod_item.lstrip("0") if cod_item else ""
            if not cod_item_norm:
                cod_item_norm = cod_item

            descr_item_c170 = (fields[2] if len(fields) > 2 else "").strip()
            cfop = (fields[9] if len(fields) > 9 else "").strip()

            # Valores fiscais
            vl_item = dec_br(fields[5]) if len(fields) > 5 else Decimal("0")
            vl_desc = dec_br(fields[6]) if len(fields) > 6 else Decimal("0")
            vl_icms = dec_br(fields[13]) if len(fields) > 13 else Decimal("0")
            vl_ipi = dec_br(fields[17]) if len(fields) > 17 else Decimal("0")

            # PIS
            cst_pis = (fields[23] if len(fields) > 23 else "").strip()
            vl_bc_pis = dec_br(fields[24]) if len(fields) > 24 else Decimal("0")
            aliq_pis = dec_br(fields[25]) if len(fields) > 25 else Decimal("0")
            vl_pis = dec_br(fields[27]) if len(fields) > 27 else Decimal("0")

            # COFINS
            cst_cofins = (fields[29] if len(fields) > 29 else "").strip()
            vl_bc_cofins = dec_br(fields[30]) if len(fields) > 30 else Decimal("0")
            aliq_cofins = dec_br(fields[31]) if len(fields) > 31 else Decimal("0")
            vl_cofins = dec_br(fields[33]) if len(fields) > 33 else Decimal("0")

            # Contábil
            cod_cta = (fields[-1] if fields else "").strip()
            conta_nome, conta_classif = conta_map.get(cod_cta, ("", ""))

            # Produto
            ncm = (
                item_to_ncm.get(cod_item, "")
                or item_to_ncm.get(cod_item_norm, "")
                if cod_item
                else ""
            )
            descr_item = (
                descr_item_c170
                or item_to_desc.get(cod_item, "")
                or item_to_desc.get(cod_item_norm, "")
            )

            # Participante
            participante_nome = (
                part_nome_map.get(current_cod_part)
                or part_nome_map.get(only_digits(current_cod_part))
                or part_nome_map.get(only_digits(current_cod_part).lstrip("0"))
                or ""
            )

            info = (
                part_data_map.get(current_cod_part)
                or part_data_map.get(only_digits(current_cod_part))
                or part_data_map.get(only_digits(current_cod_part).lstrip("0"))
            )

            participante_cnpj = only_digits(info[0]) if info else ""
            participante_cpf = only_digits(info[1]) if info else ""

            rows.append(
                ContribItemRow(
                    periodo=periodo_mmyyyy,
                    arquivo=path.name,
                    empresa=empresa_cnpj,

                    cod_part=current_cod_part,
                    participante_nome=participante_nome,
                    participante_cnpj=participante_cnpj,
                    participante_cpf=participante_cpf,

                    dt_doc=current_dt_doc,
                    num_doc=current_num_doc,
                    serie=current_serie,
                    chave=current_chv,

                    num_item=num_item,
                    cod_item=cod_item,
                    cod_item_norm=cod_item_norm,
                    descr_item=descr_item,
                    ncm=ncm,
                    cfop=cfop,

                    vl_item=vl_item,
                    vl_desc=vl_desc,
                    vl_icms=vl_icms,
                    vl_ipi=vl_ipi,

                    cst_pis=cst_pis,
                    vl_bc_pis=vl_bc_pis,
                    aliq_pis=aliq_pis,
                    vl_pis=vl_pis,

                    cst_cofins=cst_cofins,
                    vl_bc_cofins=vl_bc_cofins,
                    aliq_cofins=aliq_cofins,
                    vl_cofins=vl_cofins,

                    cod_cta=cod_cta,
                    conta_nome=conta_nome,
                    conta_classif=conta_classif,
                )
            )

    return [asdict(r) for r in rows]