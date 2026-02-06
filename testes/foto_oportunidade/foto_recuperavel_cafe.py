from __future__ import annotations

import argparse
import os
import re
from dataclasses import dataclass
from decimal import Decimal, ROUND_HALF_UP
from pathlib import Path
from typing import Dict, List, Optional, Tuple
from openpyxl import Workbook
from openpyxl.styles import Font, Alignment, PatternFill, Border, Side
from openpyxl.utils import get_column_letter
from openpyxl.formatting.rule import CellIsRule


"""
SCRIPT EXPERIMENTAL — FOTO DE OPORTUNIDADE (CAFÉ)

- Não grava banco
- Não gera EfdRevisao
- Não altera SPED
- Uso exclusivo para estimativa comercial

Fonte: leitura direta de SPED TXT

executa rodando no terminal   python foto_recuperavel.py "C:\SpedEmpresaFotoRecuperacao\" --ncm 09011110 --ncm 09011190 --ncm 09011200 --out "C:\SpedEmpresaFotoRecuperacao\foto_recuperavel.csv"

"""


ALIQUOTA_PIS = Decimal("0.0165")
ALIQUOTA_COF = Decimal("0.0760")
ALIQUOTA_TOTAL = (ALIQUOTA_PIS + ALIQUOTA_COF).quantize(Decimal("0.0001"))

CST_CREDITO = {"50", "51", "52", "53", "54", "55", "56"}

# ---- IND_TORRADO (simulação do que o auto-fix faria) ----
CFOPS_TORRADO_PADRAO = {"1101", "1102", "2101", "2102", "3101", "3102"}  # inclui 1102 como você pediu
CSTS_ORIGEM_PADRAO = {"70", "73", "75", "98", "99", "06", "07", "08"}   # ajustável

# NCM é opcional. Se quiser filtrar por café verde:
# Exemplos comuns: 09011110, 09011190, 09011200 (café não torrado), etc.
# Se deixar vazio, não filtra por NCM.
NCMS_CAFE_VERDE_PADRAO = {
    # "09011110", "09011190", "09011200",
}

def _brl_number_format() -> str:
    # Excel pt-BR costuma aceitar esse formato pra moeda.
    # Se sua máquina reclamar, troque por: '"R$" #,##0.00'
    return '[$R$-pt-BR] #,##0.00'


def _style_header(ws, row: int, col_start: int, col_end: int) -> None:
    fill = PatternFill("solid", fgColor="1F4E79")  # azul escuro
    font = Font(color="FFFFFF", bold=True)
    align = Alignment(horizontal="center", vertical="center", wrap_text=True)
    thin = Side(style="thin", color="D9D9D9")
    border = Border(left=thin, right=thin, top=thin, bottom=thin)

    for c in range(col_start, col_end + 1):
        cell = ws.cell(row=row, column=c)
        cell.fill = fill
        cell.font = font
        cell.alignment = align
        cell.border = border

    ws.row_dimensions[row].height = 22


def _apply_table_style(ws, start_row: int, end_row: int, start_col: int, end_col: int) -> None:
    thin = Side(style="thin", color="D9D9D9")
    border = Border(left=thin, right=thin, top=thin, bottom=thin)
    zebra = PatternFill("solid", fgColor="F7F7F7")

    for r in range(start_row, end_row + 1):
        for c in range(start_col, end_col + 1):
            cell = ws.cell(row=r, column=c)
            cell.border = border
            cell.alignment = Alignment(vertical="center", wrap_text=False)
            if r % 2 == 0:  # zebra
                cell.fill = zebra


def _autosize_columns(ws, max_width: int = 60) -> None:
    dims: Dict[int, int] = {}
    for row in ws.iter_rows(values_only=False):
        for cell in row:
            if cell.value is None:
                continue
            val = str(cell.value)
            dims[cell.column] = max(dims.get(cell.column, 0), len(val))
    for col, w in dims.items():
        ws.column_dimensions[get_column_letter(col)].width = min(max(10, w + 2), max_width)


def salvar_xlsx_visual(
    out_xlsx: Path,
    *,
    pasta: Path,
    ncms_validos: set[str],
    results: List["FotoArquivoResult"],
    total_base: Decimal,
    total_cred: Decimal,
) -> None:
    wb = Workbook()

    # =========================
    # Aba 1: Resumo
    # =========================
    ws = wb.active
    ws.title = "Resumo"

    title_font = Font(bold=True, size=16, color="1F4E79")
    label_font = Font(bold=True, color="404040")
    ws["A1"] = "FOTO RECUPERÁVEL — CAFÉ (SIMULAÇÃO)"
    ws["A1"].font = title_font

    ws["A3"] = "Pasta analisada:"
    ws["A3"].font = label_font
    ws["B3"] = str(pasta)

    ws["A4"] = "Filtro NCM:"
    ws["A4"].font = label_font
    ws["B4"] = ("; ".join(sorted(ncms_validos)) if ncms_validos else "Sem filtro")

    ws["A6"] = "Total base_delta (R$):"
    ws["A6"].font = label_font
    ws["B6"] = float(q2(total_base))
    ws["B6"].number_format = _brl_number_format()

    ws["A7"] = "Total crédito (R$):"
    ws["A7"].font = label_font
    ws["B7"] = float(q2(total_cred))
    ws["B7"].number_format = _brl_number_format()

    arquivos_total = len(results)
    arquivos_com_potencial = sum(1 for r in results if (r.credito_total or Decimal("0")) > 0)

    ws["A9"] = "Arquivos analisados:"
    ws["A9"].font = label_font
    ws["B9"] = arquivos_total

    ws["A10"] = "Arquivos com potencial:"
    ws["A10"].font = label_font
    ws["B10"] = arquivos_com_potencial

    # Mini tabela por período (só os que têm crédito)
    ws["A12"] = "Por período (somente > 0):"
    ws["A12"].font = label_font

    ws.append(["Período", "Base_delta (R$)", "Crédito (R$)", "Arquivos"])
    header_row = ws.max_row
    _style_header(ws, header_row, 1, 4)

    # agrega por período
    agg: Dict[str, Dict[str, Decimal | int]] = {}
    for r in results:
        per = r.periodo_mmyyyy or "??????"
        if r.credito_total <= 0:
            continue
        if per not in agg:
            agg[per] = {"base": Decimal("0"), "cred": Decimal("0"), "qtd": 0}
        agg[per]["base"] = (agg[per]["base"] or Decimal("0")) + r.base_delta
        agg[per]["cred"] = (agg[per]["cred"] or Decimal("0")) + r.credito_total
        agg[per]["qtd"] = int(agg[per]["qtd"] or 0) + 1

    for per in sorted(agg.keys(), key=mmYYYY_to_key):
        ws.append([per, float(q2(agg[per]["base"])), float(q2(agg[per]["cred"])), int(agg[per]["qtd"])])

    end_row = ws.max_row
    # formatos moeda
    for r in range(header_row + 1, end_row + 1):
        ws.cell(r, 2).number_format = _brl_number_format()
        ws.cell(r, 3).number_format = _brl_number_format()

    _apply_table_style(ws, header_row, end_row, 1, 4)
    _autosize_columns(ws)

    # =========================
    # Aba 2: Detalhado
    # =========================
    ws2 = wb.create_sheet("Detalhado")

    cols = [
        "periodo", "arquivo",
        "base_delta", "credito_pis", "credito_cof", "credito_total",
        "candidatos", "pf_excluidos",
        "cfops_top", "csts_top", "ncms_top",
    ]
    ws2.append(cols)
    _style_header(ws2, 1, 1, len(cols))

    for r in results:
        ws2.append([
            r.periodo_mmyyyy or "",
            r.path.name,
            float(q2(r.base_delta)),
            float(q2(r.credito_pis)),
            float(q2(r.credito_cof)),
            float(q2(r.credito_total)),
            int(r.candidatos),
            int(r.pf_excluidos),
            " ".join(r.cfops_encontrados),
            " ".join(r.csts_encontrados),
            " ".join(r.ncms_encontrados),
        ])

    # Freeze header e filtro
    ws2.freeze_panes = "A2"
    ws2.auto_filter.ref = f"A1:{get_column_letter(len(cols))}{ws2.max_row}"

    # Formatação moeda nas colunas C..F
    for row in range(2, ws2.max_row + 1):
        for col in (3, 4, 5, 6):
            ws2.cell(row, col).number_format = _brl_number_format()

    # Zebra/borda
    _apply_table_style(ws2, 1, ws2.max_row, 1, len(cols))

    # Destaque: crédito_total > 0 (coluna F)
    # Pintar a linha toda quando F > 0
    green_fill = PatternFill("solid", fgColor="E2F0D9")
    ws2.conditional_formatting.add(
        f"A2:{get_column_letter(len(cols))}{ws2.max_row}",
        CellIsRule(operator="greaterThan", formula=["0"], stopIfTrue=False, fill=green_fill)
    )
    # A regra acima aplica ao range inteiro, mas o Excel avalia pela célula do canto superior esquerdo.
    # Para ficar correto “por linha”, fazemos uma segunda regra só na coluna F e mantemos a zebra:
    ws2.conditional_formatting.add(
        f"F2:F{ws2.max_row}",
        CellIsRule(operator="greaterThan", formula=["0"], stopIfTrue=True, fill=green_fill)
    )

    _autosize_columns(ws2)

    wb.save(out_xlsx)

def dec_br(s: str | Decimal | None) -> Decimal:
    if s is None:
        return Decimal("0")
    if isinstance(s, Decimal):
        return s
    txt = str(s).strip()
    if not txt:
        return Decimal("0")
    # "1.234.567,89" -> "1234567.89"
    txt = txt.replace(".", "").replace(",", ".")
    try:
        return Decimal(txt)
    except Exception:
        return Decimal("0")


def q2(d: Decimal) -> Decimal:
    return d.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)


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
    if not parts:
        return "", []
    reg = parts[0].upper().strip()
    fields = parts[1:]
    return reg, fields


def digits_only(s: str) -> str:
    return re.sub(r"\D+", "", s or "")


def parse_periodo_from_0000(fields: List[str]) -> Optional[str]:
    """
    Tentativa defensiva:
    - Procura DT_INI (DDMMAAAA) e retorna MMYYYY (formato que vocês usam no 1100)
    - Se achar YYYYMM, converte pra MMYYYY
    """
    # tenta achar token 8 dígitos
    for tok in fields:
        t = digits_only(tok)
        if len(t) == 8 and t.isdigit():
            # tenta DDMMAAAA
            dd = int(t[0:2])
            mm = int(t[2:4])
            yyyy = int(t[4:8])
            if 1 <= dd <= 31 and 1 <= mm <= 12 and 1900 <= yyyy <= 2100:
                return f"{mm:02d}{yyyy:d}"  # MMYYYY
            # tenta YYYYMMDD
            yyyy2 = int(t[0:4])
            mm2 = int(t[4:6])
            dd2 = int(t[6:8])
            if 1900 <= yyyy2 <= 2100 and 1 <= mm2 <= 12 and 1 <= dd2 <= 31:
                return f"{mm2:02d}{yyyy2:d}"
    # tenta achar YYYYMM
    for tok in fields:
        t = digits_only(tok)
        if len(t) == 6 and t.isdigit():
            yyyy = int(t[0:4])
            mm = int(t[4:6])
            if 1900 <= yyyy <= 2100 and 1 <= mm <= 12:
                return f"{mm:02d}{yyyy:d}"
    return None


def mmYYYY_to_key(mmYYYY: str) -> int:
    """
    Para ordenar MMYYYY corretamente (evita erro em virada de ano).
    "012024" -> 202401
    """
    s = (mmYYYY or "").strip()
    if len(s) != 6 or not s.isdigit():
        return 0
    mm = int(s[0:2])
    yyyy = int(s[2:6])
    return yyyy * 100 + mm


@dataclass
class FotoArquivoResult:
    path: Path
    periodo_mmyyyy: Optional[str]
    base_delta: Decimal
    credito_pis: Decimal
    credito_cof: Decimal
    credito_total: Decimal
    candidatos: int
    pf_excluidos: int
    cfops_encontrados: List[str]
    csts_encontrados: List[str]
    ncms_encontrados: List[str]


def foto_recuperavel_ind_torrado(
    path: Path,
    *,
    cfops_validos: set[str],
    csts_origem: set[str],
    ncms_validos: set[str],
) -> FotoArquivoResult:
    """
    Simula IND_TORRADO_CORR sem aplicar correções:
    soma VL_ITEM dos C170 candidatos e calcula crédito (9,25%).
    """
    periodo_mmyyyy: Optional[str] = None

    # 0150: COD_PART -> is_pf
    part_is_pf: Dict[str, bool] = {}

    # 0200: COD_ITEM -> NCM
    item_to_ncm: Dict[str, str] = {}

    current_doc_is_pf = False  # herdado do C100
    cfops_seen: Dict[str, int] = {}
    csts_seen: Dict[str, int] = {}
    ncms_seen: Dict[str, int] = {}

    base_delta = Decimal("0")
    candidatos = 0
    pf_excluidos = 0

    with path.open("r", encoding="utf-8-sig", errors="ignore") as f:
        for raw in f:
            ln = clean_line(raw)
            if not ln:
                continue
            reg, fields = split_reg_fields(ln)
            if not reg:
                continue

            if reg == "0000" and periodo_mmyyyy is None:
                periodo_mmyyyy = parse_periodo_from_0000(fields)

            elif reg == "0150":
                # Layout típico: |0150|COD_PART|NOME|...|CNPJ|CPF|...
                cod_part = (fields[0] if len(fields) > 0 else "").strip()
                cnpj = digits_only(fields[4]) if len(fields) > 4 else ""
                cpf = digits_only(fields[5]) if len(fields) > 5 else ""
                is_pf = (len(cpf) == 11) and (len(cnpj) != 14)
                if cod_part:
                    part_is_pf[cod_part] = is_pf

            elif reg == "0200":
                # |0200|COD_ITEM|...|COD_NCM|...
                cod_item = (fields[0] if len(fields) > 0 else "").strip()
                ncm = digits_only(fields[6]) if len(fields) > 6 else ""
                if cod_item and ncm:
                    item_to_ncm[cod_item] = ncm

            elif reg == "C100":
                # COD_PART em fields[2] (0-based após reg): IND_OPER, IND_EMIT, COD_PART, ...
                cod_part = (fields[2] if len(fields) > 2 else "").strip()
                current_doc_is_pf = bool(part_is_pf.get(cod_part, False))

            elif reg == "C170":
                # Se o documento for PF, ignora tudo
                if current_doc_is_pf:
                    pf_excluidos += 1
                    continue

                # Índices do C170 (0-based em fields):
                # fields[5]=VL_ITEM, fields[9]=CFOP, fields[1]=COD_ITEM, fields[23]=CST_PIS
                cod_item = (fields[1] if len(fields) > 1 else "").strip()
                vl_item = dec_br(fields[5]) if len(fields) > 5 else Decimal("0")
                cfop = (fields[9] if len(fields) > 9 else "").strip()
                cst_pis = (fields[23] if len(fields) > 23 else "").strip()

                if not cfop or not cfop.isdigit() or len(cfop) != 4:
                    continue
                if cfop not in cfops_validos:
                    continue

                # Já é crédito? então não é delta (não muda nada)
                if cst_pis in CST_CREDITO:
                    continue

                # Só alguns CSTs origem
                if cst_pis not in csts_origem:
                    continue

                if vl_item <= 0:
                    continue

                # NCM opcional via 0200
                if ncms_validos:
                    ncm = item_to_ncm.get(cod_item, "")
                    if not ncm or (ncm not in ncms_validos):
                        continue
                    ncms_seen[ncm] = ncms_seen.get(ncm, 0) + 1

                base_delta += vl_item
                candidatos += 1
                cfops_seen[cfop] = cfops_seen.get(cfop, 0) + 1
                csts_seen[cst_pis] = csts_seen.get(cst_pis, 0) + 1

    base_delta = q2(base_delta)
    credito_pis = q2(base_delta * ALIQUOTA_PIS)
    credito_cof = q2(base_delta * ALIQUOTA_COF)
    credito_total = q2(credito_pis + credito_cof)

    def top_keys(d: Dict[str, int], n: int = 10) -> List[str]:
        return [k for k, _v in sorted(d.items(), key=lambda kv: kv[1], reverse=True)[:n]]

    return FotoArquivoResult(
        path=path,
        periodo_mmyyyy=periodo_mmyyyy,
        base_delta=base_delta,
        credito_pis=credito_pis,
        credito_cof=credito_cof,
        credito_total=credito_total,
        candidatos=candidatos,
        pf_excluidos=pf_excluidos,
        cfops_encontrados=top_keys(cfops_seen),
        csts_encontrados=top_keys(csts_seen),
        ncms_encontrados=top_keys(ncms_seen),
    )


def fmt_br(d: Decimal) -> str:
    s = f"{d:.2f}"
    # 1234567.89 -> 1.234.567,89
    inteiro, frac = s.split(".")
    inteiro = re.sub(r"(?<!^)(?=(\d{3})+$)", ".", inteiro)
    return f"{inteiro},{frac}"


def main() -> int:
    ap = argparse.ArgumentParser(description="Foto do recuperável (simulação) em uma pasta de SPEDs.")
    ap.add_argument("pasta", help="Pasta com arquivos .txt do SPED")
    ap.add_argument("--ncm", action="append", default=[], help="NCMs válidos (pode repetir). Se não informar, não filtra por NCM.")
    ap.add_argument("--out", default="", help="Opcional: caminho CSV de saída")
    args = ap.parse_args()

    pasta = Path(args.pasta)
    if not pasta.exists() or not pasta.is_dir():
        print("Pasta inválida:", pasta)
        return 2

    ncms_validos = set([digits_only(x) for x in (args.ncm or []) if digits_only(x)])
    if not ncms_validos:
        ncms_validos = set(NCMS_CAFE_VERDE_PADRAO)  # pode estar vazio

    results: List[FotoArquivoResult] = []
    for p in sorted(pasta.glob("*.txt")):
        r = foto_recuperavel_ind_torrado(
            p,
            cfops_validos=set(CFOPS_TORRADO_PADRAO),
            csts_origem=set(CSTS_ORIGEM_PADRAO),
            ncms_validos=set(ncms_validos),
        )
        # só reporta se tem algum potencial (ou sempre, se preferir)
        results.append(r)

    # ordena por período (MMYYYY) quando disponível
    results.sort(key=lambda x: mmYYYY_to_key(x.periodo_mmyyyy or ""))

    total_base = sum((r.base_delta for r in results), Decimal("0"))
    total_cred = sum((r.credito_total for r in results), Decimal("0"))

    print("\n=== FOTO RECUPERÁVEL (SIMULAÇÃO) — IND_TORRADO (C170→CST51) ===")
    if ncms_validos:
        print("Filtro NCM ativo:", sorted(ncms_validos)[:10], ("..." if len(ncms_validos) > 10 else ""))
    else:
        print("Sem filtro NCM (considera itens pelo CFOP+CST)")

    for r in results:
        per = r.periodo_mmyyyy or "??????"
        if r.credito_total == 0:
            continue
        print(
            f"- {per} | arquivo={r.path.name} | base_delta=R$ {fmt_br(r.base_delta)} | "
            f"credito=R$ {fmt_br(r.credito_total)} | cand={r.candidatos} | pf_excl={r.pf_excluidos} | "
            f"cfops_top={r.cfops_encontrados[:5]} | csts_top={r.csts_encontrados[:5]}"
        )

    print("\nTOTAL base_delta=R$ ", fmt_br(q2(total_base)))
    print("TOTAL crédito=R$    ", fmt_br(q2(total_cred)))

    # Saída opcional: CSV ou XLSX (visual)
    if args.out:
        out = Path(args.out)
        out.parent.mkdir(parents=True, exist_ok=True)

        if out.suffix.lower() == ".xlsx":
            salvar_xlsx_visual(
                out,
                pasta=pasta,
                ncms_validos=ncms_validos,
                results=results,
                total_base=total_base,
                total_cred=total_cred,
            )
            print("\nXLSX gerado em:", out)
        else:
            # mantém CSV (ou .txt em formato CSV)
            with out.open("w", encoding="utf-8") as f:
                f.write(
                    "periodo,arquivo,base_delta,credito_pis,credito_cof,credito_total,candidatos,pf_excluidos,cfops_top,csts_top,ncms_top\n")
                for r in results:
                    f.write(
                        f"{r.periodo_mmyyyy or ''},"
                        f"{r.path.name},"
                        f"{str(r.base_delta).replace('.', ',')},"
                        f"{str(r.credito_pis).replace('.', ',')},"
                        f"{str(r.credito_cof).replace('.', ',')},"
                        f"{str(r.credito_total).replace('.', ',')},"
                        f"{r.candidatos},"
                        f"{r.pf_excluidos},"
                        f"\"{' '.join(r.cfops_encontrados)}\","
                        f"\"{' '.join(r.csts_encontrados)}\","
                        f"\"{' '.join(r.ncms_encontrados)}\""
                        "\n"
                    )
            print("\nCSV gerado em:", out)


if __name__ == "__main__":
    raise SystemExit(main())
