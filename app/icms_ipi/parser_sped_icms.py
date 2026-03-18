from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import date
from decimal import Decimal
from pathlib import Path
from typing import Any

from app.icms_ipi.icms_helpers import (
    _parse_date_ddmmyyyy,
    _parse_decimal,
    _split_sped_line,
)


@dataclass
class ParticipanteIcms:
    cod_part: str
    nome: str | None
    cod_pais: str | None
    cnpj: str | None
    cpf: str | None
    ie: str | None
    cod_mun: str | None
    suframa: str | None
    end: str | None
    num: str | None
    compl: str | None
    bairro: str | None


@dataclass
class ProdutoIcms:
    cod_item: str
    descricao: str | None
    cod_barra: str | None
    unidade: str | None
    ncm: str | None
    ex_ipi: str | None
    cod_gen: str | None
    aliq_icms: Decimal


@dataclass
class NfIcmsPreviewNota:
    chave_nfe: str
    num_doc: str | None
    serie: str | None
    dt_doc: date | None
    vl_doc: Decimal
    vl_icms: Decimal
    cod_part: str | None = None
    participante_nome: str | None = None
    participante_cod_pais: str | None = None
    participante_cnpj: str | None = None
    participante_cpf: str | None = None
    participante_ie: str | None = None
    participante_cod_mun: str | None = None
    participante_suframa: str | None = None
    participante_end: str | None = None
    participante_num: str | None = None
    participante_compl: str | None = None
    participante_bairro: str | None = None
    ind_oper: str | None = None
    cod_sit: str | None = None
    cod_mod: str | None = None
    nome_arquivo: str = ""
    fonte: str = "EFD_ICMS_IPI"

@dataclass
class NfIcmsItemPreview:
    chave_nfe: str
    num_doc: str | None
    serie: str | None
    dt_doc: date | None
    cod_part: str | None
    participante_nome: str | None
    participante_cnpj: str | None
    num_item: str | None
    cod_item: str | None
    cod_item_norm: str | None
    descricao: str | None
    ncm: str | None
    cfop: str | None
    qtd: Decimal
    unid: str | None
    cst_icms: str | None
    aliq_icms: Decimal
    vl_item: Decimal
    vl_desc: Decimal
    vl_bc_icms: Decimal
    vl_icms: Decimal
    vl_ipi: Decimal
    origem_item: str
    nome_arquivo: str
    fonte: str = "EFD_ICMS_IPI"


def parse_sped_icms_ipi_preview(
    arquivo_path: str,
) -> dict[str, Any]:
    """
    Lê um SPED ICMS/IPI e devolve um preview simples, sem gravar no banco.

    Regras:
    - Usa 0000 para período/empresa
    - Usa 0150 para cadastro de participantes
    - Usa 0200 para cadastro de produtos
    - Usa C100 como cabeçalho da nota
    - Usa C170 para itens, quando existir
    - Soma VL_ICMS dos C190 subsequentes até o próximo C100
    - Se não houver C170 para uma nota, gera item fallback a partir do C190
    """
    path = Path(arquivo_path)
    if not path.exists():
        raise FileNotFoundError(f"Arquivo não encontrado: {arquivo_path}")

    periodo: str | None = None
    dt_ini: date | None = None
    dt_fin: date | None = None

    empresa_cnpj: str | None = None
    empresa_nome: str | None = None
    empresa_uf: str | None = None

    participantes: dict[str, ParticipanteIcms] = {}
    produtos: dict[str, ProdutoIcms] = {}

    notas: list[NfIcmsPreviewNota] = []
    itens: list[NfIcmsItemPreview] = []

    nota_atual: NfIcmsPreviewNota | None = None
    nota_tem_c170 = False

    def flush_nota() -> None:
        nonlocal nota_atual, nota_tem_c170
        if nota_atual:
            notas.append(nota_atual)
        nota_atual = None
        nota_tem_c170 = False

    with path.open("r", encoding="latin1") as f:
        for raw_line in f:
            line = raw_line.strip()
            if not line:
                continue

            reg, fields = _split_sped_line(line)
            if not reg:
                continue

            if reg == "0000":
                # |0000|COD_VER|COD_FIN|DT_INI|DT_FIN|NOME|CNPJ|CPF|UF|IE|COD_MUN|...
                dt_ini = _parse_date_ddmmyyyy(fields[2] if len(fields) > 2 else "")
                dt_fin = _parse_date_ddmmyyyy(fields[3] if len(fields) > 3 else "")
                empresa_nome = fields[4] if len(fields) > 4 else None
                empresa_cnpj = fields[5] if len(fields) > 5 else None
                empresa_uf = fields[7] if len(fields) > 7 else None

                if dt_ini:
                    periodo = dt_ini.strftime("%Y%m")

            elif reg == "0150":
                # |0150|COD_PART|NOME|COD_PAIS|CNPJ|CPF|IE|COD_MUN|SUFRAMA|END|NUM|COMPL|BAIRRO|
                cod_part = fields[0] if len(fields) > 0 else ""
                if cod_part:
                    participantes[cod_part] = ParticipanteIcms(
                        cod_part=cod_part,
                        nome=fields[1] if len(fields) > 1 else None,
                        cod_pais=fields[2] if len(fields) > 2 else None,
                        cnpj=fields[3] if len(fields) > 3 else None,
                        cpf=fields[4] if len(fields) > 4 else None,
                        ie=fields[5] if len(fields) > 5 else None,
                        cod_mun=fields[6] if len(fields) > 6 else None,
                        suframa=fields[7] if len(fields) > 7 else None,
                        end=fields[8] if len(fields) > 8 else None,
                        num=fields[9] if len(fields) > 9 else None,
                        compl=fields[10] if len(fields) > 10 else None,
                        bairro=fields[11] if len(fields) > 11 else None,
                    )

            elif reg == "0200":
                # |0200|COD_ITEM|DESCR_ITEM|COD_BARRA|COD_ANT_ITEM|UNID_INV|TIPO_ITEM|COD_NCM|EX_IPI|COD_GEN|LISTA_SERV|ALIQ_ICMS|...
                cod_item = fields[0] if len(fields) > 0 else ""
                if cod_item:
                    produtos[cod_item] = ProdutoIcms(
                        cod_item=cod_item,
                        descricao=fields[1] if len(fields) > 1 else None,
                        cod_barra=fields[2] if len(fields) > 2 else None,
                        unidade=fields[4] if len(fields) > 4 else None,
                        ncm=fields[6] if len(fields) > 6 else None,
                        ex_ipi=fields[7] if len(fields) > 7 else None,
                        cod_gen=fields[8] if len(fields) > 8 else None,
                        aliq_icms=_parse_decimal(fields[10] if len(fields) > 10 else "0"),
                    )

            elif reg == "C100":
                flush_nota()

                # |C100|IND_OPER|IND_EMIT|COD_PART|COD_MOD|COD_SIT|SER|NUM_DOC|CHV_NFE|DT_DOC|DT_E_S|VL_DOC|...
                ind_oper = fields[0] if len(fields) > 0 else None
                cod_part = fields[2] if len(fields) > 2 else None
                cod_mod = fields[3] if len(fields) > 3 else None
                cod_sit = fields[4] if len(fields) > 4 else None
                serie = fields[5] if len(fields) > 5 else None
                num_doc = fields[6] if len(fields) > 6 else None
                chave_nfe = fields[7] if len(fields) > 7 else ""
                dt_doc = _parse_date_ddmmyyyy(fields[8] if len(fields) > 8 else "")
                vl_doc = _parse_decimal(fields[10] if len(fields) > 10 else "0")

                part = participantes.get(cod_part or "")

                nota_atual = NfIcmsPreviewNota(
                    chave_nfe=chave_nfe,
                    num_doc=num_doc,
                    serie=serie,
                    dt_doc=dt_doc,
                    vl_doc=vl_doc,
                    vl_icms=Decimal("0"),
                    cod_part=cod_part,
                    participante_nome=part.nome if part else None,
                    participante_cod_pais=part.cod_pais if part else None,
                    participante_cnpj=part.cnpj if part else None,
                    participante_cpf=part.cpf if part else None,
                    participante_ie=part.ie if part else None,
                    participante_cod_mun=part.cod_mun if part else None,
                    participante_suframa=part.suframa if part else None,
                    participante_end=part.end if part else None,
                    participante_num=part.num if part else None,
                    participante_compl=part.compl if part else None,
                    participante_bairro=part.bairro if part else None,
                    ind_oper=ind_oper,
                    cod_sit=cod_sit,
                    cod_mod=cod_mod,
                    nome_arquivo=path.name,
                )
                nota_tem_c170 = False


            elif reg == "C170":

                # |C170|NUM_ITEM|COD_ITEM|DESCR_COMPL|QTD|UNID|VL_ITEM|VL_DESC|IND_MOV|CST_ICMS|CFOP|COD_NAT|VL_BC_ICMS|ALIQ_ICMS|VL_ICMS|VL_BC_ICMS_ST|ALIQ_ST|VL_ICMS_ST|...

                if nota_atual:
                    nota_tem_c170 = True
                    num_item = fields[0] if len(fields) > 0 else None
                    cod_item = fields[1] if len(fields) > 1 else None
                    cod_item_norm = (cod_item or "").lstrip("0") or cod_item
                    produto = produtos.get(cod_item or "") or produtos.get(cod_item_norm or "")

                    itens.append(
                        NfIcmsItemPreview(
                            chave_nfe=nota_atual.chave_nfe,
                            num_doc=nota_atual.num_doc,
                            serie=nota_atual.serie,
                            dt_doc=nota_atual.dt_doc,
                            cod_part=nota_atual.cod_part,
                            participante_nome=nota_atual.participante_nome,
                            participante_cnpj=nota_atual.participante_cnpj,
                            num_item=num_item,
                            cod_item=cod_item,
                            cod_item_norm=cod_item_norm,
                            descricao=(produto.descricao if produto else None),
                            ncm=(produto.ncm if produto else None),
                            cfop=fields[9] if len(fields) > 9 else None,
                            qtd=_parse_decimal(fields[3] if len(fields) > 3 else "0"),
                            unid=fields[4] if len(fields) > 4 else None,
                            cst_icms=fields[8] if len(fields) > 8 else None,
                            aliq_icms=_parse_decimal(fields[12] if len(fields) > 12 else "0"),
                            vl_item=_parse_decimal(fields[5] if len(fields) > 5 else "0"),
                            vl_desc=_parse_decimal(fields[6] if len(fields) > 6 else "0"),
                            vl_bc_icms=_parse_decimal(fields[11] if len(fields) > 11 else "0"),
                            vl_icms=_parse_decimal(fields[13] if len(fields) > 13 else "0"),
                            vl_ipi=_parse_decimal(fields[22] if len(fields) > 22 else "0"),
                            origem_item="C170",
                            nome_arquivo=path.name,
                        )

                    )

            elif reg == "C190":
                if nota_atual:
                    vl_opr = _parse_decimal(fields[3] if len(fields) > 3 else "0")
                    vl_bc_icms = _parse_decimal(fields[4] if len(fields) > 4 else "0")
                    vl_icms = _parse_decimal(fields[5] if len(fields) > 5 else "0")
                    vl_ipi = _parse_decimal(fields[9] if len(fields) > 9 else "0")
                    nota_atual.vl_icms += vl_icms
                    if not nota_tem_c170:
                        itens.append(
                            NfIcmsItemPreview(
                                chave_nfe=nota_atual.chave_nfe,
                                num_doc=nota_atual.num_doc,
                                serie=nota_atual.serie,
                                dt_doc=nota_atual.dt_doc,
                                cod_part=nota_atual.cod_part,
                                participante_nome=nota_atual.participante_nome,
                                participante_cnpj=nota_atual.participante_cnpj,
                                num_item=None,
                                cod_item=None,
                                cod_item_norm=None,
                                descricao=None,
                                ncm=None,
                                cfop=fields[1] if len(fields) > 1 else None,
                                qtd=Decimal("0"),
                                unid=None,
                                cst_icms=fields[0] if len(fields) > 0 else None,
                                aliq_icms=_parse_decimal(fields[2] if len(fields) > 2 else "0"),
                                vl_item=vl_opr,
                                vl_desc=Decimal("0"),
                                vl_bc_icms=vl_bc_icms,
                                vl_icms=vl_icms,
                                vl_ipi=vl_ipi,
                                origem_item="C190_FALLBACK",
                                nome_arquivo=path.name,
                            )

                        )

    flush_nota()

    total_notas = len(notas)
    total_itens = len(itens)
    total_vl_doc = sum((n.vl_doc for n in notas), Decimal("0"))
    total_vl_icms = sum((n.vl_icms for n in notas), Decimal("0"))
    total_vl_item = sum((i.vl_item for i in itens), Decimal("0"))

    return {
        "arquivo": path.name,
        "fonte": "EFD_ICMS_IPI",
        "periodo": periodo,
        "dt_ini": dt_ini,
        "dt_fin": dt_fin,
        "empresa": {
            "cnpj": empresa_cnpj,
            "nome": empresa_nome,
            "uf": empresa_uf,
        },
        "total_notas": total_notas,
        "total_itens": total_itens,
        "total_vl_doc": total_vl_doc,
        "total_vl_item": total_vl_item,
        "total_vl_icms": total_vl_icms,
        "participantes_count": len(participantes),
        "produtos_count": len(produtos),
        "notas_preview": [asdict(n) for n in notas[:20]],
        "itens_preview": [asdict(i) for i in itens[:50]],
        "notas": notas,
        "itens": itens,
        "participantes": participantes,
        "produtos": produtos,
    }