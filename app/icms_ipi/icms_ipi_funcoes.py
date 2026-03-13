from __future__ import annotations

from app.icms_ipi.icms_helpers import _s, _as_date_str, _as_decimal
from typing import Any, Dict, Optional
from app.icms_ipi.icms_helpers import _norm_str


def normalizar_itens_preview_icms_ipi(parsed: dict[str, Any]) -> list[dict[str, Any]]:
    """
    Normaliza a saída do parser SPED ICMS/IPI para um formato tabular único.

    Entrada esperada:
      - parsed["empresa"] -> {"cnpj": ..., "nome": ..., "uf": ...}
      - parsed["itens"] -> lista de dataclasses ou dicts
      - parsed["notas"] -> opcional

    Saída:
      - lista de dicts pronta para tabela/planilha/export

    Ajustes:
      - inclui num_item para melhorar o match por posição do item
      - preserva cod_item bruto e cod_item normalizado
      - reforça descricao/ncm/cfop com fallback seguro
    """
    empresa = parsed.get("empresa") or {}
    empresa_cnpj = _s(empresa.get("cnpj"))
    empresa_nome = _s(empresa.get("nome"))

    itens = parsed.get("itens") or []
    linhas: list[dict[str, Any]] = []

    for item in itens:
        # aceita dataclass, objeto ou dict
        if isinstance(item, dict):
            src = item
        else:
            src = item.__dict__

        participante = _s(src.get("participante_cnpj")) or _s(src.get("participante_nome"))

        cod_item_bruto = _s(src.get("cod_item"))
        cod_item_norm = cod_item_bruto.lstrip("0") if cod_item_bruto else ""
        if not cod_item_norm:
            cod_item_norm = cod_item_bruto

        descricao = (
            _s(src.get("descricao"))
            or _s(src.get("descr_item"))
            or _s(src.get("descricao_item"))
        )

        ncm = _s(src.get("ncm"))
        cfop = _s(src.get("cfop"))
        chave = _s(src.get("chave_nfe")) or _s(src.get("chave"))
        numero = _s(src.get("num_doc")) or _s(src.get("numero"))
        serie = _s(src.get("serie"))

        # importante para match por posição do item
        num_item = (
            _s(src.get("num_item"))
            or _s(src.get("item"))
            or _s(src.get("nr_item"))
            or _s(src.get("numero_item"))
        )

        linhas.append(
            {
                "empresa": empresa_cnpj or empresa_nome,
                "empresa_nome": empresa_nome,
                "periodo": _s(parsed.get("periodo")),

                "participante": participante,
                "participante_nome": _s(src.get("participante_nome")),
                "participante_cnpj": _s(src.get("participante_cnpj")),

                "data": _as_date_str(src.get("dt_doc")),
                "chave": chave,
                "numero": numero,
                "serie": serie,

                # item
                "num_item": num_item,
                "cod_item": cod_item_bruto,
                "cod_item_norm": cod_item_norm,
                "descricao": descricao,
                "ncm": ncm,
                "cfop": cfop,

                # valores
                "valor_item": _as_decimal(src.get("vl_item")),
                "valor_desconto": _as_decimal(src.get("vl_desc")),
                "valor_icms": _as_decimal(src.get("vl_icms")),
                "valor_ipi": _as_decimal(src.get("vl_ipi")),
                "contabil": _as_decimal(src.get("vl_item")),

                # metadados
                "origem": "EFD_ICMS_IPI",
                "origem_item": _s(src.get("origem_item")),
            }
        )

    return linhas

def _eh_c100(linha: Any) -> bool:
    return _norm_str(getattr(linha, "reg", "")).upper() == "C100"


def _eh_c170(linha: Any) -> bool:
    return _norm_str(getattr(linha, "reg", "")).upper() == "C170"