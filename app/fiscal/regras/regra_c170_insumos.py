from __future__ import annotations
from typing import Optional, Any, Dict
from decimal import Decimal
import time
from app.fiscal.dto import RegistroFiscalDTO
from app.fiscal.regras.base_regras import RegraBase
from app.sped.layouts.c170 import LAYOUT_C170
import logging
logger = logging.getLogger(__name__)


class RegraC170Insumos(RegraBase):
    """
    Diagnóstico apenas: sinaliza item C170 com padrão compatível com possível crédito.
    Catalog-driven + layout-driven.
    """
    codigo = "C170_INSUMO_V1"
    nome = "Possível crédito por insumo (C170) — catálogo-driven"
    tipo = "OPORTUNIDADE"

    SLUG_CFOP_ENTRADA = "CFOP_ENTRADA_REVENDA"        # ou CFOP_ENTRADA_GERAL
    SLUG_CST_PIS_CRED = "CST_PIS_CREDITO_NCUM"
    SLUG_CST_COF_CRED = "CST_COFINS_CREDITO_NCUM"

    def aplicar(self, registro: RegistroFiscalDTO) -> Optional[Dict[str, Any]]:
        try:

            if registro.reg != "C170":
                return None

            dados = registro.dados or []


            if not isinstance(dados, list):
                return None


            # layout-driven
            cfop = (dados[LAYOUT_C170.idx_cfop] or "").strip() if len(dados) > LAYOUT_C170.idx_cfop else ""
            cst_pis = (dados[LAYOUT_C170.idx_cst_pis] or "").strip() if len(dados) > LAYOUT_C170.idx_cst_pis else ""
            cst_cof = (dados[LAYOUT_C170.idx_cst_cofins] or "").strip() if len(dados) > LAYOUT_C170.idx_cst_cofins else ""
            vl_item = self.dec_br(dados[LAYOUT_C170.idx_vl_item]) if len(dados) > LAYOUT_C170.idx_vl_item else None


            if not cfop and not cst_pis and not cst_cof:
                return None

            cat = self.get_catalogo(registro)

            cfop_ok = self.cfop_match(cat, self.SLUG_CFOP_ENTRADA, cfop) if cfop else False
            pis_ok = self.cst_match(cat, self.SLUG_CST_PIS_CRED, cst_pis) if cst_pis else False
            cof_ok = self.cst_match(cat, self.SLUG_CST_COF_CRED, cst_cof) if cst_cof else False



            # sinal mínimo: entrada + (pis ou cof com crédito)
            if not (cfop_ok and (pis_ok or cof_ok)):
                return None

            prioridade = "MEDIA" if (pis_ok and cof_ok) else "BAIXA"

            return {
                "tipo": self.tipo,
                "codigo": self.codigo,
                "regra": getattr(self, "nome", self.__class__.__name__),
                "prioridade": prioridade,
                "descricao": (
                    "Item C170 com padrão compatível com possível crédito (catálogo fiscal). "
                    f"CFOP={cfop or 'N/D'}, CST_PIS={cst_pis or 'N/D'}, CST_COF={cst_cof or 'N/D'}, "
                    f"VL_ITEM={self.fmt_br(vl_item or Decimal('0'))}. "
                    "Revisar natureza do item (insumo) e enquadramento."
                ),
                "impacto_financeiro": None,
                "registro_id": int(registro.id),
                "meta": {
                    "slugs": {
                        "cfop_entrada": self.SLUG_CFOP_ENTRADA,
                        "cst_pis_credito": self.SLUG_CST_PIS_CRED,
                        "cst_cofins_credito": self.SLUG_CST_COF_CRED,
                    },
                    "match": {
                        "cfop_ok": bool(cfop_ok),
                        "pis_ok": bool(pis_ok),
                        "cof_ok": bool(cof_ok),
                    },
                    "valores": {
                        "cfop": cfop or None,
                        "cst_pis": cst_pis or None,
                        "cst_cof": cst_cof or None,
                        "vl_item": self.br_num(vl_item or Decimal("0")),
                    },
                    "contexto": {
                        "empresa_id": getattr(registro, "empresa_id", None),
                        "versao_id": getattr(registro, "versao_id", None),
                    },
                },
            }



        except Exception as e:

            logger.exception(

                "ERRO RegraC170Insumos | reg=%s id=%s linha=%s empresa_id=%s versao_id=%s | %s",

                getattr(registro, "reg", None),
                getattr(registro, "id", None),
                getattr(registro, "linha", None),
                getattr(registro, "empresa_id", None),
                getattr(registro, "versao_id", None),
                str(e),
            )

            return None
