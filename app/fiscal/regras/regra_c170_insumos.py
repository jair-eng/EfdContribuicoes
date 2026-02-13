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
    codigo = "C170_INSUMO_V2"
    nome = "Possível crédito por insumo (C170) — catálogo-driven"
    tipo = "OPORTUNIDADE"

    SLUG_CFOP_ENTRADA = "CFOP_ENTRADA_REVENDA"

    # “alvo” = CST de crédito básico (50-56)
    SLUG_CST_PIS_ALVO_CRED = "SUP_CST_PIS_CREDITO"
    SLUG_CST_COF_ALVO_CRED = "SUP_CST_COFINS_CREDITO"

    # “origem” (opcional) = CSTs típicos que viram oportunidade (sem crédito hoje)
    SLUG_CST_PIS_SEM_CRED = "CST_PIS_AQUIS_SEM_CRED"
    SLUG_CST_COF_SEM_CRED = "CST_COFINS_AQUIS_SEM_CRED"

    # NCMs
    SLUG_NCM_SUPER_PRIOR = "SUP_NCM_SUPERMERCADO_PRIORITARIO"
    SLUG_NCM_GRAOS_10 = "NCM_GRAOS_FAMILIA_10"
    SLUG_NCM_GRAOS_12 = "NCM_GRAOS_FAMILIA_12"

    def aplicar(self, registro: RegistroFiscalDTO) -> Optional[Dict[str, Any]]:
        try:
            if registro.reg != "C170":
                return None

            dados = registro.dados or []
            if not isinstance(dados, list):
                return None

            cfop = (dados[LAYOUT_C170.idx_cfop] or "").strip() if len(dados) > LAYOUT_C170.idx_cfop else ""
            cst_pis = (dados[LAYOUT_C170.idx_cst_pis] or "").strip() if len(dados) > LAYOUT_C170.idx_cst_pis else ""
            cst_cof = (dados[LAYOUT_C170.idx_cst_cofins] or "").strip() if len(dados) > LAYOUT_C170.idx_cst_cofins else ""
            vl_item = self.dec_br(dados[LAYOUT_C170.idx_vl_item]) if len(dados) > LAYOUT_C170.idx_vl_item else None

            if not cfop:
                return None

            cat = self.get_catalogo(registro)

            cfop_ok = self.cfop_match(cat, self.SLUG_CFOP_ENTRADA, cfop)

            # Se já está em CST alvo de crédito, não é “oportunidade”
            pis_ja_credito = self.cst_match(cat, self.SLUG_CST_PIS_ALVO_CRED, cst_pis) if cst_pis else False
            cof_ja_credito = self.cst_match(cat, self.SLUG_CST_COF_ALVO_CRED, cst_cof) if cst_cof else False

            if not cfop_ok:
                return None
            if pis_ja_credito or cof_ja_credito:
                return None

            # Opcional: reforço “origem sem crédito”
            pis_sem_cred = self.cst_match(cat, self.SLUG_CST_PIS_SEM_CRED, cst_pis) if cst_pis else False
            cof_sem_cred = self.cst_match(cat, self.SLUG_CST_COF_SEM_CRED, cst_cof) if cst_cof else False

            # Se quiser ser bem conservador:
            # if not (pis_sem_cred or cof_sem_cred):
            #     return None

            # NCM (se disponível via meta/enriquecimento)
            ncm = None
            meta = getattr(registro, "meta", None) or {}
            if isinstance(meta, dict):
                ncm = "".join(filter(str.isdigit, str(meta.get("ncm") or meta.get("cod_ncm") or meta.get("COD_NCM") or ""))) or None


            ncm_super = bool(ncm) and self.ncm_match(cat, self.SLUG_NCM_SUPER_PRIOR, str(ncm))
            ncm_graos = bool(ncm) and (
                self.ncm_match(cat, self.SLUG_NCM_GRAOS_10, str(ncm)) or
                self.ncm_match(cat, self.SLUG_NCM_GRAOS_12, str(ncm))
            )

            if ncm_super:
                prioridade = "ALTA"
            elif ncm_graos:
                prioridade = "MEDIA"
            else:
                prioridade = "BAIXA"

            return {
                "tipo": self.tipo,
                "codigo": self.codigo,
                "regra": getattr(self, "nome", self.__class__.__name__),
                "prioridade": prioridade,
                "descricao": (
                    "Entrada (CFOP) com CST atual sem crédito (potencial de ajuste). "
                    f"CFOP={cfop or 'N/D'}, CST_PIS={cst_pis or 'N/D'}, CST_COF={cst_cof or 'N/D'}, "
                    f"NCM={ncm or 'N/D'}, VL_ITEM={self.fmt_br(vl_item or Decimal('0'))}. "
                    "Revisar natureza do item e enquadramento (insumo/mercadoria)."
                ),
                "impacto_financeiro": None,
                "registro_id": int(registro.id),
                "meta": {
                    "slugs": {
                        "cfop_entrada": self.SLUG_CFOP_ENTRADA,
                        "cst_pis_alvo_credito": self.SLUG_CST_PIS_ALVO_CRED,
                        "cst_cof_alvo_credito": self.SLUG_CST_COF_ALVO_CRED,
                        "ncm_super_prior": self.SLUG_NCM_SUPER_PRIOR,
                        "ncm_graos_10": self.SLUG_NCM_GRAOS_10,
                        "ncm_graos_12": self.SLUG_NCM_GRAOS_12,
                    },
                    "match": {
                        "cfop_ok": bool(cfop_ok),
                        "pis_ja_credito": bool(pis_ja_credito),
                        "cof_ja_credito": bool(cof_ja_credito),
                        "pis_sem_cred": bool(pis_sem_cred),
                        "cof_sem_cred": bool(cof_sem_cred),
                        "ncm_super": bool(ncm_super),
                        "ncm_graos": bool(ncm_graos),
                    },
                    "valores": {
                        "cfop": cfop or None,
                        "cst_pis": cst_pis or None,
                        "cst_cof": cst_cof or None,
                        "ncm": str(ncm) if ncm else None,
                        "vl_item": self.br_num(vl_item or Decimal("0")),
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
