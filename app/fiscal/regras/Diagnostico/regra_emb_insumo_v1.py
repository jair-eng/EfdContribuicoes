from __future__ import annotations

from typing import Optional, Dict, Any
from decimal import Decimal, ROUND_HALF_UP
from collections import defaultdict
from app.fiscal.dto import RegistroFiscalDTO
from app.fiscal.regras.Diagnostico.base_regras import RegraBase
from app.config.settings import ALIQUOTA_TOTAL
import logging
logger = logging.getLogger(__name__)



ALIQUOTA_PIS_COFINS_NCUM = ALIQUOTA_TOTAL

class RegraEmbalagemInsumoV1(RegraBase):
    codigo = "EMB_INSUMO_V1"
    tipo = "OPORTUNIDADE"
    prioridade = "MEDIA"
    nome = "Embalagens como insumo (somente com industrialização detectada)"

    # ✅ Dependência: só roda se existir IND_AGRO_V1 ou IND_CAFE_V1 na versão
    DEPENDENCIAS_CODIGOS = ("IND_AGRO_V1", "IND_CAFE_V1")

    # ✅ Catálogo (NCM embalagem)
    CAT_NCM_EMB_39 = "NCM_EMBALAGEM_39"
    CAT_NCM_EMB_48 = "NCM_EMBALAGEM_48"
    CAT_NCM_EMB_73 = "NCM_EMBALAGEM_73"

    # ✅ CFOPs via catálogo (mesmo padrão agro/café)
    CAT_CFOP_IND = "CFOP_ENTRADA_INDUSTRIALIZACAO"
    CAT_CFOP_REVENDA = "CFOP_ENTRADA_REVENDA"

    DEBUG_MAX_SKIPS = 20


    def aplicar(self, registro: RegistroFiscalDTO) -> Optional[Dict[str, Any]]:
        try:

            # 🛡️ PF
            if getattr(registro, "is_pf", False):
                return None

            # Rodar só em agregados onde você já tem itens enriquecidos (NCM/descrição)
            reg = (registro.reg or "").strip()


            if reg not in ("C170_IND_TORRADO_AGG", "C170_SUP_ENTRADA_AGG"):
                return None

            dados = registro.dados or []

            if not isinstance(dados, list) or not dados:
                return None

            # 1) meta0 vem do header do agregador
            meta0 = {}
            if isinstance(dados[0], dict) and "_meta" in dados[0]:
                meta0 = dados[0].get("_meta") or {}
            if not isinstance(meta0, dict):
                meta0 = {}

            # 2) gate: só roda se existir indício AGRO (do agregador) OU produção interna SUP (do agregador)
            tem_indicio_agro = bool(meta0.get("tem_indicio_agro"))
            tem_producao_interna = bool(meta0.get("sup_tem_producao_interna") or meta0.get("sup_producao_interna"))


            if not (tem_indicio_agro or tem_producao_interna):

                return None

            if isinstance(dados[0], dict) and "_meta" in dados[0]:
                meta0 = dados[0].get("_meta") or {}


            # ✅ Catálogo (padrão)
            cat = self.get_catalogo(registro)
            if not cat:
                return None

            # ✅ Dependência: só roda se AGRO ou CAFÉ existirem nessa versão

            versao_id = int(getattr(registro, "versao_id", 0) or 0)

            if not versao_id:
                return None


            itens = dados[1:]

            if not itens:
                return None

            base_total = Decimal("0")
            qtd_total = 0
            qtd_validos = 0
            skips = 0

            base_por_cfop = defaultdict(lambda: Decimal("0"))
            cfops_usados = set()
            ncms_usados = defaultdict(int)

            primeiro = True
            for it in itens:
                if primeiro:

                    primeiro = False

                if not isinstance(it, dict):
                    continue

                qtd_total += 1

                rid = it.get("registro_id")
                cfop = str(it.get("cfop") or "").strip()
                if not cfop:
                    continue

                cfop_ind_ok = self.cfop_match(cat, self.CAT_CFOP_IND, cfop)
                cfop_rev_ok = self.cfop_match(cat, self.CAT_CFOP_REVENDA, cfop)
                if not (cfop_ind_ok or cfop_rev_ok):
                    continue

                ncm = str(it.get("ncm") or "").strip().replace(".", "")
                if not ncm:
                    continue

                emb_39 = self.ncm_match(cat, self.CAT_NCM_EMB_39, ncm)
                emb_48 = self.ncm_match(cat, self.CAT_NCM_EMB_48, ncm)
                emb_73 = self.ncm_match(cat, self.CAT_NCM_EMB_73, ncm)
                if not (emb_39 or emb_48 or emb_73):
                    continue

                vl_item = self.dec_br(it.get("vl_item")) or Decimal("0")
                vl_desc = self.dec_br(it.get("vl_desc")) or Decimal("0")
                vl_icms = self.dec_br(it.get("vl_icms")) or Decimal("0")

                val = vl_item - vl_desc - vl_icms
                if val <= 0:
                    continue

                qtd_validos += 1
                base_total += val
                cfops_usados.add(cfop)
                base_por_cfop[cfop] += val

                if emb_39:
                    ncms_usados["39*"] += 1
                elif emb_48:
                    ncms_usados["48*"] += 1
                else:
                    ncms_usados["73*"] += 1


            if qtd_validos == 0 or base_total <= 0:
                return None


            q2 = lambda x: (x or Decimal("0")).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
            base_total = q2(base_total)
            cred = q2(base_total * ALIQUOTA_PIS_COFINS_NCUM)

            # descrição
            top_cfops = sorted(base_por_cfop.items(), key=lambda kv: kv[1], reverse=True)[:5]
            cfops_txt = ", ".join([f"{c}" for c, _ in top_cfops]) if top_cfops else ", ".join(sorted(cfops_usados))

            desc = (
                "Embalagens detectadas como insumo (NCM 39*/48*/73*) em entradas relevantes: "
                f"base R$ {self.fmt_br(base_total)} ({qtd_validos} item(ns)). "
                f"Crédito estimado (proxy 9,25%) R$ {self.fmt_br(cred)}. "
                f"CFOP(s) (top): {cfops_txt}. "
                f"Distribuição NCM: {dict(ncms_usados)}. "
                "Aplicável apenas quando há industrialização/beneficiamento (AGRO ou CAFÉ)."
            )

            # registro_id final
            rid_final = int(getattr(registro, "id", 0) or 0)
            if not rid_final:
                # tenta ancorar (se existir)
                anchor = meta0.get("anchor_registro_id") if isinstance(meta0, dict) else None
                try:
                    rid_final = int(anchor or 0)
                except Exception:
                    rid_final = 0
            if not rid_final:
                return None

            meta_out = dict(meta0) if isinstance(meta0, dict) else {}
            meta_out.update({
                "fonte_base": reg,
                "dependencias": list(self.DEPENDENCIAS_CODIGOS),
                "cats_ncm": [self.CAT_NCM_EMB_39, self.CAT_NCM_EMB_48, self.CAT_NCM_EMB_73],
                "cats_cfop": [self.CAT_CFOP_IND, self.CAT_CFOP_REVENDA],
                "qtd_total": int(qtd_total),
                "qtd_validos": int(qtd_validos),
                "base_total": str(base_total),
                "credito_estimado": str(cred),
                "cfops_usados": sorted(cfops_usados),
                "distrib_ncm": dict(ncms_usados),
            })

            return {
                "registro_id": int(rid_final),
                "tipo": self.tipo,
                "codigo": self.codigo,
                "descricao": desc,
                "impacto_financeiro": cred,
                "prioridade": self.prioridade,
                "meta": meta_out,
                "regra": self.nome,
            }

        except Exception:
            logger.exception("Erro na regra %s", getattr(self, "codigo", "EMB_INSUMO_V1"))
            return None
