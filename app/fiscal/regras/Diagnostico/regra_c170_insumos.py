from __future__ import annotations
from typing import Optional, Any, Dict, List
from app.fiscal.dto import RegistroFiscalDTO
from app.fiscal.regras.Diagnostico.base_regras import RegraBase
from collections import defaultdict
from decimal import Decimal, ROUND_HALF_UP
import logging
logger = logging.getLogger(__name__)


class RegraC170Insumos(RegraBase):
    codigo = "C170_INSUMO_V2"
    nome = "Possível crédito por insumo (C170) — catálogo-driven"
    tipo = "OPORTUNIDADE"

    SLUG_CFOP_ENTRADA = "CFOP_ENTRADA_REVENDA"
    SLUG_CST_PIS_ALVO_CRED = "SUP_CST_PIS_CREDITO"
    SLUG_CST_COF_ALVO_CRED = "SUP_CST_COFINS_CREDITO"
    SLUG_CST_PIS_SEM_CRED = "CST_PIS_AQUIS_SEM_CRED"
    SLUG_CST_COF_SEM_CRED = "CST_COFINS_AQUIS_SEM_CRED"

    SLUG_NCM_SUPER_PRIOR = "SUP_NCM_SUPERMERCADO_PRIORITARIO"
    SLUG_NCM_GRAOS_10 = "NCM_GRAOS_FAMILIA_10"
    SLUG_NCM_GRAOS_12 = "NCM_GRAOS_FAMILIA_12"

    DEBUG_SAMPLE_IDS = 40  # quantos registro_id guardar na meta (para drilldown futuro)

    def aplicar(self, registro: RegistroFiscalDTO) -> Optional[Dict[str, Any]]:
        try:
            # ✅ agora é agregado
            if (registro.reg or "").strip() != "C170_INSUMO_AGG":
                return None

            dados = registro.dados or []
            if not isinstance(dados, list) or not dados:
                return None

            meta0 = {}
            if isinstance(dados[0], dict) and "_meta" in dados[0]:
                meta0 = dados[0].get("_meta") or {}

            itens = dados[1:]
            if not itens:
                return None

            cat = self.get_catalogo(registro)
            if not cat:
                return None

            # acumuladores
            base_total = Decimal("0")
            qtd_total = 0
            qtd_candidatos = 0

            base_por_cfop = defaultdict(lambda: Decimal("0"))
            base_por_ncm = defaultdict(lambda: Decimal("0"))
            csts_pis = defaultdict(int)
            csts_cof = defaultdict(int)

            candidatos_ids: List[int] = []

            for it in itens:
                if not isinstance(it, dict):
                    continue
                qtd_total += 1

                cfop = str(it.get("cfop") or "").strip()
                cst_pis = str(it.get("cst_pis") or "").strip()
                cst_cof = str(it.get("cst_cofins") or "").strip()
                ncm = str(it.get("ncm") or "").strip()
                rid = int(it.get("registro_id") or 0)

                if not cfop:
                    continue

                cfop_ok = self.cfop_match(cat, self.SLUG_CFOP_ENTRADA, cfop)
                if not cfop_ok:
                    continue

                # se já é CST de crédito alvo, não é oportunidade
                pis_ja_credito = self.cst_match(cat, self.SLUG_CST_PIS_ALVO_CRED, cst_pis) if cst_pis else False
                cof_ja_credito = self.cst_match(cat, self.SLUG_CST_COF_ALVO_CRED, cst_cof) if cst_cof else False
                if pis_ja_credito or cof_ja_credito:
                    continue

                # opcional: reforço “origem sem crédito”
                pis_sem_cred = self.cst_match(cat, self.SLUG_CST_PIS_SEM_CRED, cst_pis) if cst_pis else False
                cof_sem_cred = self.cst_match(cat, self.SLUG_CST_COF_SEM_CRED, cst_cof) if cst_cof else False
                # se quiser ficar conservador:
                # if not (pis_sem_cred or cof_sem_cred):
                #     continue

                # valor do item
                vl_item = self.dec_br(it.get("vl_item")) or Decimal("0")
                if vl_item <= 0:
                    continue

                qtd_candidatos += 1
                base_total += vl_item
                base_por_cfop[cfop] += vl_item
                if ncm:
                    base_por_ncm[ncm] += vl_item
                if cst_pis:
                    csts_pis[cst_pis] += 1
                if cst_cof:
                    csts_cof[cst_cof] += 1
                if rid > 0 and len(candidatos_ids) < self.DEBUG_SAMPLE_IDS:
                    candidatos_ids.append(rid)

            if qtd_candidatos <= 0 or base_total <= 0:
                return None

            # prioridade por NCM (agora por conjunto)
            # (se qualquer candidato cair nesses grupos, sobe prioridade)
            ncm_super = False
            ncm_graos = False
            for ncm in list(base_por_ncm.keys())[:200]:
                if self.ncm_match(cat, self.SLUG_NCM_SUPER_PRIOR, ncm):
                    ncm_super = True
                    break
            if not ncm_super:
                for ncm in list(base_por_ncm.keys())[:400]:
                    if self.ncm_match(cat, self.SLUG_NCM_GRAOS_10, ncm) or self.ncm_match(cat, self.SLUG_NCM_GRAOS_12, ncm):
                        ncm_graos = True
                        break

            if ncm_super:
                prioridade = "ALTA"
            elif ncm_graos:
                prioridade = "MEDIA"
            else:
                prioridade = "BAIXA"

            q2 = lambda x: (x or Decimal("0")).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
            base_total = q2(base_total)

            top_cfops = [c for c, _ in sorted(base_por_cfop.items(), key=lambda kv: kv[1], reverse=True)[:5]]
            top_ncms = [n for n, _ in sorted(base_por_ncm.items(), key=lambda kv: kv[1], reverse=True)[:5]]
            top_cst_pis = [c for c, _ in sorted(csts_pis.items(), key=lambda kv: kv[1], reverse=True)[:5]]
            top_cst_cof = [c for c, _ in sorted(csts_cof.items(), key=lambda kv: kv[1], reverse=True)[:5]]

            desc = (
                f"Possível crédito por insumo (agrupado): {qtd_candidatos} item(ns) candidato(s) "
                f"em entradas (CFOP catálogo). Base somada VL_ITEM ≈ R$ {self.fmt_br(base_total)}. "
                f"CFOP(s) top: {', '.join(top_cfops) or 'N/D'}. "
                f"NCM(s) top: {', '.join(top_ncms) or 'N/D'}. "
                f"CST PIS top: {', '.join(top_cst_pis) or 'N/D'}, CST COFINS top: {', '.join(top_cst_cof) or 'N/D'}. "
                "Revisar natureza do item e enquadramento (insumo/mercadoria)."
            )

            # anchor para não ficar sem FK
            anchor_registro_id = int(meta0.get("anchor_registro_id") or 0) if isinstance(meta0, dict) else 0
            registro_id_final = int(getattr(registro, "id", 0) or 0) or anchor_registro_id
            if not registro_id_final:
                return None

            meta_out = dict(meta0) if isinstance(meta0, dict) else {}
            meta_out.update({
                "fonte_base": "C170_INSUMO_AGG",
                "qtd_total_itens": int(qtd_total),
                "qtd_candidatos": int(qtd_candidatos),
                "base_total_vl_item": str(base_total),
                "top_cfops": top_cfops,
                "top_ncms": top_ncms,
                "top_cst_pis": top_cst_pis,
                "top_cst_cofins": top_cst_cof,
                "sample_registro_ids": candidatos_ids,
                "slugs": {
                    "cfop_entrada": self.SLUG_CFOP_ENTRADA,
                    "cst_pis_alvo_credito": self.SLUG_CST_PIS_ALVO_CRED,
                    "cst_cof_alvo_credito": self.SLUG_CST_COF_ALVO_CRED,
                    "ncm_super_prior": self.SLUG_NCM_SUPER_PRIOR,
                    "ncm_graos_10": self.SLUG_NCM_GRAOS_10,
                    "ncm_graos_12": self.SLUG_NCM_GRAOS_12,
                },
            })

            return {
                "tipo": self.tipo,
                "codigo": self.codigo,
                "regra": getattr(self, "nome", self.__class__.__name__),
                "prioridade": prioridade,
                "descricao": desc,
                "impacto_financeiro": None,
                "registro_id": int(registro_id_final),
                "meta": meta_out,
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