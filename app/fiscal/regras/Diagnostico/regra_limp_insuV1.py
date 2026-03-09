from app.fiscal.dto import RegistroFiscalDTO
from app.fiscal.regras.Diagnostico.base_regras import RegraBase
from typing import Optional, Any, Dict
from app.config.settings import ALIQUOTA_TOTAL
from decimal import Decimal, ROUND_HALF_UP
from collections import defaultdict
import logging

logger = logging.getLogger(__name__)


class RegraLimpezaInsumoV1(RegraBase):
    codigo = "SUP_LIMPEZA_INSUMO_V1"
    nome = "Possível crédito por higiene/limpeza como insumo (produção interna)"
    tipo = "OPORTUNIDADE"
    prioridade = "MEDIA"

    # Catálogo
    SLUG_CFOP_ENTRADA = "CFOP_ENTRADA_REVENDA"
    SLUG_CST_PIS_SEM_CRED = "CST_PIS_AQUIS_SEM_CRED"
    SLUG_CST_COF_SEM_CRED = "CST_COFINS_AQUIS_SEM_CRED"
    SLUG_NCM_LIMPEZA = "SUP_NCM_HIGIENE_LIMPEZA"

    def aplicar(self, registro: RegistroFiscalDTO) -> Optional[Dict[str, Any]]:
        try:

            # 🛡️ PF
            if getattr(registro, "is_pf", False):
                return None

            # ✅ Caminho B: roda apenas no agregado SUP (onde existe sup_* no _meta)
            reg = (registro.reg or "").strip()
            if reg != "C170_SUP_ENTRADA_AGG":
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

            # 2) ✅ Gate Caminho B: EXIGE produção interna detectada no agregador SUP
            tem_producao_interna = bool(meta0.get("sup_tem_producao_interna") or meta0.get("sup_producao_interna"))
            if not tem_producao_interna:
                # debug util p/ você confirmar que o detector não acionou
                print("[LIMP] GATE FAIL sup_producao_interna=", meta0.get("sup_tem_producao_interna"), meta0.get("sup_producao_interna"))
                return None

            # 3) Catálogo
            cat = self.get_catalogo(registro)
            if not cat:
                return None

            itens = dados[1:]
            if not itens:
                return None

            base_total = Decimal("0")
            qtd_total = 0
            qtd_validos = 0

            base_por_cfop = defaultdict(lambda: Decimal("0"))
            cfops_usados = set()
            ncms_usados = defaultdict(int)

            # slugs (use os que você já padronizou)
            SLUG_CFOP_ENTRADA_IND = "CFOP_ENTRADA_INDUSTRIALIZACAO"
            SLUG_CFOP_ENTRADA_REV = "CFOP_ENTRADA_REVENDA"
            SLUG_CST_PIS_SEM_CRED = "CST_PIS_AQUIS_SEM_CRED"
            SLUG_CST_COF_SEM_CRED = "CST_COFINS_AQUIS_SEM_CRED"
            SLUG_NCM_LIMPEZA = "SUP_NCM_HIGIENE_LIMPEZA"

            for it in itens:
                if not isinstance(it, dict):
                    continue

                qtd_total += 1

                rid = it.get("registro_id")
                cfop = str(it.get("cfop") or "").strip()
                if not cfop:
                    continue

                # ✅ entradas relevantes (industrialização OU revenda), igual EMB
                cfop_ind_ok = self.cfop_match(cat, SLUG_CFOP_ENTRADA_IND, cfop)
                cfop_rev_ok = self.cfop_match(cat, SLUG_CFOP_ENTRADA_REV, cfop)
                if not (cfop_ind_ok or cfop_rev_ok):
                    continue

                # CSTs (vieram enriquecidos no agregador)
                cst_pis = str(it.get("cst_pis") or "").strip()
                cst_cof = str(it.get("cst_cofins") or "").strip()

                pis_sem_cred = self.cst_match(cat, SLUG_CST_PIS_SEM_CRED, cst_pis) if cst_pis else False
                cof_sem_cred = self.cst_match(cat, SLUG_CST_COF_SEM_CRED, cst_cof) if cst_cof else False
                if not (pis_sem_cred or cof_sem_cred):
                    continue

                # NCM (enriquecido via 0200 no agregador)
                ncm = str(it.get("ncm") or "").strip().replace(".", "")
                if not ncm:
                    # conservador: sem NCM, não entra
                    continue

                ncm_limpeza = self.ncm_match(cat, SLUG_NCM_LIMPEZA, ncm)
                if not ncm_limpeza:
                    continue

                # valor (no seu agregador SUP vem como vl_item e/ou vl_opr)
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
                ncms_usados["LIMPEZA"] += 1

            if qtd_validos == 0 or base_total <= 0:
                return None

            q2 = lambda x: (x or Decimal("0")).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
            base_total = q2(base_total)
            cred = q2(base_total * ALIQUOTA_TOTAL)  # você já alinhou: usar ALIQUOTA_TOTAL

            # descrição
            top_cfops = sorted(base_por_cfop.items(), key=lambda kv: kv[1], reverse=True)[:5]
            cfops_txt = ", ".join([f"{c}" for c, _ in top_cfops]) if top_cfops else ", ".join(sorted(cfops_usados))

            desc = (
                "Higiene/limpeza detectado como possível insumo (grupo SUP_NCM_HIGIENE_LIMPEZA) "
                "em entradas relevantes, com produção interna (SUP) detectada: "
                f"base R$ {self.fmt_br(base_total)} ({qtd_validos} item(ns)). "
                f"Crédito estimado (proxy 9,25%) R$ {self.fmt_br(cred)}. "
                f"CFOP(s) (top): {cfops_txt}."
            )

            # registro_id final (âncora do agregado)
            rid_final = int(getattr(registro, "id", 0) or 0)
            if not rid_final:
                anchor = meta0.get("anchor_registro_id")
                try:
                    rid_final = int(anchor or 0)
                except Exception:
                    rid_final = 0
            if not rid_final:
                return None

            meta_out = dict(meta0) if isinstance(meta0, dict) else {}
            meta_out.update({
                "fonte_base": reg,
                "sup_producao_interna": True,
                "cats": {
                    "cfop_entrada_ind": SLUG_CFOP_ENTRADA_IND,
                    "cfop_entrada_rev": SLUG_CFOP_ENTRADA_REV,
                    "cst_pis_sem_cred": SLUG_CST_PIS_SEM_CRED,
                    "cst_cof_sem_cred": SLUG_CST_COF_SEM_CRED,
                    "ncm_limpeza": SLUG_NCM_LIMPEZA,
                },
                "qtd_total": int(qtd_total),
                "qtd_validos": int(qtd_validos),
                "base_total": str(base_total),
                "credito_estimado": str(cred),
                "cfops_usados": sorted(cfops_usados),
            })


            return {
                "registro_id": int(rid_final),
                "tipo": self.tipo,
                "codigo": self.codigo,
                "descricao": desc,
                "impacto_financeiro": str(cred),
                "prioridade": self.prioridade,
                "meta": meta_out,
                "regra": self.nome,
            }

        except Exception:
            logger.exception("Erro na regra %s", getattr(self, "codigo", "SUP_LIMPEZA_INSUMO_V1"))
            return None
