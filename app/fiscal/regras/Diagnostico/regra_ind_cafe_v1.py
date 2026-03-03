from __future__ import annotations

from decimal import Decimal, ROUND_HALF_UP
from collections import defaultdict
import logging
from typing import Optional, Dict, Any

from app.fiscal.dto import RegistroFiscalDTO
from app.fiscal.regras.Diagnostico.base_regras import RegraBase
from app.config.settings import IND_AGRO_ALIQUOTA_EFETIVA  # você já usa como proxy
from app.sped.utils_geral import _is_cafe_by_desc, _is_cafe_by_ncm

logger = logging.getLogger(__name__)


class RegraIndustrializacaoCafeV1(RegraBase):
    """
    Regra específica de CAFÉ (industrialização/beneficiamento).
    - Catálogo FIRST (NCM_CAFE_FAMILIA_09)
    - Fallback heurístico só se não houver NCM/desc suficientes
    - Filtra entradas via CFOP (catálogo): industrialização OU revenda
    - Se café dominar a base (>= LIMIAR_PCT_CAFE), gera apontamento único.
    """

    codigo = "IND_CAFE_V1"
    tipo = "OPORTUNIDADE"
    prioridade = "MEDIA"
    nome = "Industrialização / Beneficiamento — Café"

    # catálogo (NCM)
    CAT_NCM_CAFE = "NCM_CAFE_FAMILIA_09"

    # catálogo (CFOP)
    # ✅ “grupo café” (se existir e você quiser manter): pode conter 1101/1102/2101/2102/3101/3102
    CAT_CFOP_GRUPO = "CFOP_CAFE_TORRADO_ENTRADA"

    # ✅ grupos genéricos (mais corretos p/ separar)
    CAT_CFOP_INDUSTRIALIZACAO = "CFOP_ENTRADA_INDUSTRIALIZACAO"
    CAT_CFOP_REVENDA = "CFOP_ENTRADA_REVENDA"

    # limiares
    LIMIAR_PCT_CAFE = Decimal("80.00")  # % mínimo da base atribuída a café
    MIN_ITENS_CAFE = 3  # evita disparo com amostra minúscula

    DEBUG_MAX_SKIPS = 25
    DEBUG_SAMPLE = False

    def aplicar(self, registro: RegistroFiscalDTO) -> Optional[Dict[str, Any]]:
        try:
            # 🛡️ PF
            if getattr(registro, "is_pf", False):
                return None

            reg = (registro.reg or "").strip()
            if reg not in ("C190_IND_TORRADO_AGG", "C170_IND_TORRADO_AGG"):
                return None

            dados = registro.dados or []
            if not isinstance(dados, list) or not dados:
                return None

            meta = {}
            if isinstance(dados[0], dict) and "_meta" in dados[0]:
                meta = dados[0].get("_meta") or {}

            # opcional: só dispara se já houve “indício agro” via 0200
            if not self.to_bool(meta.get("tem_indicio_agro")):
                return None

            itens = dados[1:]
            if not itens:
                return None

            if self.DEBUG_SAMPLE and itens and isinstance(itens[0], dict):
                logger.debug("[IND_CAFE SAMPLE] %s", itens[0])

            # catálogo
            try:
                cat = self.get_catalogo(registro)
            except Exception:
                logger.exception("IND_CAFE: falha ao carregar catálogo")
                return None
            if not cat:
                return None

            # acumuladores
            base_total = Decimal("0")
            base_cafe = Decimal("0")
            base_revenda = Decimal("0")
            base_industrializacao = Decimal("0")

            qtd_total = 0
            qtd_validos = 0

            qtd_itens_com_info = 0
            qtd_itens_cafe = 0
            qtd_itens_cafe_cat = 0
            qtd_itens_cafe_fallback = 0

            qtd_revenda = 0
            qtd_industrializacao = 0

            cfops_usados = set()
            cfops_revenda_usados = set()
            base_por_cfop = defaultdict(lambda: Decimal("0"))

            skips_logados = 0

            for it in itens:
                if not isinstance(it, dict):
                    continue

                qtd_total += 1

                cfop = str(it.get("cfop") or "").strip()
                vl_opr = it.get("vl_opr")
                rid = it.get("registro_id")

                if not cfop:
                    continue

                # -------------------------------------------------
                # CFOP catálogo: aceita INDUSTRIALIZAÇÃO OU REVENDA
                # (e opcionalmente aceita o “grupo café” também)
                # -------------------------------------------------
                cfop_ind_ok = self.cfop_match(cat, self.CAT_CFOP_INDUSTRIALIZACAO, cfop)
                cfop_rev_ok = self.cfop_match(cat, self.CAT_CFOP_REVENDA, cfop)
                cfop_grupo_ok = self.cfop_match(cat, self.CAT_CFOP_GRUPO, cfop)

                if not (cfop_ind_ok or cfop_rev_ok or cfop_grupo_ok):
                    continue

                # valor
                try:
                    val = self.dec_br(vl_opr) or Decimal("0")
                except Exception as e:
                    if skips_logados < self.DEBUG_MAX_SKIPS:
                        logger.debug(
                            "IND_CAFE skip rid=%s cfop=%s: vl_opr inválido (%s) err=%s",
                            rid, cfop, vl_opr, e
                        )
                        skips_logados += 1
                    continue

                if val <= 0:
                    continue

                # info (NCM/desc)
                desc_item = str(it.get("descricao") or "").strip()
                ncm = str(it.get("ncm") or "").strip().replace(".", "")
                tem_info = bool(ncm) or bool(desc_item)
                if tem_info:
                    qtd_itens_com_info += 1

                # -------------------------------------------------
                # CAFÉ: catálogo FIRST (NCM), fallback heurístico
                # -------------------------------------------------
                eh_cafe = False
                eh_cafe_cat = False
                eh_cafe_fb = False

                ncm_limpo = (str(ncm or "").strip() or "").replace(".", "")
                if ncm_limpo:
                    try:
                        eh_cafe_cat = self.ncm_match(cat, self.CAT_NCM_CAFE, ncm_limpo)
                    except Exception:
                        eh_cafe_cat = False

                if eh_cafe_cat:
                    eh_cafe = True
                else:
                    if tem_info:
                        eh_cafe_fb = _is_cafe_by_ncm(ncm) or _is_cafe_by_desc(desc_item)
                        eh_cafe = bool(eh_cafe_fb)

                # -------------------------------------------------
                # Contabiliza recorte (CFOP ok) na base total
                # -------------------------------------------------
                qtd_validos += 1
                base_total += val
                cfops_usados.add(cfop)
                base_por_cfop[cfop] += val

                # -------------------------------------------------
                # Segregação revenda x industrialização (catálogo-driven)
                # regra: se bater revenda => revenda; senão => industrialização
                # (grupo_ok sozinho não define classe; então usamos rev_ok como decisor)
                # -------------------------------------------------
                if cfop_rev_ok:
                    cfops_revenda_usados.add(cfop)
                    base_revenda += val
                    qtd_revenda += 1
                else:
                    base_industrializacao += val
                    qtd_industrializacao += 1

                # café
                if eh_cafe:
                    qtd_itens_cafe += 1
                    base_cafe += val
                    if eh_cafe_cat:
                        qtd_itens_cafe_cat += 1
                    elif eh_cafe_fb:
                        qtd_itens_cafe_fallback += 1

            if qtd_validos == 0 or base_total <= 0:
                return None

            # quantiza
            base_total = base_total.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
            base_cafe = base_cafe.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
            base_revenda = base_revenda.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
            base_industrializacao = base_industrializacao.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)

            # % café
            pct_cafe = Decimal("0.00")
            if base_total > 0:
                pct_cafe = (base_cafe / base_total * Decimal("100")).quantize(
                    Decimal("0.01"), rounding=ROUND_HALF_UP
                )

            # guard-rails
            if qtd_itens_cafe < int(self.MIN_ITENS_CAFE):
                return None
            if pct_cafe < self.LIMIAR_PCT_CAFE:
                return None

            # crédito estimado (proxy)
            cred = (base_total * IND_AGRO_ALIQUOTA_EFETIVA).quantize(
                Decimal("0.01"), rounding=ROUND_HALF_UP
            )
            aliquota_pct = (IND_AGRO_ALIQUOTA_EFETIVA * Decimal("100")).quantize(Decimal("0.01"))

            # % revenda
            pct_revenda = Decimal("0.00")
            if base_total > 0:
                pct_revenda = (base_revenda / base_total * Decimal("100")).quantize(
                    Decimal("0.01"), rounding=ROUND_HALF_UP
                )

            # descrição
            desc = (
                "Possível crédito estimado (industrialização/beneficiamento de café): "
                f"base filtrada por CFOP (catálogo) = R$ {self.fmt_br(base_total)} "
                f"(alíquota efetiva {self.fmt_br(aliquota_pct)}%). "
                f"Participação café (NCM catálogo/fallback) = {self.fmt_br(pct_cafe)}% "
                f"(base café R$ {self.fmt_br(base_cafe)}). "
                f"Composição: industrialização R$ {self.fmt_br(base_industrializacao)} "
                f"({qtd_industrializacao} item(ns)); "
                f"revenda como insumo R$ {self.fmt_br(base_revenda)} "
                f"({qtd_revenda} item(ns), {self.fmt_br(pct_revenda)}%). "
            )

            if cfops_revenda_usados:
                desc += (
                    f"CFOP(s) revenda detectados: {', '.join(sorted(cfops_revenda_usados))}. "
                    "Validar tratamento como insumo."
                )
            else:
                desc += "Sem CFOP de revenda no recorte; validar enquadramento/insumo."

            # meta detalhado
            flags = dict(meta) if isinstance(meta, dict) else {}
            flags.update({
                "fonte_base": reg,
                "filtro_cat_first": True,
                "fallback_heuristico_habilitado": True,

                "cat_ncm_cafe": self.CAT_NCM_CAFE,
                "cat_cfop_grupo": self.CAT_CFOP_GRUPO,
                "cat_cfop_industrializacao": self.CAT_CFOP_INDUSTRIALIZACAO,
                "cat_cfop_revenda": self.CAT_CFOP_REVENDA,

                "qtd_itens_total": int(qtd_total),
                "qtd_itens_validos": int(qtd_validos),

                "qtd_itens_com_info": int(qtd_itens_com_info),
                "qtd_itens_cafe": int(qtd_itens_cafe),
                "qtd_itens_cafe_cat": int(qtd_itens_cafe_cat),
                "qtd_itens_cafe_fallback": int(qtd_itens_cafe_fallback),

                "base_compra_filtrada": str(base_total),
                "base_cafe": str(base_cafe),
                "pct_base_cafe": str(pct_cafe),

                "base_industrializacao": str(base_industrializacao),
                "base_revenda_como_insumo": str(base_revenda),
                "pct_base_revenda": str(pct_revenda),
                "qtd_itens_industrializacao": int(qtd_industrializacao),
                "qtd_itens_revenda": int(qtd_revenda),

                "cfops_usados": sorted(cfops_usados),
                "cfops_revenda_usados": sorted(cfops_revenda_usados),
                "base_por_cfop": {
                    k: str(v.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP))
                    for k, v in sorted(base_por_cfop.items())
                },

                "aliquota_efetiva": f"{self.fmt_br(aliquota_pct)}%",
                "credito_estimado_total": str(cred),

                "limiar_pct_cafe": str(self.LIMIAR_PCT_CAFE),
                "min_itens_cafe": int(self.MIN_ITENS_CAFE),
            })

            # registro_id: usa o próprio, mas garante âncora real se necessário
            rid = int(getattr(registro, "id", 0) or 0)
            anchor_rid = meta.get("anchor_registro_id") if isinstance(meta, dict) else None
            try:
                anchor_rid = int(anchor_rid) if anchor_rid is not None else None
            except Exception:
                anchor_rid = None

            registro_id_final = rid if rid > 0 else (anchor_rid or 0)
            if not registro_id_final:
                return None  # evita “sem FK”

            return {
                "registro_id": int(registro_id_final),
                "tipo": self.tipo,
                "codigo": self.codigo,
                "descricao": desc,
                "impacto_financeiro": cred,
                "prioridade": self.prioridade,
                "meta": flags,
                "regra": self.nome,
            }

        except Exception:
            logger.exception("Erro na regra %s", getattr(self, "codigo", "IND_CAFE_V1"))
            return None
