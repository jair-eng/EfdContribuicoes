from __future__ import annotations

from decimal import Decimal, ROUND_HALF_UP
from collections import defaultdict
from typing import Optional, Dict, Any
import logging

from app.fiscal.dto import RegistroFiscalDTO
from app.fiscal.regras.base_regras import RegraBase
from app.config.settings import IND_AGRO_ALIQUOTA_EFETIVA
from app.sped.utils_geral import _is_cafe_ou_graos, _is_cafe_by_ncm, _is_cafe_by_desc

logger = logging.getLogger(__name__)


class RegraIndustrializacaoAgroV1(RegraBase):
    codigo = "IND_AGRO_V1"
    tipo = "OPORTUNIDADE"          # pode virar ALERTA em runtime
    prioridade = "MEDIA"
    nome = "Industrialização / Beneficiamento de Commodities Agrícolas"

    # ✅ grupos do catálogo (NCM)
    CAT_NCM_CAFE = "NCM_CAFE_FAMILIA_09"
    CAT_NCM_GRAOS_10 = "NCM_GRAOS_FAMILIA_10"
    CAT_NCM_GRAOS_12 = "NCM_GRAOS_FAMILIA_12"

    # ✅ CFOPs via catálogo (entradas relevantes)
    CAT_CFOP_GRUPO = "CFOP_ENTRADA_INDUSTRIALIZACAO"
    CAT_CFOP_REVENDA = "CFOP_ENTRADA_REVENDA"

    DEBUG_MAX_SKIPS = 25
    DEBUG_SAMPLE = False

    # -------------------------------------------------
    # ✅ Guard-rails (anti falso-positivo)
    # -------------------------------------------------
    BLOQUEAR_SE_MONOFASICO = True
    LIMIAR_SCORE_MONO = 70

    MIN_SHARE_INFO_QTD = Decimal("0.60")              # 60%
    MIN_SHARE_CONFIRMADA_CAT_BASE = Decimal("0.80")   # 80%
    LIMIAR_SHARE_CAFE_PARA_ABORTAR_AGRO = Decimal("0.60")  # 60% da base com info
    MAX_SHARE_SEM_INFO_BASE_PARA_OPORTUNIDADE = Decimal("0.30")  # 30%

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

            # ✅ bloqueio por monofásico (anti falso positivo)
            if self.BLOQUEAR_SE_MONOFASICO:
                try:
                    perfil_mono = bool(meta.get("perfil_monofasico"))
                    score_mono = int(meta.get("score_monofasico") or 0)
                    if perfil_mono and score_mono >= self.LIMIAR_SCORE_MONO:
                        logger.debug("IND_AGRO: bloqueado por perfil monofásico (score=%s)", score_mono)
                        return None
                except Exception:
                    pass

            if not self.to_bool(meta.get("tem_indicio_agro")):
                logger.debug("IND_AGRO: sem indício de commodity agro (0200) -> abort")
                return None

            # ✅ Âncora do agregador
            anchor_reg_base = str(meta.get("anchor_reg_base") or "").strip() or None
            anchor_linha = meta.get("anchor_linha")
            anchor_registro_id = meta.get("anchor_registro_id")

            try:
                anchor_linha = int(anchor_linha) if anchor_linha is not None else None
            except Exception:
                anchor_linha = None

            itens = dados[1:]
            if not itens:
                return None

            if self.DEBUG_SAMPLE and itens and isinstance(itens[0], dict):
                logger.debug("[IND_AGRO SAMPLE] %s", itens[0])

            # -------------------------------------------------
            # Catálogo fiscal
            # -------------------------------------------------
            try:
                cat = self.get_catalogo(registro)
            except Exception:
                logger.exception("IND_AGRO: falha ao carregar catálogo")
                return None

            if not cat:
                logger.debug("IND_AGRO: catálogo vazio/None -> abort")
                return None

            # -------------------------------------------------
            # Acumuladores
            # -------------------------------------------------
            base_total = Decimal("0")
            qtd_total = 0
            qtd_validos = 0

            base_por_cfop = defaultdict(lambda: Decimal("0"))
            cfops_usados: set[str] = set()
            cfops_revenda_usados: set[str] = set()

            # ✅ segregação CFOP
            base_revenda = Decimal("0")
            base_industrializacao = Decimal("0")
            qtd_revenda = 0
            qtd_industrializacao = 0

            # ✅ segregação por NCM/classe
            base_cafe = Decimal("0")
            base_graos = Decimal("0")
            qtd_cafe_validos = 0
            qtd_graos_validos = 0

            # ✅ confiança (base)
            base_confirmada_catalogo = Decimal("0")  # NCM bate café/grãos no catálogo
            base_fallback = Decimal("0")             # heurística salvou
            base_sem_info = Decimal("0")             # ncm/desc vazio

            skips_logados = 0

            qtd_itens_com_info = 0
            qtd_itens_agro = 0
            qtd_itens_cafe = 0
            qtd_itens_bloqueados_nao_agro = 0

            # -------------------------------------------------
            # Loop itens
            # -------------------------------------------------
            for it in itens:
                if not isinstance(it, dict):
                    continue

                qtd_total += 1
                rid = it.get("registro_id")

                cfop = str(it.get("cfop") or "").strip()
                vl_opr = it.get("vl_opr")

                # CFOP obrigatório
                if not cfop:
                    if skips_logados < self.DEBUG_MAX_SKIPS:
                        logger.debug("IND_AGRO skip rid=%s: sem CFOP", rid)
                        skips_logados += 1
                    continue

                # ✅ CFOP catálogo: aceita INDUSTRIALIZAÇÃO OU REVENDA
                cfop_ind_ok = self.cfop_match(cat, self.CAT_CFOP_GRUPO, cfop)
                cfop_rev_ok = self.cfop_match(cat, self.CAT_CFOP_REVENDA, cfop)
                if not (cfop_ind_ok or cfop_rev_ok):
                    if skips_logados < self.DEBUG_MAX_SKIPS:
                        logger.debug(
                            "IND_AGRO skip rid=%s: CFOP %s fora (ind=%s, rev=%s)",
                            rid, cfop, self.CAT_CFOP_GRUPO, self.CAT_CFOP_REVENDA
                        )
                        skips_logados += 1
                    continue

                # Valor
                try:
                    val = self.dec_br(vl_opr) or Decimal("0")
                except Exception as e:
                    if skips_logados < self.DEBUG_MAX_SKIPS:
                        logger.debug(
                            "IND_AGRO skip rid=%s cfop=%s: vl_opr inválido (%s) err=%s",
                            rid, cfop, vl_opr, e
                        )
                        skips_logados += 1
                    continue

                if val <= 0:
                    if skips_logados < self.DEBUG_MAX_SKIPS:
                        logger.debug("IND_AGRO skip rid=%s cfop=%s: vl_opr<=0 (%s)", rid, cfop, val)
                        skips_logados += 1
                    continue

                # -------------------------------------------------
                # Filtro commodity (hard quando há NCM/desc; soft quando não há)
                # -------------------------------------------------
                desc_item = str(it.get("descricao") or "").strip()
                ncm = str(it.get("ncm") or "").strip().replace(".", "")
                tem_info = bool(ncm) or bool(desc_item)

                eh_agro = False
                eh_cafe = False

                eh_agro_cat = False
                eh_cafe_cat = False
                eh_grao_cat = False
                usou_fallback = False

                if tem_info:
                    qtd_itens_com_info += 1

                    # 1) Catálogo FIRST
                    try:
                        ncm_limpo = (str(ncm or "").strip() or "").replace(".", "")
                        if ncm_limpo:
                            eh_cafe_cat = self.ncm_match(cat, self.CAT_NCM_CAFE, ncm_limpo)
                            eh_grao_cat = (
                                self.ncm_match(cat, self.CAT_NCM_GRAOS_10, ncm_limpo)
                                or self.ncm_match(cat, self.CAT_NCM_GRAOS_12, ncm_limpo)
                            )
                    except Exception:
                        eh_cafe_cat = False
                        eh_grao_cat = False

                    eh_agro_cat = bool(eh_cafe_cat or eh_grao_cat)
                    eh_agro = eh_agro_cat
                    eh_cafe = bool(eh_cafe_cat)

                    # 2) Fallback heurístico (só se catálogo não resolveu)
                    if not eh_agro:
                        usou_fallback = True
                        eh_agro = _is_cafe_ou_graos(desc_item, ncm)
                        eh_cafe = eh_cafe or _is_cafe_by_ncm(ncm) or _is_cafe_by_desc(desc_item)

                    # 3) Bloqueio quando não é AGRO
                    if not eh_agro:
                        qtd_itens_bloqueados_nao_agro += 1
                        if skips_logados < self.DEBUG_MAX_SKIPS:
                            logger.debug(
                                "IND_AGRO skip rid=%s cfop=%s: não-agro (desc=%s ncm=%s)",
                                rid, cfop,
                                (desc_item[:60] + "...") if len(desc_item) > 60 else desc_item,
                                ncm
                            )
                            skips_logados += 1
                        continue

                    qtd_itens_agro += 1
                    if eh_cafe:
                        qtd_itens_cafe += 1

                # ✅ passou: conta como válido
                qtd_validos += 1
                base_total += val

                cfops_usados.add(cfop)
                base_por_cfop[cfop] += val

                # ✅ Classe CFOP (catálogo-driven)
                if cfop_rev_ok:
                    cfops_revenda_usados.add(cfop)
                    base_revenda += val
                    qtd_revenda += 1
                else:
                    base_industrializacao += val
                    qtd_industrializacao += 1

                # Confiança / classes por info
                if not tem_info:
                    base_sem_info += val
                else:
                    if eh_agro_cat:
                        base_confirmada_catalogo += val
                    elif usou_fallback:
                        base_fallback += val

                    if eh_cafe:
                        base_cafe += val
                        qtd_cafe_validos += 1
                    else:
                        base_graos += val
                        qtd_graos_validos += 1

            # Hard abort: se tem info e nada é agro
            if qtd_itens_com_info >= 3 and qtd_itens_agro == 0:
                logger.debug("IND_AGRO: itens com info=%s, agro_detectado=%s -> abort", qtd_itens_com_info, qtd_itens_agro)
                return None

            if qtd_validos == 0 or base_total <= 0:
                logger.debug("IND_AGRO: nenhum item válido (total=%s) -> abort", qtd_total)
                return None

            # Quantizações
            q2 = lambda x: (x or Decimal("0")).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
            base_total = q2(base_total)
            base_revenda = q2(base_revenda)
            base_industrializacao = q2(base_industrializacao)
            base_cafe = q2(base_cafe)
            base_graos = q2(base_graos)
            base_confirmada_catalogo = q2(base_confirmada_catalogo)
            base_fallback = q2(base_fallback)
            base_sem_info = q2(base_sem_info)

            # Shares
            def _share(a: Decimal, b: Decimal) -> Decimal:
                if b <= 0:
                    return Decimal("0")
                return (a / b).quantize(Decimal("0.0001"), rounding=ROUND_HALF_UP)

            share_info_qtd = Decimal("0")
            if qtd_validos > 0:
                share_info_qtd = (Decimal(qtd_itens_com_info) / Decimal(qtd_validos)).quantize(
                    Decimal("0.0001"), rounding=ROUND_HALF_UP
                )

            share_confirmada_cat_base = _share(base_confirmada_catalogo, base_total)
            share_sem_info_base = _share(base_sem_info, base_total)

            # Se "café" dominar a base com info, deixa para IND_CAFE_V1
            base_com_info = q2(base_total - base_sem_info)
            share_cafe_base_com_info = _share(base_cafe, base_com_info) if base_com_info > 0 else Decimal("0")

            if base_com_info > 0 and share_cafe_base_com_info >= self.LIMIAR_SHARE_CAFE_PARA_ABORTAR_AGRO:
                logger.debug(
                    "IND_AGRO: aborta porque café domina (share_cafe_base_com_info=%s) -> deixa para IND_CAFE_V1",
                    str(share_cafe_base_com_info)
                )
                return None

            # Crédito estimado (somente se oportunidade)
            cred = q2(base_total * IND_AGRO_ALIQUOTA_EFETIVA)
            aliquota_pct = (IND_AGRO_ALIQUOTA_EFETIVA * Decimal("100")).quantize(Decimal("0.01"))

            # Revenda %
            pct_revenda = Decimal("0.00")
            if base_total > 0:
                pct_revenda = (base_revenda / base_total * Decimal("100")).quantize(
                    Decimal("0.01"), rounding=ROUND_HALF_UP
                )

            # -------------------------------------------------
            # ✅ MODO: OPORTUNIDADE vs ALERTA
            # -------------------------------------------------
            modo_alerta = False
            motivos_alerta: list[str] = []

            if share_info_qtd < self.MIN_SHARE_INFO_QTD:
                modo_alerta = True
                motivos_alerta.append(f"baixa_cobertura_info_qtd<{self.MIN_SHARE_INFO_QTD}")

            if share_confirmada_cat_base < self.MIN_SHARE_CONFIRMADA_CAT_BASE:
                modo_alerta = True
                motivos_alerta.append(f"baixa_confirmacao_catalogo_base<{self.MIN_SHARE_CONFIRMADA_CAT_BASE}")

            if share_sem_info_base > self.MAX_SHARE_SEM_INFO_BASE_PARA_OPORTUNIDADE:
                modo_alerta = True
                motivos_alerta.append(f"muita_base_sem_info>{self.MAX_SHARE_SEM_INFO_BASE_PARA_OPORTUNIDADE}")

            # -------------------------------------------------
            # Descrição (auditável)
            # -------------------------------------------------
            usou_cfop_revenda = bool(cfops_revenda_usados)

            desc = (
                "Indícios de industrialização/beneficiamento de grãos/commodities agrícolas: "
                f"base filtrada por CFOP (catálogo) = R$ {self.fmt_br(base_total)} "
                f"(alíquota efetiva {self.fmt_br(aliquota_pct)}%). "
                f"Composição da base: industrialização R$ {self.fmt_br(base_industrializacao)} "
                f"({qtd_industrializacao} item(ns)); "
                f"revenda como insumo R$ {self.fmt_br(base_revenda)} "
                f"({qtd_revenda} item(ns), {self.fmt_br(pct_revenda)}%). "
            )

            if usou_cfop_revenda:
                desc += f"CFOP(s) revenda detectados: {', '.join(sorted(cfops_revenda_usados))}. "

            desc += (
                f"Cobertura info (qtd)={self.fmt_br((share_info_qtd*Decimal('100')).quantize(Decimal('0.01')))}%; "
                f"confirmação catálogo (base)={self.fmt_br((share_confirmada_cat_base*Decimal('100')).quantize(Decimal('0.01')))}%; "
                f"base sem info={self.fmt_br((share_sem_info_base*Decimal('100')).quantize(Decimal('0.01')))}%. "
            )

            if modo_alerta:
                desc = "⚠️ ALERTA (confiança média): " + desc + "Revisar NCM/descrições e enquadramento antes de tomar ação."
            else:
                desc += f"Crédito estimado total R$ {self.fmt_br(cred)}. Validar enquadramento/insumo."

            # -------------------------------------------------
            # Meta (super auditável)
            # -------------------------------------------------
            flags = dict(meta) if isinstance(meta, dict) else {}
            flags.update({
                "fonte_base": reg,

                # rastreabilidade
                "anchor_reg_base": anchor_reg_base,
                "anchor_linha": anchor_linha,
                "anchor_registro_id": anchor_registro_id,
                "linha_anchor": anchor_linha,

                # catálogo
                "cat_cfop_industrializacao": self.CAT_CFOP_GRUPO,
                "cat_cfop_revenda": self.CAT_CFOP_REVENDA,
                "cat_ncm_cafe": self.CAT_NCM_CAFE,
                "cat_ncm_graos_10": self.CAT_NCM_GRAOS_10,
                "cat_ncm_graos_12": self.CAT_NCM_GRAOS_12,

                # contadores
                "qtd_itens_total": int(qtd_total),
                "qtd_itens_validos": int(qtd_validos),
                "qtd_itens_com_info": int(qtd_itens_com_info),
                "qtd_itens_agro": int(qtd_itens_agro),
                "qtd_itens_cafe": int(qtd_itens_cafe),
                "qtd_itens_bloqueados_nao_agro": int(qtd_itens_bloqueados_nao_agro),

                # bases / crédito
                "base_compra_filtrada": str(base_total),
                "aliquota_efetiva": f"{self.fmt_br(aliquota_pct)}%",
                "credito_estimado_total": str(cred),

                # CFOPs / segregação
                "cfops_usados": sorted(cfops_usados),
                "cfops_revenda_usados": sorted(cfops_revenda_usados),
                "usou_cfop_revenda": bool(usou_cfop_revenda),
                "base_industrializacao": str(base_industrializacao),
                "base_revenda_como_insumo": str(base_revenda),
                "pct_base_revenda": str(pct_revenda),
                "qtd_itens_industrializacao": int(qtd_industrializacao),
                "qtd_itens_revenda": int(qtd_revenda),

                # NCM classes
                "base_cafe": str(base_cafe),
                "base_graos": str(base_graos),
                "qtd_cafe_validos": int(qtd_cafe_validos),
                "qtd_graos_validos": int(qtd_graos_validos),

                # confiança
                "base_confirmada_catalogo": str(base_confirmada_catalogo),
                "base_fallback": str(base_fallback),
                "base_sem_info": str(base_sem_info),
                "share_info_qtd": str(share_info_qtd),
                "share_confirmada_cat_base": str(share_confirmada_cat_base),
                "share_sem_info_base": str(share_sem_info_base),
                "share_cafe_base_com_info": str(share_cafe_base_com_info),
                "modo_alerta": bool(modo_alerta),
                "motivos_alerta": motivos_alerta,
            })

            # ✅ registro_id final (evita “sem FK”)
            rid = int(getattr(registro, "id", 0) or 0)
            anchor_rid = meta.get("anchor_registro_id") if isinstance(meta, dict) else None
            try:
                anchor_rid = int(anchor_rid) if anchor_rid is not None else None
            except Exception:
                anchor_rid = None

            registro_id_final = rid if rid > 0 else (anchor_rid or 0)
            if not registro_id_final:
                return None

            # ✅ ALERTA: sem impacto (e tipo ALERTA)
            tipo_out = "ALERTA" if modo_alerta else self.tipo
            impacto_out = None if modo_alerta else cred
            prioridade_out = "BAIXA" if modo_alerta else self.prioridade

            return {
                "registro_id": int(registro_id_final),
                "tipo": tipo_out,
                "codigo": self.codigo,
                "descricao": desc,
                "impacto_financeiro": impacto_out,
                "prioridade": prioridade_out,
                "meta": flags,
                "regra": self.nome,
            }

        except Exception:
            logger.exception("Erro na regra %s", getattr(self, "codigo", "IND_AGRO_V1"))
            return None
