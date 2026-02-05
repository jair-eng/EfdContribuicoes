from decimal import Decimal, ROUND_HALF_UP
from app.config.settings import IND_TORRADO_ALIQUOTA_EFETIVA  # você já queria no settings
from app.fiscal.dto import RegistroFiscalDTO
from app.fiscal.regras.base_regras import RegraBase
from collections import defaultdict
import logging

# Opcional: Configurar o logger para este módulo específico
logger = logging.getLogger(__name__)



class RegraIndustrializacaoTorradoV1(RegraBase):
    codigo = "IND_TORRADO_V1"
    tipo = "OPORTUNIDADE"
    prioridade = "MEDIA"
    nome = "Industrialização (Torrado)"

    # ✅ grupo CFOP do catálogo
    CAT_CFOP_GRUPO = "CFOP_CAFE_TORRADO_ENTRADA"

    # CFOPs de “revenda” que você quer incluir (para sinalização)
    CFOPS_REVENDA = {"1102", "2102", "3102"}

    # limita logs item-a-item pra não poluir
    DEBUG_MAX_SKIPS = 25

    def aplicar(self, registro: RegistroFiscalDTO):
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

        # ✅ Âncora do agregador (rastreabilidade quando id=0)
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

        logger.debug(
            "IND_TORRADO start: registro_id=%s reg=%s versao_id=%s empresa_id=%s itens_total=%s",
            getattr(registro, "id", None),
            reg,
            getattr(registro, "versao_id", None),
            getattr(registro, "empresa_id", None),
            len(itens),
        )

        # -------------------------------------------------
        # Catálogo fiscal (CFOP)
        # -------------------------------------------------
        try:
            cat = self.get_catalogo(registro)
        except Exception:
            logger.exception("IND_TORRADO: falha ao carregar catálogo")
            return None

        if not cat:
            logger.debug("IND_TORRADO: catálogo vazio/None -> abort")
            return None

        logger.debug("IND_TORRADO: usando grupo CFOP do catálogo: %s", self.CAT_CFOP_GRUPO)

        # -------------------------------------------------
        # Filtra por CFOP do grupo + soma base
        # -------------------------------------------------
        base_total = Decimal("0")
        qtd_total = 0
        qtd_validos = 0

        base_por_cfop = defaultdict(lambda: Decimal("0"))
        cfops_usados = set()
        cfops_revenda_usados = set()

        skips_logados = 0

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
                    logger.debug("IND_TORRADO skip rid=%s: sem CFOP", rid)
                    skips_logados += 1
                continue

            # ✅ catálogo-driven: só entra se CFOP estiver no grupo
            if not self.cfop_match(cat, self.CAT_CFOP_GRUPO, cfop):
                if skips_logados < self.DEBUG_MAX_SKIPS:
                    logger.debug(
                        "IND_TORRADO skip rid=%s: CFOP %s fora do grupo %s",
                        rid, cfop, self.CAT_CFOP_GRUPO
                    )
                    skips_logados += 1
                continue

            # Valor
            try:
                val = self.dec_br(vl_opr) or Decimal("0")
            except Exception as e:
                if skips_logados < self.DEBUG_MAX_SKIPS:
                    logger.debug("IND_TORRADO skip rid=%s cfop=%s: vl_opr inválido (%s) err=%s", rid, cfop, vl_opr, e)
                    skips_logados += 1
                continue

            if val <= 0:
                if skips_logados < self.DEBUG_MAX_SKIPS:
                    logger.debug("IND_TORRADO skip rid=%s cfop=%s: vl_opr<=0 (%s)", rid, cfop, val)
                    skips_logados += 1
                continue

            # ✅ conta como válido
            qtd_validos += 1
            base_total += val

            cfops_usados.add(cfop)
            base_por_cfop[cfop] += val

            if cfop in self.CFOPS_REVENDA:
                cfops_revenda_usados.add(cfop)

        if qtd_validos == 0 or base_total <= 0:
            logger.debug("IND_TORRADO: nenhum item válido (total=%s) -> abort", qtd_total)
            return None

        base_total = base_total.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)

        cred = (base_total * IND_TORRADO_ALIQUOTA_EFETIVA).quantize(
            Decimal("0.01"), rounding=ROUND_HALF_UP
        )

        aliquota_pct = (IND_TORRADO_ALIQUOTA_EFETIVA * Decimal("100")).quantize(Decimal("0.01"))

        usou_cfop_revenda = bool(cfops_revenda_usados)

        # resumo por CFOP para debug
        top_cfops = sorted(base_por_cfop.items(), key=lambda kv: kv[1], reverse=True)[:5]
        logger.info(
            "IND_TORRADO ok: registro_id=%s validos=%s/%s base=%s cred=%s cfops=%s revenda=%s",
            getattr(registro, "id", None),
            qtd_validos,
            qtd_total,
            self.fmt_br(base_total),
            self.fmt_br(cred),
            [k for k, _ in top_cfops],
            sorted(cfops_revenda_usados),
        )

        flags = dict(meta) if isinstance(meta, dict) else {}
        flags.update({
            "fonte_base": reg,

            # ✅ rastreabilidade (importante quando registro_id=0)
            "anchor_reg_base": anchor_reg_base,
            "anchor_linha": anchor_linha,
            "anchor_registro_id": anchor_registro_id,
            "linha_anchor": anchor_linha,  # atalho pro CSV/UI

            "qtd_itens_total": int(qtd_total),
            "qtd_itens_validos": int(qtd_validos),
            "aliquota_efetiva": f"{self.fmt_br(aliquota_pct)}%",
            "base_compra_filtrada": str(base_total),
            "credito_estimado_total": str(cred),
            "cfops_usados": sorted(cfops_usados),
            "base_por_cfop": {k: str(v.quantize(Decimal('0.01'), rounding=ROUND_HALF_UP)) for k, v in
                              sorted(base_por_cfop.items())},
            "usou_cfop_revenda": usou_cfop_revenda,
            "cfops_revenda_usados": sorted(cfops_revenda_usados),
        })

        desc = (
            "Possível crédito estimado (industrialização do café torrado): "
            "base em compras (entradas) filtrada por CFOP (catálogo) x alíquota efetiva. "
        )
        if usou_cfop_revenda:
            desc += (
                f"Atenção: inclui CFOP de comercialização (revenda) {', '.join(sorted(cfops_revenda_usados))} "
                "— comum em prática de escritório quando o item é tratado como insumo; validar."
            )
        else:
            desc += "CFOPs de industrialização predominantes; validar enquadramento/insumo."

        return {
            "registro_id": int(registro.id),
            "tipo": self.tipo,
            "codigo": self.codigo,
            "descricao": desc,
            "impacto_financeiro": cred,
            "prioridade": self.prioridade,
            "meta": flags,
            "regra": self.nome,
        }