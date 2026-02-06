import logging
from decimal import Decimal
from typing import Any, Dict, Optional
from collections import defaultdict

from app.config.settings import ALIQUOTA_PIS, ALIQUOTA_COFINS
from app.fiscal.constants import (
    GRUPO_EXPORTACAO,
    SUBGRUPO_CONSISTENCIA,
)
from app.fiscal.contexto import dec_ptbr
from app.fiscal.constants import ACAO_OVERRIDE_BASE_POR_CST
from app.fiscal.dto import RegistroFiscalDTO
from app.fiscal.regras.base_regras import RegraBase
from app.fiscal.regras.achado import Achado
from app.schemas.workflow import RevisaoFiscal

logger = logging.getLogger(__name__)

class RegraExportacaoBlocoMZeradoV1(RegraBase):
    """
    ERRO/ALERTA de consistência (DIAGNÓSTICO APENAS):

    - Há exportação (catálogo CFOP_EXPORTACAO)
    - Bloco M existe (tem_apuracao_m=True)
    - Bloco M aparenta zerado (bloco_m_zerado=True ou soma_m == 0)

    ❗ NÃO gera revisões físicas
    ❗ NÃO escreve M100/M200/M500/M600
    """

    codigo = "EXP_M_ZERADO_V1"
    nome = "Exportação com Bloco M zerado"
    tipo = "ERRO"

    SLUG_CFOP_EXPORTACAO = "CFOP_EXPORTACAO"

    def aplicar(self, registro: RegistroFiscalDTO) -> Optional[Achado]:
        try:
            # 🛡️ trava global
            if getattr(registro, "is_pf", False):
                return None

            # aceita ambos os padrões de nome (evita “reg errado”)
            if registro.reg not in (
                "C190_EXP_AGG", "C170_EXP_AGG",
                "C190_EXPORT_AGG", "C170_EXPORT_AGG",
            ):
                return None

            raw = registro.dados or []
            if not raw:
                return None

            meta_flags: Dict[str, Any] = {}
            itens = raw

            # padrão: primeiro item dict com _meta
            if isinstance(raw[0], dict) and "_meta" in raw[0]:
                meta_flags = raw[0].get("_meta") or {}
                itens = raw[1:]

            # se não tem meta ou não tem itens, nada a fazer
            if not meta_flags or not itens:
                return None

            # flags do scanner
            tem_apuracao_m = self.to_bool(meta_flags.get("tem_apuracao_m"))
            soma_m = self.dec_any(meta_flags.get("soma_valores_bloco_m")) or Decimal("0")
            bloco_m_zerado = self.to_bool(meta_flags.get("bloco_m_zerado")) or (soma_m == 0)

            # Só acusa se M existe e está zerado
            if not tem_apuracao_m or not bloco_m_zerado:
                return None

            # bloco 1 (controle)
            tem_1200 = self.to_bool(meta_flags.get("tem_1200"))
            tem_1210 = self.to_bool(meta_flags.get("tem_1210"))
            tem_1700 = self.to_bool(meta_flags.get("tem_1700"))
            ja_tem_controle = tem_1200 or tem_1210 or tem_1700

            # catálogo (exportação)
            cat = self.get_catalogo(registro)

            # base de exportação (somente CFOP do catálogo + itens válidos)
            base_export = Decimal("0")
            base_por_cfop = defaultdict(lambda: Decimal("0"))
            cfops = set()
            itens_usados = 0

            for it in itens:
                if not isinstance(it, dict):
                    continue

                cfop = str(it.get("cfop") or "").strip()
                if not cfop:
                    continue

                # ✅ catálogo decide se é exportação
                if not self.cfop_match(cat, self.SLUG_CFOP_EXPORTACAO, cfop):
                    continue

                v = self.dec_any(it.get("vl_opr")) or Decimal("0")
                if v <= 0:
                    continue

                itens_usados += 1
                cfops.add(cfop)
                base_export += v
                base_por_cfop[cfop] += v

            if base_export <= 0 or itens_usados == 0:
                return None

            base_export = self.q2(base_export)

            # concentração por CFOP
            top_cfop = None
            top_val = Decimal("0")
            if base_por_cfop:
                top_cfop, top_val = max(base_por_cfop.items(), key=lambda kv: kv[1])
            top_share = (top_val / base_export) if (base_export > 0 and top_cfop) else Decimal("0")

            # crédito estimado (proxy informativo)
            cred_pis = self.q2(base_export * ALIQUOTA_PIS)
            cred_cof = self.q2(base_export * ALIQUOTA_COFINS)
            credito_total = self.q2(cred_pis + cred_cof)

            # origem: de onde veio o agregador (se o scanner preencheu)
            fonte = str(meta_flags.get("fonte") or meta_flags.get("fonte_base") or registro.reg or "AGG")
            onde = "C190" if "C190" in fonte else "C170"

            cfops_sorted = sorted(cfops)
            cfops_top = ", ".join(cfops_sorted[:5]) if cfops_sorted else "-"

            # severidade
            tipo = self.tipo
            prioridade = "ALTA"
            criticidade = "CRITICO"
            if ja_tem_controle:
                # reduz ruído: ainda é inconsistente, mas menos “pipeline-blocker”
                tipo = "ALERTA"
                prioridade = "MEDIA"
                criticidade = "ALTO"

            desc = (
                f"Inconsistência fiscal: Exportação detectada (catálogo) no {onde}: "
                f"base R$ {self.fmt_br(base_export)} "
                f"({itens_usados} item(ns) válidos, {len(cfops)} CFOP(s); top: {cfops_top}). "
                f"Bloco M existe, mas aparenta zerado (soma_m={self.fmt_br(self.q2(soma_m))}). "
                f"{'Há 1200/1210/1700 no Bloco 1; ' if ja_tem_controle else 'Não há 1200/1210/1700 no Bloco 1; '}"
                "revisar escrituração/apuração (M200/M205/M600/M605), CST, regime e parametrizações."
            )

            logger.warning(
                "EXP_M_ZERADO | base=%s | credito_est=%s | soma_m=%s | controle=%s | top_cfop=%s share=%s",
                str(base_export), str(credito_total), str(soma_m), ja_tem_controle, str(top_cfop), str(top_share)
            )

            # ✅ FIX FK: tenta usar um registro representativo real se existir
            # (se não existir, cai no fallback do scanner via meta['fonte_base'])ok
            rid_repr = None
            try:
                rid_repr = meta_flags.get("registro_id_repr") or meta_flags.get("rid_repr")
                rid_repr = int(rid_repr) if rid_repr is not None else None
            except Exception:
                rid_repr = None

            registro_id_final = rid_repr if rid_repr else int(getattr(registro, "id", 0) or 0)

            return Achado(
                registro_id=registro_id_final,  # pode ser 0 no AGG -> scanner ancora via fonte_base
                tipo=tipo,
                codigo=self.codigo,
                descricao=desc,
                regra=self.nome,
                impacto_financeiro=None,
                prioridade=prioridade,
                meta={
                    # ✅ chave que ativa o fallback do scanner (âncora no 1º C170 real)
                    "fonte_base": registro.reg,
                    "fonte": fonte,

                    "grupo": GRUPO_EXPORTACAO,
                    "subgrupo": SUBGRUPO_CONSISTENCIA,
                    "criticidade": criticidade,
                    "erro_consistencia": True,
                    "motivo": "Bloco M zerado com exportação",

                    "metodo_estimativa": "proxy_receita_exportacao_9_25",
                    "base_exportacao": self.br_num(base_export),
                    "base_exportacao_fmt": self.fmt_br(base_export),

                    "itens_usados": int(itens_usados),
                    "base_por_cfop": {k: self.br_num(self.q2(v)) for k, v in sorted(base_por_cfop.items())},
                    "cfops_detectados": cfops_sorted,
                    "cfop_top": top_cfop,
                    "cfop_top_share": str(top_share),

                    "tem_1200": tem_1200,
                    "tem_1210": tem_1210,
                    "tem_1700": tem_1700,

                    "tem_apuracao_m": tem_apuracao_m,
                    "bloco_m_zerado": True,
                    "soma_valores_bloco_m": self.br_num(self.q2(soma_m)),

                    "credito_pis_estimado": self.br_num(cred_pis),
                    "credito_cofins_estimado": self.br_num(cred_cof),
                    "credito_total_estimado": self.br_num(credito_total),

                    # info de contexto (útil pra debug)
                    "empresa_id": getattr(registro, "empresa_id", None),
                    "versao_id": getattr(registro, "versao_id", None),
                },
            )

        except Exception:
            logger.exception("Erro na regra EXP_M_ZERADO_V1")
            return None

    def gerar_revisoes_exp_ressarc_v1(self, ctx: dict) -> list:
        """
        Caminho B: não edita M diretamente.
        Gera OVERRIDE_BASE_POR_CST para alimentar o construir_bloco_m_v3 no export.
        """
        try:
            # trava definitiva: se existir erro crítico no contexto, não gera correção
            contexto = ctx.get("contexto")
            if contexto and getattr(contexto, "tem_apontamento", None):
                if contexto.tem_apontamento("EXP_M_ZERADO_V1"):
                    return []

            ap = ctx.get("apontamento")
            meta = (getattr(ap, "meta_json", None) or {}) if ap else {}

            base_export = dec_ptbr(meta.get("base_exportacao"))
            if base_export <= 0:
                return []

            # decisão do motor (comece simples): CST 50
            base_por_cst = {"50": str(base_export.quantize(Decimal("0.01")))}

            # Constrói RevisaoFiscal compatível com seu pipeline
            # (ajuste o import/constructor conforme sua classe RevisaoFiscal real)
            return [
                RevisaoFiscal(
                    registro_id=None,
                    registro="M",
                    operacao=ACAO_OVERRIDE_BASE_POR_CST,
                    conteudo=None,
                    linha_referencia=None,
                    linha_antes=None,
                    linha_hash=None,
                    regra_codigo=self.codigo,
                    payload={
                        "base_por_cst": base_por_cst,
                        "cod_cont": "201",
                        "nat_bc": "01",
                    },
                )
            ]

        except Exception:
            logger.exception("Erro ao gerar revisão OVERRIDE_BASE_POR_CST (EXP_RESSARC_V1)")
            return []