from decimal import Decimal
from typing import Optional

from app.fiscal.constants import (
    ACAO_OVERRIDE_BASE_POR_CST,
)
from app.fiscal.contexto import dec_ptbr
from app.fiscal.dto import RegistroFiscalDTO
from app.fiscal.regras.Diagnostico.achado import Achado
from app.fiscal.regras.Diagnostico.base_regras import RegraBase
from app.schemas.workflow import RevisaoFiscal
from collections import defaultdict
from app.config.settings import (
    ALIQUOTA_PIS,
    ALIQUOTA_COFINS,
    ALIQUOTA_TOTAL,
)

import logging
logger = logging.getLogger(__name__)


def _find_line_num(linhas: list[str], prefix: str) -> int | None:
    # retorna linha_num 1-based
    for i, l in enumerate(linhas, start=1):
        if l.startswith(prefix):
            return i
    return None


class RegraExportacaoRessarcimentoV1(RegraBase):
    codigo = "EXP_RESSARC_V1"
    nome = "Exportação: possível ressarcimento/compensação (CFOP 7xxx)"
    tipo = "OPORTUNIDADE"

    def aplicar(self, registro: RegistroFiscalDTO) -> Optional[Achado]:
        try:
            # -------------------------------------------------
            # Proteções iniciais
            # -------------------------------------------------
            if registro.is_pf:
                return None

            if registro.reg not in ("C190_EXP_AGG", "C170_EXP_AGG"):
                return None

            raw = registro.dados or []
            if not raw or not isinstance(raw[0], dict):
                return None

            meta_flags = raw[0].get("_meta") or {}
            itens = raw[1:]
            if not itens:
                return None

            # -------------------------------------------------
            # Cálculo da base de exportação (somente itens válidos)
            # -------------------------------------------------
            cat = self.get_catalogo(registro)
            base_por_cfop = defaultdict(lambda: Decimal("0"))
            cfops = set()
            itens_usados = 0

            for it in itens:
                if not isinstance(it, dict):
                    continue
                cfop = str(it.get("cfop") or "").strip()
                if not cfop:
                    continue

                # ✅ catálogo-driven: só exportação
                if not self.cfop_match(cat, "CFOP_EXPORTACAO", cfop):
                    continue

                if registro.reg == "C170_EXP_AGG":
                    vl_item = self.dec_br(it.get("vl_item")) or Decimal("0")
                    vl_desc = self.dec_br(it.get("vl_desc")) or Decimal("0")
                    vl_icms = self.dec_br(it.get("vl_icms")) or Decimal("0")
                    val = vl_item - vl_desc - vl_icms
                else:
                    val = self.dec_br(it.get("vl_opr")) or Decimal("0")

                if val <= 0:
                    continue

                itens_usados += 1
                cfops.add(cfop)
                base_por_cfop[cfop] += val

            base_export = sum(base_por_cfop.values(), Decimal("0"))
            if base_export <= 0 or itens_usados == 0:
                return None

            base_export = self.q2(base_export)

            # -------------------------------------------------
            # Créditos estimados (PROXY por receita de exportação)
            # -------------------------------------------------
            cred_pis = self.q2(base_export * ALIQUOTA_PIS)
            cred_cof = self.q2(base_export * ALIQUOTA_COFINS)
            impacto = self.q2(cred_pis + cred_cof)

            # -------------------------------------------------
            # Flags de controle (Bloco 1 / Bloco M)
            # -------------------------------------------------
            tem_1200 = self.to_bool(meta_flags.get("tem_1200"))
            tem_1210 = self.to_bool(meta_flags.get("tem_1210"))
            tem_1700 = self.to_bool(meta_flags.get("tem_1700"))
            ja_tem_indicio_ressarc = tem_1200 or tem_1210 or tem_1700

            perfil_monofasico = self.to_bool(meta_flags.get("perfil_monofasico"))
            if perfil_monofasico:
                return None

            tem_apuracao_m = self.to_bool(meta_flags.get("tem_apuracao_m"))
            bloco_m_zerado = self.to_bool(meta_flags.get("bloco_m_zerado"))

            # -------------------------------------------------
            # Subcenário Bloco M
            # -------------------------------------------------
            subcenario_m = None
            if not ja_tem_indicio_ressarc:
                if bloco_m_zerado:
                    subcenario_m = "M_ZERADO"
                elif tem_apuracao_m:
                    subcenario_m = "M_COM_APURACAO"
                else:
                    subcenario_m = "M_INEXISTENTE"
            else:
                subcenario_m = "JA_CONTROLADO"

            # -------------------------------------------------
            # Concentração por CFOP (melhor que premiar variedade)
            # -------------------------------------------------
            top_cfop = None
            top_val = Decimal("0")
            if base_por_cfop:
                top_cfop, top_val = max(base_por_cfop.items(), key=lambda kv: kv[1])

            top_share = (top_val / base_export) if (base_export > 0 and top_cfop) else Decimal("0")


            # -------------------------------------------------
            # Score
            # -------------------------------------------------
            score = 0
            motivos = []

            # impacto (proxy)
            if impacto >= Decimal("50000"):
                score += 60
            elif impacto >= Decimal("20000"):
                score += 50
            elif impacto >= Decimal("5000"):
                score += 40
            elif impacto >= Decimal("1000"):
                score += 25
            else:
                score += 10
            motivos.append(f"Impacto proxy R$ {self.fmt_br(impacto)}")

            # indícios de ressarcimento
            if not ja_tem_indicio_ressarc:
                score += 25
                motivos.append("Sem 1200/1210/1700")
            else:
                score += 5
                motivos.append("Já há 1200/1210/1700")

            # bloco M
            if bloco_m_zerado:
                score += 15
                motivos.append("Bloco M zerado")
            elif tem_apuracao_m:
                score += 10
                motivos.append("Bloco M com apuração")

            # concentração
            if top_share >= Decimal("0.80"):
                score += 5
                motivos.append(f"Concentrado em CFOP {top_cfop} ({self.pct(top_share)})")
            else:
                score += 2

            score = min(100, score)

            # bucket/prioridade
            if ja_tem_indicio_ressarc:
                bucket = "JA_CONTROLADO"
                prioridade = "BAIXA"
            else:
                if score >= 80:
                    bucket = "ALTA_CHANCE"
                    prioridade = "ALTA"
                elif score >= 55:
                    bucket = "REVISAR"
                    prioridade = "MEDIA"
                else:
                    bucket = "BAIXA"
                    prioridade = "BAIXA"

            # -------------------------------------------------
            # Descrição
            # -------------------------------------------------
            base_fmt = self.fmt_br(base_export)
            impacto_fmt = self.fmt_br(impacto)
            cfops_top = ", ".join(sorted(cfops)[:5])

            desc = (
                f"Exportação detectada (CFOP 7xxx): base R$ {base_fmt} "
                f"({itens_usados} item(ns) válidos, {len(cfops)} CFOP(s)). "
                f"Crédito *proxy* estimado R$ {impacto_fmt} (9,25%). "
                f"CFOPs (top): {cfops_top}. "
            )
            if ja_tem_indicio_ressarc:
                desc += "Já há indício de controle (1200/1210/1700); validar coerência e Bloco M."
            else:
                desc += "Validar Bloco M e controle via 1200/1210/1700."

            
            # -------------------------------------------------
            # Retorno
            # -------------------------------------------------
            return Achado(
                registro_id=int(registro.id),
                tipo="OPORTUNIDADE",
                codigo=self.codigo,
                descricao=desc,
                regra=self.nome,
                impacto_financeiro=impacto,
                prioridade=prioridade,
                meta={
                    "fonte_base": registro.reg,  # ex: C170_EXP_AGG / C190_EXP_AGG
                    "empresa_id": registro.empresa_id,
                    "versao_id": registro.versao_id,
                    "metodo_estimativa": "proxy_receita_exportacao_9_25",
                    "base_exportacao": self.br_num(base_export),
                    "credito_pis_estimado": self.br_num(cred_pis),
                    "credito_cofins_estimado": self.br_num(cred_cof),
                    "impacto_consolidado": self.br_num(impacto),
                    "itens_usados": int(itens_usados),
                    "qtd_cfops": len(base_por_cfop),
                    "cfops_detectados": sorted(cfops),
                    "cfop_top": top_cfop,
                    "cfop_top_share": str(top_share),
                    "base_por_cfop": {k: self.br_num(self.q2(v)) for k, v in sorted(base_por_cfop.items())},
                    "bucket": bucket,
                    "score": score,
                    "motivos_score": motivos,
                    "tem_1200": tem_1200,
                    "tem_1210": tem_1210,
                    "tem_1700": tem_1700,
                    "tem_apuracao_m": tem_apuracao_m,
                    "bloco_m_zerado": bloco_m_zerado,
                    "subcenario_m": subcenario_m,
                    "aliquota_pis": self.pct(ALIQUOTA_PIS),
                    "aliquota_cofins": self.pct(ALIQUOTA_COFINS),
                    "aliquota_total": self.pct(ALIQUOTA_TOTAL),
                },
            )

        except Exception:
            logger.exception("Erro na regra EXP_RESSARC_V1")
            return None

    def gerar_revisoes_exp_ressarc_v1(self, ctx: dict) -> list:
        """
        Gera OVERRIDE_BASE_POR_CST para alimentar o construir_bloco_m_v3 no export.
        Não edita M diretamente.
        """
        try:
            ap = ctx.get("apontamento")
            meta = (getattr(ap, "meta_json", None) or {}) if ap else {}

            base_export = dec_ptbr(meta.get("base_exportacao"))
            if base_export <= 0:
                return []

            # decisão inicial do motor: CST 50
            base_por_cst = {
                "51": str(base_export.quantize(Decimal("0.01")))
            }

            # âncora (evita revisão sem FK)
            registro_id = int(getattr(ap, "registro_id", 0) or 0)
            registro_reg = str(getattr(ap, "registro", "") or "C170")

            ajuste_payload = {
                "tipo": "EXPORTACAO_RESSARCIMENTO",
                "origem_regra": self.codigo,
                "base_exportacao": str(base_export),
                "aliq_pis": "0,0165",
                "aliq_cofins": "0,0760",
                "cod_cont": "201",
                "nat_bc": "01",

            }

            return [
                RevisaoFiscal(
                    registro_id=registro_id if registro_id > 0 else None,
                    registro="M",
                    operacao="AJUSTE_M",
                    conteudo=None,
                    linha_referencia=None,
                    linha_antes=None,
                    linha_hash=None,
                    regra_codigo=self.codigo,
                    payload={
                        "tipo": "EXPORTACAO_RESSARCIMENTO",
                        "origem_regra": self.codigo,
                        "base_exportacao": str(base_export),  # "19262.67"
                        "cod_cont": "201",
                        "nat_bc": "01",
                        "aliq_pis": "0,0165",
                        "aliq_cofins": "0,0760",
                    },
                )
            ]

        except Exception:
            logger.exception("Erro ao gerar revisão OVERRIDE_BASE_POR_CST (EXP_RESSARC_V1)")
            return []
