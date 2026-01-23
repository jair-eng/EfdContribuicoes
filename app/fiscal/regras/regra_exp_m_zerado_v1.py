import logging
from decimal import Decimal
from typing import Any, Dict, Optional, List
from app.config.settings import ALIQUOTA_PIS, ALIQUOTA_COFINS, ALIQUOTA_TOTAL
from app.fiscal.constants import GRUPO_EXPORTACAO, SUBGRUPO_CONSISTENCIA, SUBGRUPO_RESSARCIMENTO
from app.fiscal.dto import RegistroFiscalDTO
from app.fiscal.regras.base_regras import RegraBase
from app.fiscal.regras.achado import  Achado  # ajuste se o path for outro
from app.schemas.workflow import RevisaoFiscal

logger = logging.getLogger(__name__)


def _find_line_index(linhas: list[str], prefix: str) -> int | None:
    # 1-based (igual seu helper)
    for i, l in enumerate(linhas, start=1):
        if l.startswith(prefix):
            return i
    return None


class RegraExportacaoBlocoMZeradoV1(RegraBase):
    codigo = "EXP_M_ZERADO_V1"
    nome = "Exportação com Bloco M zerado"
    tipo = "ERRO"

    # thresholds de prioridade (mantidos)
    LIM_ALTA = Decimal("5000")
    LIM_MEDIA = Decimal("1000")

    def aplicar(self, registro: RegistroFiscalDTO) -> Optional[Achado]:
        """
        ERRO de consistência:
          - exportação detectada (registros agregados export)
          - Bloco M existe (tem_apuracao_m=True)
          - Bloco M aparenta zerado (bloco_m_zerado=True ou soma=0)
        """
        try:
            if registro.reg not in ("C190_EXP_AGG", "C170_EXP_AGG"):
                return None

            raw = registro.dados or []
            if not raw:
                return None

            meta: Dict[str, Any] = {}
            itens = raw

            if isinstance(raw[0], dict) and "_meta" in raw[0]:
                meta = raw[0].get("_meta") or {}
                itens = raw[1:]

            if not meta or not itens:
                return None

            # flags essenciais (robustas)
            tem_apuracao_m = self.to_bool(meta.get("tem_apuracao_m"))
            bloco_m_zerado = self.to_bool(meta.get("bloco_m_zerado"))

            soma_m = self.dec_any(meta.get("soma_valores_bloco_m")) or Decimal("0")
            if soma_m == Decimal("0") and tem_apuracao_m:
                bloco_m_zerado = True

            # só quando existe Bloco M (apuracao) e aparenta zerado
            if not tem_apuracao_m:
                return None
            if not bloco_m_zerado:
                return None

            # base exportação (soma vl_opr dos itens)
            base_export = Decimal("0")
            cfops = set()

            for it in itens:
                if not isinstance(it, dict):
                    continue
                cfop = str(it.get("cfop") or "").strip()
                if cfop:
                    cfops.add(cfop)

                v = self.dec_any(it.get("vl_opr") or "0") or Decimal("0")
                if v > 0:
                    base_export += v

            if base_export <= 0:
                return None

            base_export = self.q2(base_export)

            # crédito estimado (para ctx/autocorreção)
            cred_pis = self.q2(base_export * ALIQUOTA_PIS)
            cred_cof = self.q2(base_export * ALIQUOTA_COFINS)
            credito_total = self.q2(cred_pis + cred_cof)

            # Bloco 1 (controle)
            tem_1200 = self.to_bool(meta.get("tem_1200"))
            tem_1210 = self.to_bool(meta.get("tem_1210"))
            tem_1700 = self.to_bool(meta.get("tem_1700"))
            ja_tem_controle = tem_1200 or tem_1210 or tem_1700

            fonte = str(meta.get("fonte") or "C190")
            onde = "C190" if fonte == "C190" else "C170"
            cfops_sorted = sorted(cfops)
            cfops_top = ", ".join(cfops_sorted[:5]) if cfops_sorted else "-"

            desc = (
                f"Inconsistência fiscal: Exportação detectada (CFOP 7xxx) no {onde}: "
                f"base R$ {self.fmt_br(base_export)} (CFOPs: {cfops_top}). "
                f"Bloco M existe, mas aparenta zerado (soma_m={self.fmt_br(soma_m)}). "
                f"{'Há registros 1200/1210/1700 no Bloco 1; ' if ja_tem_controle else 'Não há 1200/1210/1700 no Bloco 1; '}"
                "revisar escrituração/apuração (M200/M205/M600/M605), CST, regime e parametrizações."
            )

            logger.info(
                "EXP_M_ZERADO | base=%s credito=%s M_zerado=%s soma_m=%s controle=%s",
                str(base_export), str(credito_total), bloco_m_zerado, str(soma_m), ja_tem_controle
            )

            # ✅ Mantém seu meta antigo + adiciona só o necessário pro build_ctx_exportacao
            return Achado(
                registro_id=int(registro.id),
                tipo=self.tipo,
                codigo=self.codigo,
                descricao=desc,
                regra=self.nome,
                impacto_financeiro=None,
                prioridade="ALTA",
                meta={
                    "grupo": GRUPO_EXPORTACAO,
                    "subgrupo": SUBGRUPO_CONSISTENCIA,
                    "criticidade": "CRITICO",
                    "how_to_fix": [
                        "Verificar se o Bloco M (M200/M205/M600/M605) está completo no período",
                        "Conferir CST/regime e parametrizações no PVA",
                        "Se houver exportação, conferir apuração/saldos e ajustes",
                    ],
                    "erro_consistencia": True,
                    "motivo": "Bloco M zerado com exportação",
                    "base_exportacao": str(base_export),
                    "base_exportacao_fmt": self.fmt_br(base_export),
                    "cfops_detectados": cfops_sorted,
                    "tem_1200": tem_1200,
                    "tem_1210": tem_1210,
                    "tem_1700": tem_1700,
                    "tem_apuracao_m": tem_apuracao_m,
                    "bloco_m_zerado": bloco_m_zerado,
                    "regra_relacionada": "EXP_RESSARC_V1",

                    # 🔽 campos para o build_ctx_exportacao (SEM quebrar nada)
                    "fonte": fonte,
                    "soma_valores_bloco_m": str(soma_m),
                    # builder usa credito_total ou impacto_consolidado
                    "credito_total": str(credito_total),
                    # opcional (se você quiser exibir):
                    "credito_pis_estimado": str(cred_pis),
                    "credito_cofins_estimado": str(cred_cof),
                    "impacto_consolidado": str(credito_total),
                },
            )

        except Exception:
            logger.exception("Erro na regra EXP_M_ZERADO_V1")
            return None

    def gerar_revisoes(self, ctx) -> List[RevisaoFiscal]:
        """
        Autocorreção:
          - Só usa ctx (build_ctx_exportacao)
          - Preenche/ajusta M100 e M200
        Guardrails:
          - precisa de linhas_sped
          - tem_apuracao_m True e bloco_m_zerado True
          - credito_total Decimal > 0
        """
        try:
            linhas_sped: list[str] = ctx.get("linhas_sped") or []
            if not linhas_sped:
                return []

            tem_apuracao_m = self.to_bool(ctx.get("tem_apuracao_m"))
            bloco_m_zerado = self.to_bool(ctx.get("bloco_m_zerado"))
            if not (tem_apuracao_m and bloco_m_zerado):
                return []

            credito = ctx.get("credito_total")
            if not isinstance(credito, Decimal):
                credito = self.dec_any(credito) or Decimal("0")
            if credito <= 0:
                return []

            credito_fmt = f"{credito:.2f}".replace(".", ",")

            # âncoras
            linha_m001 = _find_line_index(linhas_sped, "|M001|")
            if linha_m001 is None:
                return []

            linha_m100 = _find_line_index(linhas_sped, "|M100|")
            linha_m200 = _find_line_index(linhas_sped, "|M200|")

            conteudo_m100 = (
                f"|M100|01|0,00|0,00|0,00|0,00|"
                f"{credito_fmt}|0,00|0,00|0,00|{credito_fmt}|"
            )
            conteudo_m200 = f"|M200|{credito_fmt}|0,00|0,00|0,00|"

            def linha_atual(i: int | None) -> str:
                if i is None:
                    return ""
                return (linhas_sped[i - 1] or "").strip()

            revisoes: list[RevisaoFiscal] = []

            # M100
            if linha_m100 is not None:
                if linha_atual(linha_m100) != conteudo_m100:
                    revisoes.append(RevisaoFiscal(
                        operacao="REPLACE_LINE",
                        linha_referencia=linha_m100,
                        conteudo=conteudo_m100,
                        regra_codigo=self.codigo,
                        registro="M100",
                    ))
            else:
                revisoes.append(RevisaoFiscal(
                    operacao="INSERT_AFTER",
                    linha_referencia=linha_m001,
                    conteudo=conteudo_m100,
                    regra_codigo=self.codigo,
                    registro="M100",
                ))

            # M200
            if linha_m200 is not None:
                if linha_atual(linha_m200) != conteudo_m200:
                    revisoes.append(RevisaoFiscal(
                        operacao="REPLACE_LINE",
                        linha_referencia=linha_m200,
                        conteudo=conteudo_m200,
                        regra_codigo=self.codigo,
                        registro="M200",
                    ))
            else:
                revisoes.append(RevisaoFiscal(
                    operacao="INSERT_AFTER",
                    linha_referencia=linha_m001,
                    conteudo=conteudo_m200,
                    regra_codigo=self.codigo,
                    registro="M200",
                ))

            return revisoes

        except Exception:
            logger.exception("Erro ao gerar revisões EXP_M_ZERADO_V1")
            return []