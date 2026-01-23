from decimal import Decimal, ROUND_HALF_UP
from typing import Optional, Any, Dict
from app.fiscal.dto import RegistroFiscalDTO
from app.fiscal.regras.achado import Achado, Prioridade
from app.fiscal.regras.base_regras import RegraBase
import logging
from app.schemas.workflow import RevisaoFiscal
from collections import defaultdict
from app.sped.renderer import  _sha1
from app.config.settings import (
    ALIQUOTA_PIS,
    ALIQUOTA_COFINS,
    ALIQUOTA_TOTAL,
)
from app.fiscal.constants import (
    GRUPO_EXPORTACAO,
    SUBGRUPO_RESSARCIMENTO,
    SUBGRUPO_CONSISTENCIA,
)

logger = logging.getLogger(__name__)



def _deve_aplicar(ctx) -> bool:
    credito = ctx.get("credito_total")
    tem_export = bool(ctx.get("tem_exportacao"))
    try:
        if credito is None:
            return tem_export
        if not isinstance(credito, Decimal):
            s = str(credito or "0").strip()
            s = s.replace(".", "").replace(",", ".") if "," in s else s
            credito = Decimal(s)
        return credito > 0 or tem_export
    except Exception:
        return tem_export


def _find_line_num(linhas: list[str], prefix: str) -> int | None:
    # retorna linha_num 1-based
    for i, l in enumerate(linhas, start=1):
        if l.startswith(prefix):
            return i
    return None

def _fmt_m100(credito: str) -> str:
    # Mantemos apenas campos essenciais (exemplo alinhado ao seu caso)
    # Ajuste se você já tem um formatter central
    return f"|M100|01|0,00|0,00|0,00|0,00|{credito}|0,00|0,00|0,00|{credito}|"

def _fmt_m200(credito: str) -> str:
    return f"|M200|{credito}|0,00|0,00|0,00|"


class RegraExportacaoRessarcimentoV1(RegraBase):
    codigo = "EXP_RESSARC_V1"
    nome = "Exportação: possível ressarcimento/compensação (base C190 CFOP 7xxx)"
    tipo = "OPORTUNIDADE"



    def aplicar(self, registro: RegistroFiscalDTO) -> Optional[Achado]:

        try:

            if registro.reg not in ("C190_EXP_AGG", "C170_EXP_AGG"):
                return None

            raw = registro.dados or []
            if not raw:
                return None

            # 1) extrai meta (flags) se vier no primeiro item
            meta_flags: Dict[str, Any] = {}
            itens = raw

            if isinstance(raw[0], dict) and "_meta" in raw[0]:
                meta_flags = raw[0].get("_meta") or {}
                itens = raw[1:]

            if not meta_flags:
                return None

            if not itens:
                return None

            base_por_cfop = defaultdict(lambda: Decimal("0"))
            cfops = set()

            for it in itens:
                if not isinstance(it, dict):
                    continue
                cfop = str(it.get("cfop") or "").strip()
                if not cfop:
                    continue
                cfops.add(cfop)

                val = self.dec_br(it.get("vl_opr")) or Decimal("0")
                if val > 0:
                    base_por_cfop[cfop] += val

            base_export = sum(base_por_cfop.values(), Decimal("0"))
            if base_export <= 0:
                return None

            # impacto consolidado (arredondamento consistente)
            base_export = self.q2(base_export)

            cred_pis = self.q2(base_export * ALIQUOTA_PIS)
            cred_cof = self.q2(base_export * ALIQUOTA_COFINS)
            impacto = self.q2(cred_pis + cred_cof)


            def to_bool(v) -> bool:
                if v is None:
                    return False
                if isinstance(v, bool):
                    return v
                if isinstance(v, (int, float)):
                    return v != 0
                if isinstance(v, str):
                    s = v.strip().lower()
                    if s in ("1", "true", "t", "yes", "y", "sim", "s"):
                        return True
                    if s in ("0", "false", "f", "no", "n", "nao", "não", ""):
                        return False
                return False

            tem_1200 = to_bool(meta_flags.get("tem_1200"))
            tem_1210 = to_bool(meta_flags.get("tem_1210"))
            tem_1700 = to_bool(meta_flags.get("tem_1700"))
            ja_tem_indicio_ressarc = tem_1200 or tem_1210 or tem_1700

            # --- perfil monofásico ---
            perfil_monofasico = to_bool(meta_flags.get("perfil_monofasico"))

            raw_score = meta_flags.get("score_monofasico")
            try:
                score_monofasico = int(raw_score) if raw_score is not None else None
            except Exception:
                score_monofasico = None

            if perfil_monofasico:
                logger.info(
                    "EXP_RESSARC ignorada: perfil_monofasico=True (score=%s)",
                    score_monofasico
                )
                return None

            # Apuracao M
            tem_apuracao_m = to_bool(meta_flags.get("tem_apuracao_m"))
            bloco_m_zerado = to_bool(meta_flags.get("bloco_m_zerado"))

            subcenario_m = None

            if not ja_tem_indicio_ressarc:
                if bloco_m_zerado:
                    subcenario_m = "SEM_RESSARC_M_ZERADO, Atenção: Bloco M aparenta zerado apesar de exportação. Revisar escrituração/apuração (Bloco M) e parametrizações (CST/regime)."
                elif tem_apuracao_m:
                    subcenario_m = "SEM_RESSARC_COM_M, Há apuração no Bloco M. Verificar saldo credor/crédito acumulado e avaliar pedido/controle via 1200/1210/1700."
                else:
                    subcenario_m = "SEM_RESSARC_SEM_M, Não foram encontrados registros de apuração no Bloco M (M200/M205/M600/M605). Verificar se o arquivo contém Bloco M completo para o período e se a escrituração/apuração de PIS/COFINS está presente."

            # prioridade base por impacto
            if impacto >= Decimal("5000"):
                prioridade_base = "ALTA"
            elif impacto >= Decimal("1000"):
                prioridade_base = "MEDIA"
            else:
                prioridade_base = "BAIXA"

            prioridade_final = prioridade_base

            cenario = "COM_RESSARC" if ja_tem_indicio_ressarc else "SEM_RESSARC"

            logger.info(
                "EXP_RESSARC | monof=%s(score=%s) | impacto=%s | base=%s | itens=%s | 1200=%s 1210=%s 1700=%s | prio=%s->%s | cenario=%s",
                perfil_monofasico, score_monofasico,
                self.fmt_br(impacto),
                self.fmt_br(base_export),
                len(itens),
                tem_1200, tem_1210, tem_1700,
                prioridade_base, prioridade_final,
                cenario,
            )

            # descrição com números + call-to-action
            base_fmt = self.fmt_br(base_export)  # "1.234,56"
            impacto_fmt = self.fmt_br(impacto)  # "1.234,56"
            cfops_sorted = sorted(cfops)
            cfops_top = ", ".join(cfops_sorted[:5]) if cfops_sorted else "-"

            fonte = (meta_flags.get("fonte") or "C190")
            onde = "C190" if fonte == "C190" else "C170"  # só pra texto curto
            qtd_itens = len(itens)
            qtd_cfops = len(base_por_cfop)

            if ja_tem_indicio_ressarc:
                desc = (
                    f"Exportação detectada (CFOP 7xxx) no {onde}: base R$ {base_fmt} "
                    f"({qtd_itens} item(ns), {qtd_cfops} CFOP(s)). Crédito estimado R$ {impacto} (9,25%). "
                    f"Há indícios de ressarcimento/controle no Bloco 1 "
                    f"(1200={int(tem_1200)}, 1210={int(tem_1210)}, 1700={int(tem_1700)}). "
                    f"CFOPs (top): {cfops_top}. "
                    f"Conferir consistência entre Bloco 1 (1200/1210/1700) e Bloco M (apuração/saldos)."
                )
            else:
                desc = (
                    f"Exportação detectada (CFOP 7xxx) no {onde}: base R$ {base_fmt} "
                    f"({qtd_itens} item(ns), {qtd_cfops} CFOP(s)). Crédito estimado R$ {impacto_fmt} (9,25%). "
                    f"Não há 1200/1210/1700 no Bloco 1 (possível crédito acumulado sem pedido/controle). "
                    f"CFOPs (top): {cfops_top}. "
                    f"Validar Bloco M e avaliar pedido/controle via 1200/1210/1700."
                )

            # =========================
            # SCORE DE OPORTUNIDADE
            # =========================
            score = 0
            motivos = []

            # (A) Impacto financeiro (0–60)
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
            motivos.append(f"Impacto estimado R$ {impacto}")

            # (B) Cenário de controle (0–25)
            if not ja_tem_indicio_ressarc:
                score += 25
                motivos.append("Sem 1200/1210/1700 (sem pedido/controle)")
            else:
                score += 10
                motivos.append("Já existe 1200/1210/1700")

            # (C) Bloco M – apuração/saldos (0–15)
            if not ja_tem_indicio_ressarc:
                if bloco_m_zerado:
                    score += 15
                    motivos.append("Bloco M zerado")
                elif tem_apuracao_m:
                    score += 10
                    motivos.append("Bloco M com apuração")
                else:
                    score += 5
                    motivos.append("Bloco M não identificado")
            else:
                score += 5

            # (D) Diversidade de CFOPs (0–5)
            qtd_cfops = len(base_por_cfop)
            if qtd_cfops >= 5:
                score += 5
            elif qtd_cfops >= 2:
                score += 3
            else:
                score += 1
            motivos.append(f"{qtd_cfops} CFOP(s)")

            # 🔽 REBAIXAMENTO quando já há controle/ressarcimento
            if ja_tem_indicio_ressarc:
                score = max(0, score - 15)
                motivos.append("Oportunidade parcialmente explorada (já há controle)")

            # Limite do score
            score = min(100, int(score))

            # =========================
            # BUCKET
            # =========================
            if score >= 80:
                bucket = "ALTA_CHANCE"
            elif score >= 55:
                bucket = "REVISAR"
            else:
                bucket = "BAIXA"

            # =========================
            # PRIORIDADE FINAL (100% alinhada ao score)
            # =========================
            if score >= 80:
                prioridade_final = "ALTA"
            elif score >= 55:
                prioridade_final = "MEDIA"
            else:
                prioridade_final = "BAIXA"

            return Achado(
                registro_id=int(registro.id),
                tipo="OPORTUNIDADE",
                codigo=self.codigo,
                descricao=desc,
                regra=self.nome,
                impacto_financeiro=impacto,
                prioridade=prioridade_final,
                meta={
                    "grupo": GRUPO_EXPORTACAO,
                    "subgrupo": SUBGRUPO_RESSARCIMENTO,
                    "qtd_cfops": len(base_por_cfop),
                    "qtd_itens_export": len(itens),
                    "score": score,
                    "bucket": bucket,
                    "prioridade_alinhada": prioridade_final,
                    "motivos_score": motivos[:5],
                    "impacto_por_cfop": {
                        k: self.br_num(self.q2(v * ALIQUOTA_TOTAL))
                        for k, v in sorted(base_por_cfop.items())
                    },
                    "aliquota_pis": self.pct(ALIQUOTA_PIS),
                    "aliquota_cofins": self.pct(ALIQUOTA_COFINS),
                    "aliquota_total": self.pct(ALIQUOTA_TOTAL),
                    "base_exportacao": self.br_num(base_export),        # sem milhar, pt-BR
                    "base_exportacao_fmt": base_fmt,                    # com milhar, pt-BR
                    "bases_por_cfop": {k: self.br_num(v) for k, v in sorted(base_por_cfop.items())},
                    "cenario": cenario,
                    "cfops_detectados": cfops_sorted,
                    "cfops_top": cfops_sorted[:5],
                    "credito_pis_estimado": self.br_num(cred_pis),
                    "credito_cofins_estimado": self.br_num(cred_cof),
                    "impacto_consolidado": self.br_num(impacto),
                    "tem_1200": tem_1200,
                    "tem_1210": tem_1210,
                    "tem_1700": tem_1700,
                    "prioridade_base": prioridade_base,
                    "prioridade_final": prioridade_final,
                    "metodo": "C190 CFOP 7xxx (VL_OPR x 9,25%)",
                    "fonte": meta_flags.get("fonte", "C190"),
                    "tem_apuracao_m": tem_apuracao_m,
                    "bloco_m_zerado": bloco_m_zerado,
                    "subcenario_m": subcenario_m,
                    "soma_valores_bloco_m": str(meta_flags.get("soma_valores_bloco_m") or "0"),
                    "orientacao_pva": [
                        "Bloco 1: 1200/1210/1700 (ressarcimento/controle)",
                        "Bloco M: apuração e saldos de PIS/COFINS (ex.: M200/M205, M600/M605, ajustes/saldos)",
                    ],
                    "payload_v2": {
                        "aliquotas": {
                                "pis": self.pct(ALIQUOTA_PIS),
                                "cofins": self.pct(ALIQUOTA_COFINS),
                                "total": self.pct(ALIQUOTA_PIS + ALIQUOTA_COFINS),
                            },
                        "bases": {
                            "exportacao_total": self.br_num(base_export),
                            "por_cfop": {k: self.br_num(v) for k, v in sorted(base_por_cfop.items())},
                        },
                        "creditos_estimados": {
                            "pis": self.br_num(cred_pis),
                            "cofins": self.br_num(cred_cof),
                            "total": self.br_num(impacto),
                            "impacto_por_cfop": {
                                k: self.br_num(self.q2(v * ALIQUOTA_TOTAL))
                                for k, v in sorted(base_por_cfop.items())
                            },
                        },
                        "auditoria": {
                            "cenario": cenario,
                            "cfops_detectados": cfops_sorted,
                            "cfops_top": cfops_sorted[:5],
                            "fonte": meta_flags.get("fonte", "C190"),
                            "subcenario_m": subcenario_m,
                        },
                    },

                    },

            )
        except Exception:
            logger.exception("Erro na regra EXP_RESSARC_V1")
            return None

    def gerar_revisoes(self, ctx) -> list[RevisaoFiscal]:
        if not _deve_aplicar(ctx):
            return []

        linhas_sped: list[str] = ctx["linhas_sped"]

        credito = ctx.get("credito_total")
        if not isinstance(credito, Decimal):
            return []

        credito_fmt = f"{credito:.2f}".replace(".", ",")

        revisoes: list[RevisaoFiscal] = []

        # 1) Localiza âncoras (linha_num 1-based)
        linha_m001 = _find_line_num(linhas_sped, "|M001|")
        linha_m100 = _find_line_num(linhas_sped, "|M100|")
        linha_m200 = _find_line_num(linhas_sped, "|M200|")

        if linha_m001 is None:
            return []

        # 2) Conteúdos novos
        conteudo_m100 = (
            f"|M100|01|0,00|0,00|0,00|0,00|"
            f"{credito_fmt}|0,00|0,00|0,00|{credito_fmt}|"
        )
        conteudo_m200 = f"|M200|{credito_fmt}|0,00|0,00|0,00|"

        # -------------------------
        # M100
        # -------------------------
        if linha_m100 is not None:
            linha_antes_m100 = linhas_sped[linha_m100 - 1].strip()
            revisoes.append(RevisaoFiscal(
                operacao="REPLACE_LINE",
                linha_referencia=linha_m100,
                linha_antes=linha_antes_m100,
                linha_hash=_sha1(linha_antes_m100),
                conteudo=conteudo_m100,
                regra_codigo="EXP_RESSARC_V1",
                registro="M100",
            ))
            m100_sera_inserido = False
        else:
            revisoes.append(RevisaoFiscal(
                operacao="INSERT_AFTER",
                linha_referencia=linha_m001,
                conteudo=conteudo_m100,
                regra_codigo="EXP_RESSARC_V1",
                registro="M100",
            ))
            m100_sera_inserido = True

        # -------------------------
        # M200
        # -------------------------
        if linha_m200 is not None:
            linha_antes_m200 = linhas_sped[linha_m200 - 1].strip()
            revisoes.append(RevisaoFiscal(
                operacao="REPLACE_LINE",
                linha_referencia=linha_m200,
                linha_antes=linha_antes_m200,
                linha_hash=_sha1(linha_antes_m200),
                conteudo=conteudo_m200,
                regra_codigo="EXP_RESSARC_V1",
                registro="M200",
            ))
        else:
            # Se M100 será inserido logo após M001, então M200 deve ser inserido após M100.
            # Como nosso motor só tem INSERT_AFTER por linha_num, usamos:
            #   - inserir M100 em M001+1
            #   - inserir M200 em (M001+1)+1 => INSERT_AFTER linha_m001+1
            linha_ref_para_m200 = (linha_m001 + 1) if m100_sera_inserido else linha_m001

            revisoes.append(RevisaoFiscal(
                operacao="INSERT_AFTER",
                linha_referencia=linha_ref_para_m200,
                conteudo=conteudo_m200,
                regra_codigo="EXP_RESSARC_V1",
                registro="M200",
            ))

        return revisoes