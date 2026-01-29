from decimal import Decimal, ROUND_HALF_UP
from typing import Optional, Any, Dict, List
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
            # Cálculo da base de exportação
            # -------------------------------------------------
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

            base_export = self.q2(base_export)

            # -------------------------------------------------
            # Créditos estimados
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

            # -------------------------------------------------
            # Score
            # -------------------------------------------------
            score = 0
            motivos = []

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

            motivos.append(f"Impacto R$ {self.fmt_br(impacto)}")

            if not ja_tem_indicio_ressarc:
                score += 25
                motivos.append("Sem 1200/1210/1700")
            else:
                score += 10

            if bloco_m_zerado:
                score += 15
                motivos.append("Bloco M zerado")
            elif tem_apuracao_m:
                score += 10

            qtd_cfops = len(base_por_cfop)
            score += min(5, max(1, qtd_cfops))

            score = min(100, score)

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
                f"({len(itens)} item(ns), {len(cfops)} CFOP(s)). "
                f"Crédito estimado R$ {impacto_fmt} (9,25%). "
                f"CFOPs (top): {cfops_top}. "
                f"Validar Bloco M e controle via 1200/1210/1700."
            )

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
                    "base_exportacao": self.br_num(base_export),
                    "credito_pis_estimado": self.br_num(cred_pis),
                    "credito_cofins_estimado": self.br_num(cred_cof),
                    "impacto_consolidado": self.br_num(impacto),
                    "qtd_cfops": len(base_por_cfop),
                    "cfops_detectados": sorted(cfops),
                    "bucket": bucket,
                    "score": score,
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
