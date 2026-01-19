import logging
from decimal import Decimal
from typing import Any, Dict, Optional
from app.config.settings import ALIQUOTA_PIS, ALIQUOTA_COFINS, ALIQUOTA_TOTAL
from app.fiscal.constants import GRUPO_EXPORTACAO, SUBGRUPO_CONSISTENCIA, SUBGRUPO_RESSARCIMENTO
from app.fiscal.dto import RegistroFiscalDTO
from app.fiscal.regras.base_regras import RegraBase
from app.fiscal.regras.achado import  Achado  # ajuste se o path for outro

logger = logging.getLogger(__name__)


class RegraExportacaoBlocoMZeradoV1(RegraBase):
    codigo = "EXP_M_ZERADO_V1"
    nome = "Exportação com Bloco M zerado"
    tipo = "ERRO"

    # thresholds de prioridade
    LIM_ALTA = Decimal("5000")
    LIM_MEDIA = Decimal("1000")

    def aplicar(self, registro: RegistroFiscalDTO) -> Optional[Achado]:
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

            # flags essenciais
            tem_apuracao_m = bool(meta.get("tem_apuracao_m"))
            bloco_m_zerado = bool(meta.get("bloco_m_zerado"))

            # fallback pelo soma_valores_bloco_m (string/decimal)
            soma_m = self.dec_any(meta.get("soma_valores_bloco_m") or "0")
            if soma_m == Decimal("0") and tem_apuracao_m:
                bloco_m_zerado = True

            if not tem_apuracao_m:
                return None  # essa regra é só quando existe Bloco M

            if not bloco_m_zerado:
                return None  # essa regra é só quando o M parece zerado

            # base exportação (soma vl_opr dos itens)
            base_export = Decimal("0")
            cfops = set()

            for it in itens:
                if not isinstance(it, dict):
                    continue
                cfop = str(it.get("cfop") or "").strip()
                if cfop:
                    cfops.add(cfop)

                v = self.dec_any(it.get("vl_opr") or "0")
                if v > 0:
                    base_export += v

            if base_export <= 0:
                return None

            base_export = self.q2(base_export)
            cred_pis = self.q2(base_export * ALIQUOTA_PIS)
            cred_cof = self.q2(base_export * ALIQUOTA_COFINS)
            impacto = self.q2(cred_pis + cred_cof)

            # Bloco 1 (controle)
            tem_1200 = bool(meta.get("tem_1200"))
            tem_1210 = bool(meta.get("tem_1210"))
            tem_1700 = bool(meta.get("tem_1700"))
            ja_tem_controle = tem_1200 or tem_1210 or tem_1700

            fonte = str(meta.get("fonte") or "C190")
            onde = "C190" if fonte == "C190" else "C170"
            cfops_sorted = sorted(cfops)
            cfops_top = ", ".join(sorted(cfops)[:5]) if cfops else "-"

            # prioridade
            prioridade = "ALTA"

            # descrição
            desc = (
                f"Inconsistência fiscal: Exportação detectada (CFOP 7xxx) no {onde}: "
                f"base R$ {self.fmt_br(base_export)} (CFOPs: {cfops_top}). "
                f"Bloco M existe, mas aparenta zerado (sem apuração ou saldo). "
                f"{'Há registros 1200/1210/1700 no Bloco 1; ' if ja_tem_controle else 'Não há 1200/1210/1700 no Bloco 1; '}"
                "revisar escrituração/apuração (M200/M205/M600/M605), CST, regime e parametrizações."
            )

            logger.info(
                "EXP_M_ZERADO | base=%s impacto=%s M_zerado=%s soma_m=%s controle=%s prio=%s",
                base_export, impacto, bloco_m_zerado, soma_m, ja_tem_controle, prioridade
            )

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
                        "Se houver exportação, conferir apuração/saldos e ajustes" ],
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
                },
            )

        except Exception:
            logger.exception("Erro na regra EXP_M_ZERADO_V1")
            return None
