import logging
from decimal import Decimal
from typing import Any, Dict, Optional
from collections import defaultdict

from app.config.settings import ALIQUOTA_PIS, ALIQUOTA_COFINS
from app.fiscal.constants import (
    GRUPO_EXPORTACAO,
    SUBGRUPO_CONSISTENCIA,
)
from app.fiscal.dto import RegistroFiscalDTO
from app.fiscal.regras.base_regras import RegraBase
from app.fiscal.regras.achado import Achado

logger = logging.getLogger(__name__)


class RegraExportacaoBlocoMZeradoV1(RegraBase):
    """
    ERRO de consistência (DIAGNÓSTICO APENAS):

    - Há exportação (CFOP 7xxx)
    - Bloco M existe (tem_apuracao_m=True)
    - Bloco M aparenta zerado (bloco_m_zerado=True ou soma_m == 0)

    ❗ NÃO gera revisões físicas
    ❗ NÃO escreve M100/M200/M500/M600
    """

    codigo = "EXP_M_ZERADO_V1"
    nome = "Exportação com Bloco M zerado"
    tipo = "ERRO"

    def aplicar(self, registro: RegistroFiscalDTO) -> Optional[Achado]:
        try:
            # 🛡️ trava global
            if registro.is_pf:
                return None

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

            # flags do scanner
            tem_apuracao_m = self.to_bool(meta.get("tem_apuracao_m"))
            soma_m = self.dec_any(meta.get("soma_valores_bloco_m")) or Decimal("0")
            bloco_m_zerado = self.to_bool(meta.get("bloco_m_zerado")) or soma_m == 0

            if not tem_apuracao_m or not bloco_m_zerado:
                return None

            # base de exportação
            base_export = Decimal("0")
            cfops = set()

            for it in itens:
                if not isinstance(it, dict):
                    continue

                cfop = str(it.get("cfop") or "").strip()
                if cfop:
                    cfops.add(cfop)

                v = self.dec_any(it.get("vl_opr")) or Decimal("0")
                if v > 0:
                    base_export += v

            if base_export <= 0:
                return None

            base_export = self.q2(base_export)

            # crédito estimado (apenas informativo)
            cred_pis = self.q2(base_export * ALIQUOTA_PIS)
            cred_cof = self.q2(base_export * ALIQUOTA_COFINS)
            credito_total = self.q2(cred_pis + cred_cof)

            # bloco 1
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

            logger.warning(
                "EXP_M_ZERADO | base=%s | credito_est=%s | soma_m=%s | controle=%s",
                str(base_export), str(credito_total), str(soma_m), ja_tem_controle
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
                    "erro_consistencia": True,
                    "motivo": "Bloco M zerado com exportação",
                    "base_exportacao": str(base_export),
                    "base_exportacao_fmt": self.fmt_br(base_export),
                    "cfops_detectados": cfops_sorted,
                    "tem_1200": tem_1200,
                    "tem_1210": tem_1210,
                    "tem_1700": tem_1700,
                    "tem_apuracao_m": tem_apuracao_m,
                    "bloco_m_zerado": True,
                    "fonte": fonte,
                    "soma_valores_bloco_m": str(soma_m),
                    "credito_total_estimado": str(credito_total),
                },
            )

        except Exception:
            logger.exception("Erro na regra EXP_M_ZERADO_V1")
            return None
