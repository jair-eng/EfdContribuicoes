from __future__ import annotations
from __future__ import annotations
from decimal import Decimal
from typing import Any, List, Optional
from app.config.settings import  ALIQUOTA_TOTAL
from app.fiscal.dto import RegistroFiscalDTO
from app.fiscal.regras.achado import Achado
import logging
logger = logging.getLogger(__name__)


class RegraC190CreditoPotencial:
    """
    C190 (SPED Fiscal - ICMS/IPI) — layout observado:
      dados[0] = CST_ICMS
      dados[1] = CFOP
      dados[3] = VL_OPR (valor da operação)

    Heurística: entradas (CFOP 1xxx/2xxx) com valor relevante.
    Impacto estimado (PIS+COFINS) — sujeito à validação fiscal.
    """

    codigo = "C190-ENT"
    nome = "Entrada relevante (C190) para revisão de crédito"
    tipo = "OPORTUNIDADE"

    CFOP_ENTRADA_PREFIXOS = ("1", "2")

    # filtro MVP (ajuste depois)
    VL_OPR_MIN = Decimal("1000")

    def aplicar(self, registro: RegistroFiscalDTO) -> Optional[Achado]:
        try:
            if registro.reg != "C190":
                return None

            dados: List[Any] = registro.dados or []
            if not dados:
                return None

            cst_icms = str(dados[0]).strip() if len(dados) > 0 and dados[0] is not None else None
            cfop = str(dados[1]).strip() if len(dados) > 1 and dados[1] is not None else None

            # VL_OPR (base) — aceita "1.234,56" e "1,234.56"
            vl_opr = self.dec_any(dados[3]) if len(dados) > 3 else Decimal("0")

            # validações mínimas
            if not cfop or len(cfop) != 4 or not cfop.isdigit():
                return None
            if cfop[0] not in self.CFOP_ENTRADA_PREFIXOS:
                return None
            if vl_opr <= 0:
                return None
            if vl_opr < self.VL_OPR_MIN:
                return None

            # impacto (arredonda no final)
            impacto = self.q2(vl_opr * ALIQUOTA_TOTAL)

            desc = (
                f"C190 entrada CFOP={cfop} CST={cst_icms or 'N/D'} "
                f"VL_OPR=R$ {self.fmt_br(self.q2(vl_opr))} "
                f"(impacto est. R$ {self.fmt_br(impacto)}). "
                "Validar enquadramento."
            )

            return Achado(
                registro_id=int(registro.id),
                tipo=self.tipo,
                codigo=self.codigo,
                descricao=desc,
                impacto_financeiro=self.money(impacto),  # float com 2 casas (padrão UI/DB)
                regra=self.nome,
                meta={
                    "cfop": cfop,
                    "cst_icms": cst_icms,
                    "vl_opr": str(self.q2(vl_opr)),  # padroniza 2 casas
                    "impacto": str(impacto),  # 2 casas
                    "aliquota_total": str(ALIQUOTA_TOTAL),
                    "linha": int(getattr(registro, "linha", 0) or 0),
                },
            )
        except Exception:
            logger.exception("Erro na regra %s", getattr(self, "codigo", "C190-ENT"))
            return None