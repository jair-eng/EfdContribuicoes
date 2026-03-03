from decimal import Decimal
from typing import Dict, List, Any

from app.fiscal.contexto import dec_any
from app.fiscal.dto import RegistroFiscalDTO
from app.fiscal.regras.Diagnostico.base_regras import RegraBase


class RegraTema69ICMSBaseV1(RegraBase):

    codigo = "TEMA69_ICMS_BASE_V1"
    tipo = "OPORTUNIDADE"
    alvo = "C170_SAIDA_AGG"

    def aplicar(self, dto: RegistroFiscalDTO):

        if not isinstance(dto, RegistroFiscalDTO):
            return None
        if dto.reg != "C170_SAIDA_AGG":
            return None



        itens = dto.dados[1:]  # pula _meta

        total_icms = Decimal("0")
        cfops = set()


        for item in itens:
            vl_icms = dec_any(item.get("vl_icms"))
            if vl_icms <= 0:
                continue

            total_icms += vl_icms
            cfops.add(item.get("cfop"))


        if total_icms <= 0:
            return None

        credito_pis = (total_icms * Decimal("0.0165")).quantize(Decimal("0.01"))
        credito_cof = (total_icms * Decimal("0.0760")).quantize(Decimal("0.01"))
        credito_total = credito_pis + credito_cof

        return {
            "codigo": self.codigo,
            "tipo": self.tipo,
            "descricao": (
                f"Tema 69: ICMS identificado R$ {total_icms}. "
                f"Crédito estimado R$ {credito_total}."
            ),
            "impacto_financeiro": float(credito_total),
            "registro_id": int(getattr(dto, "id", 0) or 0),  # opcional, mas ajuda
            "meta": {
                "base_icms": str(total_icms),
                "credito_estimado": str(credito_total),
                "cfops_top": list(cfops)[:5],
            }
        }