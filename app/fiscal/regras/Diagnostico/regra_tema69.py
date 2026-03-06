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
        if not isinstance(dto, RegistroFiscalDTO) or dto.reg != "C170_SAIDA_AGG":
            return None

        itens = dto.dados[1:]  # pula _meta

        total_icms = Decimal("0")
        cfops = {}
        itens_com_icms = 0

        for item in itens:
            vl_icms = dec_any(item.get("vl_icms"))
            if vl_icms <= 0:
                continue
            itens_com_icms += 1
            total_icms += vl_icms
            cfop = (item.get("cfop") or "").strip()
            if cfop:
                cfops[cfop] = cfops.get(cfop, 0) + 1

        if total_icms <= 0:
            return None

        # Tema 69 (saída): redução de base estimada ~ ICMS
        delta_base = total_icms

        delta_pis = (delta_base * Decimal("0.0165")).quantize(Decimal("0.01"))
        delta_cof = (delta_base * Decimal("0.0760")).quantize(Decimal("0.01"))
        delta_total = delta_pis + delta_cof

        cfops_top = sorted(cfops.items(), key=lambda x: x[1], reverse=True)[:5]

        return {
            "codigo": self.codigo,
            "tipo": self.tipo,
            "descricao": (
                f"Tema 69 (saídas): ICMS destacado R$ {total_icms}. "
                f"Redução estimada de débito PIS/COFINS R$ {delta_total}."
            ),
            "impacto_financeiro": float(delta_total),
            "registro_id": int(getattr(dto, "id", 0) or 0),
            "meta": {
                "icms_destacado_total": str(total_icms),
                "delta_base_estimado": str(delta_base),
                "delta_debito_pis": str(delta_pis),
                "delta_debito_cofins": str(delta_cof),
                "itens_com_icms": itens_com_icms,
                "cfops_top": [c for c, _ in cfops_top],
            },
        }