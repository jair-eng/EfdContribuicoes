from decimal import Decimal, ROUND_HALF_UP
from typing import Optional, Any, Dict
from app.fiscal.dto import RegistroFiscalDTO
from app.fiscal.regras.achado import Achado, Prioridade
from app.fiscal.regras.base_regras import RegraBase
from app.config.settings import (
    ALIQUOTA_PIS,
    ALIQUOTA_COFINS,
    ALIQUOTA_TOTAL,
)
from app.fiscal.constants import GRUPO_CAFE, SUBGRUPO_C190


class RegraCafeC190V1(RegraBase):
    codigo = "CAFE_C190_V1"
    nome = "Café: crédito acumulado (C190 1102 x 5102 CST 051)"
    tipo = "OPORTUNIDADE"


    def aplicar(self, registro: RegistroFiscalDTO) -> Optional[Achado]:
        try:
            if registro.reg != "C190_AGG":
                return None

            itens = registro.dados or []
            if not isinstance(itens, list) or not itens:
                return None

            base_entradas = Decimal("0.00")
            base_saidas = Decimal("0.00")

            for it in itens:
                # it = {"cst": "...", "cfop": "...", "vl_opr": "..."}
                cst = str(it.get("cst") or "").strip()
                cfop = str(it.get("cfop") or "").strip()
                vl_opr = self.dec_br(it.get("vl_opr")) or Decimal("0")

                if cst == "051" and cfop == "1102":
                    base_entradas += vl_opr
                elif cst == "051" and cfop == "5102":
                    base_saidas += vl_opr

            # precisa ter compra e venda no período (heurística)
            if base_entradas <= 0 or base_saidas <= 0:
                return None

            base_entradas = self.q2(base_entradas)
            base_saidas = self.q2(base_saidas)
            cred_pis = self.q2(base_entradas * ALIQUOTA_PIS)
            cred_cof = self.q2(base_entradas * ALIQUOTA_COFINS)
            impacto = self.q2(cred_pis + cred_cof)

            prioridade: Prioridade = "ALTA" if impacto >= Decimal("5000") else "MEDIA"

            return Achado(
                registro_id=int(registro.id),
                tipo="OPORTUNIDADE",
                codigo=self.codigo,
                descricao=(
                    "Detectadas entradas CFOP 1102 e saídas CFOP 5102 com CST 051 no C190. "
                    "No resumo, PIS/COFINS estão zerados; possível crédito acumulado (café) "
                    "não apropriado/ressarcível. Validar Bloco M e ressarcimento."
                ),
                regra=self.nome,
                impacto_financeiro=impacto,
                prioridade=prioridade,
                meta={
                    "grupo": GRUPO_CAFE,
                    "subgrupo": SUBGRUPO_C190,
                    "cst": "051",
                    "cfop_entrada": "1102",
                    "cfop_saida": "5102",
                    "qtd_entradas": sum(1 for it in itens if it["cst"] == "051" and it["cfop"] == "1102"),
                    "qtd_saidas": sum(1 for it in itens if it["cst"] == "051" and it["cfop"] == "5102"),
                    "base_entradas": self.br_num(base_entradas),
                    "base_saidas": self.br_num(base_saidas),
                    "aliquota_pis": self.pct(ALIQUOTA_PIS),
                    "aliquota_cofins": self.pct(ALIQUOTA_COFINS),
                    "aliquota_total": self.pct(ALIQUOTA_TOTAL),
                    "credito_pis_estimado": self.br_num(cred_pis),
                    "credito_cofins_estimado": self.br_num(cred_cof),
                    "impacto_consolidado": self.br_num(impacto),
                    "metodo": f"C190 agregado (VL_OPR x {self.pct(ALIQUOTA_TOTAL)})",
                }
                ,
            )
        except Exception:
            return None
