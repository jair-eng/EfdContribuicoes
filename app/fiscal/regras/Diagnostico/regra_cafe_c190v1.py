from decimal import Decimal
from typing import Optional
from collections import defaultdict

from app.fiscal.dto import RegistroFiscalDTO
from app.fiscal.regras.Diagnostico.achado import Achado, Prioridade
from app.fiscal.regras.Diagnostico.base_regras import RegraBase
from app.config.settings import (
    ALIQUOTA_PIS,
    ALIQUOTA_COFINS,
    ALIQUOTA_TOTAL,
)
from app.fiscal.constants import GRUPO_CAFE, SUBGRUPO_C190


class RegraCafeC190V1(RegraBase):
    codigo = "CAFE_C190_V1"
    nome = "Café: crédito acumulado (C190 catálogo-driven)"
    tipo = "OPORTUNIDADE"

    SLUG_CFOP_ENTRADA = "CFOP_CAFE_ENTRADA"
    SLUG_CFOP_SAIDA   = "CFOP_CAFE_SAIDA"
    SLUG_CST_ALVO     = "CST_ICMS_CAFE_ALVO"

    def aplicar(self, registro: RegistroFiscalDTO) -> Optional[Achado]:
        try:
            if registro.reg != "C190_AGG":
                return None

            itens = registro.dados or []
            if not isinstance(itens, list) or not itens:
                return None

            cat = self.get_catalogo(registro)

            base_entradas = Decimal("0.00")
            base_saidas = Decimal("0.00")
            qtd_entradas = 0
            qtd_saidas = 0
            itens_validos = 0
            base_por_cfop = defaultdict(lambda: Decimal("0.00"))

            for it in itens:
                if not isinstance(it, dict):
                    continue

                cst = str(it.get("cst") or "").strip()
                cfop = str(it.get("cfop") or "").strip()
                vl_opr = self.dec_br(it.get("vl_opr")) or Decimal("0")

                if not cst or not cfop or vl_opr <= 0:
                    continue

                itens_validos += 1
                base_por_cfop[cfop] += vl_opr

                # catálogo decide tudo
                cst_ok = self.cst_match(cat, self.SLUG_CST_ALVO, cst)
                if not cst_ok:
                    continue

                if self.cfop_match(cat, self.SLUG_CFOP_ENTRADA, cfop):
                    base_entradas += vl_opr
                    qtd_entradas += 1
                elif self.cfop_match(cat, self.SLUG_CFOP_SAIDA, cfop):
                    base_saidas += vl_opr
                    qtd_saidas += 1

            if base_entradas <= 0 or base_saidas <= 0:
                return None

            base_entradas = self.q2(base_entradas)
            base_saidas = self.q2(base_saidas)

            cred_pis = self.q2(base_entradas * ALIQUOTA_PIS)
            cred_cof = self.q2(base_entradas * ALIQUOTA_COFINS)
            impacto = self.q2(cred_pis + cred_cof)

            ratio_es = (base_entradas / base_saidas) if base_saidas > 0 else Decimal("0")

            if impacto >= Decimal("5000"):
                prioridade: Prioridade = "ALTA"
            elif impacto >= Decimal("1000"):
                prioridade = "MEDIA"
            else:
                prioridade = "BAIXA"

            if ratio_es >= Decimal("5") or ratio_es <= Decimal("0.20"):
                if prioridade == "ALTA":
                    prioridade = "MEDIA"
                elif prioridade == "MEDIA":
                    prioridade = "BAIXA"

            desc = (
                "Café: padrão de giro detectado no C190 via catálogo fiscal. "
                f"Entradas R$ {self.fmt_br(base_entradas)} vs Saídas R$ {self.fmt_br(base_saidas)}. "
                f"Crédito *proxy* sobre entradas: R$ {self.fmt_br(impacto)} (9,25%). "
                "Validar Bloco M e possível acúmulo/ressarcimento."
            )

            return Achado(
                registro_id=int(registro.id),
                tipo=self.tipo,
                codigo=self.codigo,
                descricao=desc,
                regra=self.nome,
                impacto_financeiro=impacto,
                prioridade=prioridade,
                meta={
                    "grupo": GRUPO_CAFE,
                    "subgrupo": SUBGRUPO_C190,
                    "slugs": {
                        "cfop_entrada": self.SLUG_CFOP_ENTRADA,
                        "cfop_saida": self.SLUG_CFOP_SAIDA,
                        "cst_alvo": self.SLUG_CST_ALVO,
                    },
                    "qtd_entradas": int(qtd_entradas),
                    "qtd_saidas": int(qtd_saidas),
                    "itens_validos": int(itens_validos),
                    "base_entradas": self.br_num(base_entradas),
                    "base_saidas": self.br_num(base_saidas),
                    "ratio_entrada_saida": str(ratio_es),
                    "aliquota_pis": self.pct(ALIQUOTA_PIS),
                    "aliquota_cofins": self.pct(ALIQUOTA_COFINS),
                    "aliquota_total": self.pct(ALIQUOTA_TOTAL),
                    "credito_pis_estimado": self.br_num(cred_pis),
                    "credito_cofins_estimado": self.br_num(cred_cof),
                    "impacto_consolidado": self.br_num(impacto),
                    "metodo_estimativa": "proxy_entrada_9_25",
                    "base_por_cfop": {k: self.br_num(self.q2(v)) for k, v in sorted(base_por_cfop.items())},
                },
            )

        except Exception:
            print("Erro na regra CAFE_C190_V1")
            return None