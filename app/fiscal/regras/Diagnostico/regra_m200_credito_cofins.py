from app.fiscal.dto import RegistroFiscalDTO
from decimal import Decimal, ROUND_HALF_UP
from app.fiscal.regras.Diagnostico.regra_m100_credito_pis import RegraM100CreditoPIS
from app.fiscal.regras.Diagnostico.base_regras import RegraBase


class RegraM200CreditoCOFINS(RegraM100CreditoPIS, RegraBase):

    def aplicar(self, registro: RegistroFiscalDTO):
        if registro.reg != "M200":
            return None

        dados = registro.dados or []

        base = self._get_decimal(dados, self.IDX_BASE)
        aliquota = self._get_decimal(dados, self.IDX_ALIQUOTA)

        # para M200, o formato também costuma ter "|||CREDITO", então reutiliza o getter
        credito = self._get_credito_m100(dados)

        if base is None or aliquota is None:
            return None

        teto = (base * aliquota / Decimal("100")).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)

        if base < 0:
            return {
                "tipo": "ALERTA",
                "codigo": "M200-BASE-NEG",
                "descricao": f"M200 com base negativa ({base}).",
                "impacto_financeiro": None,
                "registro_id": registro.id,
            }

        if credito is not None and credito > teto:
            return {
                "tipo": "ERRO",
                "codigo": "M200-CREDITO-MAIOR",
                "descricao": f"M200 crédito {credito} maior que teto legal {teto}.",
                "impacto_financeiro": credito - teto,
                "registro_id": registro.id,
            }

        if base > 0 and (credito is None or credito == 0):
            return {
                "tipo": "OPORTUNIDADE",
                "codigo": "M200-CREDITO-ZERO",
                "descricao": f"M200 com base {base} e crédito zero.",
                "impacto_financeiro": teto,
                "registro_id": registro.id,
            }

        return None