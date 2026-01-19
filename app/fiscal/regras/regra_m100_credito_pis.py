from decimal import Decimal, ROUND_HALF_UP
from typing import Optional, Dict, Any, List
from app.fiscal.dto import RegistroFiscalDTO

class RegraM100CreditoPIS:
    codigo_base = "M100"
    tipo = "M100"

    def _to_decimal(self, s):
        if not s:
            return None
        txt = str(s).replace(".", "").replace(",", ".")
        try:
            return Decimal(txt)
        except:
            return None

    def aplicar(self, registro: RegistroFiscalDTO):
        if registro.reg != "M100":
            return None

        dados: List[Any] = registro.dados or []

        base = self._to_decimal(dados[2]) if len(dados) > 2 else None
        aliquota = self._to_decimal(dados[3]) if len(dados) > 3 else None
        credito = self._to_decimal(dados[4]) if len(dados) > 4 else None

        if base is None or aliquota is None:
            return None

        teto = (base * aliquota / Decimal("100")).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)

        # 1) Base negativa
        if base < 0:
            return {
                "tipo": "ALERTA",
                "codigo": "M100-BASE-NEG",
                "descricao": f"M100 com base negativa ({base}).",
                "impacto_financeiro": None,
                "registro_id": registro.id,
            }

        # 2) Crédito maior que o teto
        if credito is not None and credito > teto:
            return {
                "tipo": "ERRO",
                "codigo": "M100-CREDITO-MAIOR",
                "descricao": f"M100 crédito {credito} maior que teto legal {teto}.",
                "impacto_financeiro": credito - teto,
                "registro_id": registro.id,
            }

        # 3) Base > 0 e crédito = 0 → dinheiro perdido
        if base > 0 and (credito is None or credito == 0):
            return {
                "tipo": "OPORTUNIDADE",
                "codigo": "M100-CREDITO-ZERO",
                "descricao": f"M100 com base {base} e crédito zero.",
                "impacto_financeiro": teto,
                "registro_id": registro.id,
            }

        return None
