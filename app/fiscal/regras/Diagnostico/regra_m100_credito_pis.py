from decimal import Decimal, ROUND_HALF_UP
from typing import Any, List, Optional
from app.fiscal.dto import RegistroFiscalDTO
from app.fiscal.regras.Diagnostico.base_regras import RegraBase


class RegraM100CreditoPIS(RegraBase):
    codigo_base = "M100"
    tipo = "M100"

    # índices alinhados com o M100 que vocês escrevem no construir_bloco_m_v3
    IDX_BASE = 2
    IDX_ALIQUOTA = 3
    IDX_CREDITO = 6  # <-- FIX: crédito está após "|||"

    def _get_decimal(self, dados: List[Any], idx: int) -> Optional[Decimal]:
        if idx < 0 or idx >= len(dados):
            return None
        v = self.dec_br(dados[idx])
        return v

    def _get_credito_m100(self, dados: List[Any]) -> Optional[Decimal]:
        """
        Preferência:
        1) índice oficial (6) compatível com construir_bloco_m_v3
        2) fallback: procura o primeiro número válido após a alíquota (a partir do idx 4)
        """
        cred = self._get_decimal(dados, self.IDX_CREDITO)
        if cred is not None:
            return cred

        # fallback conservador (não quebra em layouts antigos)
        for i in range(4, min(len(dados), 12)):
            try:
                v = self.dec_br(dados[i])
            except Exception:
                continue
            # aceita inclusive 0, mas só retorna se campo existir e for parseável
            if v is not None:
                return v

        return None

    def aplicar(self, registro: RegistroFiscalDTO):
        if registro.reg != "M100":
            return None

        dados: List[Any] = registro.dados or []

        base = self._get_decimal(dados, self.IDX_BASE)
        aliquota = self._get_decimal(dados, self.IDX_ALIQUOTA)
        credito = self._get_credito_m100(dados)

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