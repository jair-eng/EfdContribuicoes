from decimal import Decimal, ROUND_HALF_UP
from typing import Optional, Dict, Any, List
from app.fiscal.dto import RegistroFiscalDTO

class RegraF600Insumos:
    """
    Regra F600 (EFD Contribuições)
    Detecta possível crédito não aproveitado sobre insumos.
    Heurística segura:
      - Existe base de cálculo
      - Crédito informado = 0 ou vazio
    """

    codigo = "F600-IN"
    nome = "Possível crédito não aproveitado (F600)"
    tipo = "OPORTUNIDADE"

    def _to_decimal(self, s: Optional[str]) -> Optional[Decimal]:
        if not s:
            return None
        txt = str(s).strip().replace(".", "").replace(",", ".")
        try:
            return Decimal(txt)
        except Exception:
            return None

    def aplicar(self, registro: RegistroFiscalDTO) -> Optional[Dict[str, Any]]:
        if registro.reg != "F600":
            return None

        dados: List[Any] = registro.dados or []

        # ⚠️ Ajuste os índices conforme seu layout real
        base = self._to_decimal(dados[5]) if len(dados) > 5 else None
        credito = self._to_decimal(dados[7]) if len(dados) > 7 else None

        # Precisa ter base positiva
        if base is None or base <= 0:
            return None

        # Se já tem crédito, não é oportunidade
        if credito is not None and credito > 0:
            return None

        # Estimativa conservadora (PIS + COFINS não cumulativos)
        aliquota = Decimal("0.0925")
        impacto = (base * aliquota).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)

        # Se impacto for irrelevante, ignora (ruído)
        if impacto < Decimal("50"):
            return None

        return {
            "tipo": self.tipo,
            "codigo": self.codigo,
            "descricao": (
                f"F600 com base {base} e crédito zero. "
                "Possível crédito de PIS/COFINS sobre insumos não aproveitado."
            ),
            "impacto_financeiro": impacto,
            "regra": self.nome,
            "registro_id": registro.id,
        }
