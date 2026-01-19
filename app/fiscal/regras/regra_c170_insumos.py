from __future__ import annotations
from typing import Optional, Dict, Any, List
from app.fiscal.dto import RegistroFiscalDTO

class RegraC170Insumos:
    """
    Regra MVP (C170):
    Heurística para identificar itens com indício de possível crédito (insumo).
    NÃO afirma direito creditório: apenas sinaliza para revisão humana.
    """

    codigo = "C170-IN"
    nome = "Possível crédito por insumo (C170)"
    tipo = "OPORTUNIDADE"

    CFOPS_COMPRA_PREFIXOS = ("1", "2")  # entradas
    CST_CREDITO = {"50", "51", "52", "53", "54", "55", "56"}

    def _find_cfop(self, dados: List[Any]) -> Optional[str]:
        # 1) se vier dict, tenta chave explícita
        if dados and isinstance(dados[0], dict):
            v = dados[0].get("cfop") or dados[0].get("CFOP")
            if v:
                s = str(v).strip()
                if len(s) == 4 and s.isdigit():
                    return s

        # 2) tenta fallback heurístico (seu código atual)
        return self._find_cfop_fallback(dados)

    def _find_cfop_fallback(self, dados: List[Any]) -> Optional[str]:
        for v in dados:
            s = (str(v).strip() if v is not None else "")
            if len(s) == 4 and s.isdigit() and s[0] in self.CFOPS_COMPRA_PREFIXOS:
                return s
        return None

    def _find_cst(self, dados: List[Any]) -> Optional[str]:
        return self._find_cst_fallback(dados)

    def _find_cst_fallback(self, dados: List[Any]) -> Optional[str]:
        for v in dados:
            s = (str(v).strip() if v is not None else "")
            if len(s) == 2 and s.isdigit() and s in self.CST_CREDITO:
                return s
        return None

    def aplicar(self, registro: RegistroFiscalDTO) -> Optional[Dict[str, Any]]:
        if registro.reg != "C170":
            return None

        dados = registro.dados or []
        cfop = self._find_cfop(registro.dados or [])
        cst = self._find_cst(registro.dados or [])

        if not cfop and not cst:
            return None

        prioridade = "MEDIA" if (cfop and cst) else "BAIXA"

        return {
            "tipo": self.tipo,
            "codigo": self.codigo,
            "prioridade": prioridade,
            "descricao": (
                "Item C170 com indício de possível crédito (heurística). "
                f"CFOP={cfop or 'N/D'}, CST={cst or 'N/D'}. "
                "Revisar natureza do item (insumo) e enquadramento."
            ),
            "impacto_financeiro": None,

            # ✅ obrigatório pro seu schema
            "registro_id": registro.id,
        }
