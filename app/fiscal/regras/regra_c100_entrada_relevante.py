from __future__ import annotations
from __future__ import annotations
from decimal import Decimal, ROUND_HALF_UP
from typing import Any, List, Optional
from app.config.settings import ALIQUOTA_PIS, ALIQUOTA_COFINS, ALIQUOTA_TOTAL
from app.fiscal.dto import RegistroFiscalDTO
from app.fiscal.regras.achado import Achado
from app.fiscal.regras.base_regras import RegraBase


class RegraC100EntradaRelevante(RegraBase):
    """
    C100 (SPED Fiscal - ICMS/IPI) — layout observado:
      dados[1]  = IND_OPER (0 = entrada)
      dados[6]  = NUM_DOC
      dados[7]  = CHV_NFE
      dados[10] = VL_DOC (observado)
      dados[14] = VL_DOC (fallback observado)

    Heurística: notas de entrada com VL_DOC alto.
    Impacto estimado (PIS+COFINS) — sujeito à validação fiscal.
    """

    codigo = "C100-ENT"
    nome = "Nota de entrada relevante (C100) para revisão"
    tipo = "OPORTUNIDADE"

    # filtro MVP (ajuste depois)
    VL_DOC_MIN = Decimal("50000")



    def aplicar(self, registro: RegistroFiscalDTO) -> Optional[Achado]:
        if registro.reg != "C100":
            return None

        dados: List[Any] = registro.dados or []

        ind_oper = str(dados[1]).strip() if len(dados) > 1 and dados[1] is not None else None
        if ind_oper != "0":  # só entradas
            return None

        vl_doc = self.dec_br(dados[10])  if len(dados) > 10 else None
        if (vl_doc is None or vl_doc <= 0) and len(dados) > 14:
            vl_doc = self.dec_br(dados[10])

        if vl_doc is None or vl_doc <= 0:
            return None

        # evita ruído
        if vl_doc < self.VL_DOC_MIN:
            return None

        # base e impacto
        base = vl_doc
        impacto = self.q2(base * ALIQUOTA_TOTAL)

        num_doc = str(dados[6]).strip() if len(dados) > 6 and dados[6] is not None else None
        chave = str(dados[7]).strip() if len(dados) > 7 and dados[7] is not None else None
        modelo = str(dados[3]).strip() if len(dados) > 3 and dados[3] is not None else None
        serie = str(dados[5]).strip() if len(dados) > 5 and dados[5] is not None else None

        # formatação pt-BR
        vl_doc_fmt = self.fmt_br(self.q2(base))
        impacto_fmt = self.fmt_br(impacto)

        return Achado(
            registro_id=int(registro.id),
            tipo=self.tipo,
            codigo=self.codigo,
            descricao=(
                f"C100 entrada VL_DOC=R$ {vl_doc_fmt} (impacto est. R$ {impacto_fmt}). "
                f"Modelo={modelo or '-'} Série={serie or '-'} Nº={num_doc or '-'}. "
                f"{('Chave=' + chave + '. ') if chave else ''}"
                f"Validar potencial de crédito."
                        ),
            impacto_financeiro=float(impacto),  # ✅ mantém padrão do banco/UI
            regra=self.nome,
            meta={
                "ind_oper": ind_oper,
                "vl_doc": str(vl_doc),
                "impacto": str(impacto),
                "num_doc": num_doc,
                "chave_nfe": chave,
                "linha": int(registro.linha),
            },
        )
