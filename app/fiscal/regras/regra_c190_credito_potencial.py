from __future__ import annotations
from __future__ import annotations
from decimal import Decimal
from typing import Any, List, Optional
from app.config.settings import  ALIQUOTA_TOTAL
from app.fiscal.contexto import dec_any
from app.fiscal.dto import RegistroFiscalDTO
from app.fiscal.regras.achado import Achado
import logging

from app.fiscal.regras.base_regras import RegraBase

logger = logging.getLogger(__name__)

class RegraC190CreditoPotencial(RegraBase):
    """
    Preferir C190_AGG (1 linha resumo).
    Desativa C190 detalhe (C190-ENT) por padrão para não poluir a UI.
    """

    # mantém nome/tipo
    nome = "Entrada relevante (C190) para revisão de crédito"
    tipo = "OPORTUNIDADE"

    # códigos
    CODIGO_SUM = "C190-SUM"
    CODIGO_DET = "C190-ENT"

    CFOP_ENTRADA_PREFIXOS = ("1", "2")
    VL_OPR_MIN = Decimal("1000")

    # por padrão: NÃO gera detalhe
    GERAR_DETALHE_C190 = False

    def aplicar(self, registro: RegistroFiscalDTO) -> Optional[Achado]:
        try:
            # =====================================================
            # 1) RESUMO via agregado (C190_AGG)
            # =====================================================
            if registro.reg == "C190_AGG":
                itens: List[Any] = registro.dados or []
                if not isinstance(itens, list) or not itens:
                    return None

                relevantes = []
                soma = Decimal("0")

                for it in itens:
                    if not isinstance(it, dict):
                        continue

                    cfop = str(it.get("cfop") or "").strip()
                    cst_icms = str(it.get("cst") or "").strip() or None
                    vl = dec_any(it.get("vl_opr"))

                    if len(cfop) != 4 or (not cfop.isdigit()):
                        continue
                    if cfop[0] not in self.CFOP_ENTRADA_PREFIXOS:
                        continue
                    if vl <= 0 or vl < self.VL_OPR_MIN:
                        continue

                    soma += vl
                    relevantes.append({"cfop": cfop, "cst": cst_icms, "vl": vl})

                if not relevantes:
                    return None

                impacto = self.q2(soma * ALIQUOTA_TOTAL)

                # ordena por valor desc
                relevantes.sort(key=lambda x: x["vl"], reverse=True)

                # pega top3 *únicos* (evita repetir o mesmo CFOP/CST/valor)
                vistos = set()
                top3 = []
                for t in relevantes:
                    k = (t.get("cfop"), t.get("cst"), str(self.q2(t.get("vl"))))
                    if k in vistos:
                        continue
                    vistos.add(k)
                    top3.append(t)
                    if len(top3) == 3:
                        break

                exemplos = "; ".join(
                    f"R$ {self.fmt_br(self.q2(t['vl']))} (CFOP={t['cfop']} CST={t['cst'] or 'N/D'})"
                    for t in top3
                )

                desc = (
                    f"C190 (resumo): {len(relevantes)} item(ns) de entrada relevantes "
                    f"(CFOP 1xxx/2xxx, VL_OPR ≥ R$ {self.fmt_br(self.VL_OPR_MIN)}). "
                    f"Soma VL_OPR=R$ {self.fmt_br(self.q2(soma))} "
                    f"(impacto est. R$ {self.fmt_br(impacto)}). "
                    f"Exemplos (top 3): {exemplos}. "
                    "Validar enquadramento."
                )

                return Achado(
                    registro_id=int(registro.id),
                    tipo=self.tipo,
                    codigo=self.CODIGO_SUM,
                    descricao=desc,
                    impacto_financeiro=self.money(impacto),
                    regra=self.nome,
                    meta={
                        "qtd": len(relevantes),
                        "soma_vl_opr": str(self.q2(soma)),
                        "impacto": str(impacto),
                        "aliquota_total": str(ALIQUOTA_TOTAL),
                        "linha": int(getattr(registro, "linha", 0) or 0),
                        "top3": [
                            {"cfop": t["cfop"], "cst": t["cst"], "vl_opr": str(self.q2(t["vl"]))}
                            for t in top3
                        ],
                    },
                )

            # =====================================================
            # 2) DETALHE via C190 (desligado por padrão)
            # =====================================================
            if registro.reg != "C190":
                return None
            if not self.GERAR_DETALHE_C190:
                return None

            # --- seu código atual de detalhe pode ficar aqui inteiro ---
            dados: List[Any] = registro.dados or []
            if not dados:
                return None

            cst_icms = str(dados[0]).strip() if len(dados) > 0 and dados[0] is not None else None
            cfop = str(dados[1]).strip() if len(dados) > 1 and dados[1] is not None else None
            vl_opr = dec_any(dados[3]) if len(dados) > 3 else Decimal("0")

            if not cfop or len(cfop) != 4 or not cfop.isdigit():
                return None
            if cfop[0] not in self.CFOP_ENTRADA_PREFIXOS:
                return None
            if vl_opr <= 0 or vl_opr < self.VL_OPR_MIN:
                return None

            impacto = self.q2(vl_opr * ALIQUOTA_TOTAL)

            desc = (
                f"C190 entrada CFOP={cfop} CST={cst_icms or 'N/D'} "
                f"VL_OPR=R$ {self.fmt_br(self.q2(vl_opr))} "
                f"(impacto est. R$ {self.fmt_br(impacto)}). "
                "Validar enquadramento."
            )

            return Achado(
                registro_id=int(registro.id),
                tipo=self.tipo,
                codigo=self.CODIGO_DET,
                descricao=desc,
                impacto_financeiro=self.money(impacto),
                regra=self.nome,
                meta={
                    "cfop": cfop,
                    "cst_icms": cst_icms,
                    "vl_opr": str(self.q2(vl_opr)),
                    "impacto": str(impacto),
                    "aliquota_total": str(ALIQUOTA_TOTAL),
                    "linha": int(getattr(registro, "linha", 0) or 0),
                },
            )

        except Exception:
            logger.exception("Erro na regra %s", getattr(self, "nome", "RegraC190CreditoPotencial"))
            return None