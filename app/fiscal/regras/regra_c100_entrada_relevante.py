from collections import defaultdict
from decimal import Decimal
from typing import Any, List, Optional

from app.config.settings import ALIQUOTA_TOTAL
from app.fiscal.dto import RegistroFiscalDTO
from app.fiscal.regras.achado import Achado
from app.fiscal.regras.base_regras import RegraBase


class RegraC100EntradaRelevante(RegraBase):
    codigo = "C100-ENT"
    nome = "Resumo: entradas relevantes (C100) para revisão"
    tipo = "OPORTUNIDADE"
    VL_DOC_MIN = Decimal("50000")

    def aplicar(self, registro: RegistroFiscalDTO) -> Optional[Achado]:
        # ✅ agora só no agregado
        if (registro.reg or "").strip().upper() != "C100_ENT_AGG":
            return None

        if getattr(registro, "is_pf", False):
            return None

        raw: List[Any] = registro.dados or []
        if not raw or not isinstance(raw[0], dict):
            return None

        meta = raw[0].get("_meta") or {}
        itens = raw[1:]
        if not itens:
            return None

        # soma e top
        total_vl = Decimal("0")
        top = []
        for it in itens:
            if not isinstance(it, dict):
                continue
            v = self.dec_br(it.get("vl_doc")) or Decimal("0")
            if v <= 0:
                continue
            total_vl += v
            top.append((v, it))

        if total_vl <= 0:
            return None

        impacto = self.q2(total_vl * ALIQUOTA_TOTAL)

        # âncora real pra FK
        rid = int(getattr(registro, "id", 0) or 0)
        anchor_rid = meta.get("anchor_registro_id")
        try:
            anchor_rid = int(anchor_rid) if anchor_rid is not None else None
        except Exception:
            anchor_rid = None
        registro_id_final = rid if rid > 0 else (anchor_rid or 0)

        qtd_total = int(meta.get("qtd_total") or len(top))
        top_n = int(meta.get("top_n") or 30)

        # monta descrição com 3 maiores
        top_sorted = sorted(top, key=lambda x: x[0], reverse=True)[:3]
        parts = []
        for v, it in top_sorted:
            parts.append(
                f"R$ {self.fmt_br(self.q2(v))} (Mod={it.get('modelo') or '-'} Série={it.get('serie') or '-'} Nº={it.get('num_doc') or '-'})"
            )
        exemplos = "; ".join(parts) if parts else "-"

        return Achado(
            registro_id=int(registro_id_final),
            tipo=self.tipo,
            codigo=self.codigo,
            descricao=(
                f"Entradas relevantes detectadas: {qtd_total} nota(s) com VL_DOC >= R$ {self.fmt_br(self.VL_DOC_MIN)}. "
                f"Soma VL_DOC=R$ {self.fmt_br(self.q2(total_vl))} (impacto est. R$ {self.fmt_br(impacto)}). "
                f"Exemplos (top 3): {exemplos}. "
                f"Revise as maiores e confirme potencial de crédito."
            ),
            impacto_financeiro=float(impacto),
            regra=self.nome,
            meta={
                "fonte_base": "C100_ENT_AGG",
                "qtd_total": qtd_total,
                "top_n": top_n,
                "vl_doc_min": str(self.VL_DOC_MIN),
                "vl_doc_total": str(self.q2(total_vl)),
                "impacto_total": str(impacto),
                "linha": int(getattr(registro, "linha", 0) or 0),
                "amostra_top": [
                    {
                        "vl_doc": str(self.q2(v)),
                        "modelo": it.get("modelo"),
                        "serie": it.get("serie"),
                        "num_doc": it.get("num_doc"),
                        "chave_nfe": it.get("chave_nfe"),
                        "linha": it.get("linha"),
                        "registro_id": it.get("registro_id"),
                    }
                    for v, it in sorted(top, key=lambda x: x[0], reverse=True)[:top_n]
                ],
            },
        )
