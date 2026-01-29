from decimal import Decimal, ROUND_HALF_UP
from app.config.settings import IND_TORRADO_ALIQUOTA_EFETIVA  # você já queria no settings
from app.fiscal.dto import RegistroFiscalDTO
from app.fiscal.regras.base_regras import RegraBase
import logging

# Opcional: Configurar o logger para este módulo específico
logger = logging.getLogger(__name__)


class RegraIndustrializacaoTorradoV1(RegraBase):
    codigo = "IND_TORRADO_V1"
    tipo = "OPORTUNIDADE"
    prioridade = "MEDIA"
    nome = "Industrialização (Torrado)"

    def aplicar(self, registro: RegistroFiscalDTO):

        # 🛡️ Se por algum motivo o agregador for marcado como PF, ignore
        if getattr(registro, 'is_pf', False):
            return None
        reg = (registro.reg or "").strip()
        if reg not in ("C190_IND_TORRADO_AGG", "C170_IND_TORRADO_AGG"):
            return None

        dados = registro.dados or []
        if not dados or not isinstance(dados, list):
            return None

        meta = {}
        if isinstance(dados[0], dict) and "_meta" in dados[0]:
            meta = dados[0].get("_meta") or {}

        base_total = Decimal("0")
        qtd = 0

        for it in dados[1:]:
            if not isinstance(it, dict):
                continue
            valor_str = it.get("vl_opr")
            rid = it.get("registro_id")  # Para debug se necessário

            try:
                base_total += self.dec_br(valor_str)
                qtd += 1
            except Exception as e:
                logger.error(f"Erro ao converter valor no registro {rid}: {e}")

        cred = (base_total * IND_TORRADO_ALIQUOTA_EFETIVA).quantize(
            Decimal("0.01"), rounding=ROUND_HALF_UP
        )

        aliquota_pct = (IND_TORRADO_ALIQUOTA_EFETIVA * Decimal("100")).quantize(Decimal("0.01"))

        flags = dict(meta) if isinstance(meta, dict) else {}
        flags.update({
            "fonte_base": reg,
            "qtd_itens": qtd,
            "aliquota_efetiva": f"{self.fmt_br(aliquota_pct)}%",
            "base_compra_cafe_verde": str(base_total),
            "credito_estimado_total": str(cred),
        })

        return {
            "registro_id": int(registro.id),
            "tipo": self.tipo,
            "codigo": self.codigo,
            "descricao": (
                "Possível crédito estimado (industrialização do café torrado): "
                "base em compras (entradas) x alíquota efetiva. Validar enquadramento/insumo."
            ),
            "impacto_financeiro": cred,
            "prioridade": self.prioridade,
            "meta": flags,
            "regra": self.nome,
        }
