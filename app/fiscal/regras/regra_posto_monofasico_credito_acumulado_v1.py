from decimal import Decimal, ROUND_HALF_UP
from typing import Optional, Any, Dict
from app.fiscal.dto import RegistroFiscalDTO
from app.fiscal.regras.achado import Achado
from app.fiscal.regras.base_regras import RegraBase
import logging
from typing import Optional

logger = logging.getLogger(__name__)


class RegraPostoMonofasicoCreditoAcumuladoV1(RegraBase):
    codigo = "POSTO_MONOF_CRED_ACUM_V1"
    nome = "Posto/Monofásico: crédito de insumos acumulado (não ressarcível)"
    tipo = "OPORTUNIDADE"

    def aplicar(self, registro: RegistroFiscalDTO) -> Optional[Achado]:
        try:
            # 1) pega meta de forma determinística: precisa existir _meta no primeiro item
            raw = registro.dados or []
            meta: Dict[str, Any] = {}

            # -----------------------------
            # helpers
            # -----------------------------
            def to_bool(v) -> bool:
                if v is None:
                    return False
                if isinstance(v, bool):
                    return v
                if isinstance(v, (int, float)):
                    return v != 0
                if isinstance(v, str):
                    s = v.strip().lower()
                    if s in ("1", "true", "t", "yes", "y", "sim", "s"):
                        return True
                    if s in ("0", "false", "f", "no", "n", "nao", "não", ""):
                        return False
                return False

            def _parse_int(value) -> Optional[int]:
                try:
                    if value is None:
                        return None
                    if isinstance(value, bool):  # evita True/False virar 1/0
                        return None
                    return int(str(value).strip())
                except Exception:
                    return None
            # -----------------------------
            # regra
            # -----------------------------


            # padrão oficial do projeto
            if raw and isinstance(raw[0], dict) and "_meta" in raw[0]:
                meta_flags = raw[0].get("_meta") or {}

            perfil_monofasico = to_bool(meta_flags.get("perfil_monofasico"))
            score_monofasico = _parse_int(meta_flags.get("score_monofasico"))

            if perfil_monofasico:
                logger.info(
                    "POSTO_MONOF ignorada: perfil_monofasico=True (score=%s)",
                    score_monofasico
                )
                return None

            cred_pis = self.dec_any(meta.get("credito_pis"))
            cred_cof = self.dec_any(meta.get("credito_cofins"))

            impacto = (cred_pis + cred_cof).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
            if impacto <= 0:
                return None

            tem_1200 = to_bool(meta.get("tem_1200"))
            tem_1210 = to_bool(meta.get("tem_1210"))
            tem_1700 = to_bool(meta.get("tem_1700"))
            if tem_1200 or tem_1210 or tem_1700:
                # se já existe controle de ressarcimento, não dispara aqui
                return None

            # prioridade por impacto
            if impacto >= Decimal("5000"):
                prioridade = "ALTA"
            elif impacto >= Decimal("1000"):
                prioridade = "MEDIA"
            else:
                prioridade = "BAIXA"

            # ✅ desc sempre em PT-BR
            desc = (
                "Perfil monofásico (posto/combustíveis) detectado. "
                "Foram apurados créditos de PIS/COFINS sobre insumos no Bloco M "
                f"(PIS R$ {self.fmt_br(cred_pis)}, COFINS R$ {self.fmt_br(cred_cof)}, total R$ {self.fmt_br(impacto)}). "
                "Em operações monofásicas, esses créditos em regra não são passíveis de "
                "ressarcimento ou compensação administrativa, sendo usualmente utilizados para "
                "abatimento de débitos próprios. "
                "Não há 1200/1210/1700 no Bloco 1."
            )

            return Achado(
                registro_id=int(getattr(registro, "id", 0) or 0),  # mantém compatível com teu modelo
                tipo="OPORTUNIDADE",
                codigo=self.codigo,
                descricao=desc,
                regra=self.nome,
                impacto_financeiro=impacto,
                prioridade=prioridade,
                meta={
                    "perfil_monofasico": True,
                    "score_monofasico": meta.get("score_monofasico"),
                    # ✅ meta neutro (pra cálculo/CSV)
                    "credito_pis": str(cred_pis),          # "131.66"
                    "credito_cofins": str(cred_cof),
                    "impacto_consolidado": str(impacto),
                    "tem_1200": tem_1200,
                    "tem_1210": tem_1210,
                    "tem_1700": tem_1700,
                    "orientacao": [
                        "Confirmar origem do crédito (insumos/serviços).",
                        "Em monofásico, crédito não é ressarcível em regra; uso típico é abatimento de débitos próprios.",
                        "Se não há débitos suficientes, pode acumular para períodos futuros (conforme legislação/regime).",
                    ],
                },
            )

        except Exception:
            logger.exception("Erro na regra POSTO_MONOF_CRED_ACUM_V1")
            return None
