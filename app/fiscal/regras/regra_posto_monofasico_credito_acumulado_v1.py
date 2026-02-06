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
            if (registro.reg or "").strip() != "META_FISCAL":
                return None

            raw = registro.dados or []

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
                    if isinstance(value, bool):
                        return None
                    return int(str(value).strip())
                except Exception:
                    return None

            # -----------------------------
            # META (vem do DTO META_FISCAL)
            # -----------------------------
            meta_flags = {}
            if isinstance(raw, list) and raw and isinstance(raw[0], dict):
                meta_flags = raw[0].get("_meta") or {}

            perfil_monofasico = to_bool(meta_flags.get("perfil_monofasico"))
            score_monofasico = _parse_int(meta_flags.get("score_monofasico"))

            print(
                "[POSTO_MONOF] perfil_monofasico=",
                perfil_monofasico,
                "score=",
                score_monofasico,
            )

            # ✅ REGRA SÓ FAZ SENTIDO PARA PERFIL MONOFÁSICO
            if not perfil_monofasico:
                return None

            # -----------------------------
            # CRÉDITOS APURADOS NO BLOCO M
            # -----------------------------
            cred_pis = self.dec_any(meta_flags.get("credito_pis"))
            cred_cof = self.dec_any(meta_flags.get("credito_cofins"))

            impacto = (cred_pis + cred_cof).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)

            print(
                "[POSTO_MONOF] credito_pis=",
                cred_pis,
                "credito_cofins=",
                cred_cof,
                "impacto=",
                impacto,
            )

            if impacto <= 0:
                return None

            # -----------------------------
            # CONTROLES BLOCO 1
            # -----------------------------
            tem_1200 = to_bool(meta_flags.get("tem_1200"))
            tem_1210 = to_bool(meta_flags.get("tem_1210"))
            tem_1700 = to_bool(meta_flags.get("tem_1700"))

            print(
                "[POSTO_MONOF] tem_1200=",
                tem_1200,
                "tem_1210=",
                tem_1210,
                "tem_1700=",
                tem_1700,
            )

            # Se já existe controle de ressarcimento, não dispara
            if tem_1200 or tem_1210 or tem_1700:
                return None

            # -----------------------------
            # PRIORIDADE
            # -----------------------------
            if impacto >= Decimal("5000"):
                prioridade = "ALTA"
            elif impacto >= Decimal("1000"):
                prioridade = "MEDIA"
            else:
                prioridade = "BAIXA"

            desc = (
                "Perfil monofásico (posto/combustíveis) detectado. "
                "Foram apurados créditos de PIS/COFINS no Bloco M "
                f"(PIS R$ {self.fmt_br(cred_pis)}, COFINS R$ {self.fmt_br(cred_cof)}, "
                f"total R$ {self.fmt_br(impacto)}). "
                "Em operações monofásicas, esses créditos em regra não são passíveis "
                "de ressarcimento, sendo normalmente utilizados para abatimento "
                "de débitos próprios. "
                "Não há registros 1200/1210/1700 no Bloco 1."
            )

            return Achado(
                registro_id=int(getattr(registro, "id", 0) or 0),
                tipo="OPORTUNIDADE",
                codigo=self.codigo,
                descricao=desc,
                regra=self.nome,
                impacto_financeiro=impacto,
                prioridade=prioridade,
                meta={
                    "perfil_monofasico": True,
                    "score_monofasico": score_monofasico,
                    "credito_pis": str(cred_pis),
                    "credito_cofins": str(cred_cof),
                    "impacto_consolidado": str(impacto),
                    "fonte_base": "META_FISCAL",
                    "empresa_id": registro.empresa_id,
                    "versao_id": registro.versao_id,
                    "tem_1200": tem_1200,
                    "tem_1210": tem_1210,
                    "tem_1700": tem_1700,
                },
            )

        except Exception as e:
            print("❌ ERRO POSTO_MONOF_CRED_ACUM_V1:", repr(e))
            return None
