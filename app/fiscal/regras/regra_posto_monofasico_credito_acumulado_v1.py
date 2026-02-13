from decimal import Decimal, ROUND_HALF_UP
from typing import Optional
import logging
import os
from app.fiscal.dto import RegistroFiscalDTO
from app.fiscal.regras.achado import Achado
from app.fiscal.regras.base_regras import RegraBase

logger = logging.getLogger(__name__)


class RegraPostoMonofasicoCreditoAcumuladoV1(RegraBase):
    codigo = "POSTO_MONOF_CRED_ACUM_V1"
    nome = "Posto/Monofásico: crédito de insumos acumulado (não ressarcível)"
    tipo = "OPORTUNIDADE"

    # 🔧 liga via ENV: POSTO_MONOF_DEBUG=1
    debug: bool = (os.getenv("POSTO_MONOF_DEBUG", "").strip() in ("1", "true", "True", "yes", "sim", "SIM"))

    def aplicar(self, registro: RegistroFiscalDTO) -> Optional[Achado]:
        try:
            if (registro.reg or "").strip().upper() != "META_FISCAL":
                return None

            raw = registro.dados or []

            # META (vem do DTO META_FISCAL)
            meta_flags = {}
            if isinstance(raw, list) and raw and isinstance(raw[0], dict):
                meta_flags = raw[0].get("_meta") or {}

            perfil_monofasico = self.to_bool(meta_flags.get("perfil_monofasico"))
            score_monofasico = self.parse_int(meta_flags.get("score_monofasico"))

            if self.debug:
                logger.info("[POSTO_MONOF] perfil=%s score=%s meta=%s", perfil_monofasico, score_monofasico, meta_flags)

            if not perfil_monofasico:
                return None

            # Controles bloco 1
            tem_1200 = self.to_bool(meta_flags.get("tem_1200"))
            tem_1210 = self.to_bool(meta_flags.get("tem_1210"))
            tem_1700 = self.to_bool(meta_flags.get("tem_1700"))

            # Flags do Bloco M (se existirem no META_FISCAL)
            tem_apuracao_m = self.to_bool(meta_flags.get("tem_apuracao_m"))
            bloco_m_zerado = self.to_bool(meta_flags.get("bloco_m_zerado"))

            # Créditos apurados no Bloco M (se vierem zerados, vamos tratar abaixo)
            cred_pis = self.dec_any(meta_flags.get("credito_pis"))
            cred_cof = self.dec_any(meta_flags.get("credito_cofins"))
            impacto = (cred_pis + cred_cof).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)

            if self.debug:
                logger.info(
                    "[POSTO_MONOF] temM=%s m_zerado=%s pis=%s cof=%s impacto=%s 1200=%s 1210=%s 1700=%s",
                    tem_apuracao_m, bloco_m_zerado, cred_pis, cred_cof, impacto, tem_1200, tem_1210, tem_1700
                )

            # ✅ Se já existe controle de ressarcimento (1200/1210/1700), não dispara nada
            if tem_1200 or tem_1210 or tem_1700:
                return None

            # ✅ NOVO: perfil monofásico detectado, mas sem apuração/crédito no M => ALERTA (não “some”)
            if impacto <= 0:
                # Se vocês calculam Bloco M “separado”, isso aqui vai te avisar quando esse cálculo
                # ainda não gerou crédito / não foi injetado no META_FISCAL.
                desc = (
                    "Perfil monofásico (posto/combustíveis) detectado, porém não há crédito apurado "
                    "no Bloco M (crédito PIS/COFINS = 0). "
                    "Revisar se existe apuração/Bloco M no SPED ou se o cálculo/integração do Bloco M "
                    "separado ainda não foi refletido no META_FISCAL."
                )

                return Achado(
                    registro_id=int(getattr(registro, "id", 0) or 0),
                    tipo="ALERTA",
                    codigo="POSTO_MONOF_SEM_CRED_M_V1",
                    descricao=desc,
                    regra="Posto/Monofásico: perfil detectado sem crédito no M",
                    impacto_financeiro=None,
                    prioridade="ALTA",
                    meta={
                        "perfil_monofasico": True,
                        "score_monofasico": score_monofasico,
                        "tem_apuracao_m": bool(tem_apuracao_m),
                        "bloco_m_zerado": bool(bloco_m_zerado),
                        "credito_pis": str(cred_pis),
                        "credito_cofins": str(cred_cof),
                        "fonte_base": "META_FISCAL",
                        "empresa_id": registro.empresa_id,
                        "versao_id": registro.versao_id,
                        "tem_1200": tem_1200,
                        "tem_1210": tem_1210,
                        "tem_1700": tem_1700,
                    },
                )

            # Prioridade (quando existe crédito)
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
                tipo=self.tipo,
                codigo=self.codigo,
                descricao=desc,
                regra=self.nome,
                impacto_financeiro=impacto,  # ✅ mantém Decimal (seu Achado aceita Decimal)
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
                    "tem_apuracao_m": bool(tem_apuracao_m),
                    "bloco_m_zerado": bool(bloco_m_zerado),
                    "tem_1200": tem_1200,
                    "tem_1210": tem_1210,
                    "tem_1700": tem_1700,
                },
            )

        except Exception:
            logger.exception(
                "ERRO POSTO_MONOF_CRED_ACUM_V1 | reg=%s id=%s linha=%s empresa_id=%s versao_id=%s",
                getattr(registro, "reg", None),
                getattr(registro, "id", None),
                getattr(registro, "linha", None),
                getattr(registro, "empresa_id", None),
                getattr(registro, "versao_id", None),
            )
            return None