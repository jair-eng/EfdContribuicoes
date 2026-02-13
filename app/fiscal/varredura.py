from __future__ import annotations
from dataclasses import dataclass , field
from typing import List, Optional , Any , Dict
from app.fiscal.dto import RegistroFiscalDTO
from app.fiscal.regras.regra_cafe_c190v1 import RegraCafeC190V1
from app.fiscal.regras.regra_exportacao import RegraExportacaoRessarcimentoV1
from app.fiscal.regras.regra_f600_insumos import RegraF600Insumos
from app.fiscal.regras.regra_c170_insumos import RegraC170Insumos
from app.fiscal.regras.regra_c190_credito_potencial import RegraC190CreditoPotencial
from app.fiscal.regras.regra_ind_cafe_v1 import RegraIndustrializacaoCafeV1
from app.fiscal.regras.regra_industrializacao_graos_v1 import RegraIndustrializacaoAgroV1
from app.fiscal.regras.regra_m100_credito_pis import RegraM100CreditoPIS
from app.fiscal.regras.regra_m200_credito_cofins import RegraM200CreditoCOFINS
from app.fiscal.regras.regra_c100_entrada_relevante import RegraC100EntradaRelevante
from decimal import Decimal
from inspect import signature
from app.fiscal.regras.achado import Achado
from app.fiscal.regras.regra_posto_monofasico_credito_acumulado_v1 import RegraPostoMonofasicoCreditoAcumuladoV1
from app.fiscal.regras.regra_exp_m_zerado_v1 import RegraExportacaoBlocoMZeradoV1
from app.fiscal.regras.base_regras import aplicar_supressao_por_erros_dict, aplicar_bloqueio_por_grupo_dict, \
    aplicar_rebaixamento_por_presenca_dict, RegraBase

print("ACHADO_CLASS =", Achado)
print("ACHADO_MODULE =", Achado.__module__)
print("ACHADO_INIT =", signature(Achado.__init__))
REGRAS_ATIVAS = [
    RegraF600Insumos(),
    RegraC170Insumos(),
    RegraC190CreditoPotencial(),
    RegraM100CreditoPIS(),
    RegraM200CreditoCOFINS(),
    RegraC100EntradaRelevante(),
    RegraCafeC190V1(),
    RegraExportacaoRessarcimentoV1(),
    RegraPostoMonofasicoCreditoAcumuladoV1(),
    RegraExportacaoBlocoMZeradoV1(),
    RegraIndustrializacaoAgroV1(),
    RegraIndustrializacaoCafeV1(),

]

@dataclass(slots=True)
class ApontamentoDTO:
    # ---- obrigatórios (SEM default) ----
    registro_id: int
    tipo: str
    codigo: str
    descricao: str

    # ---- opcionais (COM default) ----
    impacto_financeiro: Optional[float] = None
    prioridade: Optional[str] = None
    meta: Dict[str, Any] = field(default_factory=dict)
    regra: Optional[str] = None

@dataclass(slots=True)
class ResultadoVarredura:
    apontamentos: List[ApontamentoDTO]
    erros: List[str]

def _safe_float(v: Any) -> Optional[float]:
    if v is None:
        return None
    if isinstance(v, (int, float)):
        return float(v)
    if isinstance(v, Decimal):
        return float(v)

    s = str(v).strip()
    if not s:
        return None

    # Se tiver vírgula, assume pt-BR: "1.234,56"
    if "," in s:
        s = s.replace(".", "").replace(",", ".")
    # Senão, assume padrão: "1234.56" (não remove ponto)

    try:
        return float(s)
    except Exception:
        return None


def _norm_codigo(c: Any) -> Optional[str]:
    if c is None:
        return None
    s = str(c).strip()
    return s or None



def executar_varredura(
    registros: List[RegistroFiscalDTO],
    *,
    capturar_erros: bool = True
) -> ResultadoVarredura:
    apontamentos: List[ApontamentoDTO] = []
    erros: List[str] = []

    achados_raw = []

    # ✅ Warm-up catálogo (1x por varredura)
    try:
        if registros:
            # pega um registro qualquer que tenha empresa_id
            r0 = next((r for r in registros if getattr(r, "empresa_id", None)), registros[0])
            # chama uma vez pra encher o cache por empresa_id
            RegraBase.get_catalogo(r0, db=None)
    except Exception:
        # não pode derrubar varredura por isso
        pass
    print("[DBG] REGRAS_ATIVAS:", [getattr(r, "codigo", r.__class__.__name__) for r in REGRAS_ATIVAS], flush=True)

    for registro in registros:
        for regra in REGRAS_ATIVAS:


            try:
                achado = regra.aplicar(registro)
                if not achado:
                    continue

                # aceita Achado (dataclass) OU dict (MVP)
                if hasattr(achado, "__dataclass_fields__") and achado.__class__.__name__ == "Achado":
                    raw_meta = getattr(achado, "meta", None) or {}
                    achado = {
                        "registro_id": achado.registro_id,
                        "tipo": achado.tipo,
                        "codigo": achado.codigo,
                        "descricao": achado.descricao,
                        "impacto_financeiro": achado.impacto_financeiro,
                        "prioridade": getattr(achado, "prioridade", None),
                        "meta": dict(raw_meta) if isinstance(raw_meta, dict) else {},
                        "regra": achado.regra or getattr(regra, "nome", None),
                    }

                # garante mínimos (para não guardar lixo)
                descricao = str(achado.get("descricao") or "").strip()
                if not descricao:
                    continue

                # normaliza campos básicos já aqui (facilita postprocess)
                achado["tipo"] = str(achado.get("tipo") or getattr(regra, "tipo", "OPORTUNIDADE"))
                achado["codigo"] = _norm_codigo(achado.get("codigo") or getattr(regra, "codigo", None))
                achado["regra"] = achado.get("regra") or getattr(regra, "nome", None)

                rid = achado.get("registro_id")
                achado["registro_id"] = int(rid) if rid is not None else int(registro.id)

                raw_meta = achado.get("meta") or {}
                achado["meta"] = dict(raw_meta) if isinstance(raw_meta, dict) else {}

                achados_raw.append(achado)

            except Exception as e:
                if capturar_erros:
                    erros.append(
                        f"{getattr(regra, 'codigo', regra.__class__.__name__)} "
                        f"(reg={registro.reg} linha={registro.linha} id={registro.id}): {e}"
                    )
                else:
                    raise
    print("CÓDIGOS antes:", sorted({a.get("codigo") for a in achados_raw}), flush=True)
    # ✅ pós-processamento 1 (bloqueio/supressão pontual por código)
    achados_raw = aplicar_supressao_por_erros_dict(achados_raw)
    print("CÓDIGOS depois:", sorted({a.get("codigo") for a in achados_raw}), flush=True)

    # ✅ pós-processamento 1.1 (rebaixamento por presença — evita poluição)
    achados_raw = aplicar_rebaixamento_por_presenca_dict(
        achados_raw,
        se_existe="CAFE_C190_V1",
        rebaixar=["IND_AGRO_V1"],
        prioridade_alvo="BAIXA",
    )

    # ✅ pós-processamento 2 (agrupamento + erro crítico bloqueia/rebaixa oportunidades do grupo)
    achados_raw = aplicar_bloqueio_por_grupo_dict(
        achados_raw,
        erros_criticos=("EXP_M_ZERADO_V1",),
        rebaixar_prioridade=True,
    )

    # ✅ agora converte para DTO
    for achado in achados_raw:
        impacto = _safe_float(achado.get("impacto_financeiro"))

        apontamentos.append(
            ApontamentoDTO(
                registro_id=int(achado["registro_id"]),
                tipo=str(achado.get("tipo")),
                codigo=str(achado.get("codigo")),
                descricao=str(achado.get("descricao")),
                impacto_financeiro=impacto,
                prioridade=achado.get("prioridade"),
                meta=achado.get("meta") or {},
                regra=achado.get("regra"),
            )
        )

    return ResultadoVarredura(apontamentos=apontamentos, erros=erros)
