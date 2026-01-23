from abc import ABC, abstractmethod
from typing import Optional, List, Dict, Set, Tuple, Any
from app.fiscal.dto import RegistroFiscalDTO
from decimal import Decimal, ROUND_HALF_UP
from .achado import Achado


class RegraBase(ABC):
    codigo: str
    nome: str
    tipo: str

    @abstractmethod
    def aplicar(self, registro: RegistroFiscalDTO) -> Optional[Achado]:
        """
        Retorna um Achado ou None.
        Nunca lança exceção.
        """

    @staticmethod
    def dec_br(v) -> Decimal | None:
        if v is None:
            return None
        s = str(v).strip()
        if not s:
            return None
        s = s.replace(".", "").replace(",", ".")
        try:
            return Decimal(s)
        except Exception:
            return None

    @staticmethod
    def to_bool(v) -> bool:
        """
        Converte valores “soltos” (bool/int/str) para booleano real.

        Regras:
          - None / "" -> False
          - bool -> o próprio
          - int/float -> != 0
          - str: aceita variações pt/en:
              True:  "1","true","t","yes","y","sim","s","on"
              False: "0","false","f","no","n","nao","não","off",""
          - Qualquer outra coisa -> False (fail-safe)
        """
        if v is None:
            return False
        if isinstance(v, bool):
            return v
        if isinstance(v, (int, float)):
            return v != 0
        if isinstance(v, str):
            s = v.strip().lower()
            if s in ("1", "true", "t", "yes", "y", "sim", "s", "on"):
                return True
            if s in ("0", "false", "f", "no", "n", "nao", "não", "off", ""):
                return False
        return False

    @staticmethod
    def money(d: Decimal | None) -> float | None:
        if d is None:
            return None
        return float(d.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP))

    @staticmethod
    def dec_any(v) -> Decimal:
        """
        Aceita:
          - '131,66' (pt-BR)
          - '131.66' (en-US)
          - '1.234,56' (pt-BR)
          - '1,234.56' (en-US)
        Retorna Decimal seguro.
        """
        s = str(v or "").strip()
        if not s:
            return Decimal("0")

        if "," in s and "." in s:
            # o último separador define o decimal
            if s.rfind(",") > s.rfind("."):
                # 1.234,56
                s = s.replace(".", "").replace(",", ".")
            else:
                # 1,234.56
                s = s.replace(",", "")
            return Decimal(s)

        if "," in s:
            # 1234,56
            s = s.replace(".", "").replace(",", ".")
            return Decimal(s)

        # 1234.56 ou 1234
        return Decimal(s)

    @staticmethod
    def fmt_br(d: Decimal) -> str:
        """
        1234.56 -> '1.234,56'
        """
        d = d if isinstance(d, Decimal) else Decimal(str(d or "0"))
        return f"{d:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")

    @staticmethod
    def q2(d: Decimal | None) -> Decimal:
        d = d if isinstance(d, Decimal) else Decimal(str(d or "0"))
        return d.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)

    @staticmethod
    def br_num(d: Decimal | None) -> str:
        """
        Decimal -> '1234567,89' (sem separador de milhar)
        """
        d2 = RegraBase.q2(d)
        return str(d2).replace(".", ",")

    @staticmethod
    def pct(d: Decimal | None) -> str:
        """
        Decimal('0.0165') -> '1,65%'
        """
        d = d if isinstance(d, Decimal) else Decimal(str(d or "0"))
        return RegraBase.br_num(d * Decimal("100")) + "%"

def _rebaixar_prioridade(p: str | None) -> str | None:
    if p == "ALTA":
        return "MEDIA"
    if p == "MEDIA":
        return "BAIXA"
    return p


# Mapeia: ERRO -> quais oportunidades ele deve rebaixar
SUPRESSAO_MAP: Dict[str, Set[str]] = {
    "EXP_M_ZERADO_V1": {"EXP_RESSARC_V1"},
    # no futuro:
    # "EXP_INCONSIST_1200_V1": {"EXP_RESSARC_V1"},
}


def aplicar_supressao_por_erros_dict(achados: List[Dict]) -> List[Dict]:
    """
    Pós-processamento em dict (compatível com varredura MVP):
    - Se existir ERRO 'EXP_M_ZERADO_V1', marca 'EXP_RESSARC_V1' como bloqueada.
    - Não rebaixa prioridade (conforme sua decisão).
    """

    erros = {str(a.get("codigo") or "") for a in achados if str(a.get("tipo")) == "ERRO"}

    # direcionado: ERRO -> alvo(s)
    if "EXP_M_ZERADO_V1" not in erros:
        return achados

    for a in achados:
        if str(a.get("tipo")) != "OPORTUNIDADE":
            continue
        if str(a.get("codigo")) != "EXP_RESSARC_V1":
            continue

        meta = a.get("meta") or {}
        if not isinstance(meta, dict):
            meta = {}

        meta["bloqueada_por_erro"] = True
        meta["erro_critico_codigo"] = "EXP_M_ZERADO_V1"
        meta["bloqueio_motivo"] = "Erro crítico de consistência no Bloco M impede ação"

        a["meta"] = meta

    return achados


# Exemplo de prioridade (ajuste para seu padrão)
# Se no seu projeto prioridade for "ALTA/MEDIA/BAIXA" string, mantém assim.
PRIORIDADE_ORDEM = {"ALTA": 3, "MEDIA": 2, "BAIXA": 1, None: 0}

def _rebaixar_para_baixa(prio: Optional[str]) -> str:
    # sempre devolve string
    return "BAIXA"

def aplicar_rebaixamento_por_presenca_dict(
    achados: List[Dict[str, Any]],
    *,
    se_existe: str,
    rebaixar: List[str],
    prioridade_alvo: str = "BAIXA",
) -> List[Dict[str, Any]]:
    presentes = {str(a.get("codigo") or "").strip() for a in achados}
    if se_existe not in presentes:
        return achados

    rebaixar_set = set(rebaixar)

    for a in achados:
        codigo = str(a.get("codigo") or "").strip()
        if codigo in rebaixar_set:
            a["prioridade"] = prioridade_alvo
            meta = a.get("meta") or {}
            if isinstance(meta, dict):
                meta["rebaixado_por"] = se_existe
                a["meta"] = meta

    return achados

def _mapa_grupo_por_codigo(codigo: str) -> Optional[str]:
    """
    Fallback: se a regra ainda não setou meta['grupo'],
    a gente infere por codigo.
    """
    if not codigo:
        return None
    if codigo.startswith("EXP_"):
        return "EXPORTACAO"
    if codigo.startswith("POSTO_") or "MONOF" in codigo:
        return "POSTO_MONOFASICO"
    if codigo.startswith("CAFE_"):
        return "CAFE"
    return None

def aplicar_bloqueio_por_grupo_dict(
    achados: List[Dict],
    *,
    # quais erros são críticos e bloqueiam o grupo
    erros_criticos: Tuple[str, ...] = ("EXP_M_ZERADO_V1",),
    # se True, rebaixa oportunidades do grupo
    rebaixar_prioridade: bool = True,
) -> List[Dict]:
    """
    Pós-processamento:
    - identifica grupos que possuem ERRO crítico
    - marca oportunidades do mesmo grupo como bloqueadas
    - opcionalmente rebaixa prioridade para BAIXA
    """

    # 1) Mapeia grupo -> codigo do erro crítico que ocorreu
    grupo_para_erro: Dict[str, str] = {}

    for a in achados:
        if str(a.get("tipo")) != "ERRO":
            continue

        cod = str(a.get("codigo") or "")
        if cod not in erros_criticos:
            continue

        meta = a.get("meta") or {}
        grupo = None
        if isinstance(meta, dict):
            grupo = meta.get("grupo")

        if not grupo:
            grupo = _mapa_grupo_por_codigo(cod)

        if grupo:
            # se tiver mais de um erro no mesmo grupo, guarda o primeiro (ou o mais crítico, se quiser evoluir)
            grupo_para_erro.setdefault(str(grupo), cod)

    if not grupo_para_erro:
        return achados

    # 2) Marca oportunidades do mesmo grupo
    for a in achados:
        if str(a.get("tipo")) != "OPORTUNIDADE":
            continue

        meta = a.get("meta") or {}
        if not isinstance(meta, dict):
            meta = {}

        grupo = meta.get("grupo")
        if not grupo:
            grupo = _mapa_grupo_por_codigo(str(a.get("codigo") or ""))

        if not grupo:
            continue

        grupo = str(grupo)
        erro_causador = grupo_para_erro.get(grupo)
        if not erro_causador:
            continue

        meta["bloqueada_por_erro"] = True
        meta["erro_critico_codigo"] = erro_causador
        meta["bloqueio_motivo"] = "erro_critico_no_grupo"
        meta["grupo"] = grupo  # garante que fica persistido

        a["meta"] = meta

        if rebaixar_prioridade:
            a["prioridade"] = _rebaixar_para_baixa(a.get("prioridade"))

    return achados