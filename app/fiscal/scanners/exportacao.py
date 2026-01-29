from typing import Sequence, List, Dict, Any, Optional
from app.db.models import EfdRegistro
from app.fiscal.dto import RegistroFiscalDTO
from decimal import Decimal
from typing import Sequence, Tuple




def _dec_br(v) -> Decimal:
    try:
        s = str(v or "").strip()
        if not s:
            return Decimal("0")
        # aceita "1.234,56" e "1234.56"
        s = s.replace(".", "").replace(",", ".")
        return Decimal(s)
    except Exception:
        return Decimal("0")

def coletar_flags_bloco_m(registros_db: Sequence["EfdRegistro"]) -> Dict[str, Any]:
    """
    MVP/Heurística: detecta presença de registros chave do Bloco M e se parece zerado.
    Não depende de layout exato: soma vários campos numéricos comuns.
    """
    tem_m200 = tem_m205 = tem_m600 = tem_m605 = False
    soma = Decimal("0")

    for r in registros_db:
        reg = (r.reg or "").strip()
        if reg not in ("M200", "M205", "M600", "M605"):
            continue

        if reg == "M200": tem_m200 = True
        elif reg == "M205": tem_m205 = True
        elif reg == "M600": tem_m600 = True
        elif reg == "M605": tem_m605 = True

        dados = (r.conteudo_json or {}).get("dados") or []

        # soma campos numéricos desde o início (dados[0] já é o primeiro valor do registro)
        # limita pra evitar pegar campos texto caso existam em layouts diferentes
        for idx in range(0, min(len(dados), 12)):
            v = _dec_br(dados[idx])
            if v is not None:
                soma += v

    tem_apuracao_m = tem_m200 or tem_m205 or tem_m600 or tem_m605
    return {
        "tem_m200": tem_m200,
        "tem_m205": tem_m205,
        "tem_m600": tem_m600,
        "tem_m605": tem_m605,
        "tem_apuracao_m": tem_apuracao_m,
        "soma_valores_bloco_m": str(soma),
        # "zerado" só faz sentido se o bloco existir
        "bloco_m_zerado": bool(tem_apuracao_m and soma == Decimal("0")),
    }

def detectar_perfil_monofasico(registros_db: Sequence["EfdRegistro"]) -> Tuple[bool, int]:
    """
    Detecta perfil monofásico (posto/combustíveis) por evidências combinadas.
    Retorna (bool, score 0-100).

    Evidências:
      A) 0200/NCM (forte)
      B) C170/CFOP típicos de posto (médio)
      C) 0200/descrição contendo palavras de combustível (médio)
    """
    score = 0

    # A) 0200 - NCM forte
    ncm_hits = 0
    for r in registros_db:
        if (r.reg or "").strip() != "0200":
            continue
        dados = (r.conteudo_json or {}).get("dados") or []

        # tenta posição clássica 7, mas faz fallback procurando um campo com 8 dígitos
        ncm = ""
        if len(dados) > 7:
            ncm = str(dados[7] or "").strip()
        if not ncm:
            for v in dados:
                vv = str(v or "").strip()
                if vv.isdigit() and len(vv) in (8, 10):
                    ncm = vv[:8]
                    break

        if ncm.startswith(("2710", "2711", "2207")):
            ncm_hits += 1

        # C) descrição
        desc = str(dados[1] or "").strip().upper() if len(dados) > 1 else ""
        if desc and any(k in desc for k in ("GASOL", "DIESEL", "ETANOL", "ALCOOL", "COMBUST", "GNV")):
            score += 10

    if ncm_hits >= 1:
        score += 45
    if ncm_hits >= 2:
        score += 20  # bônus

    # B) CFOP típico (posto costuma ter muitas entradas 1652/1403)
    cfop_hits = 0
    for r in registros_db:
        reg = (r.reg or "").strip()
        if reg == "C170":
            dados = (r.conteudo_json or {}).get("dados") or []
            cfop = str(dados[9] or "").strip() if len(dados) > 9 else ""
            if cfop in ("1652", "1403"):
                cfop_hits += 1
        elif reg == "C190":
            dados = (r.conteudo_json or {}).get("dados") or []
            cfop = str(dados[1] or "").strip() if len(dados) > 1 else ""
            if cfop in ("1652", "1403"):
                cfop_hits += 1

    if cfop_hits >= 5:
        score += 35
    elif cfop_hits >= 2:
        score += 20
    elif cfop_hits >= 1:
        score += 10

    score = min(100, int(score))
    return (score >= 60), score

def montar_c190_export_agg(registros_db: Sequence[EfdRegistro]) -> Optional[RegistroFiscalDTO]:
    # flags bloco 1 (ressarcimento)
    regs_presentes = {(r.reg or "").strip() for r in registros_db}
    flags = {
        "tem_1200": "1200" in regs_presentes,
        "tem_1210": "1210" in regs_presentes,
        "tem_1700": "1700" in regs_presentes,
    }

    # ✅ flags bloco M (apuração/saldos)
    flags.update(coletar_flags_bloco_m(registros_db))

    c190s = [r for r in registros_db if (r.reg or "").strip() == "C190"]
    if not c190s:
        return None

    # filtra CFOP 7xxx (exportação)
    itens: List[Dict[str, Any]] = []
    anchor = None

    for r in c190s:
        dados = (r.conteudo_json or {}).get("dados") or []
        if len(dados) < 4:
            continue

        cfop = str(dados[1] or "").strip()
        if not cfop.startswith("7"):
            continue



        if anchor is None or int(r.id) < int(anchor.id):
            anchor = r

        itens.append({
            "cst_icms": str(dados[0] or "").strip(),
            "cfop": cfop,
            "vl_opr": dados[3],  # vem "1.234,56" -> regra converte
            "registro_id": r.id,  # Adicione isso para auditoria na regra
        })

    if not itens or anchor is None:
        return None

    # perfil monofásico
    flags.update(coletar_creditos_bloco_m(registros_db))

    perfil_monofasico, score_monofasico = detectar_perfil_monofasico(registros_db)
    flags["perfil_monofasico"] = perfil_monofasico
    flags["score_monofasico"] = score_monofasico

    # fonte
    flags["fonte"] = "C190"

    dados_final: List[Any] = [{"_meta": flags}] + itens

    return RegistroFiscalDTO(
        id=int(anchor.id),
        reg="C190_EXP_AGG",
        linha=int(anchor.linha),
        dados=dados_final,
    )
def montar_c170_export_agg(registros_db: Sequence[EfdRegistro]) -> Optional[RegistroFiscalDTO]:
    """
    Agregador de exportação baseado no C170.
    Usado quando não há C190.
    - CFOP: dados[9]
    - VL_ITEM (base): dados[5]
    """

    # Flags Bloco 1 (ressarcimento)
    regs_presentes = {(r.reg or "").strip() for r in registros_db}
    flags = {
        "tem_1200": "1200" in regs_presentes,
        "tem_1210": "1210" in regs_presentes,
        "tem_1700": "1700" in regs_presentes,
    }

    # Flags Bloco M (apuração)
    flags.update(coletar_flags_bloco_m(registros_db))

    # Seleciona C170
    c170s = [r for r in registros_db if (r.reg or "").strip() == "C170"]
    if not c170s:
        return None

    itens: List[Dict[str, Any]] = []
    anchor: Optional[EfdRegistro] = None

    for r in c170s:
        dados = (r.conteudo_json or {}).get("dados") or []
        if len(dados) < 10:
            continue

        cfop = str(dados[9] or "").strip()
        if not (len(cfop) == 4 and cfop.isdigit() and cfop.startswith("7")):
            continue

        vl_item = dados[5] if len(dados) > 5 else "0"

        if anchor is None or int(r.id) < int(anchor.id):
            anchor = r

        itens.append({
            "cfop": cfop,
            "vl_opr": vl_item,  # string "404,16" -> regra converte
            "registro_id": r.id,
            "pai_id": getattr(r, "pai_id", None)
        })

    if not itens or anchor is None:
        return None

    flags.update(coletar_creditos_bloco_m(registros_db))

    perfil_monofasico, score_monofasico = detectar_perfil_monofasico(registros_db)
    flags["perfil_monofasico"] = perfil_monofasico
    flags["score_monofasico"] = score_monofasico

    flags["fonte"] = "C170"
    dados_final: List[Any] = [{"_meta": flags}] + itens

    return RegistroFiscalDTO(
        id=int(anchor.id),
        reg="C170_EXP_AGG",
        linha=int(anchor.linha),
        dados=dados_final,
    )
def coletar_creditos_bloco_m(registros_db):
    """
    Heurística segura para coletar créditos apurados:
      - PIS: M100 (VL_CRED) e/ou M200 (VL_TOT_CONT_NC)
      - COFINS: M500 (VL_CRED) e/ou M600 (VL_TOT_CONT_NC)
    Retorna strings pra não quebrar JSON/meta.
    """
    cred_pis = Decimal("0")
    cred_cof = Decimal("0")

    def pick_max(dados, idxs):
        best = Decimal("0")
        for idx in idxs:
            if 0 <= idx < len(dados):
                v = _dec_br(dados[idx])
                if v > best:
                    best = v
        return best

    for r in registros_db:
        reg = (r.reg or "").strip()
        dados = (r.conteudo_json or {}).get("dados") or []

        # -------- PIS --------
        if reg == "M100":
            # índices comuns onde aparece VL_CRED no M100 (varia por leiaute),
            # mas geralmente fica perto do meio/final.
            # Tentamos alguns candidatos sem explodir.
            cred_pis = max(cred_pis, pick_max(dados, [7, 11, 14]))

        elif reg == "M200":
            # M200 normalmente começa com o total do período e repete no final
            cred_pis = max(cred_pis, pick_max(dados, [0, len(dados) - 1]))

        # -------- COFINS --------
        elif reg == "M500":
            cred_cof = max(cred_cof, pick_max(dados, [7, 11, 14]))

        elif reg == "M600":
            cred_cof = max(cred_cof, pick_max(dados, [0, len(dados) - 1]))

    return {
        "credito_pis": str(cred_pis),
        "credito_cofins": str(cred_cof),
    }
def montar_meta_fiscal(registros_db: Sequence[EfdRegistro]) -> Optional[RegistroFiscalDTO]:
    regs_presentes = {(r.reg or "").strip() for r in registros_db}
    flags: Dict[str, Any] = {
        "tem_1200": "1200" in regs_presentes,
        "tem_1210": "1210" in regs_presentes,
        "tem_1700": "1700" in regs_presentes,
    }

    # bloco M (presença/soma/zerado)
    flags.update(coletar_flags_bloco_m(registros_db))

    # créditos (PIS/COF)
    flags.update(coletar_creditos_bloco_m(registros_db))

    # perfil monofásico
    perfil_monofasico, score_monofasico = detectar_perfil_monofasico(registros_db)
    flags["perfil_monofasico"] = perfil_monofasico
    flags["score_monofasico"] = score_monofasico

    # ancora: menor id só pra ter registro_id estável
    anchor = min(registros_db, key=lambda r: int(r.id), default=None)
    if anchor is None:
        return None

    return RegistroFiscalDTO(
        id=int(anchor.id),
        reg="META_FISCAL",
        linha=int(anchor.linha or 1),
        dados=[{"_meta": flags}],
    )

def montar_c190_ind_torrado_agg(registros_db: Sequence[EfdRegistro]) -> Optional[RegistroFiscalDTO]:
    regs_presentes = {(r.reg or "").strip() for r in registros_db}
    flags = {
        "tem_1200": "1200" in regs_presentes,
        "tem_1210": "1210" in regs_presentes,
        "tem_1700": "1700" in regs_presentes,
    }
    flags.update(coletar_flags_bloco_m(registros_db))

    c190s = [r for r in registros_db if (r.reg or "").strip() == "C190"]
    if not c190s:
        return None

    itens: List[Dict[str, Any]] = []
    anchor = None

    for r in c190s:
        dados = (r.conteudo_json or {}).get("dados") or []
        if len(dados) < 4:
            continue

        cfop = str(dados[1] or "").strip()

        # entradas: CFOP 1xxx/2xxx (compras)
        if not (cfop.startswith("1") or cfop.startswith("2")):
            continue

        if anchor is None or int(r.id) < int(anchor.id):
            anchor = r

        itens.append({
            "cst_icms": str(dados[0] or "").strip(),
            "cfop": cfop,
            "vl_opr": dados[3],  # string br -> regra converte
            "registro_id": r.id,
        })

    if not itens or anchor is None:
        return None

    flags.update(coletar_creditos_bloco_m(registros_db))
    perfil_monofasico, score_monofasico = detectar_perfil_monofasico(registros_db)
    flags["perfil_monofasico"] = perfil_monofasico
    flags["score_monofasico"] = score_monofasico
    flags["fonte"] = "C190"

    dados_final: List[Any] = [{"_meta": flags}] + itens

    return RegistroFiscalDTO(
        id=int(anchor.id),
        reg="C190_IND_TORRADO_AGG",
        linha=int(anchor.linha),
        dados=dados_final,
    )


def montar_c170_ind_torrado_agg(registros_db: Sequence[EfdRegistro]) -> Optional[RegistroFiscalDTO]:
    # 1. Identificação rápida dos registros presentes
    regs_presentes = {(r.reg or "").strip() for r in registros_db}

    # 2. Coleta de Metadados (Flags)
    flags = {
        "tem_1200": "1200" in regs_presentes,
        "tem_1210": "1210" in regs_presentes,
        "tem_1700": "1700" in regs_presentes,
    }
    flags.update(coletar_flags_bloco_m(registros_db))

    # 3. Filtragem de C170
    c170s = [r for r in registros_db if (r.reg or "").strip() == "C170"]
    if not c170s:
        return None

    itens: List[Dict[str, Any]] = []
    anchor: Optional[EfdRegistro] = None

    for r in c170s:
        # Se o FiscalScanner já marcou o registro como PF, ele nem deveria estar no registros_db
        # (se você passou o rows_limpas). Mas adicionamos segurança extra aqui.

        dados = (r.conteudo_json or {}).get("dados") or []
        if len(dados) < 10:
            continue

        cfop = str(dados[9] or "").strip()
        # Filtro de entradas (1xxx e 2xxx)
        if not (len(cfop) == 4 and cfop.isdigit() and (cfop.startswith("1") or cfop.startswith("2"))):
            continue

        vl_item = dados[5] if len(dados) > 5 else "0"

        if anchor is None or int(r.id) < int(anchor.id):
            anchor = r

        # ✅ ADICIONADO: registro_id e pai_id para permitir que a regra valide PF
        itens.append({
            "cfop": cfop,
            "vl_opr": vl_item,
            "registro_id": int(r.id),
            "pai_id": getattr(r, "pai_id", None)
        })

    if not itens or anchor is None:
        return None

    # 4. Atualização de indicadores de perfil
    flags.update(coletar_creditos_bloco_m(registros_db))
    perfil_monofasico, score_monofasico = detectar_perfil_monofasico(registros_db)
    flags["perfil_monofasico"] = perfil_monofasico
    flags["score_monofasico"] = score_monofasico
    flags["fonte"] = "C170"

    dados_final: List[Any] = [{"_meta": flags}] + itens

    return RegistroFiscalDTO(
        id=int(anchor.id),
        reg="C170_IND_TORRADO_AGG",
        linha=int(anchor.linha),
        dados=dados_final,
        # O DTO do agregador em si nunca é PF, pois ele é uma soma de vários registros.
        is_pf=False
    )
