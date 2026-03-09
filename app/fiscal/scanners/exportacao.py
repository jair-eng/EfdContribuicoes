from typing import Sequence, List, Dict, Any, Optional, Tuple
from app.db.models import EfdRegistro
from app.fiscal.contexto import match_prefix_star, digits_only
from app.fiscal.dto import RegistroFiscalDTO
from decimal import Decimal

from app.fiscal.regras.Autocorrigivel.shared import _detectar_producao_interna_super
from app.sped.utils_geral import dec_br, pick_cod_item_c170, _detectar_indicio_agro_0200, _cfop_gate_entrada_compra

import logging

logger = logging.getLogger(__name__)




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
            v = dec_br(dados[idx])
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
                v = dec_br(dados[idx])
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

def detectar_perfil_monofasico(
    registros_db: Sequence["EfdRegistro"],
    *,
    catalogo: Optional[Any] = None,
    debug: bool = False,
) -> Tuple[bool, int]:
    """
    Detecta perfil monofásico (posto/combustíveis) por evidências combinadas.
    Retorna (bool, score 0-100).

    Se catalogo vier, tenta usar slugs:
      - NCM_COMBUSTIVEIS_MONO (ex: 2710*, 2711*, 2207*, 3826*)
      - CFOP_COMBUSTIVEIS_ENTRADA (ex: 1403, 1652, 2652, 3652 etc.)
    Se catalogo não vier, cai no fallback antigo (hardcoded).
    """
    score = 0

    # tenta puxar listas do catálogo (se existir)
    ncm_tokens = None
    cfop_tokens = None

    if catalogo is not None:
        try:
            # adapte se seu CatalogoFiscal tiver outro formato.
            # aqui assumo algo como: catalogo.get_itens(slug) -> set[str]
            ncm_tokens = set(catalogo.get_itens("NCM_COMBUSTIVEIS_MONO") or [])
            cfop_tokens = set(catalogo.get_itens("CFOP_COMBUSTIVEIS_ENTRADA") or [])
        except Exception:
            ncm_tokens = None
            cfop_tokens = None

    # fallback hardcoded (se catálogo não fornecer)
    if not ncm_tokens:
        ncm_tokens = {"2710*", "2711*", "2207*", "3826*"}
    if not cfop_tokens:
        cfop_tokens = {"1652", "1403"}  # seu fallback original

    # -------------------------
    # A) 0200 - NCM forte + descrição
    # -------------------------
    ncm_hits = 0

    for r in registros_db:
        if (r.reg or "").strip() != "0200":
            continue
        dados = (r.conteudo_json or {}).get("dados") or []

        # NCM padrão: idx 6 (seu scanner montou via 6); mas aqui fica defensivo
        ncm = ""
        if len(dados) > 6:
            ncm = digits_only(str(dados[6] or ""))
        if not ncm:
            # fallback procurando 8 dígitos
            for v in dados:
                vv = digits_only(str(v or ""))
                if len(vv) >= 8:
                    ncm = vv[:8]
                    break

        if ncm and any(match_prefix_star(tok, ncm) for tok in ncm_tokens):
            ncm_hits += 1

        # descrição (seu critério antigo)
        desc = str(dados[1] or "").strip().upper() if len(dados) > 1 else ""
        if desc and any(k in desc for k in ("GASOL", "DIESEL", "ETANOL", "ALCOOL", "COMBUST", "GNV", "GLP", "Biodiesel".upper())):
            score += 10

    if ncm_hits >= 1:
        score += 45
    if ncm_hits >= 2:
        score += 20  # bônus

    # -------------------------
    # B) CFOP típico (C170/C190)
    # -------------------------
    cfop_hits = 0
    for r in registros_db:
        reg = (r.reg or "").strip()
        dados = (r.conteudo_json or {}).get("dados") or []

        cfop = ""
        if reg == "C170":
            cfop = str(dados[9] or "").strip() if len(dados) > 9 else ""
        elif reg == "C190":
            cfop = str(dados[1] or "").strip() if len(dados) > 1 else ""

        if cfop and cfop in cfop_tokens:
            cfop_hits += 1

    if cfop_hits >= 5:
        score += 35
    elif cfop_hits >= 2:
        score += 20
    elif cfop_hits >= 1:
        score += 10

    score = min(100, int(score))
    ok = (score >= 60)

    if debug:
        print("[MONO] ncm_hits=", ncm_hits, "cfop_hits=", cfop_hits, "score=", score, "ok=", ok, flush=True)

    return ok, score

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
        vl_desc = dados[6] if len(dados) > 6 else "0"
        vl_icms = dados[13] if len(dados) > 13 else "0"

        if anchor is None or int(r.id) < int(anchor.id):
            anchor = r

        itens.append({
            "cfop": cfop,
            "vl_opr": vl_item,  # mantém compatibilidade
            "vl_item": vl_item,
            "vl_desc": vl_desc,
            "vl_icms": vl_icms,
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

def montar_meta_fiscal(
    registros_db: Sequence[EfdRegistro],
    *,
    catalogo=None,
    debug: bool = False,
) -> Optional[RegistroFiscalDTO]:
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

    # perfil monofásico (agora catálogo-driven quando disponível)
    perfil_monofasico, score_monofasico = detectar_perfil_monofasico(
        registros_db,
        catalogo=catalogo,
        debug=debug,
    )
    flags["perfil_monofasico"] = perfil_monofasico
    flags["score_monofasico"] = score_monofasico

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

    # ---------------------------------------------------------
    # 1) Identificação rápida dos registros presentes
    # ---------------------------------------------------------
    regs_presentes = {(r.reg or "").strip() for r in registros_db}

    flags = {
        "tem_1200": "1200" in regs_presentes,
        "tem_1210": "1210" in regs_presentes,
        "tem_1700": "1700" in regs_presentes,
    }
    flags.update(coletar_flags_bloco_m(registros_db))

    # ---------------------------------------------------------
    # 2) Filtragem de C190
    # ---------------------------------------------------------
    c190s = [r for r in registros_db if (r.reg or "").strip() == "C190"]

    print(f"[C190_AGG] total C190 encontrados: {len(c190s)}")

    if not c190s:
        return None

    itens: List[Dict[str, Any]] = []
    anchor_linha: Optional[int] = None

    # ---------------------------------------------------------
    # 3) Loop de agregação
    # ---------------------------------------------------------
    for r in c190s:

        dados = (r.conteudo_json or {}).get("dados") or []

        if len(dados) < 4:
            continue

        cfop = str(dados[1] or "").strip()

        if not _cfop_gate_entrada_compra(cfop):
            continue

        vl_opr = dados[3]

        linha_r = int(getattr(r, "linha", 0) or 0)

        if linha_r > 0 and (anchor_linha is None or linha_r < anchor_linha):
            anchor_linha = linha_r

        itens.append({
            "cst_icms": str(dados[0] or "").strip(),
            "cfop": cfop,
            "vl_opr": vl_opr,
            "registro_id": int(getattr(r, "id", 0) or 0),  # sintético
            "linha": linha_r,
        })

    print(f"[C190_AGG] itens válidos: {len(itens)} anchor_linha={anchor_linha}")

    if not itens:
        print("[C190_AGG] abortando (sem itens)")
        return None

    if not anchor_linha:
        print("[C190_AGG] abortando (sem anchor_linha)")
        return None

    # ---------------------------------------------------------
    # 4) Atualização de indicadores de perfil
    # ---------------------------------------------------------
    flags.update(coletar_creditos_bloco_m(registros_db))

    perfil_monofasico, score_monofasico = detectar_perfil_monofasico(registros_db)
    flags["perfil_monofasico"] = perfil_monofasico
    flags["score_monofasico"] = score_monofasico
    flags["fonte"] = "C190"

    # Rastreabilidade
    flags["anchor_reg_base"] = "C190"
    flags["anchor_linha"] = anchor_linha
    flags["anchor_registro_id"] = None

    dados_final: List[Any] = [{"_meta": flags}] + itens



    # ---------------------------------------------------------
    # 5) Retorno do DTO agregado
    # ---------------------------------------------------------
    return RegistroFiscalDTO(
        id=0,  # agregado sintético
        reg="C190_IND_TORRADO_AGG",
        linha=anchor_linha,
        dados=dados_final,
    )

def montar_c170_ind_torrado_agg(registros_db: Sequence[EfdRegistro]) -> Optional[RegistroFiscalDTO]:

    map_item_desc: dict[str, str] = {}
    map_item_ncm: dict[str, str] = {}

    # --------------------------------------------
    # 0200 -> mapa COD_ITEM -> (DESC, NCM)
    # --------------------------------------------
    for r in registros_db:
        if (r.reg or "").strip() != "0200":
            continue

        d = (r.conteudo_json or {}).get("dados") or []

        cod = str(d[0] or "").strip() if len(d) > 0 else ""
        desc = str(d[1] or "").strip() if len(d) > 1 else ""
        ncm_raw = str(d[6] or "").strip() if len(d) > 6 else ""

        ncm = "".join(ch for ch in ncm_raw if ch.isdigit())

        if cod:
            if desc:
                map_item_desc[cod] = desc
            if ncm:
                map_item_ncm[cod] = ncm

    # --------------------------------------------
    # Identificação dos registros presentes
    # --------------------------------------------
    regs_presentes = {(r.reg or "").strip() for r in registros_db}

    flags = {
        "tem_1200": "1200" in regs_presentes,
        "tem_1210": "1210" in regs_presentes,
        "tem_1700": "1700" in regs_presentes,
    }

    flags.update(coletar_flags_bloco_m(registros_db))

    # --------------------------------------------
    # Filtragem de C170
    # --------------------------------------------
    c170s = [r for r in registros_db if (r.reg or "").strip() == "C170"]

    if not c170s:
        return None

    itens: List[Dict[str, Any]] = []
    anchor_linha: Optional[int] = None

    # --------------------------------------------
    # Loop de agregação
    # --------------------------------------------
    for r in c170s:

        dados = (r.conteudo_json or {}).get("dados") or []

        if len(dados) < 10:
            continue

        cfop = str(dados[9] or "").strip()

        if not _cfop_gate_entrada_compra(cfop):
            continue

        # -----------------------------
        # Valores do item
        # -----------------------------
        vl_item = dados[5] if len(dados) > 5 else "0"
        vl_desc = dados[6] if len(dados) > 6 else "0"
        vl_icms = dados[13] if len(dados) > 13 else "0"

        # -----------------------------
        # Item / NCM
        # -----------------------------
        cod_item = pick_cod_item_c170(dados)

        ncm = map_item_ncm.get(cod_item, "") if cod_item else ""
        descricao = map_item_desc.get(cod_item, "") if cod_item else ""

        # -----------------------------
        # Linha
        # -----------------------------
        linha_r = int(getattr(r, "linha", 0) or 0)

        if linha_r > 0 and (anchor_linha is None or linha_r < anchor_linha):
            anchor_linha = linha_r

        # -----------------------------
        # ID real
        # -----------------------------
        rid_real = int(getattr(r, "id", 0) or 0)

        if rid_real <= 0:
            rid_real = int(getattr(r, "registro_id", 0) or 0)

        if flags.get("anchor_registro_id") is None and rid_real > 0:
            flags["anchor_registro_id"] = rid_real

        itens.append({
            "cfop": cfop,
            "vl_item": vl_item,
            "vl_desc": vl_desc,
            "vl_icms": vl_icms,
            "ncm": ncm,
            "descricao": descricao,
            "cod_item": cod_item,
            "registro_id": rid_real,
            "pai_id": getattr(r, "pai_id", None),
            "linha": linha_r,
        })

    print(f"[C170_AGG] itens válidos: {len(itens)} anchor_linha={anchor_linha}")

    if not itens:
        print("[C170_AGG] abortando (sem itens)")
        return None

    if not anchor_linha:
        print("[C170_AGG] abortando (sem anchor_linha)")
        return None

    # --------------------------------------------
    # Atualização de indicadores
    # --------------------------------------------
    flags.update(coletar_creditos_bloco_m(registros_db))

    perfil_monofasico, score_monofasico = detectar_perfil_monofasico(registros_db)

    flags["perfil_monofasico"] = perfil_monofasico
    flags["score_monofasico"] = score_monofasico
    flags["fonte"] = "C170"

    flags["anchor_reg_base"] = "C170"
    flags["anchor_linha"] = anchor_linha
    flags.setdefault("anchor_registro_id", None)

    flags["tem_indicio_agro"] = _detectar_indicio_agro_0200(registros_db)

    dados_final: List[Any] = [{"_meta": flags}] + itens

    # --------------------------------------------
    # Retorno
    # --------------------------------------------
    anchor_id = int(flags.get("anchor_registro_id") or 0)

    return RegistroFiscalDTO(
        id=anchor_id,
        reg="C170_IND_TORRADO_AGG",
        linha=anchor_linha,
        dados=dados_final,
        is_pf=False
    )

def montar_c170_insumo_agg(registros_db: Sequence[EfdRegistro]) -> Optional[RegistroFiscalDTO]:
    map_item_desc: dict[str, str] = {}
    map_item_ncm: dict[str, str] = {}

    # 0200 -> mapa COD_ITEM -> (DESC, NCM)
    for r in registros_db:
        if (r.reg or "").strip() != "0200":
            continue
        d = (r.conteudo_json or {}).get("dados") or []
        cod = str(d[0] or "").strip() if len(d) > 0 else ""
        desc = str(d[1] or "").strip() if len(d) > 1 else ""
        ncm_raw = str(d[6] or "").strip() if len(d) > 6 else ""
        ncm = "".join(ch for ch in ncm_raw if ch.isdigit())
        if cod:
            if desc:
                map_item_desc[cod] = desc
            if ncm:
                map_item_ncm[cod] = ncm

    regs_presentes = {(r.reg or "").strip() for r in registros_db}
    flags: Dict[str, Any] = {
        "tem_1200": "1200" in regs_presentes,
        "tem_1210": "1210" in regs_presentes,
        "tem_1700": "1700" in regs_presentes,
    }
    flags.update(coletar_flags_bloco_m(registros_db))

    c170s = [r for r in registros_db if (r.reg or "").strip() == "C170"]
    if not c170s:
        return None

    itens: List[Dict[str, Any]] = []
    anchor_linha: Optional[int] = None

    for r in c170s:
        dados = (r.conteudo_json or {}).get("dados") or []
        if len(dados) < 10:
            continue

        # CFOP entrada (1xxx/2xxx). (deixa a regra filtrar catálogo depois)
        cfop = str(dados[9] or "").strip()
        if not _cfop_gate_entrada_compra(cfop):
            continue

        # valores padrão do teu parser
        vl_item = dados[5] if len(dados) > 5 else "0"
        vl_desc = dados[6] if len(dados) > 6 else "0"
        vl_icms = dados[13] if len(dados) > 13 else "0"

        cst_pis = str(dados[23] or "").strip() if len(dados) > 23 else ""
        cst_cof = str(dados[29] or "").strip() if len(dados) > 29 else ""

        cod_item = pick_cod_item_c170(dados)
        ncm = map_item_ncm.get(cod_item, "") if cod_item else ""
        descricao = map_item_desc.get(cod_item, "") if cod_item else ""

        linha_r = int(getattr(r, "linha", 0) or 0)
        if linha_r > 0 and (anchor_linha is None or linha_r < anchor_linha):
            anchor_linha = linha_r

        rid_real = int(getattr(r, "id", 0) or 0) or int(getattr(r, "registro_id", 0) or 0)

        # ancora no 1º registro real válido
        if flags.get("anchor_registro_id") is None and rid_real > 0:
            flags["anchor_registro_id"] = rid_real

        itens.append({
            "cfop": cfop,
            "vl_item": vl_item,
            "vl_desc": vl_desc,
            "vl_icms": vl_icms,
            "cst_pis": cst_pis,
            "cst_cofins": cst_cof,
            "ncm": ncm,
            "descricao": descricao,
            "cod_item": cod_item,
            "registro_id": rid_real,
            "pai_id": getattr(r, "pai_id", None),
            "linha": linha_r,
        })

    if not itens or not anchor_linha:
        return None

    # indicadores / perfil (se você quiser manter consistência com os outros AGGs)
    flags.update(coletar_creditos_bloco_m(registros_db))
    perfil_monofasico, score_monofasico = detectar_perfil_monofasico(registros_db)
    flags["perfil_monofasico"] = perfil_monofasico
    flags["score_monofasico"] = score_monofasico

    flags["fonte"] = "C170"
    flags["anchor_reg_base"] = "C170"
    flags["anchor_linha"] = anchor_linha
    flags.setdefault("anchor_registro_id", None)

    dados_final: List[Any] = [{"_meta": flags}] + itens
    anchor_id = int(flags.get("anchor_registro_id") or 0)
    if not anchor_id:
        return None

    return RegistroFiscalDTO(
        id=anchor_id,
        reg="C170_INSUMO_AGG",
        linha=anchor_linha,
        dados=dados_final,
        is_pf=False
    )

def montar_c170_sup_entrada_agg(registros_db: Sequence[EfdRegistro], *, cat: Any) -> Optional[RegistroFiscalDTO]:
    map_item_desc: dict[str, str] = {}
    map_item_ncm: dict[str, str] = {}

    for r in registros_db:
        if (r.reg or "").strip() != "0200":
            continue
        d = (r.conteudo_json or {}).get("dados") or []
        cod = str(d[0] or "").strip() if len(d) > 0 else ""
        desc = str(d[1] or "").strip() if len(d) > 1 else ""
        ncm_raw = str(d[6] or "").strip() if len(d) > 6 else ""
        ncm = "".join(ch for ch in ncm_raw if ch.isdigit())
        if cod:
            if desc:
                map_item_desc[cod] = desc
            if ncm:
                map_item_ncm[cod] = ncm

    regs_presentes = {(r.reg or "").strip() for r in registros_db}
    flags: Dict[str, Any] = {
        "tem_1200": "1200" in regs_presentes,
        "tem_1210": "1210" in regs_presentes,
        "tem_1700": "1700" in regs_presentes,
    }
    flags.update(coletar_flags_bloco_m(registros_db))
    flags.update(coletar_creditos_bloco_m(registros_db))

    perfil_monofasico, score_monofasico = detectar_perfil_monofasico(registros_db)
    flags["perfil_monofasico"] = perfil_monofasico
    flags["score_monofasico"] = score_monofasico

    # ✅ contexto SUP (produção interna)
    sup_ctx = _detectar_producao_interna_super(registros_db, cat)
    flags.update({f"sup_{k}": v for k, v in sup_ctx.items()})

    c170s = [r for r in registros_db if (r.reg or "").strip() == "C170"]
    if not c170s:
        return None

    itens: List[Dict[str, Any]] = []
    anchor_linha: Optional[int] = None

    for r in c170s:
        dados = (r.conteudo_json or {}).get("dados") or []
        if len(dados) < 10:
            continue

        cfop = str(dados[9] or "").strip()
        # ✅ entradas apenas (1xxx/2xxx)
        if not _cfop_gate_entrada_compra(cfop):
            continue

        vl_item = dados[5] if len(dados) > 5 else "0"
        vl_desc = dados[6] if len(dados) > 6 else "0"
        vl_icms = dados[13] if len(dados) > 13 else "0"
        cst_pis = str(dados[23] or "").strip() if len(dados) > 23 else ""
        cst_cof = str(dados[29] or "").strip() if len(dados) > 29 else ""

        cod_item = pick_cod_item_c170(dados)
        ncm = map_item_ncm.get(cod_item, "") if cod_item else ""
        descricao = map_item_desc.get(cod_item, "") if cod_item else ""

        linha_r = int(getattr(r, "linha", 0) or 0)
        if linha_r > 0 and (anchor_linha is None or linha_r < anchor_linha):
            anchor_linha = linha_r

        rid_real = int(getattr(r, "id", 0) or 0) or int(getattr(r, "registro_id", 0) or 0)
        if flags.get("anchor_registro_id") is None and rid_real > 0:
            flags["anchor_registro_id"] = rid_real

        itens.append({
            "cfop": cfop,
            "vl_item": vl_item,
            "cst_pis": cst_pis,
            "cst_cofins": cst_cof,
            "ncm": ncm,
            "vl_desc": vl_desc,
            "vl_icms": vl_icms,
            "descricao": descricao,
            "cod_item": cod_item,
            "registro_id": rid_real,
            "pai_id": getattr(r, "pai_id", None),
            "linha": linha_r,
        })

    if not itens or not anchor_linha:
        return None

    flags["fonte"] = "C170"
    flags["anchor_reg_base"] = "C170"
    flags["anchor_linha"] = anchor_linha
    flags.setdefault("anchor_registro_id", None)

    dados_final: List[Any] = [{"_meta": flags}] + itens
    anchor_id = int(flags.get("anchor_registro_id") or 0)
    if not anchor_id:
        return None
    print("[SUP_AGG] sup_ctx=", sup_ctx)
    print("[SUP_AGG] flags sup_tem_producao_interna=", flags.get("sup_tem_producao_interna"), "sup_producao_interna=",
          flags.get("sup_producao_interna"))

    return RegistroFiscalDTO(
        id=anchor_id,
        reg="C170_SUP_ENTRADA_AGG",
        linha=anchor_linha,
        dados=dados_final,
        is_pf=False
    )


def montar_c170_saida_agg(registros_db: Sequence[EfdRegistro]) -> Optional[RegistroFiscalDTO]:
    c170s = [r for r in registros_db if (r.reg or "").strip().upper() == "C170"]
    if not c170s:
        return None

    itens: list[dict[str, Any]] = []
    anchor_linha: int | None = None
    anchor_registro_id: int | None = None

    for r in c170s:
        dados = (r.conteudo_json or {}).get("dados") or []
        if len(dados) < 10:
            continue

        cfop = str(dados[9] or "").strip()

        # SAÍDAS: 5/6/7
        if not (cfop and cfop[0] in ("5", "6", "7")):
            continue

        # ✅ VL_ICMS (índice correto)
        vl_icms = dados[14] if len(dados) > 14 else "0"

        linha_r = int(getattr(r, "linha", 0) or 0)
        rid = int(getattr(r, "id", 0) or 0)

        if anchor_linha is None or linha_r < anchor_linha:
            anchor_linha = linha_r
            anchor_registro_id = rid

        itens.append({
            "cfop": cfop,
            "vl_icms": vl_icms,
            "linha": linha_r,
            "registro_id": rid,
        })

    if not itens or not anchor_linha:
        return None

    meta = {"_meta": {"anchor_registro_id": int(anchor_registro_id or 0), "anchor_linha": int(anchor_linha)}}

    return RegistroFiscalDTO(
        id=int(anchor_registro_id or 0),
        reg="C170_SAIDA_AGG",
        linha=int(anchor_linha),
        dados=[meta] + itens,   # ✅ mantém contrato do scanner/regra
    )