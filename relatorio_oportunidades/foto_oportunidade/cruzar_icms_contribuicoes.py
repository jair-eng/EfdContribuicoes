from __future__ import annotations

from decimal import Decimal, ROUND_HALF_UP
from typing import Any, Dict, List, Tuple

from relatorio_oportunidades.foto_oportunidade.aux_funcoes_foto import _digits, _s, _q2, _key_chave_cod_item, _key_chave, _participante, \
    _dec, _key_chave_cod_item_norm, _key_chave_num_item

# ============================================================
# Constantes de simulação
# ============================================================

ALIQUOTA_PIS = Decimal("0.0165")
ALIQUOTA_COFINS = Decimal("0.0760")

CFOPS_VALIDOS = {
    "1101", "1102", "2101", "2102", "3101", "3102"
}

CSTS_ORIGEM = {
    "06", "07", "08",
    "70", "73", "74", "75",
    "98", "99",
}

CSTS_CREDITAVEIS = {
    "50", "51", "52", "53", "54", "55", "56"
}


NCM_CAFE_PREFIXOS = ("0901",)
TERMOS_CAFE = ("CAFE", "CAFÉ")

def _eh_item_cafe_ou_agro(row: Dict[str, Any]) -> bool:
    ncm = _digits(row.get("ncm"))
    desc = _s(row.get("descricao")).upper()
    cod_item = _s(row.get("cod_item")).upper()

    if any(ncm.startswith(p) for p in NCM_CAFE_PREFIXOS):
        return True

    texto = f"{desc} {cod_item}"
    if any(t in texto for t in TERMOS_CAFE):
        return True

    return False

# ============================================================
# Simulação
# ============================================================

def _simular_credito(
    *,
    cfop: str,
    cst_pis: str,
    valor_item: Decimal,
    valor_desconto: Decimal,
    valor_icms: Decimal,
    sem_efd: bool = False,
) -> Dict[str, Any]:
    """
    Simula a lógica do Foto Recuperação / motor:
      - guard-rail por CFOP
      - se CST origem -> simula CST 51
      - se CST já creditável -> mantém
      - base = valor_item - desconto - icms
    """
    base_simulada = Decimal("0")
    pis_simulado = Decimal("0")
    cofins_simulado = Decimal("0")
    credito_total = Decimal("0")

    cst_simulado = ""
    regra = ""
    elegivel = False
    motivo = ""

    if cfop not in CFOPS_VALIDOS:
        motivo = "CFOP fora do guard-rail"
        return {
            "elegivel": False,
            "motivo_simulacao": motivo,
            "base_simulada": base_simulada,
            "pis_simulado": pis_simulado,
            "cofins_simulado": cofins_simulado,
            "credito_simulado": credito_total,
            "cst_simulado": cst_simulado,
            "regra_simulada": regra,
        }

    base_simulada = valor_item - valor_desconto
    if base_simulada < 0:
        base_simulada = Decimal("0")

    if sem_efd:
        elegivel = True
        cst_simulado = "51"
        regra = "NAO_ESCRITURADO_CST51"
        motivo = "Item não escriturado na EFD Contribuições"

    elif cst_pis in CSTS_CREDITAVEIS:
        elegivel = True
        cst_simulado = cst_pis
        regra = "JA_CREDITAVEL"
        motivo = "Item já creditável no EFD"

    elif cst_pis in CSTS_ORIGEM:
        elegivel = True
        cst_simulado = "51"
        regra = "IND_AGRO_CST51"
        motivo = "CST origem elegível para simulação"

    else:
        motivo = "CST fora da simulação"

    if elegivel and base_simulada > 0:
        pis_simulado = base_simulada * ALIQUOTA_PIS
        cofins_simulado = base_simulada * ALIQUOTA_COFINS
        credito_total = pis_simulado + cofins_simulado

    return {
        "elegivel": elegivel,
        "motivo_simulacao": motivo,
        "base_simulada": _q2(base_simulada),
        "pis_simulado": _q2(pis_simulado),
        "cofins_simulado": _q2(cofins_simulado),
        "credito_simulado": _q2(credito_total),
        "cst_simulado": cst_simulado,
        "regra_simulada": regra,
    }


# ============================================================
# Cruzamento principal
# ============================================================
def cruzar_icms_ipi_com_efd_contrib(
    linhas_icms: List[Dict[str, Any]],
    linhas_contrib: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    """
    Cruza linhas normalizadas de ICMS/IPI com EFD Contribuições.

    Estratégia:
      1) chave + cod_item
      2) chave + cod_item_norm
      3) chave + num_item
      4) chave + cfop
      5) chave
    """

    # --------------------------------------------------------
    # Índices do Contribuições
    # --------------------------------------------------------
    contrib_by_chave_item: Dict[Tuple[str, str], List[Dict[str, Any]]] = {}
    contrib_by_chave_item_norm: Dict[Tuple[str, str], List[Dict[str, Any]]] = {}
    contrib_by_chave_num_item: Dict[Tuple[str, str], List[Dict[str, Any]]] = {}
    contrib_by_chave: Dict[str, List[Dict[str, Any]]] = {}

    for row in linhas_contrib:
        k1 = _key_chave_cod_item(row)
        k1n = _key_chave_cod_item_norm(row)
        k2 = _key_chave(row)
        k3 = _key_chave_num_item(row)

        if k2:
            contrib_by_chave.setdefault(k2, []).append(row)

        if k1[0] and k1[1]:
            contrib_by_chave_item.setdefault(k1, []).append(row)

        if k1n[0] and k1n[1]:
            contrib_by_chave_item_norm.setdefault(k1n, []).append(row)

        if k3[0] and k3[1]:
            contrib_by_chave_num_item.setdefault(k3, []).append(row)

    # --------------------------------------------------------
    # Cruzamento
    # --------------------------------------------------------
    saida: List[Dict[str, Any]] = []

    for icms in linhas_icms:
        chave = _key_chave(icms)
        cod_item = _s(icms.get("cod_item"))
        cod_item_norm = _s(icms.get("cod_item_norm"))
        num_item = _s(icms.get("num_item"))

        match = None
        tipo_match = ""

        # 1) chave + cod_item
        if chave and cod_item:
            candidatos = contrib_by_chave_item.get((chave, cod_item), [])
            if candidatos:
                match = candidatos[0]
                tipo_match = "CHAVE_COD_ITEM"

        # 2) chave + cod_item_norm
        if match is None and chave and cod_item_norm:
            candidatos = contrib_by_chave_item_norm.get((chave, cod_item_norm), [])
            if candidatos:
                match = candidatos[0]
                tipo_match = "CHAVE_COD_ITEM_NORM"

        # 3) chave + num_item
        if match is None and chave and num_item:
            candidatos = contrib_by_chave_num_item.get((chave, num_item), [])
            if candidatos:
                match = candidatos[0]
                tipo_match = "CHAVE_NUM_ITEM"

        # 4/5) fallback por chave
        if match is None and chave:
            candidatos = contrib_by_chave.get(chave, [])

            if len(candidatos) == 1:
                match = candidatos[0]
                tipo_match = "CHAVE"

            elif len(candidatos) > 1:
                cfop_icms = _s(icms.get("cfop"))

                for cand in candidatos:
                    if _s(cand.get("cfop")) == cfop_icms:
                        match = cand
                        tipo_match = "CHAVE_CFOP"
                        break

                if match is None:
                    match = candidatos[0]
                    tipo_match = "CHAVE_MULTI"

        participante_icms = _participante(icms)
        participante_contrib = _participante(match) if match else ""

        empresa = _s(icms.get("empresa")) or (_s(match.get("empresa")) if match else "")
        participante = participante_icms or participante_contrib

        data = _s(icms.get("data")) or (_s(match.get("dt_doc")) if match else "")
        chave_out = _s(icms.get("chave")) or (_s(match.get("chave")) if match else "")
        numero = _s(icms.get("numero")) or (_s(match.get("num_doc")) if match else "")
        serie = _s(icms.get("serie")) or (_s(match.get("serie")) if match else "")

        cod_item_out = _s(icms.get("cod_item")) or (_s(match.get("cod_item")) if match else "")
        num_item_out = _s(icms.get("num_item")) or (_s(match.get("num_item")) if match else "")
        descricao = _s(icms.get("descricao")) or (_s(match.get("descr_item")) if match else "")
        ncm = _s(icms.get("ncm")) or (_s(match.get("ncm")) if match else "")
        cfop = _s(icms.get("cfop")) or (_s(match.get("cfop")) if match else "")

        valor_item = _dec(icms.get("valor_item"))
        valor_desconto = _dec(icms.get("valor_desconto"))
        valor_icms = _dec(icms.get("valor_icms"))
        valor_ipi = _dec(icms.get("valor_ipi"))

        cst_pis = _s(match.get("cst_pis")) if match else "SEM_EFD"
        vl_base_pis = _dec(match.get("vl_bc_pis")) if match else Decimal("0")
        vl_aliq_pis = _dec(match.get("aliq_pis")) if match else Decimal("0")
        vl_pis = _dec(match.get("vl_pis")) if match else Decimal("0")

        cst_cofins = _s(match.get("cst_cofins")) if match else "SEM_EFD"
        vl_base_cofins = _dec(match.get("vl_bc_cofins")) if match else Decimal("0")
        vl_aliq_cofins = _dec(match.get("aliq_cofins")) if match else Decimal("0")
        vl_cofins = _dec(match.get("vl_cofins")) if match else Decimal("0")

        status_cruzamento = "MATCH" if match else "NAO_ESCRITURADO"

        dominio_ok = _eh_item_cafe_ou_agro({
            "ncm": ncm,
            "descricao": descricao,
            "cod_item": cod_item_out,
        })

        # ----------------------------------------------------
        # Simulação
        # ----------------------------------------------------
        if not dominio_ok:
            sim = {
                "elegivel": False,
                "motivo_simulacao": "Item fora do domínio café/agro",
                "base_simulada": Decimal("0"),
                "pis_simulado": Decimal("0"),
                "cofins_simulado": Decimal("0"),
                "credito_simulado": Decimal("0"),
                "cst_simulado": "",
                "regra_simulada": "",
            }
        else:
            sim = _simular_credito(
                cfop=cfop,
                cst_pis=cst_pis,
                valor_item=valor_item,
                valor_desconto=valor_desconto,
                valor_icms=valor_icms,
                sem_efd=not bool(match),
            )

        linha = {
            # Cabeçalho/base
            "competencia": _s(icms.get("periodo")) or _s(match.get("periodo") if match else ""),
            "empresa": empresa,
            "participante": participante,
            "data": data,
            "chave": chave_out,
            "numero": numero,
            "serie": serie,

            # Produto/item
            "num_item": num_item_out,
            "cod_item": cod_item_out,
            "cod_item_norm": _s(icms.get("cod_item_norm")) or (_s(match.get("cod_item_norm")) if match else ""),
            "descricao": descricao,
            "ncm": ncm,
            "cfop": cfop,

            # Valores ICMS/IPI
            "valor_item": _q2(valor_item),
            "valor_desconto": _q2(valor_desconto),
            "valor_icms": _q2(valor_icms),
            "valor_ipi": _q2(valor_ipi),

            # PIS atual
            "cst_pis": cst_pis,
            "vl_base_pis": _q2(vl_base_pis),
            "vl_aliq_pis": _q2(vl_aliq_pis),
            "vl_pis": _q2(vl_pis),

            # COFINS atual
            "cst_cofins": cst_cofins,
            "vl_base_cofins": _q2(vl_base_cofins),
            "vl_aliq_cofins": _q2(vl_aliq_cofins),
            "vl_cofins": _q2(vl_cofins),

            # Originais
            "original_cst_pis": cst_pis,
            "original_vl_base_pis": _q2(vl_base_pis),
            "original_vl_aliq_pis": _q2(vl_aliq_pis),
            "original_vl_pis": _q2(vl_pis),

            "original_cst_cofins": cst_cofins,
            "original_vl_base_cofins": _q2(vl_base_cofins),
            "original_vl_aliq_cofins": _q2(vl_aliq_cofins),
            "original_vl_cofins": _q2(vl_cofins),

            # Contábil
            "contabil": _q2(_dec(icms.get("contabil", icms.get("valor_item")))),

            # Simulação
            "base_simulada": sim["base_simulada"],
            "pis_simulado": sim["pis_simulado"],
            "cofins_simulado": sim["cofins_simulado"],
            "credito_simulado": sim["credito_simulado"],
            "cst_simulado": sim["cst_simulado"],
            "regra_simulada": sim["regra_simulada"],
            "elegivel_simulacao": sim["elegivel"],
            "motivo_simulacao": sim["motivo_simulacao"],

            # Domínio
            "dominio_ok": dominio_ok,

            # Metadados
            "origem": "ICMS_IPI + EFD_CONTRIBUICOES" if match else "ICMS_IPI",
            "origem_item": _s(icms.get("origem_item")),
            "tipo_match": tipo_match,
            "match_encontrado": bool(match),
            "status_cruzamento": status_cruzamento,
        }

        saida.append(linha)

    return saida


def resumir_cruzamento(linhas_cruzadas: List[Dict[str, Any]]) -> Dict[str, Any]:
    total_linhas = len(linhas_cruzadas)
    total_match = sum(1 for x in linhas_cruzadas if x.get("match_encontrado"))
    total_nao_escriturado = sum(1 for x in linhas_cruzadas if x.get("status_cruzamento") == "NAO_ESCRITURADO")
    total_sem_match = total_linhas - total_match  # mantido para compatibilidade

    elegiveis = [x for x in linhas_cruzadas if x.get("elegivel_simulacao")]
    total_elegiveis = len(elegiveis)

    linhas_match = [x for x in linhas_cruzadas if x.get("status_cruzamento") == "MATCH"]
    linhas_nao_esc = [x for x in linhas_cruzadas if x.get("status_cruzamento") == "NAO_ESCRITURADO"]

    total_base_simulada = sum((_dec(x.get("base_simulada")) for x in linhas_cruzadas), Decimal("0"))
    total_pis_simulado = sum((_dec(x.get("pis_simulado")) for x in linhas_cruzadas), Decimal("0"))
    total_cofins_simulado = sum((_dec(x.get("cofins_simulado")) for x in linhas_cruzadas), Decimal("0"))
    total_credito_simulado = sum((_dec(x.get("credito_simulado")) for x in linhas_cruzadas), Decimal("0"))

    total_credito_match = sum((_dec(x.get("credito_simulado")) for x in linhas_match), Decimal("0"))
    total_credito_nao_escriturado = sum((_dec(x.get("credito_simulado")) for x in linhas_nao_esc), Decimal("0"))

    por_match: Dict[str, int] = {}
    por_cst: Dict[str, int] = {}
    por_regra: Dict[str, int] = {}

    for x in linhas_cruzadas:
        tm = _s(x.get("tipo_match")) or "SEM_MATCH"
        por_match[tm] = por_match.get(tm, 0) + 1

        cst = _s(x.get("cst_pis")) or "SEM_CST"
        por_cst[cst] = por_cst.get(cst, 0) + 1

        regra = _s(x.get("regra_simulada")) or "SEM_REGRA"
        por_regra[regra] = por_regra.get(regra, 0) + 1

    return {
        "total_linhas": total_linhas,
        "total_match": total_match,
        "total_sem_match": total_sem_match,
        "total_nao_escriturado": total_nao_escriturado,
        "total_elegiveis": total_elegiveis,

        "total_base_simulada": _q2(total_base_simulada),
        "total_pis_simulado": _q2(total_pis_simulado),
        "total_cofins_simulado": _q2(total_cofins_simulado),
        "total_credito_simulado": _q2(total_credito_simulado),

        "total_credito_match": _q2(total_credito_match),
        "total_credito_nao_escriturado": _q2(total_credito_nao_escriturado),

        "por_match": por_match,
        "por_cst_pis": por_cst,
        "por_regra": por_regra,
    }