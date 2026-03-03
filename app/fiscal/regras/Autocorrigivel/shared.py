
from decimal import Decimal
from typing import Dict, Any, Optional, List, Tuple, Sequence
from sqlalchemy.orm import Session
from sqlalchemy import  text
from app.db.models import EfdRegistro
from app.fiscal.ent_cat_fiscal import carregar_catalogo_fiscal
from app.fiscal.regras.Diagnostico.base_regras import RegraBase
from app.fiscal.settings_fiscais import CSTS_TRIB_NCUM
from app.services.c170_service import revisar_c170_lote
from app.sped.logic.consolidador import popular_pai_id
import traceback


def aplicar_correcao_sup_por_grupos_ncm_cst51_hibrido(
    db: Session,
    *,
    versao_origem_id: int,
    empresa_id: Optional[int] = None,
    incluir_revenda: bool = True,
    grupos_ncm: List[str],
    csts_origem: Optional[List[str]] = None,
    apontamento_id: Optional[int] = None,
    motivo_codigo: str = "SUP_EMBALAGEM_INSUMO_V1",
) -> Dict[str, Any]:
    """
    HÍBRIDO:
      - SQL filtra CFOP + CST (barato)
      - Python resolve NCM via 0200 e filtra por catálogo matcher (consistente)
      - aplica CST 51/51 via revisar_c170_lote
    """
    versao_origem_id = int(versao_origem_id)

    if "51" not in set(CSTS_TRIB_NCUM or set()):
        raise ValueError("settings_fiscais.CSTS_TRIB_NCUM não contém '51'.")

    if not csts_origem:
        csts_origem = ["70", "73", "75", "98", "99", "06", "07", "08"]
    csts_origem = [str(x).strip() for x in csts_origem if str(x).strip()]

    # CFOPs de entrada
    cfops = ["1101", "2101", "3101"]
    if incluir_revenda:
        cfops += ["1102", "2102", "3102"]

    # 0) garante pai_id (trava PF no lote)
    popular_pai_id(db, versao_origem_id)

    # 1) carrega catálogo (mesmo do motor)
    cat = carregar_catalogo_fiscal(db, empresa_id)  # ajuste se sua assinatura for diferente
    if not cat:
        return {"status": "erro", "msg": "Catálogo fiscal vazio/None.", "candidatos_sql": 0}

    # 2) candidatos via SQL (CFOP+CST)
    candidatos = _buscar_candidatos_c170_cfop_cst(
        db,
        versao_id=versao_origem_id,
        cfops=cfops,
        csts_origem=csts_origem,
    )
    if not candidatos:
        return {"status": "vazio", "candidatos_sql": 0}

    # 3) resolve NCM via 0200 (1 query)
    cod_items = sorted(set(cod for _, cod in candidatos))
    map_ncm = _map_cod_item_para_ncm_0200(db, versao_id=versao_origem_id, cod_items=cod_items)

    # 4) filtro por catálogo matcher (Python)
    ids_ok: List[int] = []
    pulou_sem_ncm = 0
    for rid, cod in candidatos:
        ncm = map_ncm.get(cod)
        if not ncm:
            pulou_sem_ncm += 1
            continue  # ✅ conservador: sem NCM = não auto-fixa
        if any(RegraBase.ncm_match_static(cat, slug, ncm) for slug in (grupos_ncm or [])):
            ids_ok.append(int(rid))

    if not ids_ok:
        return {
            "status": "vazio",
            "candidatos_sql": len(candidatos),
            "pulou_sem_ncm": pulou_sem_ncm,
            "candidatos_pos_catalogo": 0,
        }

    # 5) aplica lote CST 51/51
    lote = [{"registro_id": rid, "cfop": None, "cst_pis": "51", "cst_cofins": "51"} for rid in ids_ok]

    res = revisar_c170_lote(
        db,
        versao_origem_id=versao_origem_id,
        alteracoes=lote,
        motivo_codigo=motivo_codigo,
        apontamento_id=apontamento_id,
    )

    return {
        "status": "ok",
        "versao_origem_id": versao_origem_id,
        "motivo_codigo": motivo_codigo,
        "incluiu_revenda": bool(incluir_revenda),
        "cfops_usados": cfops,
        "csts_origem": csts_origem,
        "grupos_ncm": grupos_ncm,
        "candidatos_sql": len(candidatos),
        "pulou_sem_ncm": pulou_sem_ncm,
        "candidatos_pos_catalogo": len(ids_ok),
        "total_alterado": int(res.get("total_alterado") or 0),
        "total_ignorado_pf": int(res.get("total_ignorado_pf") or 0),
        "total_erros": int(res.get("total_erros") or 0),
        "erros_detalhe": res.get("erros_detalhe") or [],
    }



def _carregar_prefixos_ncm_embalagem(db: Session) -> List[str]:
    """
    Lê do catálogo fiscal (fiscal_grupo/fiscal_grupo_item) os códigos dos grupos de embalagem
    e converte em prefixos (ex.: '39*' -> '39').

    Observação:
    - Estou buscando empresa_id IS NULL (catálogo global). Se vocês usam por empresa, dá pra evoluir depois.
    """
    slugs = ("NCM_EMBALAGEM_39", "NCM_EMBALAGEM_48", "NCM_EMBALAGEM_73")

    rows = db.execute(
        text("""
            SELECT gi.codigo
            FROM fiscal_grupo g
            JOIN fiscal_grupo_item gi ON gi.grupo_id = g.id
            WHERE g.slug IN :slugs
              AND g.ativo = 1
              AND gi.ativo = 1
              AND (gi.empresa_id IS NULL)
        """),
        {"slugs": slugs},
    ).fetchall()

    codes = [str(r[0]).strip() for r in rows if r and r[0]]
    prefixes = []
    for c in codes:
        c = c.replace(".", "").strip()
        if not c:
            continue
        if c.endswith("*"):
            prefixes.append(c[:-1])
        else:
            prefixes.append(c)
    # dedup mantendo ordem
    out, seen = [], set()
    for p in prefixes:
        if p in seen:
            continue
        seen.add(p)
        out.append(p)
    return out or ["39", "48", "73"]  # fallback seguro




def _detectar_producao_interna_super(
    registros_db: Sequence[EfdRegistro],
    cat: Any
) -> Dict[str, Any]:
    """
    Detector robusto de produção interna para SUP (supermercado).

    Camadas:
    1) Forte: CFOP de saída de produção (C190)
    2) Média: NCM de setores produtivos (0200)
    3) Fraca: palavras-chave na descrição (0200)

    Retorna flags auditáveis e explicativas.
    """

    flags: Dict[str, Any] = {
        "tem_producao_interna": False,
        "motivos": [],
        "evidencias": [],
        "sum_saida_producao": Decimal("0"),
        "cfops_saida_producao": [],
        "qtd_0200_total": 0,
        "qtd_0200_padaria_rot": 0,
        "qtd_0200_acougue": 0,
        "qtd_desc_keywords": 0,
        "erros_detectados": [],
    }

    try:
        # ==========================================================
        # 🥇 CAMADA FORTE — CFOP de saída de produção (C190)
        # ==========================================================
        cfops_detectados = set()
        soma = Decimal("0")

        for r in registros_db:
            if (r.reg or "").strip() != "C190":
                continue

            d = (r.conteudo_json or {}).get("dados") or []

            cfop = ""
            for cand in d:
                s = str(cand or "").strip()
                if len(s) == 4 and s.isdigit() and s[0] in ("5", "6", "7"):
                    cfop = s
                    break

            if not cfop:
                continue

            if not RegraBase.cfop_match_static(cat, "SUP_CFOP_SAIDA_PRODUCAO", cfop):
                continue

            val = Decimal("0")
            for cand in d:
                try:
                    v = RegraBase.dec_br(cand)
                    if v and v > 0:
                        val = v
                        break
                except Exception:
                    continue

            soma += val
            cfops_detectados.add(cfop)

        if soma > 0:
            flags["tem_producao_interna"] = True
            flags["sum_saida_producao"] = str(soma)
            flags["cfops_saida_producao"] = sorted(cfops_detectados)
            flags["motivos"].append("cfop_saida_producao_detectado")
            flags["evidencias"].append(
                f"C190 com CFOP(s) {sorted(cfops_detectados)} somando R$ {soma}"
            )
            return flags  # Forte = encerra aqui


        # ==========================================================
        # 🥈 CAMADA MÉDIA — NCM setor produtivo (0200)
        # ==========================================================
        qtd_total = 0
        qtd_pad = 0
        qtd_ac = 0

        for r in registros_db:
            if (r.reg or "").strip() != "0200":
                continue

            qtd_total += 1
            d = (r.conteudo_json or {}).get("dados") or []

            ncm_raw = str(d[6] or "").strip() if len(d) > 6 else ""
            ncm = "".join(ch for ch in ncm_raw if ch.isdigit())
            if not ncm:
                continue

            if RegraBase.ncm_match_static(cat, "SUP_NCM_PADARIA_ROTISSERIA", ncm):
                qtd_pad += 1

            if RegraBase.ncm_match_static(cat, "SUP_NCM_ACOUGUE", ncm):
                qtd_ac += 1

        flags["qtd_0200_total"] = qtd_total
        flags["qtd_0200_padaria_rot"] = qtd_pad
        flags["qtd_0200_acougue"] = qtd_ac

        if (qtd_pad + qtd_ac) >= 2:
            flags["tem_producao_interna"] = True
            flags["motivos"].append("ncm_setores_produtivos_detectados")
            flags["evidencias"].append(
                f"0200 com {qtd_pad} itens padaria/rotisseria e {qtd_ac} açougue"
            )


        # ==========================================================
        # 🥉 CAMADA FRACA — Palavras-chave em descrição (0200)
        # ==========================================================
        KEYWORDS = (
            "PAO", "PÃO", "PADARIA", "BOLO", "MASSA",
            "ROTISSERIA", "FRANGO ASSADO",
            "CARNE", "LINGUICA", "MOIDA"
        )

        qtd_desc = 0

        for r in registros_db:
            if (r.reg or "").strip() != "0200":
                continue

            d = (r.conteudo_json or {}).get("dados") or []
            descricao = str(d[1] or "").upper() if len(d) > 1 else ""

            if any(k in descricao for k in KEYWORDS):
                qtd_desc += 1

        flags["qtd_desc_keywords"] = qtd_desc

        # descrição sozinha NÃO ativa
        if qtd_desc >= 10:
            flags["motivos"].append("descricao_setor_produtivo_detectada")
            flags["evidencias"].append(
                f"{qtd_desc} descrições sugerem setor produtivo"
            )

        return flags

    except Exception as e:
        flags["erros_detectados"].append(str(e))
        flags["motivos"].append("erro_no_detector_producao_interna")
        flags["evidencias"].append(traceback.format_exc())
        return flags


def _map_cod_item_para_ncm_0200(
    db: Session,
    *,
    versao_id: int,
    cod_items: List[str],
) -> Dict[str, str]:
    """
    Busca NCM no 0200 (dados[6]) para uma lista de COD_ITEM (dados[0]).
    Retorna dict {cod_item: ncm_digits}.
    """
    if not cod_items:
        return {}

    sql = """
        SELECT
            JSON_UNQUOTE(JSON_EXTRACT(conteudo_json, '$.dados[0]')) AS cod_item,
            REPLACE(JSON_UNQUOTE(JSON_EXTRACT(conteudo_json, '$.dados[6]')), '.', '') AS ncm_raw
        FROM efd_registro
        WHERE versao_id = :versao_id
          AND reg = '0200'
          AND JSON_UNQUOTE(JSON_EXTRACT(conteudo_json, '$.dados[0]')) IN :cod_items
    """
    rows = db.execute(
        text(sql),
        {"versao_id": int(versao_id), "cod_items": tuple(cod_items)},
    ).fetchall()

    out: Dict[str, str] = {}
    for cod_item, ncm_raw in rows:
        cod_item = str(cod_item or "").strip()
        ncm = "".join(ch for ch in str(ncm_raw or "") if ch.isdigit())
        if cod_item and ncm:
            out[cod_item] = ncm
    return out

def _buscar_candidatos_c170_cfop_cst(
    db: Session,
    *,
    versao_id: int,
    cfops: List[str],
    csts_origem: List[str],
) -> List[Tuple[int, str]]:
    """
    Retorna lista de (c170_id, cod_item) candidatos, filtrando por CFOP e CST origem no SQL.
    COD_ITEM é usado depois pra resolver NCM no 0200.
    """
    if not cfops:
        return []
    if not csts_origem:
        return []

    sql = """
        SELECT
          id AS c170_id,
          JSON_UNQUOTE(JSON_EXTRACT(conteudo_json, '$.dados[1]')) AS cod_item
        FROM efd_registro
        WHERE versao_id = :versao_id
          AND reg = 'C170'
          AND JSON_UNQUOTE(JSON_EXTRACT(conteudo_json, '$.dados[9]')) IN :cfops
          AND (
              JSON_UNQUOTE(JSON_EXTRACT(conteudo_json, '$.dados[23]')) IN :csts
              OR
              JSON_UNQUOTE(JSON_EXTRACT(conteudo_json, '$.dados[29]')) IN :csts
          )
    """

    rows = db.execute(
        text(sql),
        {"versao_id": int(versao_id), "cfops": tuple(cfops), "csts": tuple(csts_origem)},
    ).fetchall()

    out: List[Tuple[int, str]] = []
    for c170_id, cod_item in rows:
        rid = int(c170_id or 0)
        cod = str(cod_item or "").strip()
        if rid > 0 and cod:
            out.append((rid, cod))
    return out
