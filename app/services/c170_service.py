from __future__ import annotations
from typing import Dict, Any, List, Optional
from sqlalchemy.orm import Session
from app.fiscal.settings_fiscais import CSTS_TRIB_NCUM
from app.db.models.efd_registro import EfdRegistro
from app.db.models.efd_revisao import EfdRevisao
from sqlalchemy import func, or_, and_
from app.sped.blocoC.c100_utils import patch_c100_totais_imposto, salvar_revisao_c100_automatica
from app.sped.blocoC.c170_utils import patch_c170_campos, _validar_linha_c170
from app.sped.formatter import formatar_linha
from app.sped.logic.consolidador import (
    _get_dados,
    calcular_totais_filhos,norm,
    popular_pai_id, eh_pf_por_c100, ensure_len  # Importante: deve estar no seu consolidador.py
)


def revisar_c170(
    db: Session,
    *,
    registro_id: int,
    versao_origem_id: int,
    cfop: Optional[str],
    cst_pis: Optional[str],
    cst_cofins: Optional[str],
    motivo_codigo: str,
    apontamento_id: Optional[int] = None,
) -> Dict[str, Any]:
    """
    Realiza a revisão de um registro C170 (CNPJ já validado no chamador).
    Blindado contra registros com poucos campos (IndexError).
    """

    cfop = norm(cfop)
    cst_pis = norm(cst_pis)
    cst_cofins = norm(cst_cofins)

    # 1) Localiza o registro na versão certa
    r = (
        db.query(EfdRegistro)
        .filter(
            EfdRegistro.id == int(registro_id),
            EfdRegistro.versao_id == int(versao_origem_id),
            EfdRegistro.reg == "C170",
        )
        .first()
    )
    if not r:
        raise ValueError("Registro C170 não encontrado na versão informada.")

    dados_originais = _get_dados(r) or []

    # 2) Blindagem: garante tamanho mínimo para índices usados
    # CFOP (9), CST_PIS (23), CST_COFINS (29)
    for idx in (9, 23, 29):
        ensure_len(dados_originais, idx)

    # 3) Aplica patch
    novos_campos = patch_c170_campos(
        dados_originais,
        cfop=cfop,
        cst_pis=cst_pis,
        cst_cofins=cst_cofins,
    )

    # blindagem extra
    for idx in (9, 23, 29):
        ensure_len(novos_campos, idx)

    # 4) Formata linha nova + valida
    linha_nova = formatar_linha("C170", novos_campos).strip()
    linha_nova = _validar_linha_c170(linha_nova)

    payload_rev = {
        "linha_referencia": int(getattr(r, "linha", 0)),
        "linha_nova": linha_nova,
        "detalhe": {
            "tipo": "PATCH_C170_FINAL",
            "set": {
                "cfop": cfop,
                "cst_pis": cst_pis,
                "cst_cofins": cst_cofins,
                "pf": False,
            },
        },
    }

    # 5) Persistência (apaga só PENDENTES da mesma linha/registro)
    db.query(EfdRevisao).filter(
        EfdRevisao.versao_origem_id == int(versao_origem_id),
        EfdRevisao.versao_revisada_id.is_(None),
        EfdRevisao.registro_id == int(r.id),
        EfdRevisao.reg == "C170",
        EfdRevisao.acao == "REPLACE_LINE",
    ).delete(synchronize_session=False)

    db.add(
        EfdRevisao(
            versao_origem_id=int(versao_origem_id),
            versao_revisada_id=None,
            registro_id=int(r.id),
            reg="C170",
            acao="REPLACE_LINE",
            revisao_json=payload_rev,
            motivo_codigo=str(motivo_codigo),
            apontamento_id=apontamento_id,
        )
    )

    db.flush()
    return {"status": "alterado", "registro_id": int(r.id)}


# =====================================================================
# Revisão em lote C170 (+ consolidação C100)
# =====================================================================
def revisar_c170_lote(
    db: Session,
    *,
    versao_origem_id: int,
    alteracoes: List[Dict[str, Any]],
    motivo_codigo: str,
    apontamento_id: Optional[int] = None,
) -> Dict[str, Any]:
    """
    Executa a revisão em lote dos registros C170 e consolida os totais nos pais (C100).
    Implementa contagem de registros ignorados por trava de CPF.
    """
    import traceback

    resultados: List[Dict[str, Any]] = []
    c100_afetados = set()

    total_alterado = 0
    total_ignorado_pf = 0
    total_erros = 0
    erros_detalhe: List[Dict[str, Any]] = []  # top 10

    # Garante hierarquia pai_id populada
    popular_pai_id(db, versao_origem_id)

    for item in alteracoes:
        reg_id = item.get("registro_id")
        try:
            # --- validação defensiva do input ---
            if reg_id is None:
                raise ValueError("Item sem registro_id")

            reg_db = db.get(EfdRegistro, int(reg_id))
            if not reg_db:
                raise ValueError("Registro não encontrado no banco")

            # ✅ valida versão + tipo de registro cedo (evita ruído)
            if int(reg_db.versao_id) != int(versao_origem_id):
                raise ValueError("Registro não pertence à versão informada")
            if (reg_db.reg or "").strip().upper() != "C170":
                raise ValueError("Registro_id não é C170")

            # 🛡️ TRAVA PF
            if reg_db.pai_id:
                is_pf = eh_pf_por_c100(
                    db,
                    versao_id=int(versao_origem_id),
                    registro_id=int(reg_db.id),
                )
                if is_pf:
                    total_ignorado_pf += 1
                    resultados.append({
                        "status": "ignorado_pf",
                        "registro_id": int(reg_db.id),
                        "c100_id": int(reg_db.pai_id),
                        "msg": "Ignorado: participante PF (CPF) não gera crédito."
                    })
                    continue

            # ✅ processa revisão
            res = revisar_c170(
                db,
                registro_id=int(reg_db.id),
                versao_origem_id=int(versao_origem_id),
                cfop=item.get("cfop"),
                cst_pis=item.get("cst_pis"),
                cst_cofins=item.get("cst_cofins"),
                motivo_codigo=motivo_codigo,
                apontamento_id=apontamento_id,
            )

            resultados.append(res)

            if res.get("status") == "alterado":
                total_alterado += 1
                if reg_db.pai_id:
                    c100_afetados.add(int(reg_db.pai_id))

        except Exception as e:
            total_erros += 1
            err = str(e)
            resultados.append({"error": err, "registro_id": reg_id})

            # log detalhado
            print("❌ ERRO LOTE registro_id=", reg_id, "err=", err)
            print(traceback.format_exc())

            if len(erros_detalhe) < 10:
                erros_detalhe.append({"registro_id": reg_id, "erro": err})

    print(
        f"✅ SUCESSO LOTE: Alterados (CNPJ)={total_alterado} | "
        f"Ignorados (CPF)={total_ignorado_pf} | Erros={total_erros}"
    )
    print("LOTE> c100_afetados=", len(c100_afetados), "ex=", list(c100_afetados)[:5])

    # --- CONSOLIDAÇÃO C100 ---
    for c100_id in c100_afetados:
        try:
            total_pis, total_cofins = calcular_totais_filhos(db, versao_origem_id, c100_id)

            reg_c100 = db.get(EfdRegistro, int(c100_id))
            if not reg_c100:
                continue

            dados_c100 = _get_dados(reg_c100)
            campos_atualizados = patch_c100_totais_imposto(dados_c100, total_pis, total_cofins)

            salvar_revisao_c100_automatica(
                db,
                versao_origem_id=int(versao_origem_id),
                motivo_codigo=f"{motivo_codigo}_AUTO_SUM",
                reg_c100=reg_c100,
                novos_dados=campos_atualizados,
                apontamento_id=apontamento_id,
            )

        except Exception as e:
            print(f"❌ Erro na consolidação C100 {c100_id}: {e}")
            print(traceback.format_exc())

    db.flush()

    return {
        "status": "ok",
        "total_alterado": total_alterado,
        "total_ignorado_pf": total_ignorado_pf,
        "total_erros": total_erros,
        "erros_detalhe": erros_detalhe,
        "detalhes": resultados,
    }



# =====================================================================
# Revisão global (por filtros origem) -> monta lote e executa
# =====================================================================
def revisar_c170_global(
    db: Session,
    *,
    versao_origem_id: int,
    filtros_origem: Dict[str, Any],
    valores_novos: Dict[str, Any],
    motivo_codigo: str,
    apontamento_id: Optional[int] = None,
) -> Dict[str, Any]:
    # 1) Garante hierarquia (pai_id)
    popular_pai_id(db, versao_origem_id)

    # 2) Query base (ATENÇÃO: filtra origem SEM overlay pendente)
    query = (
        db.query(EfdRegistro)
        .filter(
            EfdRegistro.versao_id == int(versao_origem_id),
            EfdRegistro.reg == "C170",
        )
    )

    cfop_f = str(filtros_origem.get("cfop") or "").strip()
    cst_pis_f = str(filtros_origem.get("cst_pis") or "").strip()

    # JSON: CFOP = dados[9], CST_PIS = dados[23]
    if cfop_f:
        query = query.filter(
            func.json_unquote(func.json_extract(EfdRegistro.conteudo_json, "$.dados[9]")) == cfop_f
        )

    if cst_pis_f:
        query = query.filter(
            func.json_unquote(func.json_extract(EfdRegistro.conteudo_json, "$.dados[23]")) == cst_pis_f
        )

    registros = query.all()
    if not registros:
        return {
            "status": "vazio",
            "escopo_filtro": "origem_sem_overlay",
            "candidatos": 0,
            "total_alterado": 0,
            "total_ignorado_pf": 0,
            "total_erros": 0,
        }

    # 3) Monta lote com novos valores (None => não altera aquele campo)
    novo_cfop = valores_novos.get("cfop") if valores_novos.get("cfop") not in ("", None) else None
    novo_pis = valores_novos.get("cst_pis") if valores_novos.get("cst_pis") not in ("", None) else None
    novo_cof = valores_novos.get("cst_cofins") if valores_novos.get("cst_cofins") not in ("", None) else None

    lote: List[Dict[str, Any]] = []
    for r in registros:
        lote.append({
            "registro_id": int(r.id),
            "cfop": str(novo_cfop).strip() if novo_cfop is not None else None,
            "cst_pis": str(novo_pis).strip() if novo_pis is not None else None,
            "cst_cofins": str(novo_cof).strip() if novo_cof is not None else None,
        })

    # 4) Executa lote
    res_lote = revisar_c170_lote(
        db,
        versao_origem_id=int(versao_origem_id),
        alteracoes=lote,
        motivo_codigo=motivo_codigo,
        apontamento_id=apontamento_id,
    )

    # 5) Retorna contadores reais (nomes consistentes)
    alterados = int(res_lote.get("total_alterado") or 0)
    ignorados_pf = int(res_lote.get("total_ignorado_pf") or 0)
    erros = int(res_lote.get("total_erros") or 0)

    return {
        "status": "ok",
        "escopo_filtro": "origem_sem_overlay",
        "candidatos": len(registros),
        "total_alterado": alterados,
        "total_ignorado_pf": ignorados_pf,
        "total_erros": erros,
        "detalhes": res_lote,
    }



def aplicar_correcao_ind_agro_cst51(
    db: Session,
    *,
    versao_origem_id: int,
    # ✅ NOVO: por padrão NÃO corrige revenda (1102/2102/3102)
    incluir_revenda: bool = False,
    # ✅ NOVO: opcionalmente obriga NCM agro via catálogo (match por prefixo)
    # Se vazio/None => não filtra por NCM (mais permissivo)
    ncm_prefixos_permitidos: Optional[List[str]] = None,
    # CSTs de origem (sem crédito hoje)
    csts_origem: Optional[List[str]] = None,
    apontamento_id: Optional[int] = None,
    motivo_codigo: str = "IND_AGRO_V1",
) -> Dict[str, Any]:
    """
    Correção automática (conservadora por padrão) para tese IND_AGRO:
      - Seleciona C170 por CFOP de ENTRADA
        * industrialização: 1101/2101/3101 (sempre)
        * revenda:         1102/2102/3102 (opcional, incluir_revenda=True)
      - (Opcional) Filtra NCM por prefixos permitidos (ex.: famílias agro)
      - Filtra CSTs de origem (sem crédito hoje)
      - Aplica CST_PIS=51 e CST_COFINS=51 via revisar_c170_lote
      - Bloco M será recalculado no export (base por VL_ITEM + CST_CREDITO/CSTS_TRIB_NCUM)
    """

    versao_origem_id = int(versao_origem_id)

    # Guard-rail: garante que 51 é CST de crédito no teu settings
    if "51" not in set(CSTS_TRIB_NCUM or set()):
        raise ValueError("settings_fiscais.CSTS_TRIB_NCUM não contém '51'.")

    # CFOPs de entrada
    cfops_ind = ["1101", "2101", "3101"]
    cfops_rev = ["1102", "2102", "3102"]
    cfops = list(cfops_ind) + (list(cfops_rev) if incluir_revenda else [])

    if not cfops:
        return {"status": "vazio", "msg": "Sem CFOPs após filtros.", "candidatos": 0}

    # CSTs de origem (conservador/agressivo conforme você já vinha usando)
    if not csts_origem:
        csts_origem = ["70", "73", "75", "98", "99", "06", "07", "08"]
    csts_origem = [str(x).strip() for x in csts_origem if str(x).strip()]

    # Garante pai_id (para trava PF no lote)
    popular_pai_id(db, versao_origem_id)

    # Expressões JSON (layout C170)
    cfop_expr = func.json_unquote(func.json_extract(EfdRegistro.conteudo_json, "$.dados[9]"))
    cst_pis_expr = func.json_unquote(func.json_extract(EfdRegistro.conteudo_json, "$.dados[23]"))
    cst_cof_expr = func.json_unquote(func.json_extract(EfdRegistro.conteudo_json, "$.dados[29]"))

    # NCM: no seu pipeline você já enriquece NCM via meta/0200,
    # mas aqui estamos no banco (EfdRegistro). Se o seu parser grava COD_NCM no JSON do C170,
    # você pode ajustar o índice. Como isso varia, deixei opcional por prefixos e "best-effort".
    # Se não conseguir extrair, não filtra (para não quebrar).
    def _try_ncm_expr() -> Optional[Any]:
        # Tentativas comuns (ajuste conforme seu parser):
        # Algumas implementações colocam NCM no C170, outras não (vem do 0200).
        for idx in (12, 11, 13, 10):
            try:
                return func.json_unquote(func.json_extract(EfdRegistro.conteudo_json, f"$.dados[{idx}]"))
            except Exception:
                continue
        return None

    ncm_expr = _try_ncm_expr()

    # Query candidatos por CFOP + CST
    cfop_filters = [cfop_expr == c for c in cfops]

    q = (
        db.query(EfdRegistro.id)
        .filter(
            EfdRegistro.versao_id == versao_origem_id,
            EfdRegistro.reg == "C170",
            or_(*cfop_filters),
        )
        .filter((cst_pis_expr.in_(csts_origem)) | (cst_cof_expr.in_(csts_origem)))
    )

    # (Opcional) filtro por NCM prefixo
    ncm_prefixos = [p.strip().replace(".", "") for p in (ncm_prefixos_permitidos or []) if str(p).strip()]
    if ncm_prefixos and ncm_expr is not None:
        # Normaliza NCM removendo ponto; se vier vazio, não passa no filtro
        # MySQL: REPLACE(expr,'.','')
        ncm_limpo = func.replace(ncm_expr, ".", "")
        ncm_like_filters = [ncm_limpo.like(f"{p}%") for p in ncm_prefixos]
        q = q.filter(or_(*ncm_like_filters))

    try:
        ids = [int(x[0]) for x in q.all()]
    except Exception as e:
        print("❌ [IND_AGRO_CORR] ERRO query candidatos:", repr(e))
        raise

    if not ids:
        return {"status": "vazio", "candidatos": 0}

    # Monta lote: só CSTs (não mexe no CFOP)
    lote = [{"registro_id": rid, "cfop": None, "cst_pis": "51", "cst_cofins": "51"} for rid in ids]

    # Aplica via teu service robusto (trava PF + consolidação C100)
    res = revisar_c170_lote(
        db,
        versao_origem_id=versao_origem_id,
        alteracoes=lote,
        motivo_codigo=motivo_codigo,  # ✅ mantém vínculo com a regra/código
        apontamento_id=apontamento_id,
    )

    out = {
        "status": "ok",
        "versao_origem_id": int(versao_origem_id),
        "motivo_codigo": str(motivo_codigo),
        "incluiu_revenda": bool(incluir_revenda),
        "cfops_usados": cfops,
        "csts_origem": csts_origem,
        "filtrou_ncm_prefixos": ncm_prefixos,
        "candidatos": len(ids),
        "total_alterado": int(res.get("total_alterado") or 0),
        "total_ignorado_pf": int(res.get("total_ignorado_pf") or 0),
        "total_erros": int(res.get("total_erros") or 0),
        "erros_detalhe": res.get("erros_detalhe") or [],
    }

    print("[IND_AGRO_CORR] RESUMO:", out)
    return out


# ============================================================
# Correção automática (Café) — catálogo NCM família 09 + CFOP grupo
# ============================================================
def aplicar_correcao_ind_cafe_cst51(
    db: Session,
    *,
    versao_origem_id: int,
    incluir_revenda: bool = True,
    csts_origem: Optional[List[str]] = None,
    apontamento_id: Optional[int] = None,
) -> Dict[str, Any]:
    """
    Correção automática (determinística) para tese IND_CAFE:
      - Seleciona C170 por CFOP do grupo (1101/1102/2101/2102/3101/3102)
      - Filtra apenas itens cujo NCM (0200) bate família 09 (café) via catálogo/token (ex: 09*)
      - Filtra CSTs de origem (sem crédito)
      - Aplica CST_PIS=51 e CST_COFINS=51 via revisar_c170_lote
      - C100 é consolidado automaticamente no revisar_c170_lote
    """

    versao_origem_id = int(versao_origem_id)

    # Guard-rail: garante que 51 é permitido como CST de crédito no settings
    if "51" not in set(CSTS_TRIB_NCUM or set()):
        raise ValueError("settings_fiscais.CSTS_TRIB_NCUM não contém '51'.")

    # Índices do seu parser (já confirmados no seu service)
    IDX_CFOP = 9
    IDX_CST_PIS = 23
    IDX_CST_COFINS = 29

    # ⚠️ Ajuste aqui se COD_ITEM não for [1] no seu parser
    IDX_COD_ITEM = 1     # <- se necessário, trocar

    # CFOPs do seu grupo de café (industrialização + revenda)
    cfops = ["1101", "1102", "2101", "2102", "3101", "3102"]
    if not incluir_revenda:
        cfops = [c for c in cfops if c not in ("1102", "2102", "3102")]

    if not cfops:
        return {"status": "vazio", "msg": "Sem CFOPs após filtros.", "candidatos": 0}

    # CSTs de origem (agressivo por padrão — igual você já faz)
    if not csts_origem:
        csts_origem = ["70", "73", "75", "98", "99", "06", "07", "08"]
    csts_origem = [str(x).strip() for x in csts_origem if str(x).strip()]

    # Garante pai_id (trava PF no lote)
    popular_pai_id(db, versao_origem_id)

    # Expressões JSON
    cfop_expr = func.json_unquote(func.json_extract(EfdRegistro.conteudo_json, f"$.dados[{IDX_CFOP}]"))
    cst_pis_expr = func.json_unquote(func.json_extract(EfdRegistro.conteudo_json, f"$.dados[{IDX_CST_PIS}]"))
    cst_cof_expr = func.json_unquote(func.json_extract(EfdRegistro.conteudo_json, f"$.dados[{IDX_CST_COFINS}]"))
    cod_item_expr = func.json_unquote(func.json_extract(EfdRegistro.conteudo_json, f"$.dados[{IDX_COD_ITEM}]"))

    # --- join C170 -> 0200 (mesma versao) ---
    C0200 = db.query(EfdRegistro).subquery()
    # melhor: alias explícito
    from sqlalchemy.orm import aliased
    r0200 = aliased(EfdRegistro)

    # NCM no 0200: índice 6 (padrão que você já usa no scanner)
    ncm_expr_0200 = func.json_unquote(func.json_extract(r0200.conteudo_json, "$.dados[6]"))

    # Match família 09: aqui eu uso LIKE '09%' (token 09*)
    # Se seu catálogo permite padrões mais complexos, dá pra sofisticar depois.
    ncm_like_cafe = ncm_expr_0200.like("09%")

    # filtros CFOP
    cfop_filters = [cfop_expr == c for c in cfops]

    # Query candidatos:
    # - C170 da versão
    # - CFOP do grupo
    # - CSTs origem sem crédito (pis ou cofins)
    # - COD_ITEM linka com 0200 e NCM começa com 09
    q = (
        db.query(EfdRegistro.id)
        .join(
            r0200,
            and_(
                r0200.versao_id == EfdRegistro.versao_id,
                r0200.reg == "0200",
                func.json_unquote(func.json_extract(r0200.conteudo_json, "$.dados[0]")) == cod_item_expr,
            ),
        )
        .filter(
            EfdRegistro.versao_id == versao_origem_id,
            EfdRegistro.reg == "C170",
            or_(*cfop_filters),
            ncm_like_cafe,
        )
        .filter(
            (cst_pis_expr.in_(csts_origem)) | (cst_cof_expr.in_(csts_origem))
        )
    )

    ids = [int(x[0]) for x in q.all()]
    if not ids:
        return {"status": "vazio", "candidatos": 0}

    # Monta lote: só CSTs (não mexe no CFOP)
    lote = [{"registro_id": rid, "cfop": None, "cst_pis": "51", "cst_cofins": "51"} for rid in ids]

    # Aplica via seu service robusto (com trava PF + consolidação C100)
    res = revisar_c170_lote(
        db,
        versao_origem_id=versao_origem_id,
        alteracoes=lote,
        motivo_codigo="IND_CAFE_V1",
        apontamento_id=apontamento_id,
    )

    return {
        "status": "ok",
        "candidatos": len(ids),
        "total_alterado": int(res.get("total_alterado") or 0),
        "total_ignorado_pf": int(res.get("total_ignorado_pf") or 0),
        "total_erros": int(res.get("total_erros") or 0),
        "erros_detalhe": res.get("erros_detalhe") or [],
    }