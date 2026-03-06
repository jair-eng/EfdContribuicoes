from __future__ import annotations
from typing import Dict, Any, List, Optional
from sqlalchemy.orm import Session, aliased
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
    total_ignorado_sit = 0
    sit_ignorados_detalhe: List[Dict[str, Any]] = []
    total_erros = 0
    erros_detalhe: List[Dict[str, Any]] = []  # top 10

    # Garante hierarquia pai_id populada
    popular_pai_id(db, versao_origem_id)

    # ✅ mapa estrutural (estilo Foto): C170 -> último C100 anterior (por linha)
    rows_min = (
        db.query(EfdRegistro.id, EfdRegistro.reg)
        .filter(EfdRegistro.versao_id == int(versao_origem_id))
        .order_by(EfdRegistro.linha.asc())
        .all()
    )

    c170_to_c100: dict[int, int] = {}
    last_c100_id = 0
    for rid, reg in rows_min:
        reg_u = (reg or "").strip().upper()
        if reg_u == "C100":
            last_c100_id = int(rid)
        elif reg_u == "C170":
            if last_c100_id:
                c170_to_c100[int(rid)] = int(last_c100_id)

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

            # 🛡️ TRAVA COD_SIT (C100 complementar/cancelada) — não depende de pai_id
            c100_id = int(getattr(reg_db, "pai_id", 0) or 0)
            if c100_id <= 0:
                c100_id = int(c170_to_c100.get(int(reg_db.id), 0) or 0)

            if c100_id > 0:
                reg_c100 = db.get(EfdRegistro, int(c100_id))
                if reg_c100 and (reg_c100.reg or "").strip().upper() == "C100":
                    dados_c100 = _get_dados(reg_c100) or []
                    off_c100 = 1 if dados_c100 and str(dados_c100[0]).strip().upper() == "C100" else 0
                    cod_sit_raw = str(dados_c100[4 + off_c100]).strip() if len(dados_c100) > 4 + off_c100 else ""
                    cod_sit = "".join(ch for ch in cod_sit_raw if ch.isdigit()).zfill(2)

                    if cod_sit in {"06", "07"}:
                        print("🧱 BLOQUEADO COD_SIT", cod_sit, "C170=", reg_db.id, "C100=", c100_id)
                        total_ignorado_sit += 1
                        if len(sit_ignorados_detalhe) < 20:
                            sit_ignorados_detalhe.append({
                                "status": "ignorado_cod_sit",
                                "registro_id": int(reg_db.id),
                                "c100_id": int(c100_id),
                                "cod_sit": cod_sit,
                            })
                        resultados.append({
                            "status": "ignorado_cod_sit",
                            "registro_id": int(reg_db.id),
                            "c100_id": int(c100_id),
                            "msg": f"Ignorado: C100 COD_SIT={cod_sit} (complementar/cancelada)."
                        })
                        continue

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
        f"Ignorados (COD_SIT)={total_ignorado_sit} | "
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
        "total_ignorado_sit": total_ignorado_sit,
        "sit_ignorados_detalhe": sit_ignorados_detalhe,
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

