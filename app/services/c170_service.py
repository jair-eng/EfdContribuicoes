from __future__ import annotations
from typing import Any, Dict, Optional, List
from sqlalchemy.orm import Session
from sqlalchemy import func
from app.db.models.efd_registro import EfdRegistro
from app.db.models.efd_revisao import EfdRevisao
from app.sped.blocoC.c100_utils import patch_c100_totais_imposto, salvar_revisao_c100_automatica
from app.sped.blocoC.c170_utils import patch_c170_campos
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

    # 2) Blindagem: muitos C170 vêm com menos campos -> patch pode estourar índice
    # índices que você usa/mostrou:
    # CFOP (9), CST_PIS (23), CST_COFINS (29)
    for idx in (9, 23, 29):
        ensure_len(dados_originais, idx)

    # 3) Aplica patch (agora com tamanho garantido)
    novos_campos = patch_c170_campos(
        dados_originais,
        cfop=cfop,
        cst_pis=cst_pis,
        cst_cofins=cst_cofins,
    )

    # blindagem extra: se patch devolveu lista curta, garante também
    for idx in (9, 23, 29):
        ensure_len(novos_campos, idx)

    # 4) Formata linha nova
    linha_nova = formatar_linha("C170", novos_campos).strip()

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

    # 5) Persistência da revisão (apaga só as PENDENTES, não as materializadas)
    db.query(EfdRevisao).filter(
        EfdRevisao.versao_origem_id == int(versao_origem_id),
        EfdRevisao.versao_revisada_id.is_(None),  # <<< MUITO IMPORTANTE
        EfdRevisao.registro_id == int(r.id),
        EfdRevisao.reg == "C170",
        EfdRevisao.acao == "REPLACE_LINE",
    ).delete(synchronize_session=False)

    db.add(
        EfdRevisao(
            versao_origem_id=int(versao_origem_id),
            versao_revisada_id=None,              # <<< explícito
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

    resultados = []
    c100_afetados = set()

    total_alterado = 0
    total_ignorado_pf = 0
    total_erros = 0
    erros_detalhe = []  # top 10

    popular_pai_id(db, versao_origem_id)

    for item in alteracoes:
        reg_id = item.get("registro_id")
        try:
            reg_db = db.get(EfdRegistro, int(reg_id))
            if not reg_db:
                total_erros += 1
                msg = "Registro não encontrado no banco"
                resultados.append({"error": msg, "registro_id": reg_id})
                if len(erros_detalhe) < 10:
                    erros_detalhe.append({"registro_id": reg_id, "erro": msg})
                continue

            # 🛡️ TRAVA PF: chame a função com a assinatura correta
            if reg_db.pai_id:
                is_pf = eh_pf_por_c100(
                    db,
                    versao_id=int(versao_origem_id),
                    registro_id=int(reg_db.id),  # <<< AQUI é o fix
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

            # ✅ SE CHEGOU AQUI, VAI PROCESSAR
            res = revisar_c170(
                db,
                registro_id=int(reg_db.id),
                versao_origem_id=int(versao_origem_id),
                cfop=item.get("cfop"),
                cst_pis=item.get("cst_pis"),
                cst_cofins=item.get("cst_cofins"),
                motivo_codigo=motivo_codigo,
                apontamento_id=apontamento_id
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

            # log detalhado (muito importante agora)
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

    db.commit()

    return {
        "status": "ok",
        "total_alterado": total_alterado,
        "total_ignorado_pf": total_ignorado_pf,
        "total_erros": total_erros,
        "erros_detalhe": erros_detalhe,   # <<< pra você ver o motivo real
        "detalhes": resultados,
    }




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

    # 2) Query base
    query = (
        db.query(EfdRegistro)
        .filter(
            EfdRegistro.versao_id == int(versao_origem_id),
            EfdRegistro.reg == "C170",
        )
    )

    # --- filtros (backend deve aceitar str)
    cfop_f = (filtros_origem.get("cfop") or "")
    cst_pis_f = (filtros_origem.get("cst_pis") or "")

    cfop_f = str(cfop_f).strip() if cfop_f is not None else ""
    cst_pis_f = str(cst_pis_f).strip() if cst_pis_f is not None else ""

    # ATENÇÃO: isso assume que seu JSON é {"dados":[...]} e que CFOP é dados[9], CST_PIS dados[23]
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
            "candidatos": 0,
            "total_alterado": 0,
            "total_ignorado_pf": 0,
            "erros": 0,
        }

    # 3) Monta lote
    novo_cfop = (valores_novos.get("cfop") or None)
    novo_pis = (valores_novos.get("cst_pis") or None)
    novo_cof = (valores_novos.get("cst_cofins") or None)

    lote = []
    for r in registros:
        lote.append({
            "registro_id": int(r.id),
            "cfop": str(novo_cfop).strip() if novo_cfop not in (None, "") else None,
            "cst_pis": str(novo_pis).strip() if novo_pis not in (None, "") else None,
            "cst_cofins": str(novo_cof).strip() if novo_cof not in (None, "") else None,
        })

    # 4) Executa lote (a verdade está aqui)
    res_lote = revisar_c170_lote(
        db,
        versao_origem_id=int(versao_origem_id),
        alteracoes=lote,
        motivo_codigo=motivo_codigo,
    )

    # 5) Retorna contadores reais (NÃO len(registros))
    # Ajuste os nomes conforme o que seu revisar_c170_lote retorna
    alterados = int(res_lote.get("alterados") or res_lote.get("total_alterado") or 0)
    ignorados_pf = int(res_lote.get("ignorados_pf") or res_lote.get("total_ignorado_pf") or 0)
    erros = int(res_lote.get("erros") or 0)

    return {
        "status": "ok",
        "candidatos": len(registros),
        "total_alterado": alterados,
        "total_ignorado_pf": ignorados_pf,
        "erros": erros,
        "detalhes": res_lote,
    }