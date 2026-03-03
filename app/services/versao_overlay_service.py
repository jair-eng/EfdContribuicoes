from __future__ import annotations
from app.sped.revisao_overlay import LinhaLogica, aplicar_revisoes_replace_line
from sqlalchemy.orm import Session
from app.db.models.efd_registro import EfdRegistro
from app.db.models.efd_revisao import EfdRevisao


def carregar_linhas_logicas_com_revisoes(
    db: Session,
    *,
    versao_origem_id: int,
    versao_final_id: int | None = None,
) -> list[LinhaLogica]:
    print("LOADER EXECUTANDO", "origem=", versao_origem_id, "final=", versao_final_id, flush=True)

    # 1) Base: registros da versão origem
    regs = (
        db.query(EfdRegistro)
        .filter(EfdRegistro.versao_id == int(versao_origem_id))
        .order_by(EfdRegistro.linha.asc())
        .all()
    )

    rid_to_pai: dict[int, int] = {int(r.id): int(getattr(r, "pai_id", 0) or 0) for r in regs}
    rid_to_reg: dict[int, str] = {int(r.id): str(getattr(r, "reg", "") or "").strip() for r in regs}

    linhas_originais: list[LinhaLogica] = [
        LinhaLogica.from_efd_registro(r) for r in regs
    ]

    if not linhas_originais:
        return []

    # 2) Busca revisões
    q = (
        db.query(EfdRevisao)
        .filter(EfdRevisao.acao.in_(["REPLACE_LINE", "DELETE"]))
    )

    if versao_final_id is not None:
        # export de versão revisada (ex: 63)
        q = q.filter(EfdRevisao.versao_revisada_id == int(versao_final_id))
    else:
        # tela de revisão (pendentes)
        q = q.filter(EfdRevisao.versao_origem_id == int(versao_origem_id))
        q = q.filter(EfdRevisao.versao_revisada_id.is_(None))

    revisoes_db = q.order_by(EfdRevisao.created_at.asc(), EfdRevisao.id.asc()).all()

    # 3) Monta revisoes_dict (COM linha_referencia)
    revisoes_dict: list[dict] = []

    for rv in revisoes_db:
        acao = str(getattr(rv, "acao", "") or "").upper()
        j = getattr(rv, "revisao_json", None) or {}

        rid = int(getattr(rv, "registro_id", 0) or 0)
        linha_ref = int((j.get("linha_referencia") or j.get("linha_num") or 0) or 0)

        if acao == "DELETE":
            revisoes_dict.append({
                "id": int(rv.id),
                "registro_id": rid,
                "linha_num": linha_ref,
                "acao": "DELETE",
                "revisao_json": j,
            })
            continue

        if acao == "REPLACE_LINE":
            linha_txt = str((j.get("linha_nova") or "")).strip()
            if not linha_txt:
                continue
            revisoes_dict.append({
                "id": int(rv.id),
                "registro_id": rid,
                "linha_num": linha_ref,
                "acao": "REPLACE_LINE",
                "linha": linha_txt,  # ✅ sempre linha_nova
                "revisao_json": j,
            })
            continue

    # 4) 🔥 APLICA OVERLAY
    linhas_finais = aplicar_revisoes_replace_line(
        linhas_originais=linhas_originais,
        revisoes=revisoes_dict,
        preferir_ultima=True,
    )

    # 🔎 DEBUG TEMPORÁRIO — É AQUI
    alteradas = [l for l in linhas_finais if l.origem == "REVISAO"]
    print("LOADER> linhas:", len(linhas_finais))
    print("LOADER> revisoes carregadas:", len(revisoes_dict))
    print("LOADER> linhas alteradas:", len(alteradas))
    if alteradas:
        ex = alteradas[0]
        print(
            "LOADER> EXEMPLO:",
            "registro_id=", ex.registro_id,
            "linha=", ex.linha,
            "reg=", ex.reg,
            "cst_pis=", ex.dados[23] if len(ex.dados) > 23 else None,
            "cst_cof=", ex.dados[29] if len(ex.dados) > 29 else None,
        )
        # DEBUG focado: confirmar vencedora do registro 1025
        rid_debug = 1025
        x = next((l for l in linhas_finais if int(getattr(l, "registro_id", 0) or 0) == rid_debug), None)
        if x:
            cst_pis = x.dados[23] if len(x.dados) > 23 else None
            cst_cof = x.dados[29] if len(x.dados) > 29 else None
            print(
                "LOADER> DEBUG rid=1025",
                "origem=", getattr(x, "origem", None),
                "revisao_id=", getattr(x, "revisao_id", None),
                "cst_pis=", cst_pis,
                "cst_cof=", cst_cof,
            )
        else:
            print("LOADER> DEBUG rid=1025 nao encontrado")

    # ✅ Enriquecer linhas com pai_id do DB (mantém hierarquia no overlay)
    for l in linhas_finais:
        try:
            rid = int(getattr(l, "registro_id", 0) or 0)
            if rid > 0:
                pai = int(rid_to_pai.get(rid, 0) or 0)
                if pai > 0:
                    setattr(l, "pai_id", pai)

                # opcional: garantir reg coerente, se precisar
                if not getattr(l, "reg", None):
                    setattr(l, "reg", rid_to_reg.get(rid, ""))
        except Exception:
            pass

    # 5) RETURN FINAL
    return linhas_finais
