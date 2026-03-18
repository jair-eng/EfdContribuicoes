from __future__ import annotations

from app.services.service_revisoes_insert import aplicar_revisoes_insert
from app.sped.revisao_overlay import LinhaLogica, aplicar_revisoes_replace_line
from sqlalchemy.orm import Session
from app.db.models.efd_registro import EfdRegistro
from app.db.models.efd_revisao import EfdRevisao
import logging

log = logging.getLogger(__name__)



def carregar_linhas_logicas_com_revisoes(
    db: Session,
    *,
    versao_origem_id: int,
    versao_final_id: int | None = None,
) -> list[LinhaLogica]:
    log.info(
        "LOADER revisoes | origem=%s final=%s",
        versao_origem_id,
        versao_final_id,
    )

    regs = (
        db.query(EfdRegistro)
        .filter(EfdRegistro.versao_id == int(versao_origem_id))
        .order_by(EfdRegistro.linha.asc())
        .all()
    )

    rid_to_pai: dict[int, int] = {int(r.id): int(getattr(r, "pai_id", 0) or 0) for r in regs}
    rid_to_reg: dict[int, str] = {int(r.id): str(getattr(r, "reg", "") or "").strip() for r in regs}

    linhas_originais: list[LinhaLogica] = [LinhaLogica.from_efd_registro(r) for r in regs]

    if not linhas_originais:
        log.debug("LOADER revisoes sem linhas base | origem=%s", versao_origem_id)
        return []

    q = (
        db.query(EfdRevisao)
        .filter(EfdRevisao.acao.in_(["REPLACE_LINE", "DELETE"]))
    )

    if versao_final_id is not None:
        q = q.filter(EfdRevisao.versao_revisada_id == int(versao_final_id))
    else:
        q = q.filter(EfdRevisao.versao_origem_id == int(versao_origem_id))
        q = q.filter(EfdRevisao.versao_revisada_id.is_(None))

    revisoes_db = q.order_by(EfdRevisao.created_at.asc(), EfdRevisao.id.asc()).all()

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
                "linha": linha_txt,
                "revisao_json": j,
            })

    linhas_finais = aplicar_revisoes_replace_line(
        linhas_originais=linhas_originais,
        revisoes=revisoes_dict,
        preferir_ultima=True,
    )

    alteradas = [l for l in linhas_finais if l.origem == "REVISAO"]

    log.debug(
        "LOADER revisoes stats | origem=%s final=%s linhas=%s revisoes=%s alteradas=%s",
        versao_origem_id,
        versao_final_id,
        len(linhas_finais),
        len(revisoes_dict),
        len(alteradas),
    )

    if alteradas:
        ex = alteradas[0]
        log.debug(
            "LOADER revisoes exemplo | registro_id=%s linha=%s reg=%s origem=%s revisao_id=%s",
            ex.registro_id,
            ex.linha,
            ex.reg,
            ex.origem,
            getattr(ex, "revisao_id", None),
        )

        if str(ex.reg).upper() == "C170":
            log.debug(
                "LOADER revisoes exemplo C170 | cst_pis=%s cst_cof=%s",
                ex.dados[23] if len(ex.dados) > 23 else None,
                ex.dados[29] if len(ex.dados) > 29 else None,
            )

    rid_debug = 1025
    x = next((l for l in linhas_finais if int(getattr(l, "registro_id", 0) or 0) == rid_debug), None)
    if x:
        log.debug(
            "LOADER revisoes rid_debug | rid=%s reg=%s origem=%s revisao_id=%s",
            rid_debug,
            x.reg,
            x.origem,
            getattr(x, "revisao_id", None),
        )

    for l in linhas_finais:
        try:
            rid = int(getattr(l, "registro_id", 0) or 0)
            if rid > 0:
                pai = int(rid_to_pai.get(rid, 0) or 0)
                if pai > 0:
                    setattr(l, "pai_id", pai)

                if not getattr(l, "reg", None):
                    setattr(l, "reg", rid_to_reg.get(rid, ""))
        except Exception:
            pass

    return linhas_finais


def carregar_linhas_logicas_com_revisoes_e_insert(
    db: Session,
    *,
    versao_origem_id: int,
    versao_final_id: int | None = None,
) -> list[LinhaLogica]:
    log.info(
        "LOADER revisoes+insert | origem=%s final=%s",
        versao_origem_id,
        versao_final_id,
    )

    regs = (
        db.query(EfdRegistro)
        .filter(EfdRegistro.versao_id == int(versao_origem_id))
        .order_by(EfdRegistro.linha.asc())
        .all()
    )

    rid_to_pai: dict[int, int] = {int(r.id): int(getattr(r, "pai_id", 0) or 0) for r in regs}
    rid_to_reg: dict[int, str] = {int(r.id): str(getattr(r, "reg", "") or "").strip() for r in regs}

    linhas_originais: list[LinhaLogica] = [LinhaLogica.from_efd_registro(r) for r in regs]

    if not linhas_originais:
        log.debug("LOADER revisoes+insert sem linhas base | origem=%s", versao_origem_id)
        return []

    q = db.query(EfdRevisao).filter(
        EfdRevisao.acao.in_(["REPLACE_LINE", "DELETE", "INSERT_AFTER", "INSERT_BEFORE"])
    )

    if versao_final_id is not None:
        q = q.filter(EfdRevisao.versao_revisada_id == int(versao_final_id))
    else:
        q = q.filter(EfdRevisao.versao_origem_id == int(versao_origem_id))
        q = q.filter(EfdRevisao.versao_revisada_id.is_(None))

    revisoes_db = q.order_by(EfdRevisao.created_at.asc(), EfdRevisao.id.asc()).all()

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
                "linha": linha_txt,
                "revisao_json": j,
            })
            continue

        if acao in {"INSERT_AFTER", "INSERT_BEFORE"}:
            linha_txt = str((j.get("linha_nova") or "")).strip()
            if not linha_txt:
                continue
            revisoes_dict.append({
                "id": int(rv.id),
                "registro_id": rid,
                "linha_num": linha_ref,
                "acao": acao,
                "linha": linha_txt,
                "revisao_json": j,
            })

    linhas_base = aplicar_revisoes_replace_line(
        linhas_originais=linhas_originais,
        revisoes=revisoes_dict,
        preferir_ultima=True,
    )

    for l in linhas_base:
        try:
            rid = int(getattr(l, "registro_id", 0) or 0)
            if rid > 0:
                pai = int(rid_to_pai.get(rid, 0) or 0)
                if pai > 0:
                    setattr(l, "pai_id", pai)

                if not getattr(l, "reg", None):
                    setattr(l, "reg", rid_to_reg.get(rid, ""))
        except Exception:
            pass

    linhas_finais = aplicar_revisoes_insert(
        linhas_base=linhas_base,
        revisoes=revisoes_dict,
        preferir_ultima=True,
    )

    rev_por_id = {int(r["id"]): r for r in revisoes_dict if int(r.get("id") or 0) > 0}

    for l in linhas_finais:
        try:
            if getattr(l, "origem", "") != "INSERIDO":
                continue
            if str(getattr(l, "reg", "")).upper() != "C170":
                continue

            pai_atual = int(getattr(l, "pai_id", 0) or 0)

            if pai_atual > 0:
                reg_pai_atual = str(rid_to_reg.get(pai_atual, "") or "").strip().upper()
                if reg_pai_atual == "C100":
                    continue

            revisao_id = int(getattr(l, "revisao_id", 0) or 0)
            rv = rev_por_id.get(revisao_id)
            if not rv:
                continue

            alvo_rid = int(rv.get("registro_id") or 0)
            if alvo_rid <= 0:
                continue

            cursor = pai_atual if pai_atual > 0 else alvo_rid

            while cursor > 0:
                reg_cursor = str(rid_to_reg.get(cursor, "") or "").strip().upper()
                if reg_cursor == "C100":
                    setattr(l, "pai_id", cursor)
                    break
                cursor = int(rid_to_pai.get(cursor, 0) or 0)

            log.debug(
                "LOADER inserido final | linha=%s revisao_id=%s pai_id=%s reg=%s dados0=%s",
                l.linha,
                revisao_id,
                getattr(l, "pai_id", None),
                l.reg,
                (l.dados[:5] if getattr(l, "dados", None) else []),
            )
        except Exception as e:
            log.warning("LOADER inserido final erro | erro=%r", e)

    qtd_insert = len([r for r in revisoes_dict if r.get("acao") in {"INSERT_AFTER", "INSERT_BEFORE"}])
    qtd_replace_delete = len([r for r in revisoes_dict if r.get("acao") in {"REPLACE_LINE", "DELETE"}])

    log.debug(
        "LOADER revisoes+insert stats | origem=%s final=%s base=%s final_linhas=%s revisoes=%s replace_delete=%s inserts=%s",
        versao_origem_id,
        versao_final_id,
        len(linhas_base),
        len(linhas_finais),
        len(revisoes_dict),
        qtd_replace_delete,
        qtd_insert,
    )

    return linhas_finais