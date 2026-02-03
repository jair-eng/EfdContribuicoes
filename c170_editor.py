import streamlit as st
import pandas as pd
import requests
from ui_utils import api_url, post, TIMEOUT
import traceback


# =========================================================
# ########## DEBUG ##########
# Troque para False quando terminar
# =========================================================
DEBUG = False


def _d(title: str, obj):
    """Helper de debug: imprime somente quando DEBUG=True."""
    if DEBUG:
        st.write(f"########## DEBUG: {title} ##########")
        try:
            st.json(obj)
        except Exception:
            st.write(obj)


def render_editor_c170():

    st.subheader("C170 — Editor de Alta Performance (Modo Global)")

    if st.session_state.get("selected_versao_id") is None:
        st.info("Selecione uma versão na etapa '2 — Selecionar Versão'.")
        st.stop()

    versao_id = int(st.session_state.get("selected_versao_id") or 0)

    # ---------------------------------------------------------
    # PAINEL DE FILTROS
    # ---------------------------------------------------------
    with st.expander("🔍 Filtros de Busca e Escopo", expanded=True):
        f1, f2, f3 = st.columns(3)
        with f1:
            filtro_cfop = st.text_input("Filtrar CFOP atual", placeholder="ex: 1102")
        with f2:
            filtro_cst_pis = st.text_input("Filtrar CST PIS atual", placeholder="ex: 73")
        with f3:
            limit_preview = st.number_input("Preview (linhas na tela)", 10, 500, 100)

        st.caption("⚠️ O preview abaixo mostra apenas uma amostra. A 'Ação em Massa' atingirá TODO o banco.")

    # Normaliza inputs (evita espaços / comparações erradas)
    filtro_cfop_norm = (filtro_cfop or "").strip()
    filtro_cst_pis_norm = (filtro_cst_pis or "").strip()

    _d("INPUTS", {
        "versao_id": versao_id,
        "filtro_cfop_raw": filtro_cfop,
        "filtro_cst_raw": filtro_cst_pis,
        "filtro_cfop_norm": filtro_cfop_norm,
        "filtro_cst_norm": filtro_cst_pis_norm,
        "limit_preview": int(limit_preview),
    })

    # ---------------------------------------------------------
    # CARGA DE PREVIEW (preferir filtro no servidor via params)
    # ---------------------------------------------------------
    url_preview = api_url(f"/workflow/versao/{versao_id}/c170")
    params = {"limit": int(limit_preview)}

    # filtros server-side (endpoint agora suporta)
    if filtro_cfop_norm:
        params["cfop"] = filtro_cfop_norm
    if filtro_cst_pis_norm:
        params["cst_pis"] = filtro_cst_pis_norm

    _d("PREVIEW GET", {"url_preview": url_preview, "params": params})

    try:
        resp = requests.get(url_preview, params=params, timeout=TIMEOUT)
        if resp.status_code != 200:
            st.error(f"Erro ao carregar preview: {resp.text}")
            st.stop()

        payload = resp.json() or {}

        _d("PREVIEW GET RESPONSE", {
            "status_code": resp.status_code,
            "final_url": getattr(resp, "url", "(no resp.url)"),
            "payload_keys": list(payload.keys()),
            "text_head": (resp.text[:500] if resp.text else ""),
        })

        # =========================
        # META DO BACKEND (PF/PJ)
        # =========================
        totais_pf_pj = payload.get("totais_pf_pj") or None

        pj_count = None
        pf_count = None
        total_bruto = None

        if totais_pf_pj:
            pj_count = int(totais_pf_pj.get("pj_cnpj") or 0)
            pf_count = int(totais_pf_pj.get("pf_cpf") or 0)
            total_bruto = int(totais_pf_pj.get("total_bruto") or (pj_count + pf_count))

            st.markdown("### 📊 Resultado do Filtro (Banco de Dados)")
            c1, c2, c3 = st.columns(3)
            with c1:
                st.metric("Total bruto", total_bruto)
            with c2:
                st.metric("PJ (CNPJ)", pj_count)
            with c3:
                st.metric("PF (CPF)", pf_count)

            st.caption(
                "ℹ️ Registros de Pessoa Física (CPF) são ignorados nas correções, pois não geram crédito de PIS/COFINS."
            )

        items = payload.get("items") or []

        _d("PREVIEW PAYLOAD SHAPE", {
            "items_count": len(items),
            "first_item_keys": list(items[0].keys()) if items else [],
            "overlay_revisoes_aplicadas": payload.get("overlay_revisoes_aplicadas"),
            "total": payload.get("total"),
            "limit": payload.get("limit"),
            "offset": payload.get("offset"),
            "totais_pf_pj": totais_pf_pj,
        })

        # ---------------------------------------------------------
        # MONTA DF
        # ---------------------------------------------------------
        rows = []
        for it in items:
            d = it.get("dados") or []

            cfop_atual = str(d[9]).strip() if len(d) > 9 and d[9] is not None else ""
            cst_pis_atual = str(d[23]).strip() if len(d) > 23 and d[23] is not None else ""
            cst_cof_atual = str(d[29]).strip() if len(d) > 29 and d[29] is not None else ""

            rows.append({
                "registro_id": it.get("registro_id"),
                "linha": it.get("linha", 0),
                "cfop": cfop_atual,
                "cst_pis": cst_pis_atual,
                "cst_cofins": cst_cof_atual,
            })

        df_preview = pd.DataFrame(rows)

        _d("PREVIEW DF BEFORE CLIENT FILTER", {
            "rows_len": len(rows),
            "df_len": len(df_preview),
            "cols": df_preview.columns.tolist() if not df_preview.empty else [],
            "first_row": (rows[0] if rows else None),
        })

        if DEBUG and not df_preview.empty:
            _d("UNIQUES (AMOSTRA)", {
                "cfop_unique_head": df_preview["cfop"].astype(str).str.strip().unique()[:15].tolist()
                if "cfop" in df_preview.columns else [],
                "cst_pis_unique_head": df_preview["cst_pis"].astype(str).str.strip().unique()[:15].tolist()
                if "cst_pis" in df_preview.columns else [],
            })

        # Filtro client-side (deixa como proteção, mas agora o server já filtra)
        before_len = len(df_preview)
        if filtro_cfop_norm and "cfop" in df_preview.columns:
            df_preview = df_preview[df_preview["cfop"].astype(str).str.strip() == filtro_cfop_norm]
        if filtro_cst_pis_norm and "cst_pis" in df_preview.columns:
            df_preview = df_preview[df_preview["cst_pis"].astype(str).str.strip() == filtro_cst_pis_norm]
        after_len = len(df_preview)

        _d("PREVIEW DF AFTER CLIENT FILTER", {
            "before_len": before_len,
            "after_len": after_len,
            "filtro_cfop_norm": filtro_cfop_norm,
            "filtro_cst_norm": filtro_cst_pis_norm,
        })

        if DEBUG and before_len > 0 and after_len == 0 and (filtro_cfop_norm or filtro_cst_pis_norm):
            st.warning(
                "DEBUG: Preview zerou após filtro no client. "
                "Se o server já filtra, isso não deveria ocorrer; revisar índices ou normalização."
            )

    except Exception as e:
        st.error(f"Falha de conexão: {e}")
        st.text(traceback.format_exc())
        st.stop()

    # ---------------------------------------------------------
    # AÇÃO EM MASSA INTELIGENTE (Escopo Global)
    # ---------------------------------------------------------
    with st.form("form_massa_global"):
        st.markdown("### ⚡ Aplicar Alteração em TODA a Base (Filtro Global)")

        st.write(
            f"A alteração será aplicada a **todos os registros PJ (CNPJ)** com CFOP '{filtro_cfop_norm}' "
            f"e CST '{filtro_cst_pis_norm}' no banco. Registros PF (CPF) serão ignorados."
        )

        # Extra: se temos o meta do backend, mostra a expectativa
        if totais_pf_pj:
            st.caption(f"Estimativa pelo filtro atual: **PJ={pj_count}** | PF={pf_count} | Total bruto={total_bruto}")

        c1, c2, c3, c4 = st.columns(4)
        with c1:
            novo_cfop = st.text_input("Novo CFOP", placeholder="Ex: 1102")
        with c2:
            novo_pis = st.text_input("Novo CST PIS", placeholder="Ex: 51")
        with c3:
            novo_cof = st.text_input("Novo CST COFIN", placeholder="Ex: 51")
        with c4:
            motivo = st.text_input("Motivo", value="CORRECAO_GLOBAL_CFOP_CST")

        confirmar = st.form_submit_button("🚀 EXECUTAR ALTERAÇÃO GLOBAL")

        if confirmar:
            novo_cfop_norm = (novo_cfop or "").strip()
            novo_pis_norm = (novo_pis or "").strip()
            novo_cof_norm = (novo_cof or "").strip()

            _d("BEFORE POST (VALIDATION)", {
                "filtro_cfop_norm": filtro_cfop_norm,
                "filtro_cst_norm": filtro_cst_pis_norm,
                "novo_cfop_norm": novo_cfop_norm,
                "novo_pis_norm": novo_pis_norm,
                "novo_cof_norm": novo_cof_norm,
                "pj_count": pj_count,
                "pf_count": pf_count,
            })

            # Segurança: exige filtro para evitar varrer o banco todo sem querer
            if not filtro_cfop_norm and not filtro_cst_pis_norm:
                st.warning("⚠️ Por segurança, defina ao menos um filtro (CFOP ou CST) para a alteração global.")
                st.stop()

            # Segurança: exige pelo menos um valor novo
            if not (novo_cfop_norm or novo_pis_norm or novo_cof_norm):
                st.warning("Defina ao menos um valor novo para aplicar.")
                st.stop()

            # ✅ Bloqueio inteligente: se o backend disse que não existe PJ, não roda
            if totais_pf_pj and int(pj_count or 0) == 0:
                st.warning(
                    "Nenhum registro PJ (CNPJ) encontrado para os filtros informados. "
                    "Nada será alterado. (PF/CPF é ignorado por regra de crédito)."
                )
                st.stop()

            payload_post = {
                "versao_origem_id": versao_id,
                "motivo_codigo": motivo,
                "escopo": "GLOBAL",
                "filtros_origem": {
                    "cfop": filtro_cfop_norm or None,
                    "cst_pis": filtro_cst_pis_norm or None,
                },
                "valores_novos": {
                    "cfop": novo_cfop_norm or None,
                    "cst_pis": novo_pis_norm or None,
                    "cst_cofins": novo_cof_norm or None,
                },
            }

            _d("POST REQUEST", {
                "endpoint": api_url(f"/workflow/versao/{versao_id}/c170/revisar-global"),
                "payload": payload_post,
            })

            with st.spinner("O Backend está processando CNPJs, ignorando CPFs e recalculando C100..."):
                res = requests.post(
                    api_url(f"/workflow/versao/{versao_id}/c170/revisar-global"),
                    json=payload_post,
                    timeout=120,
                )

                _d("POST RESPONSE", {
                    "status_code": res.status_code,
                    "text_head": (res.text[:1000] if res.text else ""),
                })

                if res.status_code == 200:
                    data = res.json() or {}
                    qtd_sucesso = int(data.get("total_alterado", 0) or 0)
                    qtd_ignorado = int(data.get("total_ignorado_pf", 0) or 0)

                    st.success(
                        f"Sucesso! {qtd_sucesso} registros foram corrigidos e as notas (C100) foram recalculadas."
                    )

                    if qtd_ignorado > 0:
                        st.warning(
                            f"ℹ️ {qtd_ignorado} registros foram mantidos com a informação antiga pois pertencem "
                            f"a Pessoas Físicas (CPF) e não geram crédito."
                        )

                    st.balloons()
                    st.rerun()
                else:
                    st.error(f"Falha na API: {res.text}")

    # ---------------------------------------------------------
    # VISUALIZAÇÃO DE CONFERÊNCIA
    # ---------------------------------------------------------
    st.markdown("---")
    st.caption(f"Amostra atual (limitada a {limit_preview} linhas):")
    st.dataframe(df_preview, use_container_width=True, hide_index=True)
