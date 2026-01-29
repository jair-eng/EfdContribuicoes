import streamlit as st
import pandas as pd
import requests
from ui_utils import api_url, post, TIMEOUT
import traceback


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

    # ---------------------------------------------------------
    # CARGA DE PREVIEW (preferir filtro no servidor via params)
    # ---------------------------------------------------------
    url_preview = api_url(f"/workflow/versao/{versao_id}/c170")
    params = {"limit": int(limit_preview)}

    # Se o backend suportar, já filtra no servidor.
    # Se não suportar, ele deve ignorar ou retornar erro claro (422/400),
    # mas não deve quebrar o front com KeyError.
    if filtro_cfop_norm:
        params["cfop"] = filtro_cfop_norm
    if filtro_cst_pis_norm:
        params["cst_pis"] = filtro_cst_pis_norm

    try:
        resp = requests.get(url_preview, params=params, timeout=TIMEOUT)
        if resp.status_code != 200:
            st.error(f"Erro ao carregar preview: {resp.text}")
            st.stop()

        items = resp.json().get("items") or []
        rows = []

        for it in items:
            d = it.get("dados") or []

            # Defensivo: evita KeyError e evita comparar int vs str
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

        # Se o backend NÃO filtrar (ou retornar amostra maior), filtra também no client.
        if filtro_cfop_norm and "cfop" in df_preview.columns:
            df_preview = df_preview[df_preview["cfop"].astype(str).str.strip() == filtro_cfop_norm]
        if filtro_cst_pis_norm and "cst_pis" in df_preview.columns:
            df_preview = df_preview[df_preview["cst_pis"].astype(str).str.strip() == filtro_cst_pis_norm]

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
            f"A alteração será aplicada a **todos** os registros com CFOP '{filtro_cfop_norm}' "
            f"e CST '{filtro_cst_pis_norm}' no banco de dados."
        )

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

            if not filtro_cfop_norm and not filtro_cst_pis_norm:
                st.warning("⚠️ Por segurança, defina ao menos um filtro (CFOP ou CST) para a alteração global.")
            elif not (novo_cfop_norm or novo_pis_norm or novo_cof_norm):
                st.warning("Defina ao menos um valor novo para aplicar.")
            else:
                payload = {
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

                with st.spinner("O Backend está processando CNPJs, ignorando CPFs e recalculando C100..."):
                    res = requests.post(
                        api_url(f"/workflow/versao/{versao_id}/c170/revisar-global"),
                        json=payload,
                        timeout=120,
                    )

                    if res.status_code == 200:
                        data = res.json()
                        qtd_sucesso = data.get("total_alterado", 0)
                        qtd_ignorado = data.get("total_ignorado_pf", 0)

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
