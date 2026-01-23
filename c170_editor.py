
import streamlit as st
import pandas as pd
import requests
from ui_utils import clear_after_workflow, api_url



def render_editor_c170():
    st.subheader("C170 — Editor manual (CFOP / CST PIS / CST COFINS)")

    if st.session_state.get("selected_versao_id") is None:
        st.info("Selecione uma versão na etapa '2 — Selecionar Versão'.")
        st.stop()

    versao_id = int(st.session_state.get("selected_versao_id") or 0)

    # -----------------------------
    # Controles
    # -----------------------------
    col1, col2, col3 = st.columns([2, 2, 2])
    with col1:
        limit = st.number_input("Linhas por página", min_value=50, max_value=1000, value=200, step=50)
    with col2:
        offset = st.number_input("Offset", min_value=0, value=0, step=int(limit))
    with col3:
        somente_alterados = st.checkbox("Somente alterados", value=False)

    # se seu endpoint não suporta, comente esta linha e o checkbox
    q_alt = "&alterado=1" if somente_alterados else ""

    # -----------------------------
    # Carrega C170 (GET)
    # -----------------------------
    url = api_url(f"/workflow/versao/{versao_id}/c170?limit={int(limit)}&offset={int(offset)}{q_alt}")

    try:
        resp = requests.get(url, timeout=60)
        if resp.status_code != 200:
            st.error(f"Erro {resp.status_code}: {resp.text}")
            st.stop()

        data = resp.json() or {}
        items = data.get("items") or []
        total = int(data.get("total", 0) or 0)

    except Exception as e:
        st.exception(e)
        st.stop()

    st.caption(f"Versão: {versao_id} | Total C170: {total} | Mostrando: {len(items)}")

    if not items:
        st.warning("Nenhum C170 encontrado nessa página/filtro.")
        st.stop()

    # -----------------------------
    # Tabela (resumo)
    # CFOP = 9, CST_PIS = 23, CST_COFINS = 29
    # -----------------------------
    rows = []
    for it in items:
        dados = it.get("dados") or []

        def get(idx, default=""):
            return dados[idx] if idx < len(dados) else default

        rows.append({
            "registro_id": int(it["registro_id"]),
            "linha": int(it.get("linha", 0) or 0),
            "alterado": bool(it.get("alterado", False)),
            "cfop": get(9),
            "cst_pis": get(23),
            "cst_cofins": get(29),
            "vl_item": get(5),
            "cst_icms": get(8),
        })

    df = pd.DataFrame(rows).sort_values(["linha"])

    st.dataframe(df, use_container_width=True, hide_index=True)

    # -----------------------------
    # Seleção por registro_id
    # -----------------------------
    st.markdown("### Editar um item")

    default_registro_id = int(df.iloc[0]["registro_id"])
    registro_id = st.number_input("registro_id (C170)", min_value=1, value=default_registro_id, step=1)

    selected = df[df["registro_id"] == int(registro_id)]
    if not selected.empty:
        sel = selected.iloc[0].to_dict()
        st.caption(
            f"Linha SPED: {sel['linha']} | Atual: CFOP={sel['cfop']} | PIS={sel['cst_pis']} | COFINS={sel['cst_cofins']}")
        default_cfop = str(sel["cfop"] or "")
        default_pis = str(sel["cst_pis"] or "")
        default_cof = str(sel["cst_cofins"] or "")
    else:
        st.warning("registro_id não está nesta página. Ajuste offset/limit ou digite mesmo assim.")
        default_cfop = ""
        default_pis = ""
        default_cof = ""

    colA, colB, colC, colD = st.columns([2, 2, 2, 2])
    with colA:
        cfop = st.text_input("Novo CFOP", value=default_cfop, placeholder="ex: 1102").strip() or None
    with colB:
        cst_pis = st.text_input("Novo CST PIS", value=default_pis, placeholder="ex: 50").strip() or None
    with colC:
        cst_cofins = st.text_input("Novo CST COFINS", value=default_cof, placeholder="ex: 50").strip() or None
    with colD:
        motivo_codigo = st.text_input("motivo_codigo", value="MANUAL_C170").strip() or "MANUAL_C170"

    # -----------------------------
    # Aplicar (POST)
    # -----------------------------
    if st.button("✅ Aplicar revisão (REPLACE_LINE)", key="aplicar_patch_c170"):
        payload = {
            "versao_origem_id": int(versao_id),
            "cfop": cfop,
            "cst_pis": cst_pis,
            "cst_cofins": cst_cofins,
            "motivo_codigo": motivo_codigo,
            "apontamento_id": None,
        }

        if not payload["cfop"] and not payload["cst_pis"] and not payload["cst_cofins"]:
            st.error("Informe pelo menos 1 campo para alterar (CFOP ou CSTs).")
            st.stop()

        try:
            url2 = api_url(f"/workflow/registro/{int(registro_id)}/revisar-c170")
            resp2 = requests.post(url2, json=payload, timeout=60)

            if resp2.status_code != 200:
                st.error(f"Erro {resp2.status_code}: {resp2.text}")
                st.stop()

            out = resp2.json() or {}
            if out.get("status") != "OK":
                st.error(out)
                st.stop()

            st.success(f"Revisão criada! revisao_id={out.get('revisao_id')} (linha {out.get('linha')})")

            warnings = out.get("warnings") or []
            if warnings:
                st.warning("Avisos:\n- " + "\n- ".join(map(str, warnings)))

            try:
                clear_after_workflow()
            except Exception:
                pass

            st.rerun()

        except Exception as e:
            st.exception(e)
            st.stop()
