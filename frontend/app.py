import streamlit as st
import requests
import pandas as pd
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]  # pasta Projeto_Sped
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
from ui_utils import goto, cached_empresas, cached_arquivos, cached_versoes, cached_resumo_versao, \
    cached_apontamentos, clear_after_confirm, cached_empresa_resumo, show_error, clear_after_workflow, parse_bool, get, \
    post, patch, api_url, TIMEOUT
from ui_utils import cached_health
from c170_editor import render_editor_c170


import sys, os
print("CWD:", os.getcwd())
print("SYSPATH[0]:", sys.path[0])
print("SYSPATH:", sys.path)

st.set_option("client.showErrorDetails", True)



st.set_page_config(page_title="SPED Créditos", layout="wide")

# init session state (uma vez)
for k, default in {
    "selected_empresa_id": None,
    "selected_arquivo_id": None,
    "selected_versao_id": None,
    "ap_cache_bust": 0,
    "confirm_reproc_total": False,
    "ap_plan_page": 1,
}.items():
    if k not in st.session_state:
        st.session_state[k] = default

# --- Config ---
DEFAULT_API = "http://127.0.0.1:8000"
API_BASE = st.sidebar.text_input("API Base", value=DEFAULT_API).rstrip("/")




# --- Session State (único lugar) ---
if "last_preview" not in st.session_state:
    st.session_state.last_preview = None
if "last_confirm" not in st.session_state:
    st.session_state.last_confirm = None

if "selected_empresa_id" not in st.session_state:
    st.session_state.selected_empresa_id = None
if "selected_arquivo_id" not in st.session_state:
    st.session_state.selected_arquivo_id = None
if "selected_versao_id" not in st.session_state:
    st.session_state.selected_versao_id = None

if "menu" not in st.session_state:
    st.session_state["menu"] = "Home"


# --- Layout ---
st.title("SPED Créditos — Front (MVP)")

# Health check rápido (sidebar)
with st.sidebar:
    if st.button("Testar /health"):
        try:
            data = cached_health(API_BASE)
            st.success("API OK")
            st.json(data)
        except Exception as e:
            st.error(str(e))

PAGES = [
    "Home",
    "0 — Importar SPED",
    "1 — Selecionar Empresa",
    "2 — Selecionar Versão",
    "3 — Revisar & Apontamentos",
    "4 — C170 - Editor",
    "5 — Exportar",
]

if "page" not in st.session_state:
    st.session_state["page"] = "Home"
page = st.session_state["page"]

menu = st.sidebar.radio(
    "Fluxo de Trabalho",
    PAGES,
    index=PAGES.index(page),
    key=f"menu_widget_{page}",  # <-- chave muda quando a página muda
)

# Se usuário clicar no radio, atualiza page
if menu != page:
    st.session_state["page"] = menu
    st.rerun()

# Recarrega page depois da possível mudança
page = st.session_state["page"]


# ===========================
# HOME
# ===========================

if page == "Home":

    st.markdown("## SPED Créditos")
    st.markdown("### Inteligência Tributária")

    st.caption(
        "Análise, revisão e exportação segura de EFD Contribuições (PIS e COFINS), "
        "com versionamento, rastreabilidade e controle fiscal."
    )

    st.divider()

    c1, c2, c3, c4 = st.columns(4)
    with c1:
        st.markdown("📤 **Importar SPED**")
        st.caption("Envie arquivos EFD e gere versões auditáveis.")
    with c2:
        st.markdown("🔍 **Revisar & Apontar**")
        st.caption("Identifique inconsistências fiscais automaticamente.")
    with c3:
        st.markdown("✅ **Validar com segurança**")
        st.caption("Controle de status e workflow fiscal.")
    with c4:
        st.markdown("📊 **Exportar com confiança**")
        st.caption("Arquivos prontos para transmissão ou retificação.")

    st.divider()

    st.markdown("### Começar")
    colA, colB, colC = st.columns(3)

    with colA:
        if st.button("Importar novo SPED"):
            goto("0 — Importar SPED")

    with colB:
        if st.session_state.get("selected_versao_id"):
            if st.button("➡️ Continuar última versão"):
                goto("3 — Revisar & Apontamentos")
        else:
            st.caption("Nenhuma versão ativa.")

    with colC:
        if st.button("🔎 Buscar empresa por CNPJ"):
            goto("1 — Selecionar Empresa")

    st.divider()

    if st.session_state.get("selected_empresa_id") or st.session_state.get("selected_versao_id"):
        st.markdown("### Contexto atual")
        st.write(
            f"**Empresa ID:** {st.session_state.get('selected_empresa_id', '—')}  \n"
            f"**Arquivo ID:** {st.session_state.get('selected_arquivo_id', '—')}  \n"
            f"**Versão ID:** {st.session_state.get('selected_versao_id', '—')}"
        )

    st.divider()
    st.markdown("### Ações rápidas")
    colL, colR = st.columns([1, 1])

    with colL:
        if st.button("🧹 Limpar contexto atual"):
            st.session_state.selected_empresa_id = None
            st.session_state.selected_arquivo_id = None
            st.session_state.selected_versao_id = None
            st.session_state.last_preview = None
            st.session_state.last_confirm = None

            # limpa listas locais de browse
            st.session_state.pop("_empresas_browse", None)

            st.success("Contexto limpo. Nenhuma empresa/versão ativa.")
            st.rerun()
    with colR:
        if st.button("♻️ Limpar caches (debug)"):
            cached_empresas.clear()
            cached_arquivos.clear()
            cached_versoes.clear()
            cached_resumo_versao.clear()
            cached_apontamentos.clear()
            st.success("Caches limpos.")
            st.rerun()

    # ---------------------------
    # Status do sistema
    # ---------------------------
    st.divider()
    colX, colY = st.columns([1, 1])

    with colX:
        if st.button("🩺 Status do sistema"):
            try:
                data = cached_health(API_BASE)
                st.success("Sistema operacional")
                st.json(data)
            except Exception as e:
                st.error(str(e))

    with colY:
        st.caption("Versão do sistema: 0.1.0")

# 0 — IMPORTAR SPED (LOTE)
# ===========================

elif page == "0 — Importar SPED":

    st.subheader("Importar SPED (Preview → Confirm) — Lote")
    st.caption("Fluxo: envie um ou mais arquivos, confira o preview e confirme para criar empresa/arquivo/versão.")

    # 🔽 ALTERAÇÃO: múltiplos arquivos
    ups = st.file_uploader(
        "Selecione um ou mais arquivos SPED (.txt)",
        type=["txt"],
        accept_multiple_files=True,
    )

    col1, col2 = st.columns([1, 1])

    # ===========================
    # PREVIEW BATCH
    # ===========================
    with col1:
        if st.button("Gerar preview", disabled=not ups):
            if not ups:
                st.warning("Envie ao menos um arquivo.")
            else:
                with st.spinner("Gerando preview em lote..."):
                    r = post(
                        "/sped/upload/preview-batch",
                        files=[
                            ("files", (f.name, f.getvalue(), "text/plain"))
                            for f in ups
                        ],

                    )

                if r:
                    data = r.json()
                    st.session_state.preview_items = data.get("items", [])
                    st.session_state.preview_errors = data.get("errors", [])
                    st.success(
                        f"Preview concluído: "
                        f"{data.get('total_sucesso', 0)} sucesso(s), "
                        f"{data.get('total_erro', 0)} erro(s)"
                    )

    with col2:
        st.info("Depois do preview, confirme apenas os arquivos válidos.")

    # ===========================
    # RESULTADO DO PREVIEW
    # ===========================
    if st.session_state.get("preview_items"):
        st.markdown("### ✅ Arquivos válidos (preview)")
        st.table(st.session_state.preview_items)

    if st.session_state.get("preview_errors"):
        st.markdown("### ❌ Erros no preview")
        st.table(st.session_state.preview_errors)

    st.divider()

    # ===========================
    # CONFIRM BATCH
    # ===========================
    st.subheader("Confirmar importação (lote)")

    if st.session_state.get("preview_items"):
        if st.button("✅ Confirmar importação dos válidos"):
            payload = [
                {
                    "temp_id": item["temp_id"],
                    "nome_arquivo": item.get("nome_arquivo"),
                }
                for item in st.session_state.preview_items
            ]

            with st.spinner("Confirmando importações..."):
                r = post(
                    "/sped/upload/confirm-batch",
                    json=payload,

                )

            if r:
                data = r.json()

                st.session_state.last_confirm_batch = data

                # limpa caches globais (empresas, arquivos, etc.)
                clear_after_confirm()
                st.session_state.pop("_empresas_browse", None)

                st.success(
                    f"Confirmação finalizada: "
                    f"{data.get('total_sucesso', 0)} sucesso(s), "
                    f"{data.get('total_erro', 0)} erro(s)"
                )

    # ===========================
    # RESULTADO DO CONFIRM
    # ===========================
    last_confirm = st.session_state.get("last_confirm_batch")
    if isinstance(last_confirm, dict):

        st.markdown("### 📦 Resultado da importação")

        if last_confirm.get("items"):
            st.markdown("#### Importações confirmadas")
            st.table(last_confirm["items"])

            # 👉 mantém o comportamento atual: seleciona a ÚLTIMA versão criada
            last_item = last_confirm["items"][-1]
            st.session_state.selected_empresa_id = int(last_item["empresa_id"])
            st.session_state.selected_arquivo_id = int(last_item["arquivo_id"])
            st.session_state.selected_versao_id = int(last_item["versao_id"])

        if last_confirm.get("errors"):
            st.markdown("#### ⚠️ Erros na confirmação")
            st.table(last_confirm["errors"])

        st.divider()
        colX, colY = st.columns(2)
        with colX:
            if st.button("➡️ Ir para Revisar & Apontamentos"):
                goto("3 — Revisar & Apontamentos")
        with colY:
            if st.button("➡️ Ir para Selecionar Versão"):
                goto("2 — Selecionar Versão")



# ===========================
# 1 — SELECIONAR EMPRESA
# ===========================
elif page == "1 — Selecionar Empresa":

    st.subheader("Selecionar Empresa")

    if st.session_state.selected_empresa_id:
        st.success(f"Empresa selecionada atualmente: {st.session_state.selected_empresa_id}")

    st.caption("Você pode buscar por CNPJ (recomendado) ou listar empresas cadastradas.")

    tab1, tab2 = st.tabs(["Buscar por CNPJ", "Listar empresas"])

    with tab1:
        cnpj = st.text_input("CNPJ", placeholder="Ex: 40832748000175")
        col1, col2 = st.columns([1, 2])

        with col1:
            if st.button("Buscar", key="buscar_cnpj"):
                if not cnpj.strip():
                    st.warning("Informe um CNPJ.")
                else:
                    r = get("/empresa/buscar", params={"cnpj": cnpj.strip()})
                    if r:
                        emp = r.json()
                        st.success("Empresa encontrada!")
                        st.json(emp)

                        if isinstance(emp, dict) and emp.get("id"):
                            st.session_state.selected_empresa_id = int(emp["id"])
                            st.session_state.selected_arquivo_id = None
                            st.session_state.selected_versao_id = None
                            st.success(f"Empresa selecionada: {emp['id']}")

        with col2:
            st.info("Dica: após selecionar a empresa, vá para o Passo 2 e escolha o arquivo/versão.")

    with tab2:
        colA, colB = st.columns([1, 1])

        with colA:
            if st.button("Listar empresas", key="listar_empresas"):
                try:
                    st.session_state._empresas_browse = cached_empresas(API_BASE)
                except Exception as e:
                    st.error(str(e))
                    st.session_state._empresas_browse = []

        empresas = st.session_state.get("_empresas_browse", [])

        with colB:
            if empresas:
                def label_empresa(e: dict) -> str:
                    nome = e.get("razao_social") or e.get("nome") or ""
                    cnpj_e = e.get("cnpj") or ""
                    return f"#{e.get('id')} — {cnpj_e} — {nome}".strip(" —")

                options = [label_empresa(e) for e in empresas]
                sel = st.selectbox("Escolha uma empresa", options=options, index=0)
                sel_id = int(sel.split("—")[0].replace("#", "").strip())

                if st.button("Selecionar empresa", key="selecionar_empresa_browse"):
                    st.session_state.selected_empresa_id = sel_id
                    st.session_state.selected_arquivo_id = None
                    st.session_state.selected_versao_id = None
                    st.success(f"Empresa selecionada: {sel_id}")
            else:
                st.caption("Clique em “Listar empresas” para carregar.")

        if empresas:
            with st.expander("Ver JSON (browse/empresas)", expanded=False):
                st.json(empresas)

    st.divider()

    if st.session_state.selected_empresa_id is not None:
        if st.button("➡️ Selecionar Versão"):
            goto("2 — Selecionar Versão")

    else:
        st.info("Selecione uma empresa para continuar.")

# ===========================
# 2 — SELECIONAR VERSÃO
# ===========================
elif page == "2 — Selecionar Versão":

    if st.session_state.selected_empresa_id is None:
        st.info("Selecione uma empresa primeiro.")
        st.stop()

    empresa_id = int(st.session_state.selected_empresa_id)

    st.subheader("Selecionar Versão")

    # --- carrega resumo da empresa (robusto) ---
    try:
        # ✅ rota real (sem /workflow): /empresa/{empresa_id}/resumo
        emp_resumo = cached_empresa_resumo(API_BASE, empresa_id)
    except Exception as e:
        st.error(f"Erro ao carregar resumo da empresa: {e}")
        st.stop()

    razao_social = (emp_resumo or {}).get("razao_social", "—")
    cnpj = (emp_resumo or {}).get("cnpj", "—")
    st.markdown(f"**Empresa:** {razao_social} (**{cnpj}**)")

    items = (emp_resumo or {}).get("versoes_items") or []
    if not items:
        st.warning("Nenhuma versão encontrada para esta empresa.")
        st.stop()

    # -----------------------------
    # Filtros + ordenação
    # -----------------------------
    colF1, colF2, colF3 = st.columns([1, 1, 1])

    with colF1:
        filtro_status = st.selectbox(
            "Filtrar por status",
            options=["(Todos)", "GERADA", "EM_REVISAO", "VALIDADA", "EXPORTADA"],
            index=0
        )

    with colF2:
        somente_pendentes = st.checkbox("Somente com pendências", value=False)

    with colF3:
        ordenar_por = st.selectbox(
            "Ordenar por",
            options=[
                "Prioridade (Alta→Impacto)",
                "Impacto (desc)",
                "Pendentes (desc)",
                "Período (desc)",
            ],
            index=0
        )


    def _ok_row(r: dict) -> bool:
        stt = str(r.get("status", "") or "").upper()
        if filtro_status != "(Todos)" and stt != filtro_status:
            return False
        if somente_pendentes and int(r.get("pendentes", 0) or 0) <= 0:
            return False
        return True

    filtered = [r for r in items if _ok_row(r)]
    if not filtered:
        st.info("Nenhum item encontrado com os filtros atuais.")
        st.stop()

    def _sort_key_prioridade(r: dict):
        p = (r.get("pendentes_por_prioridade") or {})
        alta = int(p.get("alta", 0) or 0)
        media = int(p.get("media", 0) or 0)
        pend = int(r.get("pendentes", 0) or 0)
        impacto = float(r.get("impacto_estimado_total", 0) or 0)
        periodo = str(r.get("periodo") or "")
        return (alta, media, pend, impacto, periodo)

    def _sort_key_impacto(r: dict):
        impacto = float(r.get("impacto_estimado_total", 0) or 0)
        periodo = str(r.get("periodo") or "")
        return (impacto, periodo)

    def _sort_key_pendentes(r: dict):
        p = (r.get("pendentes_por_prioridade") or {})
        alta = int(p.get("alta", 0) or 0)
        pend = int(r.get("pendentes", 0) or 0)
        periodo = str(r.get("periodo") or "")
        return (alta, pend, periodo)

    def _sort_key_periodo(r: dict):
        periodo = str(r.get("periodo") or "")
        return (periodo,)

    if ordenar_por == "Impacto (desc)":
        filtered_sorted = sorted(filtered, key=_sort_key_impacto, reverse=True)
    elif ordenar_por == "Pendentes (desc)":
        filtered_sorted = sorted(filtered, key=_sort_key_pendentes, reverse=True)
    elif ordenar_por == "Período (desc)":
        filtered_sorted = sorted(filtered, key=_sort_key_periodo, reverse=True)
    else:
        filtered_sorted = sorted(filtered, key=_sort_key_prioridade, reverse=True)

    # -----------------------------
    # Tabela mini-resumo
    # -----------------------------
    def _fmt_money(v: float) -> str:
        return f"R$ {float(v or 0):,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")

    table_rows = []
    for r in filtered_sorted:
        p = (r.get("pendentes_por_prioridade") or {})
        table_rows.append({
            "Versão": f"{r.get('versao_id')} (v{r.get('numero', 1)})",
            "Período": r.get("periodo"),
            "Arquivo": r.get("nome_arquivo"),
            "Status": r.get("status"),
            "Pendentes": int(r.get("pendentes", 0) or 0),
            "Alta": int(p.get("alta", 0) or 0),
            "Média": int(p.get("media", 0) or 0),
            "Baixa": int(p.get("baixa", 0) or 0),
            "Impacto": _fmt_money(float(r.get("impacto_estimado_total", 0) or 0)),
        })

    st.dataframe(table_rows, use_container_width=True, hide_index=True)


    # -----------------------------
    # Seleção para revisar (1 clique)
    # -----------------------------
    def _label(r: dict) -> str:
        p = (r.get("pendentes_por_prioridade") or {})
        return (
            f"Versão {r.get('versao_id')} (v{r.get('numero', 1)}) — "
            f"{r.get('periodo','—')} — {r.get('status','—')} — "
            f"Pendentes: {int(r.get('pendentes', 0) or 0)} "
            f"(A:{int(p.get('alta',0) or 0)} M:{int(p.get('media',0) or 0)} B:{int(p.get('baixa',0) or 0)})"
        )

    sel_map = {_label(r): r for r in filtered_sorted}
    sel_label = st.selectbox("Escolha uma versão para revisar", options=list(sel_map.keys()), index=0)

    sel = sel_map[sel_label]
    versao_id = int(sel["versao_id"])
    arquivo_id = int(sel.get("arquivo_id") or 0)

    # seta o contexto (mantém compatibilidade)
    st.session_state.selected_versao_id = versao_id
    if arquivo_id:
        st.session_state.selected_arquivo_id = arquivo_id

    status = str(sel.get("status", "") or "").upper()
    is_exportada = (status == "EXPORTADA")

    # badge
    if status == "EXPORTADA":
        st.success("Status da versão: EXPORTADA")
    elif status == "VALIDADA":
        st.success("Status da versão: VALIDADA")
    elif status in ("EM_REVISAO", "EM REVISÃO", "EM_REVISAO"):
        st.warning("Status da versão: EM REVISÃO")
    else:
        st.info(f"Status da versão: {sel.get('status','—')}")

    colA, colB, colC = st.columns([1, 1, 1])

    with colA:
        if st.button("➡️ Revisar & Apontamentos"):
            goto("3 — Revisar & Apontamentos")

    with colB:
        # ⚠️ ajuste o path se sua rota for diferente
        if st.button("♻️ Reprocessar versão", disabled=is_exportada):
            payload = {"preservar_resolvidos": False}
            r = post(f"/workflow/versao/{versao_id}/reprocessar", json=payload)
            if r:
                st.success(f"Reprocessado: versão {versao_id}")
                try:
                    cached_empresa_resumo.clear()
                except Exception:
                    pass
                try:
                    cached_resumo_versao.clear()
                except Exception:
                    pass
                st.rerun()

    with colC:
        # export unitário: baixa um SPED
        if st.button("⬇️ Exportar SPED (unitário)"):
            try:
                resp = requests.get(f"{API_BASE}/export/versao/{versao_id}", timeout=TIMEOUT)
                if resp.status_code >= 400:
                    show_error(resp)
                else:
                    st.download_button(
                        "Baixar arquivo SPED",
                        data=resp.content,
                        file_name=f"SPED_versao_{versao_id}.txt",
                        mime="text/plain",
                    )
            except Exception as e:
                st.error(f"Falha ao exportar: {e}")

    st.divider()

    # -----------------------------
    # Ações rápidas (TOP 10)
    # -----------------------------
    q_alta = sum(1 for r in filtered_sorted if (r.get("pendentes_por_prioridade") or {}).get("alta", 0))
    with st.expander(f"⚡ Ações rápidas (Top 10) — {q_alta} com pendência ALTA", expanded=False):
        top_n = filtered_sorted[:10]
        for r in top_n:
            p = (r.get("pendentes_por_prioridade") or {})
            vid = int(r["versao_id"])
            stt = str(r.get("status", "") or "").upper()
            frozen = (stt == "EXPORTADA")

            col1, col2, col3, col4 = st.columns([4, 1, 1, 1])
            with col1:
                st.write(
                    f"**Versão {vid} (v{r.get('numero', 1)})** — {r.get('periodo', '—')} — {r.get('nome_arquivo', '—')}\n\n"
                    f"Status: **{r.get('status', '—')}** | Pend: {int(r.get('pendentes', 0) or 0)} "
                    f"(A:{int(p.get('alta', 0) or 0)} M:{int(p.get('media', 0) or 0)} B:{int(p.get('baixa', 0) or 0)}) | "
                    f"Impacto: {_fmt_money(float(r.get('impacto_estimado_total', 0) or 0))}"
                )

            with col2:
                if st.button("Revisar", key=f"rev_{vid}"):
                    st.session_state.selected_versao_id = vid
                    if r.get("arquivo_id"):
                        st.session_state.selected_arquivo_id = int(r["arquivo_id"])
                    goto("3 — Revisar & Apontamentos")

            with col3:
                if st.button("Reprocessar", key=f"rep_{vid}", disabled=frozen):
                    payload = {"preservar_resolvidos": False}
                    rr = post(f"/workflow/versao/{vid}/reprocessar", json=payload)
                    if rr:
                        st.success(f"Reprocessado: versão {vid}")
                        try:
                            cached_empresa_resumo.clear()
                        except Exception:
                            pass
                        st.rerun()

            with col4:
                if st.button("Exportar", key=f"exp_{vid}"):
                    try:
                        resp = requests.get(f"{API_BASE}/export/versao/{vid}", timeout=TIMEOUT)
                        if resp.status_code >= 400:
                            show_error(resp)
                        else:
                            st.download_button(
                                "Baixar SPED",
                                data=resp.content,
                                file_name=f"SPED_versao_{vid}.txt",
                                mime="text/plain",
                                key=f"dl_{vid}",
                            )
                    except Exception as e:
                        st.error(f"Falha ao exportar: {e}")

    st.divider()

    # -----------------------------
    # Export em lote (ZIP)
    # -----------------------------

    st.markdown("### 📦 Exportação em lote (ZIP)")

    default_zip = ["VALIDADA", "EXPORTADA"]
    if filtro_status in ("VALIDADA", "EXPORTADA"):
        default_zip = [filtro_status]

    status_sel = st.multiselect(
        "Status das versões",
        ["VALIDADA", "EXPORTADA"],
        default=default_zip,
        key="zip_status_sel",
    )

    # empresa_id precisa estar selecionada
    params = "&".join([f"status={s}" for s in status_sel]) if status_sel else "status=VALIDADA&status=EXPORTADA"
    url_zip = f"{API_BASE}/export/empresa/{empresa_id}/versoes-zip?{params}"
    st.link_button("📦 Baixar ZIP das versões", url_zip)

    st.caption("Gera um ZIP com todas as versões da empresa nos status selecionados (VALIDADA/EXPORTADA).")
    st.divider()


# ===========================
# 3 — REVISAR & APONTAMENTOS
# ===========================
elif page == "3 — Revisar & Apontamentos":

    if st.session_state.get("selected_versao_id") is None:
        st.info("Selecione uma versão primeiro.")
        st.stop()

    versao_id = st.session_state.get("selected_versao_id")
    total_ui = 0
    apontamentos = []

    # --- reset robusto quando muda a versão ---
    last_key = "last_versao_id_apontamentos"
    prev = st.session_state.get(last_key)

    if prev != int(versao_id):
        st.session_state[last_key] = int(versao_id)

        # limpa caches de API
        try:
            cached_apontamentos.clear()
        except:
            pass
        try:
            cached_resumo_versao.clear()
        except:
            pass

        # limpa estados da planilha da versão anterior e da atual
        ids_to_clear = [int(versao_id)]
        if prev is not None:
            try:
                ids_to_clear.append(int(prev))
            except Exception:
                pass

        for vid in ids_to_clear:
            if prev is not None:
                try:
                    prev_vid = int(prev)
                    for k in list(st.session_state.keys()):
                        if str(k).startswith(f"ap_df_{prev_vid}") or str(k).startswith(f"ap_base_{prev_vid}"):
                            st.session_state.pop(k, None)
                        if str(k).startswith(f"ap_editor_{prev_vid}"):
                            st.session_state.pop(k, None)
                        if str(k).startswith(f"ap_pending_apply_{prev_vid}"):
                            st.session_state.pop(k, None)
                        if str(k).startswith(f"ap_flash_{prev_vid}"):
                            st.session_state.pop(k, None)
                        if str(k).startswith(f"ap_plan_page_size_{prev_vid}"):
                            st.session_state.pop(k, None)
                        if str(k).startswith(f"ap_plan_page_input_{prev_vid}"):
                            st.session_state.pop(k, None)
                except Exception:
                    pass

            st.session_state.ap_plan_page = 1

    st.subheader("Revisão & Apontamentos")
    st.caption(
        f"Empresa ID: {st.session_state.selected_empresa_id} | "
        f"Arquivo ID: {st.session_state.selected_arquivo_id} | "
        f"Versão ID: {versao_id}"
    )

    # refresh manual
    colT1, colT2 = st.columns([1, 1])
    with colT1:
        if st.button("↩️ Trocar versão"):
            goto("2 — Selecionar Versão")
    with colT2:
        if st.button("🔃 Atualizar dados"):
            clear_after_workflow()
            st.rerun()

        # =========================
        # Resumo da versão (1x) + Apontamentos (fonte da verdade) + Métricas
        # =========================

        # --- Resumo (cache curto) ---
        try:
            resumo = cached_resumo_versao(API_BASE, int(versao_id))
            empresa = (resumo or {}).get("empresa") or {}
            arquivo = (resumo or {}).get("arquivo") or {}
            versao_info = (resumo or {}).get("versao") or {}

            # status lógico (para regras da página)
            status_versao = resumo.get("status", "—")

            # status para UI (normalizado)
            raw_status = status_versao
            status_code = str(raw_status).strip().upper().replace(" ", "_")
            status_label = status_code.replace("_", " ")

            # Header (empresa/arquivo/período)
            st.markdown(
                f"""
    **Empresa:** {empresa.get('razao_social', '—')} (**{empresa.get('cnpj', '—')}**)  
    **Arquivo:** {arquivo.get('nome_arquivo', '—')}  
    **Período:** {arquivo.get('periodo', '—')} | **Line ending:** {arquivo.get('line_ending', '—')}  
    """
            )

            # Status (uma vez só)
            if status_code == "EXPORTADA":
                st.success(f"Status da versão: {status_label}")
            elif status_code == "EM_REVISAO":
                st.warning(f"Status da versão: {status_label}")
            elif status_code == "VALIDADA":
                st.success(f"Status da versão: {status_label}")
            else:
                st.info(f"Status da versão: {raw_status}")

        except Exception as e:
            st.error(f"Erro ao carregar resumo da versão: {e}")
            st.stop()


        # ---------------------------
        # Carrega + normaliza apontamentos (fonte da verdade p/ contagem)
        # ---------------------------



        def normalize_apontamento(item, idx: int):
            if isinstance(item, str):
                return {
                    "id": f"str_{idx}",
                    "tipo": "MSG",
                    "status": "Pendente",
                    "mensagem": item,
                    "registro": None,
                    "linha": None,
                    "campo": "",
                    "prioridade": "",
                    "impacto_financeiro": None,
                    "resolvido": False,
                    "_raw": item,
                }

            if not isinstance(item, dict):
                return None

            resolvido = parse_bool(item.get("resolvido"))
            status = "Resolvido" if resolvido else "Pendente"
            reg = item.get("registro") or {}

            return {
                "id": item.get("id"),
                "tipo": item.get("tipo", "—"),
                "status": status,
                "mensagem": item.get("descricao") or "",
                "registro": reg.get("reg"),
                "linha": reg.get("linha"),
                "campo": item.get("codigo") or "",
                "prioridade": item.get("prioridade") or "",
                "impacto_financeiro": item.get("impacto_financeiro"),
                "resolvido": resolvido,
                "_raw": item,
            }


        try:
            apontamentos_raw = cached_apontamentos(API_BASE, int(versao_id), st.session_state.ap_cache_bust)

            if st.checkbox("Mostrar debug de apontamentos", value=False):
                st.json(apontamentos_raw)

        except Exception as e:
            st.error(str(e))
            apontamentos_raw = {"total": 0, "items": []}

        total_backend = None
        raw = apontamentos_raw or []
        if isinstance(raw, dict):
            total_backend = raw.get("total")
            raw = raw.get("items") or raw.get("apontamentos") or raw.get("data") or []

        apontamentos = []
        if isinstance(raw, list):
            for i, it in enumerate(raw, start=1):
                n = normalize_apontamento(it, i)
                if n:
                    apontamentos.append(n)

        total_ui = int(total_backend) if total_backend is not None else len(apontamentos)
        pendentes_ui = sum(1 for a in apontamentos if a.get("status") == "Pendente")

        # ---------------------------
        # Métricas (1x, sem duplicar)
        # ---------------------------
        total_registros = int(resumo.get("total_registros", 0) or 0)
        impacto = float(resumo.get("impacto_estimado_total", 0) or 0)

        c1, c2, c3 = st.columns(3)
        c1.metric("Registros", total_registros)
        c2.metric("Apontamentos", int(total_ui))
        c3.metric("Pendentes", int(pendentes_ui))

        st.metric(
            "💰 Impacto estimado (pendentes)",
            f"R$ {impacto:,.2f}".replace(",", "X").replace(".", ",").replace("X", "."),
        )

        with st.expander("Ver resumo completo", expanded=False):
            st.json(resumo)



    # ---------------------------
    # Ações da versão
    # ---------------------------
    st.divider()
    st.markdown("### Ações da Versão")

    colA, colB = st.columns([1, 1], gap="large")

    with colA:
        status_up = str(status_versao).strip().upper()

        if status_up == "GERADA":
            if st.button("📝 Iniciar Revisão"):
                rr = post(f"/workflow/versao/{int(versao_id)}/revisar")
                if rr:
                    clear_after_workflow()
                    st.success("Revisão iniciada.")
                    st.rerun()
            st.caption("Inicia a etapa de revisão desta versão.")
        else:
            st.caption(f"Revisão não pode ser iniciada porque a versão está em **{status_up}**.")

    with colB:
        if st.button("✅ Validar"):
            rr = post(f"/workflow/versao/{int(versao_id)}/validar")
            if rr:
                clear_after_workflow()
                st.success("Versão validada.")
                st.rerun()

        st.caption(
            "A validação só é bloqueada se houver **ERROS pendentes**. "
            "ALERTAS/OPORTUNIDADES não bloqueiam."
        )

    # ---------------------------
    # Tabs: cards / planilha
    # ---------------------------
    st.divider()
    view = st.radio(
        "",
        ["Apontamentos", "Planilha de revisão"],
        horizontal=True,
        key="ap_view",
        label_visibility="collapsed",
    )

    # ===========================
    # TAB 1 — CARDS
    # ===========================
    if view == "Apontamentos":
        st.markdown("### Apontamentos")

        # controla qual apontamento está expandido (accordion)
        if "ap_expanded_id" not in st.session_state:
            st.session_state.ap_expanded_id = None

        colf1, colf2, colf3 = st.columns([1, 1, 2])
        with colf1:
            status_filtro = st.selectbox("Status", ["Todos", "Pendente", "Resolvido"], index=0, key="ap_status")
        with colf2:
            busca_texto = st.text_input("Buscar", value="", placeholder="ex: M100, C190, CFOP...", key="ap_busca")
        with colf3:
            st.caption("Resolva os apontamentos e depois valide a versão.")

        # reset de página quando filtro muda
        if "ap_last_filters" not in st.session_state:
            st.session_state.ap_last_filters = ("Todos", "")

        current_filters = (status_filtro, busca_texto.strip())
        if current_filters != st.session_state.ap_last_filters:
            st.session_state.ap_last_filters = current_filters
            st.session_state.ap_page = 1

        # ---------------------------
        # Ações em lote
        # ---------------------------
        st.divider()
        st.markdown("### Ações em lote")

        pendentes = [a for a in apontamentos if a.get("status") == "Pendente"]

        if not pendentes:
            st.caption("Nenhum apontamento pendente.")
        else:
            if st.button("✅ Resolver TODOS os pendentes", key="resolver_todos"):
                versao_id = int(st.session_state.selected_versao_id)

                resp = patch(f"/workflow/versao/{versao_id}/resolver_todos", json={})
                if not resp:
                    st.stop()  # utils.show_error já mostrou o 500/400

                data = resp.json() or {}
                clear_after_workflow()

                updated = int(data.get("updated_total", 0) or 0)
                rest = data.get("pendentes_restantes", "?")

                st.success(f"{updated} resolvidos. Pendentes restantes: {rest}")
                st.rerun()

        # --- SEMPRE avalia confirmar revisão (fora do if acima)
        pendentes_erro = int(resumo.get("pendentes_erro", 0) or 0)
        status_versao = str(resumo.get("status") or "")  # ou de onde você pega o status

        if status_versao == "EM_REVISAO" and pendentes_erro == 0:
            if st.button("✅ Confirmar revisão", key="confirmar_revisao"):
                resp = post(f"/workflow/versao/{versao_id}/confirmar-revisao")
                data = resp.json() or {}
                vrid = data.get("versao_revisada_id")

                if not vrid:
                    st.error(f"Resposta sem versao_revisada_id: {data}")
                    st.stop()

                # ✅ troca para a versão revisada
                st.session_state.selected_versao_id = int(vrid)

                # ⚠️ se você tem selectbox de versão com key, atualize ela também:
                # st.session_state["versao_select"] = int(vrid)

                clear_after_workflow()
                st.success(f"Revisão confirmada. Versão revisada: {int(vrid)}")
                st.rerun()
        else:
            st.info(f"Confirmação indisponível. Status={status_versao}, pendentes_erro={pendentes_erro}")


        # ---------------------------
        # Filtro
        # ---------------------------
        def match_status(a: dict) -> bool:
            if status_filtro == "Todos":
                return True
            return a.get("status") == status_filtro

        def match_text(a: dict) -> bool:
            if not busca_texto.strip():
                return True
            q = busca_texto.strip().lower()
            blob = " ".join([
                str(a.get("tipo", "")),
                str(a.get("status", "")),
                str(a.get("mensagem", "")),
                str(a.get("registro", "")),
                str(a.get("campo", "")),
                str(a.get("prioridade", "")),
            ]).lower()
            return q in blob

        filtrados = [a for a in apontamentos if match_status(a) and match_text(a)]
        total = len(filtrados)

        # ---------------------------
        # Paginação
        # ---------------------------
        page_size = st.selectbox("Por página", [25, 50, 100, 200], index=1, key="ap_page_size")

        if "ap_page" not in st.session_state:
            st.session_state.ap_page = 1

        total_pages = max(1, (total + page_size - 1) // page_size)
        st.session_state.ap_page = min(max(st.session_state.ap_page, 1), total_pages)

        st.caption(f"Página {st.session_state.ap_page} de {total_pages} — Total: {total}")

        st.session_state.ap_page = st.number_input(
            "Ir para página",
            min_value=1,
            max_value=total_pages,
            value=int(st.session_state.ap_page),
            step=1,
            key="ap_page_input",
        )

        start = (st.session_state.ap_page - 1) * page_size
        end = start + page_size
        page_items = filtrados[start:end]

        st.write(f"Mostrando **{len(page_items)}** nesta página.")

        if not page_items:
            st.info("Nada para mostrar com os filtros atuais.")
        else:

            # controla qual apontamento está em foco
            if "ap_focus_id" not in st.session_state:
                st.session_state.ap_focus_id = None

            # se já tem foco, mostra só ele
            if st.session_state.ap_focus_id is not None:
                page_items = [x for x in page_items if x.get("id") == st.session_state.ap_focus_id]

            for a in page_items:
                a_id = a.get("id")
                a_status = a.get("status", "—")
                a_tipo = a.get("tipo", "—")
                a_msg = a.get("mensagem", "")

                raw_meta = (a.get("_raw") or {}).get("meta") or {}

                badges = []
                if a.get("tipo") == "ERRO":
                    badges.append("🔴 ERRO")
                else:
                    badges.append("🟡 OPORTUNIDADE")

                if raw_meta.get("bloqueada_por_erro") is True:
                    badges.append("⚫ BLOQUEADA")

                # ✅ Revisão aplicada (mesmo após reprocess)
                if a.get("tem_revisao") is True:
                    rid = a.get("revisao_id")
                    badges.append(f"✅ REVISADO{f' #{rid}' if rid else ''}")

                badge_txt = " | ".join(badges)

                # HEADER
                col_h1, col_h2, col_h3 = st.columns([10, 1, 2])
                with col_h1:
                    st.markdown(f"**#{a_id} | {a_tipo} | {a_status}**")

                with col_h2:
                    if st.button("🔍", key=f"focus_{a_id}"):
                        st.session_state.ap_focus_id = a_id
                        st.rerun()

                with col_h3:
                    if st.session_state.ap_focus_id == a_id:
                        if st.button("⬅️ Voltar para lista", key=f"unfocus_{a_id}"):
                            st.session_state.ap_focus_id = None
                            st.rerun()

                st.markdown(f"**#{a_id} | {badge_txt} | {a_status}**")


                if a_msg:
                    st.write(a_msg)
                else:
                    st.caption("Sem detalhes.")

                meta_cols = st.columns(5)
                meta_cols[0].write(f"**Registro:** {a.get('registro', '—')}")
                meta_cols[1].write(f"**Linha:** {a.get('linha', '—')}")
                meta_cols[2].write(f"**Código:** {a.get('campo', '—')}")
                meta_cols[3].write(f"**Prioridade:** {a.get('prioridade', '—')}")
                meta_cols[4].write(f"**Revisão:** {'Sim' if a.get('tem_revisao') else 'Não'}")

                # ações
                b1, b2, b3 = st.columns([1, 1, 1.4])
                with b1:
                    if a_status != "Resolvido":
                        if st.button("✅ Resolver", key=f"resolver_{a_id}"):
                            if str(a_id).isdigit():
                                rr = patch(f"/workflow/apontamento/{int(a_id)}/resolver")
                                if rr:
                                    clear_after_workflow()
                                    st.success("Resolvido.")
                                    st.rerun()

                with b2:
                    if a_status == "Resolvido":
                        if st.button("↩️ Reabrir", key=f"reabrir_{a_id}"):
                            if str(a_id).isdigit():
                                rr = patch(f"/workflow/apontamento/{int(a_id)}/reabrir")
                                if rr:
                                    clear_after_workflow()
                                    st.success("Reaberto.")
                                    st.rerun()

                with b3:
                    vr = a.get("versao_revisada_id")
                    if a.get("tem_revisao") and vr:
                        url = f"{API_BASE}/export/versao/{int(vr)}"
                        st.link_button(f"⬇️ Baixar revisado (v{vr})", url)

                with st.expander("Ver JSON", expanded=False):
                    st.json(a.get("_raw", a))

                st.markdown("---")


    # ===========================
    # TAB 2 — PLANILHA
    # ===========================
    else:
        st.markdown("### Planilha de revisão")

        if not apontamentos:
            st.info("Sem apontamentos para exibir.")
        else:

            # monta rows (foto original vinda do backend)
            rows = []
            for a in apontamentos:
                rows.append({
                    "ID": a.get("id"),
                    "Tipo": a.get("tipo"),
                    "Código": a.get("campo"),
                    "Prioridade": a.get("prioridade"),
                    "Impacto": a.get("impacto_financeiro"),
                    "Registro": a.get("registro") or "",
                    "Linha": a.get("linha"),
                    "Resolvido": True if a.get("resolvido") is True else False,

                })

            df = pd.DataFrame(rows)
            # snapshot base SEMPRE vindo do backend (fonte da verdade)
            df_base = df[["ID", "Resolvido"]].copy()

            st.caption("Marque/desmarque “Resolvido”. Depois clique em aplicar.")

            # --- estado da planilha (para milhares + selecionar todos) ---
            df_key = f"ap_df_{int(versao_id)}"
            base_key = f"ap_base_{int(versao_id)}"  # foto original (before)

            # se é primeira vez ou mudou o conjunto (tamanho), reseta
            if df_key not in st.session_state or len(st.session_state[df_key]) != len(df):
                st.session_state[df_key] = df.copy()
            if base_key not in st.session_state or len(st.session_state[base_key]) != len(df_base):
                st.session_state[base_key] = df_base.copy()

            # --- Ações rápidas ---
            cA, cB, cC = st.columns([1, 1, 2])
            with cA:
                if st.button("✅ Marcar todos", key=f"ap_all_res_{versao_id}"):
                    st.session_state[df_key]["Resolvido"] = True
                    st.rerun()
            with cB:
                if st.button("↩️ Desmarcar todos", key=f"ap_all_unres_{versao_id}"):
                    st.session_state[df_key]["Resolvido"] = False
                    st.rerun()
            with cC:
                st.caption("Dica: marque em lote e ajuste linha a linha.")

            # --- (opcional) paginação na planilha para milhares ---
            plan_page_size = st.selectbox(
                "Linhas na planilha",
                [200, 500, 1000, 2000],
                index=0,
                key=f"ap_plan_page_size_{versao_id}",
            )

            if "ap_plan_page" not in st.session_state:
                st.session_state.ap_plan_page = 1

            total_plan = len(st.session_state[df_key])
            total_plan_pages = max(1, (total_plan + plan_page_size - 1) // plan_page_size)
            st.session_state.ap_plan_page = min(max(int(st.session_state.ap_plan_page), 1), total_plan_pages)

            st.caption(f"Planilha: página {st.session_state.ap_plan_page} de {total_plan_pages} — total {total_plan}")

            st.session_state.ap_plan_page = st.number_input(
                "Ir para página (planilha)",
                min_value=1,
                max_value=total_plan_pages,
                value=int(st.session_state.ap_plan_page),
                step=1,
                key=f"ap_plan_page_input_{versao_id}",
            )

            s = (st.session_state.ap_plan_page - 1) * plan_page_size
            e = s + plan_page_size

            # garante coluna de seleção ANTES do slice
            if "Selecionar" not in st.session_state[df_key].columns:
                st.session_state[df_key].insert(0, "Selecionar", False)
            st.session_state[df_key]["Selecionar"] = st.session_state[df_key]["Selecionar"].fillna(False).astype(bool)

            # garante ID numérico (evita sujeira antiga)
            st.session_state[df_key]["ID"] = pd.to_numeric(st.session_state[df_key]["ID"], errors="coerce")

            # fatia exibida (editor só para a página atual)
            df_slice = st.session_state[df_key].iloc[s:e].copy()

            edited_slice = st.data_editor(
                df_slice,
                use_container_width=True,
                hide_index=True,
                num_rows="fixed",
                # deixa editável SOMENTE Selecionar e Resolvido
                disabled=["ID", "Tipo", "Código", "Prioridade", "Impacto", "Registro", "Linha"],
                key=f"ap_editor_{versao_id}_{st.session_state.ap_plan_page}",
            )

            # salva de volta SOMENTE as colunas editáveis (evita corromper ID/colunas)
            idx = st.session_state[df_key].index[s:e]
            st.session_state[df_key].loc[idx, "Selecionar"] = edited_slice["Selecionar"].fillna(False).astype(
                bool).values
            st.session_state[df_key].loc[idx, "Resolvido"] = edited_slice["Resolvido"].fillna(False).astype(bool).values

            # validação rápida de ID
            if st.session_state[df_key]["ID"].isna().any():
                st.error("Há linhas com ID inválido/NaN no dataframe de apontamentos. Clique em 🔃 Atualizar dados.")
                st.stop()

            # ---------------------------
            # Preparar aplicação (confirmação em 2 passos)
            # ---------------------------
            prep_key = f"ap_pending_apply_{versao_id}"


            def _safe_int(v):
                try:
                    if v is None:
                        return None
                    s = str(v).strip()
                    if s == "":
                        return None
                    # aceita "15.0" vindo de pandas às vezes
                    if "." in s:
                        s = s.split(".")[0]
                    return int(s)
                except Exception:
                    return None


            bad = st.session_state[df_key][~st.session_state[df_key]["ID"].astype(str).str.match(r"^\d+(\.0)?$")]
            if not bad.empty:
                st.error("Existem linhas com ID inválido (texto) — isso impede preparar aplicação.")
                st.dataframe(bad[["ID", "Tipo", "Código", "Linha", "Resolvido"]].head(50))
                st.stop()


            def compute_changes(df_base, df_atual):
                base_map = {}
                invalid_base = 0
                for _, row in df_base.iterrows():
                    ap_id = _safe_int(row.get("ID"))
                    if ap_id is None:
                        invalid_base += 1
                        continue
                    base_map[ap_id] = parse_bool(row.get("Resolvido"))

                curr_map = {}
                invalid_curr = 0
                for _, row in df_atual.iterrows():
                    ap_id = _safe_int(row.get("ID"))
                    if ap_id is None:
                        invalid_curr += 1
                        continue
                    curr_map[ap_id] = parse_bool(row.get("Resolvido"))

                # (opcional) log no Streamlit se tiver inválidos
                if invalid_base or invalid_curr:
                    st.warning(
                        f"Algumas linhas foram ignoradas por ID inválido: base={invalid_base}, atual={invalid_curr}"
                    )

                to_resolver = []
                to_reabrir = []

                for ap_id, base_res in base_map.items():
                    curr_res = curr_map.get(ap_id, base_res)

                    if (base_res is False) and (curr_res is True):
                        to_resolver.append(ap_id)
                    elif (base_res is True) and (curr_res is False):
                        to_reabrir.append(ap_id)

                return to_resolver, to_reabrir


            # ---------------------------
            # Preparar aplicação (confirmação em 2 passos)
            # ---------------------------
            prep_key = f"ap_pending_apply_{versao_id}"
            flash_key = f"ap_flash_{versao_id}"

            # mostra “flash” após rerun
            flash = st.session_state.pop(flash_key, None)
            if flash:
                kind = flash.get("kind", "info")
                msg = flash.get("msg", "")
                if kind == "success":
                    st.success(msg)
                elif kind == "warning":
                    st.warning(msg)
                else:
                    st.info(msg)

            colP, colC = st.columns([1, 2])
            with colP:
                if st.button("📌 Preparar aplicação", key=f"ap_prepare_{versao_id}"):
                    to_resolver, to_reabrir = compute_changes(
                        st.session_state[base_key],
                        st.session_state[df_key],
                    )

                    st.session_state[prep_key] = {
                        "versao_id": int(versao_id),
                        "to_resolver": to_resolver,
                        "to_reabrir": to_reabrir,
                    }

                    if not to_resolver and not to_reabrir:
                        st.session_state[flash_key] = {"kind": "info", "msg": "Nenhuma alteração detectada."}
                    else:
                        st.session_state[flash_key] = {
                            "kind": "success",
                            "msg": f"Alterações preparadas: {len(to_resolver) + len(to_reabrir)}"
                        }

                    st.rerun()

            pending = st.session_state.get(prep_key)

            if pending and (pending.get("to_resolver") or pending.get("to_reabrir")):
                to_resolver = pending.get("to_resolver", [])
                to_reabrir = pending.get("to_reabrir", [])

                st.warning(
                    f"Confirme para aplicar {len(to_resolver) + len(to_reabrir)} alteração(ões): "
                    f"{len(to_resolver)} resolver, {len(to_reabrir)} reabrir."
                )

                c1, c2 = st.columns([1, 1])
                with c1:
                    if st.button("✅ Confirmar revisão e ir para Validar", key=f"confirm_review_{versao_id}"):
                        try:
                            pending = st.session_state.get(prep_key)

                            if not pending or (not pending.get("to_resolver") and not pending.get("to_reabrir")):
                                st.warning(
                                    "Antes de confirmar, clique em 📌 Preparar aplicação (nenhuma alteração preparada).")
                                st.stop()

                            # 1) APLICA EM LOTE (1 chamada)
                            batch_payload = {
                                "versao_id": int(versao_id),
                                "to_resolver": pending.get("to_resolver", []),
                                "to_reabrir": pending.get("to_reabrir", []),
                            }

                            url_batch = f"{API_BASE}/workflow/apontamento/batch"
                            rb = requests.patch(url_batch, json=batch_payload, timeout=60)

                            if not rb.ok:
                                st.error("Não foi possível aplicar as alterações em lote.")
                                try:
                                    st.json(rb.json())
                                except Exception:
                                    st.code(rb.text)
                                st.stop()

                            batch_resp = rb.json()

                            # Se tiver IDs ignorados, já avisa (versão errada / IDs inválidos)
                            ignorados = int(batch_resp.get("nao_encontrados_ou_outra_versao", 0) or 0)
                            pendentes_restantes = int(batch_resp.get("pendentes_restantes", 0) or 0)

                            if ignorados > 0:
                                st.warning(f"Atenção: {ignorados} ID(s) foram ignorados (fora da versão ou inválidos).")

                            # 2) CONFIRMA REVISÃO (sem payload)
                            url_confirm = f"{API_BASE}/workflow/versao/{int(versao_id)}/confirmar-revisao"
                            rc = requests.post(url_confirm, timeout=60)

                            if not rc.ok:
                                st.error("Não foi possível confirmar a revisão.")
                                try:
                                    st.json(rc.json())
                                except Exception:
                                    st.code(rc.text)
                                st.stop()

                            # tenta ler a resposta
                            confirm_resp = {}
                            try:
                                confirm_resp = rc.json() or {}
                            except Exception:
                                confirm_resp = {}

                            versao_revisada_id = confirm_resp.get("versao_revisada_id") or confirm_resp.get(
                                "versao_id_revisada")

                            # feedback
                            msg_extra = ""
                            if versao_revisada_id:
                                try:
                                    vr_int = int(versao_revisada_id)
                                    msg_extra = f" Versão revisada criada: v{vr_int}."
                                    # ✅ muda para a versão revisada para exportar o arquivo com alterações materializadas
                                    st.session_state.selected_versao_id = vr_int
                                except Exception:
                                    # se vier algo estranho, só ignora
                                    pass

                            st.success(
                                f"Alterações aplicadas: {batch_resp.get('updated_total', 0)}. "
                                f"Pendentes restantes: {pendentes_restantes}. "
                                f"Revisão confirmada!{msg_extra} Indo para Exportar..."
                            )

                            # limpa o que estava preparado para não reaplicar sem querer
                            st.session_state.pop(prep_key, None)

                            clear_after_workflow()

                            # ✅ vai para exportar
                            st.session_state.page = "4 — Exportar"
                            st.rerun()


                        except Exception as e:
                            st.error("Erro ao confirmar revisão.")
                            st.exception(e)

                with c2:
                    if st.button("❌ Cancelar", key=f"ap_cancel_apply_{versao_id}"):
                        st.session_state.pop(prep_key, None)
                        st.info("Aplicação cancelada.")
                        st.rerun()
            else:
                st.caption("Clique em “Preparar aplicação” para ver um resumo e confirmar antes de aplicar.")

    # ---------------------------

    # ----- Confirmar revisão (ir para validação)
    st.divider()
    st.subheader("Confirmar revisão")
    st.caption(
        "Use a **Planilha de revisão** para: marcar/desmarcar, clicar em **📌 Preparar aplicação** e depois **✅ Confirmar revisão**. "
        "Isso aplica em lote (robusto) e evita erros."
    )

    # Reprocessar
    # ---------------------------
    st.divider()
    st.markdown("### Reprocessar Apontamentos")

    motivo = st.text_input("Motivo (opcional)", value="")
    # --- Reprocessamento TOTAL (1 botão + confirmação por estado) ---
    if "confirm_reproc_total" not in st.session_state:
        st.session_state.confirm_reproc_total = False

    if not st.session_state.confirm_reproc_total:
        if st.button("🔄 Reprocessar TODOS os apontamentos"):
            st.session_state.confirm_reproc_total = True
            st.rerun()
    else:
        st.warning(
            "Isso irá reabrir TODOS os apontamentos e executar novamente as regras. "
            "Resoluções manuais serão perdidas."
        )

        col1, col2 = st.columns(2)

        with col1:
            if st.button("✅ Confirmar reprocessamento", type="primary"):
                payload = {"preservar_resolvidos": False}
                if motivo.strip():
                    payload["motivo"] = motivo

                rr = post(f"/workflow/versao/{int(versao_id)}/reprocessar", json=payload)

                if rr:

                    st.session_state.ap_cache_bust += 1
                    # 🔥 limpa caches para não mostrar dados antigos
                    cached_apontamentos.clear()
                    # cached_resumo_versao.clear()  # se existir; senão, deixe comentado

                    clear_after_workflow()
                    st.session_state.confirm_reproc_total = False
                    st.success("Reprocessamento total concluído.")
                    st.rerun()

        with col2:
            if st.button("Cancelar"):
                st.session_state.confirm_reproc_total = False
                st.rerun()

    # ---------------------------
    # CTA Exportar
    # ---------------------------
    st.divider()
    if str(status_versao).upper() == "VALIDADA":
        st.success("Versão validada. Pronta para exportação.")
        if st.button("➡️ Ir para Exportar"):
            goto("5 — Exportar")
    else:
        st.info("Resolva os apontamentos e valide a versão para liberar a exportação.")

# ===========================
# 4 — Editor C170
# ===========================
elif page == "4 — C170 - Editor":

    render_editor_c170()


# ===========================
# 5 — EXPORTAR
# ===========================
elif page == "5 — Exportar":

    if st.session_state.get("selected_versao_id") is None:
        st.info("Selecione uma versão primeiro.")
        st.stop()

    versao_id = st.session_state.get("selected_versao_id")

    st.subheader("Exportação")

    st.caption(f"Versão ID: {versao_id}")

    # Resumo (cache curto)
    try:
        resumo = cached_resumo_versao(API_BASE, int(versao_id))
    except Exception as e:
        st.error(str(e))
        resumo = {}

    status_versao = str(resumo.get("status", "—"))
    status_upper = status_versao.strip().upper()

    if status_upper in ("VALIDADA", "EXPORTADA"):
        st.success(f"Status da versão: {status_upper} (export liberado)")
    elif status_upper in ("EM_REVISAO", "EM REVISÃO"):
        st.warning("Status da versão: EM REVISÃO (necessário validar antes de exportar)")
    else:
        st.warning(f"Status da versão: {status_versao} (export bloqueado)")

    with st.expander("Ver resumo completo", expanded=False):
        st.json(resumo)

    # ------------------------------------------------------------------
    # Gate de validação automática (Opção 1)
    # ------------------------------------------------------------------
    def garantir_validacao() -> bool:
        """
        Regras finais:
        - VALIDADA ou EXPORTADA → pode exportar
        - EM_REVISAO → tenta validar
        - outros → bloqueia
        """
        if status_upper in ("VALIDADA", "EXPORTADA"):
            return True

        if status_upper in ("EM_REVISAO", "EM REVISÃO"):
            st.info("Validando versão antes de exportar...")
            resp = requests.post(
                api_url(f"/workflow/versao/{int(versao_id)}/validar"),
                timeout=TIMEOUT
            )

            if resp.status_code >= 400:
                show_error(resp)
                return False

            clear_after_workflow()
            st.success("Versão validada com sucesso. Export liberado.")
            st.rerun()

        st.warning("Export bloqueado. Volte ao Passo 3 e finalize a revisão.")
        return False


    # ------------------------------------------------------------------
    # Exportar SPED
    # ------------------------------------------------------------------
    st.divider()
    st.markdown("### Baixar SPED")

    if st.button("Download do SPED"):
        if not garantir_validacao():
            st.stop()

        url = api_url(f"/export/versao/{int(versao_id)}")
        resp = requests.get(url, timeout=TIMEOUT)

        if resp.status_code >= 400:
            show_error(resp)
        else:
            cd = resp.headers.get("content-disposition", "")
            filename = f"sped_versao_{int(versao_id)}.txt"

            if "filename=" in cd:
                filename = cd.split("filename=")[-1].split(";")[0].strip().strip('"')

            st.download_button(
                "⬇️ Baixar SPED",
                data=resp.content,
                file_name=filename,
                mime="text/plain",
            )

    # ------------------------------------------------------------------
    # Exportar apontamentos CSV
    # ------------------------------------------------------------------
    st.divider()
    st.markdown("### Baixar apontamentos (CSV)")

    st.info(
        "📄 **Apontamentos (CSV)** podem ser baixados **mesmo durante a revisão**. "
        "Use este arquivo para análise, conferência ou trabalho externo.\n\n"
        "⚠️ **O SPED oficial só é liberado após validação.**"
    )

    if st.button("Download dos apontamentos.csv"):
        url = api_url(f"/export/versao/{int(versao_id)}/apontamentos.csv")
        resp = requests.get(url, timeout=TIMEOUT)

        if resp.status_code >= 400:
            show_error(resp)
        else:
            st.download_button(
                "⬇️ Baixar apontamentos.csv",
                data=resp.content,
                file_name=f"apontamentos_versao_{int(versao_id)}.csv",
                mime="text/csv",
            )

    st.divider()
    if st.button("⬅️ Revisar & Apontamentos"):
        goto("3 — Revisar & Apontamentos")
