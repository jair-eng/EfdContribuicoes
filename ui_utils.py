from urllib.parse import urljoin

import streamlit as st
import requests

API_BASE = "http://127.0.0.1:8000"
TIMEOUT = 300 # 5 min



def parse_bool(v):
    if v is True or v is False:
        return v
    if v is None:
        return False
    if isinstance(v, (int, float)):
        return bool(v)
    if isinstance(v, str):
        s = v.strip().lower()
        if s in ("true", "1", "sim", "yes"):
            return True
        if s in ("false", "0", "nao", "não", "no", ""):
            return False
    return False


def goto(target: str):
    st.session_state["page"] = target
    st.rerun()


def api_url(path: str) -> str:
    return urljoin(API_BASE + "/", path.lstrip("/"))


def show_error(r: requests.Response):
    try:
        st.error(f"Erro {r.status_code}: {r.json()}")
    except Exception:
        st.error(f"Erro {r.status_code}: {r.text}")


def get(path, params=None):
    r = requests.get(api_url(path), params=params, timeout=TIMEOUT)
    if r.status_code >= 400:
        show_error(r)
        return None
    return r


def post(path, json=None, files=None):
    r = requests.post(api_url(path), json=json, files=files, timeout=TIMEOUT)
    if r.status_code >= 400:
        show_error(r)
        return None
    return r


def patch(path, json=None):
    r = requests.patch(api_url(path), json=json, timeout=TIMEOUT)
    if r.status_code >= 400:
        show_error(r)
        return None
    return r


# ---------------------------
# Cache leve (somente GETs repetidos)
# ---------------------------
@st.cache_data(ttl=300)  # 5 min
def cached_health(api_base: str):
    r = requests.get(f"{api_base}/health", timeout=TIMEOUT)
    r.raise_for_status()
    return r.json()


@st.cache_data(ttl=120)  # 2 min
def cached_empresas(api_base: str):
    r = requests.get(f"{api_base}/browse/empresas", timeout=TIMEOUT)
    r.raise_for_status()
    return r.json() or []

@st.cache_data(ttl=30)
def cached_empresa_resumo(api_base: str, empresa_id: int):
    r = requests.get(f"{api_base}/empresa/{empresa_id}/resumo", timeout=TIMEOUT)
    r.raise_for_status()
    return r.json() or {}


@st.cache_data(ttl=120)  # 2 min
def cached_arquivos(api_base: str, empresa_id: int):
    r = requests.get(f"{api_base}/browse/empresas/{empresa_id}/arquivos", timeout=TIMEOUT)
    r.raise_for_status()
    return r.json() or []


@st.cache_data(ttl=120)  # 2 min
def cached_versoes(api_base: str, arquivo_id: int):
    r = requests.get(f"{api_base}/browse/arquivos/{arquivo_id}/versoes", timeout=TIMEOUT)
    r.raise_for_status()
    return r.json() or []


@st.cache_data(ttl=30)  # curto
def cached_resumo_versao(api_base: str, versao_id: int):
    r = requests.get(f"{api_base}/workflow/versao/{versao_id}/resumo", timeout=TIMEOUT)
    r.raise_for_status()
    return r.json() or {}


@st.cache_data(ttl=15)  # bem curto
def cached_apontamentos(api_base: str, versao_id: int, bust: int):
    r = requests.get(f"{api_base}/workflow/versao/{versao_id}/apontamentos", timeout=TIMEOUT)
    r.raise_for_status()
    return r.json() or []



def clear_after_confirm():
    cached_empresas.clear()
    cached_arquivos.clear()
    cached_versoes.clear()
    cached_resumo_versao.clear()
    cached_apontamentos.clear()


def clear_after_workflow():
    cached_resumo_versao.clear()
    cached_apontamentos.clear()



def clear_after_retificar():
    cached_versoes.clear()
    cached_resumo_versao.clear()
    cached_apontamentos.clear()

