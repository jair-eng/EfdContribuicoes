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



def post(path: str, json: dict | None = None, timeout: int = 60, **kwargs): # <-- Adicione **kwargs
    url = api_url(path)
    try:
        # Repasse o **kwargs para o requests.post
        resp = requests.post(url, json=json, timeout=timeout, **kwargs)
        return resp
    except requests.RequestException as e:
        st.error(f"Falha na chamada POST {path}: {e}")
        raise


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



