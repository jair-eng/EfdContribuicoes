from typing import Any, Iterable, Optional


def resolver_cod_cta_para_insert_c170(
    *,
    alvo: Any,
    linhas_base: Iterable[Any],
    cod_cta_padrao_0500: str | None = None,
) -> str:
    """
    Resolve COD_CTA para C170 inserido.

    Prioridade:
    1) C170 do mesmo documento (mesmo C100 pai)
    2) fallback do 0500
    """
    alvo_reg = str(getattr(alvo, "reg", "") or "").strip().upper()
    alvo_rid = int(getattr(alvo, "registro_id", 0) or 0)
    alvo_pai = int(getattr(alvo, "pai_id", 0) or 0)

    # Descobre o C100 do documento
    c100_id = 0
    if alvo_reg == "C100" and alvo_rid > 0:
        c100_id = alvo_rid
    elif alvo_pai > 0:
        c100_id = alvo_pai

    if c100_id > 0:
        for ln in linhas_base or []:
            reg = str(getattr(ln, "reg", "") or "").strip().upper()
            if reg != "C170":
                continue

            pai_id = int(getattr(ln, "pai_id", 0) or 0)
            if pai_id != c100_id:
                continue

            dados = list(getattr(ln, "dados", None) or [])
            if len(dados) >= 36:
                cod_cta = str(dados[35] or "").strip()
                if cod_cta:
                    return cod_cta

    return str(cod_cta_padrao_0500 or "").strip()

def resolver_cod_cta_padrao_0500(linhas_base: Iterable[Any]) -> str:
    for ln in linhas_base or []:
        reg = str(getattr(ln, "reg", "") or "").strip().upper()
        if reg != "0500":
            continue

        dados = list(getattr(ln, "dados", None) or [])
        if len(dados) > 4:
            cod_cta = str(dados[4] or "").strip()
            if cod_cta:
                return cod_cta
    return ""