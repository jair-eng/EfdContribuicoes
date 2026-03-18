from __future__ import annotations

from typing import Dict, List, Optional
from app.sped.blocoC.c170_utils import _parse_linha_sped_to_reg_dados_preservando_finais_vazios
from app.sped.revisao_overlay import LinhaLogica
from app.sped.utils_cod_cta import resolver_cod_cta_padrao_0500, resolver_cod_cta_para_insert_c170


def aplicar_revisoes_insert(
    *,
    linhas_base: List["LinhaLogica"],
    revisoes: List[Dict],
    preferir_ultima: bool = True,
) -> List["LinhaLogica"]:
    if not linhas_base:
        return []

    if not revisoes:
        return list(linhas_base)

    resultado: List[LinhaLogica] = [l for l in linhas_base]

    inserts_after: List[Dict] = []
    inserts_before: List[Dict] = []

    for r in revisoes:
        acao = str(r.get("acao") or "").upper()
        if acao == "INSERT_AFTER":
            inserts_after.append(r)
        elif acao == "INSERT_BEFORE":
            inserts_before.append(r)

    def _preparar(rv: Dict) -> Optional[Dict]:
        rj = rv.get("revisao_json") or {}
        linha_txt = rj.get("linha_nova") or rv.get("linha") or rj.get("linha") or ""
        if not linha_txt:
            return None

        linha_ref = rv.get("linha_num") or rj.get("linha_num") or rj.get("linha_referencia") or 0

        rr = dict(rv)
        rr["linha_final"] = str(linha_txt)
        rr["linha_ref"] = int(linha_ref or 0)
        return rr

    def _rev_key(rv: Dict) -> int:
        try:
            return int(rv.get("id") or 0)
        except Exception:
            return 0

    def _alvo_key(rv: Dict) -> int:
        rid = int(rv.get("registro_id") or 0)
        if rid > 0:
            return rid
        lr = int(rv.get("linha_ref") or 0)
        return -lr if lr > 0 else 0

    def _renumerar_resultado() -> None:
        for i, l in enumerate(resultado, start=1):
            l.linha = i

    def _escolher_vencedoras(lista: List[Dict]) -> List[Dict]:
        preparadas = []
        for rv in lista:
            p = _preparar(rv)
            if p:
                preparadas.append(p)

        vencedora_por_alvo: Dict[int, Dict] = {}
        for rv in preparadas:
            k = _alvo_key(rv)
            if not k:
                continue

            atual = vencedora_por_alvo.get(k)
            if atual is None:
                vencedora_por_alvo[k] = rv
                continue

            if preferir_ultima:
                if _rev_key(rv) >= _rev_key(atual):
                    vencedora_por_alvo[k] = rv
            else:
                if _rev_key(rv) <= _rev_key(atual):
                    vencedora_por_alvo[k] = rv

        return list(vencedora_por_alvo.values())

    inserts_before_final = _escolher_vencedoras(inserts_before)
    inserts_after_final = _escolher_vencedoras(inserts_after)

    def _achar_indice_alvo(rv: Dict) -> int:
        rid = int(rv.get("registro_id") or 0)
        linha_ref = int(rv.get("linha_ref") or 0)

        for idx, l in enumerate(resultado):
            if rid and int(getattr(l, "registro_id", 0) or 0) == rid:
                return idx
            if linha_ref and int(getattr(l, "linha", 0) or 0) == linha_ref:
                return idx
        return -1

    #Trazendo a conta
    cod_cta_padrao_0500 = resolver_cod_cta_padrao_0500(linhas_base)

    def _criar_linha_inserida(rv: Dict,alvo: "LinhaLogica",linhas_base: list["LinhaLogica"],cod_cta_padrao_0500: str,
    ) -> Optional["LinhaLogica"]:
        try:
            reg, dados = _parse_linha_sped_to_reg_dados_preservando_finais_vazios(str(rv["linha_final"]))
            if not reg:
                return None

            if str(reg).upper() == "C170":
                cod_cta = str(dados[35] or "").strip() if len(dados) >= 36 else ""

                if not cod_cta:
                    cod_cta = resolver_cod_cta_para_insert_c170(
                        alvo=alvo,
                        linhas_base=linhas_base,
                        cod_cta_padrao_0500=cod_cta_padrao_0500,
                    )

                    while len(dados) < 36:
                        dados.append("")

                    dados[35] = cod_cta

            pai_id = getattr(alvo, "pai_id", None)

            if str(reg).upper() == "C170":
                if str(getattr(alvo, "reg", "")).upper() == "C100":
                    pai_id = getattr(alvo, "registro_id", None)
                else:
                    pai_id = getattr(alvo, "pai_id", None)

            nova = LinhaLogica(
                linha=0,
                reg=str(reg).upper(),
                dados=list(dados or []),
                origem="INSERIDO",
                registro_id=None,
                pai_id=int(pai_id or 0) or None,
                revisao_id=int(rv.get("id") or 0) or None,
            )


            return nova

        except Exception as e:
            print("[DBG CRIAR INSERIDA ERRO]", repr(e), flush=True)
            return None

    # INSERT_BEFORE primeiro
    for rv in inserts_before_final:
        idx = _achar_indice_alvo(rv)
        if idx < 0:
            continue

        alvo = resultado[idx]
        nova = _criar_linha_inserida(
            rv,
            alvo,
            linhas_base,
            cod_cta_padrao_0500,
        )
        if nova:
            resultado.insert(idx, nova)
            _renumerar_resultado()

    # INSERT_AFTER depois
    for rv in inserts_after_final:
        idx = _achar_indice_alvo(rv)
        if idx < 0:
            continue

        alvo = resultado[idx]
        nova = _criar_linha_inserida(rv,alvo,linhas_base,cod_cta_padrao_0500,)
        if nova:
            resultado.insert(idx + 1, nova)
            _renumerar_resultado()

    # renumera
    _renumerar_resultado()

    print(
        f"OVERLAY_INSERT> final_lines={len(resultado)} "
        f"inserts_before={len(inserts_before_final)} "
        f"inserts_after={len(inserts_after_final)}"
    )

    return resultado