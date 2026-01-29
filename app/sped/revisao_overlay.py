from __future__ import annotations
from dataclasses import dataclass
from typing import Iterable, List, Dict, Any, Optional, Literal, Tuple, Set
from app.db.models import EfdRevisao,EfdRegistro
from app.sped.c170_utils import _parse_linha_sped_to_reg_dados
from dataclasses import dataclass



@dataclass
class LinhaLogica:
    linha: int
    reg: str
    dados: List[Any]
    origem: Literal["ORIGINAL", "REVISAO", "INSERIDO", "DELETADO"] = "ORIGINAL"
    registro_id: Optional[int] = None
    revisao_id: Optional[int] = None


    @classmethod
    def from_efd_registro(cls, r: "EfdRegistro") -> "LinhaLogica":
        """
        Converte um EfdRegistro do banco em LinhaLogica (campos SEM o REG).
        Suporta conteudo_json['dados'] em dois formatos:
          - ["C170", [...campos...]]
          - [...campos...]
        """
        cj: Dict[str, Any] = getattr(r, "conteudo_json", None) or {}
        dados_raw = cj.get("dados") or []

        reg = str(getattr(r, "reg", "") or "").strip().upper()

        # normaliza dados
        dados: List[Any] = []
        if isinstance(dados_raw, list):
            if (
                len(dados_raw) == 2
                and isinstance(dados_raw[0], str)
                and isinstance(dados_raw[1], list)
                and (dados_raw[0] or "").strip().upper() == reg
            ):
                dados = list(dados_raw[1] or [])
            else:
                dados = list(dados_raw or [])
        else:
            dados = []

        linha_num = int(getattr(r, "linha", 0) or getattr(r, "linha_num", 0) or 0)

        return cls(
            linha=linha_num,
            reg=reg,
            dados=dados,
            origem="ORIGINAL",
            registro_id=int(getattr(r, "id", 0) or 0) or None,
            revisao_id=None,
        )


def aplicar_revisoes_replace_line(
        *,
        linhas_originais: List["LinhaLogica"],
        revisoes: List[Dict[str, Any]],
        preferir_ultima: bool = True,
) -> List["LinhaLogica"]:
    if not linhas_originais or not revisoes:
        return linhas_originais or []

    # Indexadores para busca rápida
    por_id: Dict[int, LinhaLogica] = {
        int(l.registro_id): l
        for l in linhas_originais
        if getattr(l, "registro_id", None) is not None
    }
    por_linha: Dict[int, LinhaLogica] = {
        int(l.linha): l for l in linhas_originais if getattr(l, "linha", None) is not None
    }

    # Separamos DELETE de REPLACE para garantir a ordem de execução
    deletes: List[Dict[str, Any]] = []
    replaces: List[Dict[str, Any]] = []

    for r in revisoes:
        acao = str(r.get("acao") or "").upper()
        if acao == "DELETE":
            deletes.append(r)
        elif acao == "REPLACE_LINE":
            replaces.append(r)

    # 1) PROCESSAR DELETES (Prioridade Máxima)
    # Se deletar, a linha nem deve ser considerada para REPLACE
    registros_deletados: Set[int] = set()
    for d in deletes:
        rid = int(d.get("registro_id") or 0)
        if rid:
            alvo = por_id.get(rid)
            if alvo:
                alvo.origem = "DELETADO"
                registros_deletados.add(rid)

    # 2) PROCESSAR REPLACES
    revisoes_validas: List[Dict[str, Any]] = []
    for r in replaces:
        rj = r.get("revisao_json") or {}
        linha_txt = r.get("linha") or rj.get("linha_nova") or rj.get("linha") or ""
        if not linha_txt:
            continue

        linha_ref = r.get("linha_num") or rj.get("linha_num") or rj.get("linha_referencia") or 0
        rr = dict(r)
        rr["linha_final"] = str(linha_txt)
        rr["linha_ref"] = int(linha_ref or 0)
        revisoes_validas.append(rr)

    if not preferir_ultima:
        revisoes_validas = list(reversed(revisoes_validas))

    hits_id = hits_linha = miss = 0

    for rv in revisoes_validas:
        rid = int(rv.get("registro_id") or 0)

        # Se o registro já foi deletado, ignoramos o replace
        if rid in registros_deletados:
            continue

        linha_ref = int(rv.get("linha_ref") or 0)
        linha_txt = str(rv["linha_final"])

        alvo = por_id.get(rid) if rid else None
        if alvo is None and linha_ref:
            alvo = por_linha.get(linha_ref)

        if not alvo or alvo.origem == "DELETADO":
            miss += 1
            continue

        try:
            reg, dados = _parse_linha_sped_to_reg_dados(linha_txt)
            if not reg or str(reg).strip().upper() != str(alvo.reg).strip().upper():
                miss += 1
                continue

            alvo.dados = list(dados or [])
            alvo.origem = "REVISAO"
            alvo.revisao_id = int(rv.get("id") or 0) or None

            if rid and por_id.get(rid) is alvo:
                hits_id += 1
            else:
                hits_linha += 1
        except Exception:
            miss += 1
            continue

    # 3) FILTRAGEM FINAL
    # Removemos fisicamente da lista o que foi marcado como DELETADO
    resultado = [l for l in linhas_originais if l.origem != "DELETADO"]

    print(
        f"OVERLAY> final_lines={len(resultado)} (deletados={len(registros_deletados)}) "
        f"hits_id={hits_id} hits_linha={hits_linha} miss={miss}"
    )

    return resultado