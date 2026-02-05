from app.db.models.efd_registro import EfdRegistro
from app.db.models.efd_revisao import EfdRevisao
from app.db.models.efd_versao import EfdVersao
from app.db.models.ref_models import RefCstPisCofins, RefCfop
from app.sped.blocoC.c170_utils import _parse_linha_sped_to_reg_dados, _parse_sped_float
from typing import Any, Dict, Optional, List, Tuple
from sqlalchemy.orm import Session
from decimal import Decimal
from app.sped.revisao_overlay import LinhaLogica
from sqlalchemy import func
import re

CPF_RE = re.compile(r"^\d{11}$")

def _to_decimal(self, s):
    if not s:
        return None
    txt = str(s).replace(".", "").replace(",", ".")
    try:
        return Decimal(txt)
    except:
        return None

def norm(x: Optional[str]) -> Optional[str]:
    if x is None:
        return None
    s = str(x).strip()
    return s if s else None

def ensure_len(d: list, idx: int) -> None:
    # garante que existe d[idx]
    if len(d) <= idx:
        d.extend([""] * (idx + 1 - len(d)))


def obter_linha_final(db: Session, registro_id: int, linha_original: str, versao_id: int) -> str:
    """
    ESSA É A FUNÇÃO CHAVE: Usada pelo Writer e pelo Renderer.
    Ela garante que a tela e o arquivo final mostrem a mesma coisa.
    """
    revisao = db.query(EfdRevisao).filter(
        EfdRevisao.registro_id == registro_id,
        EfdRevisao.versao_origem_id == versao_id,
        EfdRevisao.versao_revisada_id.is_(None)
    ).first()

    if revisao and "linha_nova" in revisao.revisao_json:
        return revisao.revisao_json["linha_nova"]

    return linha_original


def calcular_totais_filhos(db: Session, versao_id: int, c100_id: int):
    """Soma PIS/COFINS dos C170 vinculados ao C100 via pai_id."""
    itens_c170 = db.query(EfdRegistro).filter(
        EfdRegistro.versao_id == versao_id,
        EfdRegistro.pai_id == c100_id,
        EfdRegistro.reg == "C170"
    ).all()

    t_pis = 0.0
    t_cofins = 0.0

    for item in itens_c170:
        # Usa obter_linha_final para considerar itens já revisados na soma
        linha = obter_linha_final(db, item.id, "", versao_id)
        if not linha:  # Se não houver revisão, pega o original
            dados = _get_dados(item)
        else:
            _, dados = _parse_linha_sped_to_reg_dados(linha)

        # C170
        # VL_PIS    -> índice 28
        # VL_COFINS -> índice 34
        if len(dados) > 34:
            t_pis += _parse_sped_float(dados[28])
            t_cofins += _parse_sped_float(dados[34])

    return t_pis, t_cofins

def buscar_pai_c100(db: Session, versao_id: int, linha_item: int) -> EfdRegistro:
    """
    Busca o C100 que precede o item atual.
    """
    return db.query(EfdRegistro).filter(
        EfdRegistro.versao_id == versao_id,
        EfdRegistro.reg == "C100",
        EfdRegistro.linha < linha_item
    ).order_by(EfdRegistro.linha.desc()).first()


def popular_pai_id(db: Session, versao_id: int):
    """
    Lógica Rígida: Vincula registros filhos (C170, C190, etc.) ao último C100
    encontrado acima deles no arquivo, garantindo a integridade hierárquica.
    """
    print(f"🔍 Iniciando vinculação hierárquica para a versão {versao_id}...")

    # Buscamos apenas os registros que participam da hierarquia do Bloco C
    # Ordenar pela 'linha' é OBRIGATÓRIO para a lógica sequencial funcionar
    registros = db.query(EfdRegistro).filter(
        EfdRegistro.versao_id == versao_id,
        EfdRegistro.reg.in_(['C100', 'C110', 'C170', 'C190'])
    ).order_by(EfdRegistro.linha.asc()).all()

    ultimo_c100_id = None
    vinculados_count = 0
    erros_count = 0

    for r in registros:
        if r.reg == "C100":
            # Novo cabeçalho encontrado: ele passa a ser o pai dos próximos registros
            ultimo_c100_id = r.id
        elif r.reg in ["C110", "C170", "C190"]:
            # Tenta vincular ao pai atual
            if ultimo_c100_id:
                r.pai_id = ultimo_c100_id
                vinculados_count += 1
            else:
                # Caso o arquivo tenha um item antes de qualquer nota (erro de estrutura)
                erros_count += 1
                print(f"⚠️ Alerta: Registro {r.reg} na linha {r.linha} não possui um C100 antecedente.")

    # Persiste as alterações no banco
    db.commit()

    print(f"✅ Vinculação concluída!")
    print(f"   - Sucesso: {vinculados_count} registros vinculados.")
    if erros_count > 0:
        print(f"   - Erros: {erros_count} registros órfãos encontrados.")

    return vinculados_count


def _reg_of(item) -> str:
    """Extrai o nome do registro com suporte a múltiplos formatos de objeto."""
    # 1. Se for string pura (Ex: |C100|...)
    if isinstance(item, str):
        parts = item.split("|")
        if len(parts) > 1 and parts[1].strip():
            return parts[1].strip().upper()
        return "IGNORAR"

    # 2. Se for um objeto com atributo 'reg' (O caso do seu Loader)
    reg = getattr(item, "reg", None)
    if reg:
        return str(reg).strip().upper()

    # 3. Se for um objeto com atributo 'dados' (Caso da LinhaSpedDinamica)
    if hasattr(item, "dados") and isinstance(item.dados, list) and len(item.dados) > 0:
        # Tenta inferir se for um objeto genérico
        return "IGNORAR"

    return "IGNORAR"

def _get_dados(r: EfdRegistro) -> list[Any]:
    cj: Dict[str, Any] = getattr(r, "conteudo_json", None) or {}
    dados_raw = cj.get("dados")

    if not isinstance(dados_raw, list):
        raise ValueError("Registro não possui dados em formato lista.")

    reg = (getattr(r, "reg", "") or "").strip().upper()

    # ✅ Normaliza QUALQUER registro no formato ["REG", [...]]
    if (
        len(dados_raw) == 2
        and isinstance(dados_raw[0], str)
        and isinstance(dados_raw[1], list)
        and (dados_raw[0] or "").strip().upper() == reg
    ):
        return list(dados_raw[1] or [])

    return list(dados_raw or [])



def aplicar_overlay_revisoes_c170(
    db,
    *,
    versao_id: int,
    items: List[Dict[str, Any]],
) -> Tuple[List[Dict[str, Any]], int]:
    if not items:
        return items, 0

    registro_ids = [int(it["registro_id"]) for it in items if it.get("registro_id")]
    if not registro_ids:
        return items, 0

    # Descobre se esta versão é revisada (retifica outra)
    versao = db.get(EfdVersao, int(versao_id))
    retifica_de = int(getattr(versao, "retifica_de_versao_id", 0) or 0) if versao else 0

    q = (
        db.query(EfdRevisao)
        .filter(EfdRevisao.reg == "C170")
        .filter(EfdRevisao.acao == "REPLACE_LINE")
        .filter(EfdRevisao.registro_id.in_(registro_ids))
    )

    if retifica_de:
        # ✅ estamos olhando a VERSÃO REVISADA (ex.: 63):
        # queremos as revisões "carimbadas" para essa versão revisada
        q = q.filter(EfdRevisao.versao_revisada_id == int(versao_id))
        # (opcional) se existir coluna versao_origem_id, pode restringir:
        # q = q.filter(EfdRevisao.versao_origem_id == retifica_de)
    else:
        # ✅ estamos olhando a VERSÃO ORIGEM (ex.: 62):
        # queremos revisões PENDENTES (ainda não carimbadas)
        q = q.filter(EfdRevisao.versao_origem_id == int(versao_id))
        q = q.filter(EfdRevisao.versao_revisada_id.is_(None))

    revs = q.order_by(EfdRevisao.created_at.desc(), EfdRevisao.id.desc()).all()

    by_registro: Dict[int, EfdRevisao] = {}
    for rev in revs:
        rid = int(rev.registro_id or 0)
        if rid and rid not in by_registro:
            by_registro[rid] = rev

    aplicadas = 0
    for it in items:
        rid = int(it.get("registro_id") or 0)
        rev = by_registro.get(rid)
        if not rev:
            continue

        rj = rev.revisao_json or {}
        linha_nova = rj.get("linha_nova")
        if not linha_nova:
            continue

        reg, dados = _parse_linha_sped_to_reg_dados(str(linha_nova))
        if (reg or "").strip().upper() != "C170":
            continue

        it["dados"] = dados
        it["alterado"] = True
        it["revisao_id"] = int(rev.id)
        aplicadas += 1

    return items, aplicadas


def calcular_totais_pis_cofins_filhos(linhas: List[LinhaLogica], c100_id: int) -> Tuple[float, float]:
    total_pis = 0.0
    total_cofins = 0.0

    filhos = [l for l in linhas if l.reg == "C170" and getattr(l, "pai_id", None) == c100_id]

    for filho in filhos:
        if len(filho.dados) > 34:
            total_pis += _parse_sped_float(filho.dados[28])
            total_cofins += _parse_sped_float(filho.dados[34])

    return total_pis, total_cofins




def aplicar_overlay_generico(db, versao_id: int, items: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    if not items:
        return items

    registro_ids = [int(it["registro_id"]) for it in items if it.get("registro_id")]
    versao = db.get(EfdVersao, int(versao_id))
    retifica_de = int(getattr(versao, "retifica_de_versao_id", 0) or 0) if versao else 0

    # Busca revisões para qualquer REG que esteja nos itens
    q = db.query(EfdRevisao).filter(EfdRevisao.registro_id.in_(registro_ids))

    if retifica_de:
        q = q.filter(EfdRevisao.versao_revisada_id == int(versao_id))
    else:
        q = q.filter(EfdRevisao.versao_origem_id == int(versao_id), EfdRevisao.versao_revisada_id.is_(None))

    revs = q.order_by(EfdRevisao.created_at.desc(), EfdRevisao.id.desc()).all()

    # Mapeia ID do registro -> Linha Nova
    revisoes_map = {rev.registro_id: rev.revisao_json.get("linha_nova") for rev in revs}

    for it in items:
        rid = it.get("registro_id")
        linha_nova = revisoes_map.get(rid)
        if linha_nova:
            # Aqui usamos o parser para devolver os dados em lista para o Front/Writer
            reg_nome, novos_dados = _parse_linha_sped_to_reg_dados(str(linha_nova))
            it["dados"] = novos_dados
            it["alterado"] = True

    return items


def obter_conteudo_final(r: Any) -> str:
    """
    Centraliza a lógica de priorização:
    1. Revisão Direta > 2. Atributos reg/dados > 3. Render Interno > 4. JSON
    """
    from app.sped.formatter import formatar_linha

    # 1. Se já for string, devolve
    if isinstance(r, str):
        return r

    # 2. Se tiver o atributo 'reg' e 'dados' diretamente (Caso das LinhasLogicas e Dinâmicas)
    reg_attr = getattr(r, "reg", None)
    dados_attr = getattr(r, "dados", None)
    if reg_attr and dados_attr is not None:
        return formatar_linha(str(reg_attr), list(dados_attr))

    # 3. Se já houver uma revisão aplicada em string
    if hasattr(r, "linha_revisada") and r.linha_revisada:
        return str(r.linha_revisada)

    # 4. Se for um objeto com lógica de renderização própria
    if hasattr(r, "render_linha"):
        return r.render_linha()

    # 5. Se for o objeto EfdRegistro bruto do banco (JSON)
    if hasattr(r, "conteudo_json") and r.conteudo_json:
        dados = r.conteudo_json.get("dados", [])
        return formatar_linha(getattr(r, "reg", ""), dados)

    return ""

def _cod_part_do_c100(reg_c100: EfdRegistro) -> str | None:
    dados = _get_dados(reg_c100)
    # C100 campo 4 = COD_PART (layout sem REG)
    # índice 3 (0-based)
    if len(dados) > 3:
        v = (dados[3] or "").strip()
        return v or None
    return None

def _cpf_do_0150(dados_0150: list) -> str | None:
    # 0150 campo 6 = CNPJ
    # 0150 campo 7 = CPF
    # layout sem REG -> índices 5 e 6
    cpf = None
    if len(dados_0150) > 6:
        cpf = (dados_0150[6] or "").strip()
    if not cpf:
        return None
    cpf_digits = "".join(ch for ch in cpf if ch.isdigit())
    return cpf_digits or None

def _normaliza_doc(s: Any) -> str:
    s = "" if s is None else str(s)
    # remove pontuação comum
    return (
        s.replace(".", "")
         .replace("-", "")
         .replace("/", "")
         .replace(" ", "")
         .strip()
    )



def eh_pf_por_c100(db: Session, versao_id: int, registro_id: int) -> bool:
    """
    True = PF (CPF) / False = PJ (CNPJ) ou ambíguo (não bloqueia).
    Regra: se existir QUALQUER CNPJ, NUNCA bloqueia como PF.
    """

    def only_digits(x: object) -> str:
        return "".join(ch for ch in str(x or "") if ch.isdigit())

    def offset_if_reg_first(dados: list, reg: str) -> int:
        return 1 if dados and str(dados[0]).strip().upper() == reg else 0

    def norm_cpf(raw: object) -> str:
        d = only_digits(raw)
        if not d:
            return ""
        if len(d) > 11:
            d = d[-11:]
        return d.zfill(11)

    reg_atual = db.get(EfdRegistro, int(registro_id))
    if not reg_atual:
        return False

    # acha C100 pai (por id anterior)
    reg_c100 = reg_atual
    if reg_atual.reg == "C170":
        reg_c100 = (
            db.query(EfdRegistro)
            .filter(
                EfdRegistro.versao_id == int(versao_id),
                EfdRegistro.reg == "C100",
                EfdRegistro.id < int(reg_atual.id),
            )
            .order_by(EfdRegistro.id.desc())
            .first()
        )
    if not reg_c100:
        return False

    dados_c100 = _get_dados(reg_c100)
    off_c100 = offset_if_reg_first(dados_c100, "C100")

    cod_part = str(dados_c100[2 + off_c100]).strip() if len(dados_c100) > (2 + off_c100) else ""
    if not cod_part:
        return False

    # tenta achar 0150 por $.dados[0] e fallback $.dados[1]
    reg_0150 = (
        db.query(EfdRegistro)
        .filter(
            EfdRegistro.versao_id == int(versao_id),
            EfdRegistro.reg == "0150",
            func.json_unquote(func.json_extract(EfdRegistro.conteudo_json, "$.dados[0]")) == cod_part,
        )
        .first()
    )
    if not reg_0150:
        reg_0150 = (
            db.query(EfdRegistro)
            .filter(
                EfdRegistro.versao_id == int(versao_id),
                EfdRegistro.reg == "0150",
                func.json_unquote(func.json_extract(EfdRegistro.conteudo_json, "$.dados[1]")) == cod_part,
            )
            .first()
        )
    if not reg_0150:
        return False

    d = _get_dados(reg_0150)
    off_0150 = offset_if_reg_first(d, "0150")

    raw_cnpj = d[3 + off_0150] if len(d) > (3 + off_0150) else ""
    raw_cpf  = d[4 + off_0150] if len(d) > (4 + off_0150) else ""

    # se tiver QUALQUER CNPJ => PJ
    if only_digits(raw_cnpj):
        return False

    cpf = norm_cpf(raw_cpf)
    return len(cpf) == 11 and cpf != "00000000000"

