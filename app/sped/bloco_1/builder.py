from __future__ import annotations
from decimal import Decimal
from typing import List, Tuple
from sqlalchemy.orm import Session
from app.services.versao_overlay_service import carregar_linhas_logicas_com_revisoes
from app.sped.blocoM.m_utils import _clean_sped_line, _reg_of_line, _d
from app.sped.bloco_1.reg1100 import linha_1100
from app.sped.logic.consolidador import obter_conteudo_final
from dataclasses import dataclass
from pathlib import Path
from typing import Optional
import re


@dataclass(frozen=True)
class SpedInfo:
    path: Path
    cnpj: Optional[str]
    periodo: Optional[int]  # YYYYMM
    mtime: float

@dataclass(frozen=True)
class Ultimo1100:
    periodo: str      # YYYYMM
    saldo: Decimal
    linha: str        # linha original


def _parse_reg_dados(linha: str) -> Tuple[str, List[str]]:
    ln = _clean_sped_line(linha)
    parts = ln.strip("|").split("|")
    if not parts:
        return "", []
    return parts[0].upper(), parts[1:]


def _extrair_periodo(dados: List[str]) -> str:
    return (dados[0] or "").strip()


def _extrair_saldo(dados: List[str]) -> Decimal:
    """
    Estratégia robusta:
    pega o último campo numérico não vazio
    (bate com arquivos retificados reais)
    """
    for v in reversed(dados):
        s = (v or "").strip()
        if not s:
            continue
        return _d(s)
    return Decimal("0")


def encontrar_1100(linhas_sped: List[str]) -> List[Ultimo1100]:
    encontrados: List[Ultimo1100] = []

    for ln in linhas_sped or []:
        if _reg_of_line(ln) != "1100":
            continue

        reg, dados = _parse_reg_dados(ln)
        if reg != "1100" or len(dados) < 1:
            continue

        periodo = _extrair_periodo(dados)
        if not periodo or len(periodo) != 6:
            continue

        saldo = _extrair_saldo(dados)
        encontrados.append(
            Ultimo1100(periodo=periodo, saldo=saldo, linha=_clean_sped_line(ln))
        )

    return encontrados


def montar_bloco_1_1100_cumulativo(
    *,
    linhas_sped: List[str],
    periodo_atual: str,     # YYYYMM
    cod_cont: str,          # "201"
    credito_mes: Decimal,
) -> List[str]:
    """
    - Remove 1100 do período atual (se existir)
    - Soma saldo anterior + crédito do mês
    - Gera UM único 1100 correto
    """

    encontrados = encontrar_1100(linhas_sped)

    # separa 1100 do período atual
    anteriores = [x for x in encontrados if x.periodo < periodo_atual]
    mesmo_periodo = [x for x in encontrados if x.periodo == periodo_atual]

    # saldo anterior = último período < atual
    if anteriores:
        anteriores.sort(key=lambda x: x.periodo)
        saldo_anterior = anteriores[-1].saldo
    else:
        saldo_anterior = Decimal("0")

    saldo_novo = (saldo_anterior + credito_mes)

    novo_1100 = linha_1100(
        periodo=periodo_atual,
        cod_cont=cod_cont,
        valor=saldo_novo,
    )

    # reconstrói bloco 1:
    bloco = ["|1001|0|"]

    # mantém 1100 antigos (menos o do período atual)
    for x in sorted(anteriores, key=lambda x: x.periodo):
        bloco.append(x.linha)

    # adiciona o novo 1100 (substitui o do período atual)
    bloco.append(novo_1100)

    bloco.append(f"|1990|{len(bloco)+1}|")
    return bloco

def buscar_sped_exportado_anterior_por_pasta(
    *,
    pasta_speds_corrigidos: Path,
    cnpj_empresa: str,
    periodo_atual: Optional[int],
    ignorar_path: Optional[Path] = None,
) -> Optional[Path]:
    cnpj_empresa = _limpar_cnpj(cnpj_empresa)

    candidatos = []
    for p in pasta_speds_corrigidos.glob("*.txt"):
        if ignorar_path and p.resolve() == ignorar_path.resolve():
            continue

        # extrai CNPJ e período do 0000 desse arquivo
        cnpj_p, periodo_p = _parse_0000_cnpj_periodo(p)  # sua função de parser do arquivo
        if cnpj_p != cnpj_empresa:
            continue

        st = p.stat()
        candidatos.append((p, periodo_p, st.st_mtime))

    if not candidatos:
        return None

    # ✅ modo normal: usa período se tiver
    if periodo_atual is not None:
        candidatos_ok = [(p, per, mt) for (p, per, mt) in candidatos if per is not None and per < periodo_atual]
        if candidatos_ok:
            candidatos_ok.sort(key=lambda x: (x[1], x[2]))  # (periodo, mtime)
            return candidatos_ok[-1][0]

    # ✅ fallback: usa o mais recente por mtime
    candidatos.sort(key=lambda x: x[2])  # mtime
    return candidatos[-1][0]

def materializar_conteudo_versao(db: Session, *, versao_id: int) -> List[str]:
    # como é EXPORTADA, não precisa overlay final. Mas pode usar o mesmo loader.
    linhas_prev = carregar_linhas_logicas_com_revisoes(
        db=db,
        versao_origem_id=int(versao_id),
        versao_final_id=None,
    )
    return [obter_conteudo_final(l) for l in linhas_prev]

def ler_linhas_sped(path: Path) -> list[str]:
    with path.open("r", encoding="utf-8-sig", errors="ignore") as f:
        return [ln.rstrip("\n") for ln in f]


def _limpar_cnpj(s: str) -> str:
    return re.sub(r"\D+", "", s or "")

def _parse_0000_cnpj_periodo(path: Path) -> tuple[Optional[str], Optional[int]]:
    """
    Lê poucas linhas do início e tenta extrair CNPJ e período pelo registro 0000.
    Layout do 0000 varia, então fazemos parsing defensivo.
    """
    try:
        with path.open("r", encoding="utf-8-sig", errors="ignore") as f:
            for _ in range(400):  # suficiente pra achar 0000 na prática
                line = f.readline()
                if not line:
                    break
                line = line.strip()
                if not line.startswith("|0000|"):
                    continue

                parts = line.strip("|").split("|")  # ["0000", ...]
                # Tentativas comuns:
                # - CNPJ costuma estar em algum campo por volta do final.
                # - Período às vezes vem como AAAAMM, ou tem DT_INI/DT_FIN (AAAAMMDD).
                cnpj = None
                periodo = None

                # 1) procurar primeiro token com 14 dígitos (CNPJ)
                for tok in parts:
                    t = _limpar_cnpj(tok)
                    if len(t) == 14:
                        cnpj = t
                        break

                # 2) procurar token AAAAMM (6 dígitos) ou derivar de AAAAMMDD
                for tok in parts:
                    t = re.sub(r"\D+", "", tok or "")
                    if len(t) == 6 and t.isdigit():
                        yyyymm = int(t)
                        if 190001 <= yyyymm <= 210012:
                            periodo = yyyymm
                            break

                # 3) se não achou, tenta derivar de data (YYYYMMDD OU DDMMAAAA)
                if periodo is None:
                    for tok in parts:
                        t = re.sub(r"\D+", "", tok or "")
                        if len(t) == 8 and t.isdigit():
                            # tenta YYYYMMDD
                            yyyymm1 = int(t[:6])
                            if 190001 <= yyyymm1 <= 210012:
                                periodo = yyyymm1
                                break

                            # tenta DDMMAAAA -> AAAAMM
                            dd = int(t[0:2])
                            mm = int(t[2:4])
                            yyyy = int(t[4:8])
                            if 1 <= dd <= 31 and 1 <= mm <= 12 and 1900 <= yyyy <= 2100:
                                periodo = yyyy * 100 + mm
                                break

                return cnpj, periodo
    except Exception:
        return None, None

    return None, None

def _listar_speds_exportados(pasta: Path) -> list[SpedInfo]:
    files = []
    for p in pasta.glob("*.txt"):
        try:
            st = p.stat()
            cnpj, periodo = _parse_0000_cnpj_periodo(p)
            files.append(SpedInfo(path=p, cnpj=cnpj, periodo=periodo, mtime=st.st_mtime))
        except Exception:
            continue
    return files



# --- ETAPA 2: Reconstrução da Ordem do Arquivo Writer (Com Pesos Explícitos) ---
def get_peso(registro: str) -> int:
    if not registro:
        return 999

    bloco = registro[0].upper()

    ordem = {
        '0': 0,
        'A': 5,
        'C': 10,
        'D': 20,
        'E': 30,
        'F': 40,
        'G': 50,
        'H': 60,
        'I': 70,
        '1': 75,
        'M': 80,
        'P': 90,
        '9': 100,
    }

    return ordem.get(bloco, 95)

# Usando Linhas .. extraindo cnpj

def extrair_cnpj_periodo_do_0000(linhas: list[str]) -> Tuple[Optional[str], Optional[int]]:
    for ln in linhas or []:
        ln = (ln or "").strip()
        if not ln.startswith("|0000|"):
            continue

        parts = ln.strip("|").split("|")

        # CNPJ = primeiro token com 14 dígitos
        cnpj = None
        for tok in parts:
            t = re.sub(r"\D+", "", tok or "")
            if len(t) == 14:
                cnpj = t
                break

        # período: DT_INI no seu layout está em parts[5] = "01022024" (DDMMAAAA)
        periodo = None
        dt_ini = parts[5] if len(parts) > 5 else ""
        dt = re.sub(r"\D+", "", dt_ini or "")
        if len(dt) == 8 and dt.isdigit():
            dd = int(dt[0:2])
            mm = int(dt[2:4])
            yyyy = int(dt[4:8])
            if 1 <= dd <= 31 and 1 <= mm <= 12 and 1900 <= yyyy <= 2100:
                periodo = yyyy * 100 + mm

        return cnpj, periodo

    return None, None