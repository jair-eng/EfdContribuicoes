from decimal import Decimal, ROUND_HALF_UP
import traceback
from typing import Any
from sqlalchemy.orm import Session

from app.sped.blocoM import construir_bloco_m_v2
from app.db.models import EfdVersao, EfdArquivo
from app.services.versao_overlay_service import carregar_linhas_logicas_com_revisoes
from app.sped.parser import parse_sped_from_lines
from app.sped.writer import gerar_sped
import app.sped.writer as writer_module

from app.sped.layouts.c170 import LAYOUT_C170
from app.sped.c170_utils import _parse_linha_sped_to_reg_dados
from app.sped.logic.consolidador import obter_conteudo_final, eh_pf_por_c100
from app.sped.formatter import formatar_linha

# ✅ IMPORT DO PARSER FULL


# DTO simples para linhas dinâmicas
class LinhaSpedDinamica:
    def __init__(self, reg, dados, linha):
        self.reg = reg
        self.dados = dados
        self.linha = linha
        self.origem = "BLOCO_M_RECALCULADO"

    def render_linha(self):
        return formatar_linha(self.reg, self.dados)


def _dec_br(v: Any) -> Decimal:
    if v is None:
        return Decimal("0")
    s = str(v).strip()
    if not s:
        return Decimal("0")
    s = s.replace(".", "").replace(",", ".")
    try:
        return Decimal(s)
    except Exception:
        return Decimal("0")


def exportar_sped(*, versao_id: int, caminho_saida: str, db: Session) -> str:
    credito_pis = Decimal("0.00")
    credito_cofins = Decimal("0.00")

    versao = db.get(EfdVersao, int(versao_id))
    if not versao:
        raise ValueError("Versão não encontrada")

    retifica_de = getattr(versao, "retifica_de_versao_id", None)
    if retifica_de:
        versao_origem_id = int(retifica_de)
        versao_final_id = int(versao.id)
    else:
        versao_origem_id = int(versao.id)
        versao_final_id = None

    print(f"EXPORT> versao_id={versao_id} | Origem={versao_origem_id} | Final={versao_final_id}")

    # 1) carrega linhas com overlay aplicado
    linhas = carregar_linhas_logicas_com_revisoes(
        db=db,
        versao_origem_id=versao_origem_id,
        versao_final_id=versao_final_id,
    )

    # 2) configurações do arquivo
    arquivo = db.get(EfdArquivo, int(versao.arquivo_id))
    if not arquivo:
        raise ValueError("Arquivo não encontrado para a versão")

    line_ending = getattr(arquivo, "line_ending", "CRLF")
    newline = "\r\n" if str(line_ending).upper() == "CRLF" else "\n"


    try:
        # 3) soma C170 CST=51 (layout-driven)
        total_base = Decimal("0.00")
        qtd_itens = 0
        qtd_pf = 0

        for ln in linhas:
            conteudo = obter_conteudo_final(ln) or ""
            if "|C170|" not in conteudo:
                continue

            try:
                reg, dados = _parse_linha_sped_to_reg_dados(conteudo)
            except Exception:
                continue

            if reg != "C170":
                continue

            if len(dados) <= LAYOUT_C170.idx_cst_pis:
                continue

            cst_pis = str(dados[LAYOUT_C170.idx_cst_pis] or "").zfill(2)
            if cst_pis != "51":
                continue

            registro_id = getattr(ln, "registro_id", None)
            if registro_id and eh_pf_por_c100(db, versao_origem_id, int(registro_id)):
                qtd_pf += 1
                continue

            if len(dados) <= LAYOUT_C170.idx_vl_item:
                continue

            vl_item = _dec_br(dados[LAYOUT_C170.idx_vl_item])
            if vl_item <= 0:
                continue

            total_base += vl_item
            qtd_itens += 1

        credito_pis = (total_base * Decimal("0.0165")).quantize(Decimal("0.01"), ROUND_HALF_UP)
        credito_cofins = (total_base * Decimal("0.0760")).quantize(Decimal("0.01"), ROUND_HALF_UP)
        credito_total = (credito_pis + credito_cofins).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)

        print(
            f"🔥 BASE EXPORTAÇÃO={total_base} | "
            f"PIS={credito_pis} | COFINS={credito_cofins} | Credito Total={credito_total} "
            f"C170={qtd_itens} | PF bloqueados={qtd_pf}"
        )

        # 4) REMOVE COMPLETAMENTE qualquer Bloco M existente
        linhas_sem_m = [l for l in linhas if not str(getattr(l, "reg", "")).startswith("M")]

        # ✅ materializa o conteúdo em string (isso é o "conteudo_sem_m")
        conteudo_sem_m = [obter_conteudo_final(l) for l in linhas_sem_m]

        # ✅ parsed em memória (a partir do conteúdo já materializado)
        parsed = parse_sped_from_lines(conteudo_sem_m)
        base_credito = total_base
        # 5) CONSTRÓI Bloco M único e consistente (agora com parsed)
        bloco_m_override = construir_bloco_m_v2(
            linhas_sped=conteudo_sem_m,
            parsed=parsed,
            base_credito=base_credito,
            credito_pis=credito_pis,
            credito_cofins=credito_cofins,
            cod_cont="201",
        )

        # 6) Exporta: writer vai inserir M no lugar certo conforme override interno dele
        print(f"📂 Writer: {writer_module.__file__}")
        gerar_sped(linhas_sem_m, caminho_saida, newline=newline, bloco_m_override=bloco_m_override)

    except Exception as e:
        print(f"❌ ERRO ao construir/exportar SPED: {e}")
        traceback.print_exc()

        # fallback: exporta como estava (sem mexer no M)
        print(f"📂 Writer: {writer_module.__file__}")
        gerar_sped(linhas, caminho_saida, newline=newline, bloco_m_override=None)

    print("🏁 Exportação concluída com sucesso.")
    return caminho_saida
