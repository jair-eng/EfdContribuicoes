from decimal import Decimal, ROUND_HALF_UP
import traceback
from typing import Any
from sqlalchemy.orm import Session
from app.sped.blocoM.blocoM import construir_bloco_m_v2
from app.db.models import EfdVersao, EfdArquivo
from app.services.versao_overlay_service import carregar_linhas_logicas_com_revisoes
from app.sped.blocoM.m_utils import ler_linhas_exportado, caminho_sped_corrigido, nome_sped_corrigido
from app.sped.bloco_1.builder import montar_bloco_1_1100_cumulativo, materializar_conteudo_versao, \
    buscar_sped_exportado_anterior_por_pasta, ler_linhas_sped, extrair_cnpj_periodo_do_0000
from app.sped.parser import parse_sped_from_lines
from app.sped.writer import gerar_sped
import app.sped.writer as writer_module
from app.sped.layouts.c170 import LAYOUT_C170
from app.sped.blocoC.c170_utils import _parse_linha_sped_to_reg_dados
from app.sped.logic.consolidador import obter_conteudo_final, eh_pf_por_c100
from app.sped.bloco_1.utils_1500 import montar_bloco_1_1500_cumulativo, yyyymm_to_mmyyyy
from app.sped.formatter import formatar_linha
from typing import Optional
from pathlib import Path




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


def exportar_sped(*, versao_id: int, caminho_saida: Optional[str] = None, db: Session,valor_utilizado_mes: float = 0.0) -> str:
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
    nome_arquivo = nome_sped_corrigido(arquivo, versao)

    if not caminho_saida:
        caminho_saida = caminho_sped_corrigido(nome_arquivo)

    final_path = Path(caminho_saida) if caminho_saida else Path(caminho_sped_corrigido(nome_arquivo))
    # garante diretório
    final_path.parent.mkdir(parents=True, exist_ok=True)

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

        # converte o input (float do front) para Decimal com 2 casas
        valor_utilizado_mes_dec = Decimal(str(valor_utilizado_mes or 0)).quantize(Decimal("0.01"))

        print(
            f"🔥 BASE EXPORTAÇÃO={total_base} | "
            f"PIS={credito_pis} | COFINS={credito_cofins} | Credito Total={credito_total} "
            f"C170={qtd_itens} | PF bloqueados={qtd_pf}"
        )

        # 4) REMOVE COMPLETAMENTE qualquer Bloco M existente
        linhas_sem_m = [l for l in linhas if not str(getattr(l, "reg", "")).startswith("M")]

        # ✅ materializa o conteúdo em string (isso é o "conteudo_sem_m")
        conteudo_sem_m = [obter_conteudo_final(l) for l in linhas_sem_m]

        # =========================
        # 🔎 EXTRAI CNPJ / PERÍODO
        # =========================
        cnpj_empresa, periodo_0000 = extrair_cnpj_periodo_do_0000(conteudo_sem_m)

        if not cnpj_empresa:
            print("⚠️ HISTÓRICO> CNPJ não encontrado no 0000. Histórico desativado.")
            linhas_prev = []
        else:
            print("DEBUG 0000> cnpj =", cnpj_empresa, "periodo =", periodo_0000)

            pasta_historico = Path.home() / "Downloads" / "Speds Corrigidos"

            if not pasta_historico.exists():
                print("⚠️ HISTÓRICO> pasta não existe:", pasta_historico)
                linhas_prev = []
            else:
                prev_path = buscar_sped_exportado_anterior_por_pasta(
                    pasta_speds_corrigidos=pasta_historico,
                    cnpj_empresa=cnpj_empresa,
                    periodo_atual=int(periodo_0000) if periodo_0000 else None,
                    ignorar_path=final_path,
                )

                if prev_path:
                    print("📁 HISTÓRICO FS> usando:", prev_path.name)
                    linhas_prev = ler_linhas_sped(prev_path)
                else:
                    print("📁 HISTÓRICO FS> não encontrado. saldo_anterior=0")
                    linhas_prev = []

        # Base do 1100 = conteúdo do exportado anterior (se existir)

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

        # =========================
        # BLOCO 1 – 1100 CUMULATIVO
        # =========================
        periodo_atual = getattr(arquivo, "periodo", None)
        if not periodo_atual:
            raise ValueError("EfdArquivo.periodo não preenchido (YYYYMM).")
        periodo_atual_yyyymm = str(periodo_atual)
        periodo_atual_mmaaaa = yyyymm_to_mmyyyy(periodo_atual_yyyymm)

        # Bloco 1500

        if valor_utilizado_mes > 0:
            bloco_1500_override = montar_bloco_1_1500_cumulativo(
                linhas_sped=linhas_prev,
                periodo_atual=periodo_atual_mmaaaa,
                cod_cont="201",
                valor_utilizado_mes=valor_utilizado_mes_dec,
            )
        else:
            bloco_1500_override = []

        bloco_1_override = montar_bloco_1_1100_cumulativo(
            linhas_sped=linhas_prev,
            periodo_atual=periodo_atual_mmaaaa,
            cod_cont="201",
            credito_mes=credito_total,
        )

        # injeta 1500 dentro do mesmo bloco_1_override (writer só conhece bloco_1_override)
        if bloco_1_override and bloco_1_override[-1].startswith("|1990|"):
            bloco_1_override = bloco_1_override[:-1]

        bloco_1_override += bloco_1500_override

        bloco_1_override.append(f"|1990|{len(bloco_1_override) + 1}|")

        # 6) Exporta: writer vai inserir M/1 no lugar certo conforme override
        print(f"📂 Writer: {writer_module.__file__}")
        print("B1> periodo_atual =", periodo_atual)
        print("B1> 1100 linhas =", sum(1 for x in bloco_1_override if str(x).startswith("|1100|")))
        print("B1> 1500 linhas =", sum(1 for x in bloco_1_override if str(x).startswith("|1500|")))
        print("B1> periodo_0000 =", periodo_0000)
        print("B1> valor_utilizado_mes =", valor_utilizado_mes_dec)
        print("B1> 1500_override_len =", len(bloco_1500_override or []))
        print("B1> 1500_override_ex  =", (bloco_1500_override or [])[:1])
        gerar_sped(
            linhas_sem_m,
            str(final_path),
            newline=newline,
            bloco_m_override=bloco_m_override,
            bloco_1_override=bloco_1_override,
        )

        # ✅ marca/salva export (opcional mas recomendado)
        try:
            versao.status = "EXPORTADA"
            if hasattr(versao, "caminho_exportado"):
                versao.caminho_exportado = str(final_path)
            db.add(versao)
            db.commit()
        except Exception as _e:
            # não derruba a exportação por causa disso
            db.rollback()
            print("⚠️ Não consegui persistir status/caminho_exportado:", repr(_e))

        print("🏁 Exportação concluída com sucesso.")
        return str(final_path)

    except Exception as e:
        print(f"❌ ERRO ao construir/exportar SPED: {e}")
        traceback.print_exc()

        # fallback: exporta como estava (sem mexer no M/1)
        print("⚠️ Gerando fallback (sem overrides)...")
        gerar_sped(
            linhas,
            str(final_path),
            newline=newline,
            bloco_m_override=None,
            bloco_1_override=None,

        )

        print("🏁 Exportação concluída em modo fallback.")
        return str(final_path)


