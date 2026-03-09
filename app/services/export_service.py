from __future__ import annotations
from decimal import Decimal, ROUND_HALF_UP
import traceback
from typing import Any, Optional, Dict
from pathlib import Path
from sqlalchemy.orm import Session
from app.config.settings import ALIQUOTA_PIS, ALIQUOTA_COFINS  # 0.0165 / 0.0760
from app.sped.bloco_1.historico_fs import extrair_cnpj_periodo_do_0000, buscar_sped_exportado_anterior_por_pasta, \
    ler_linhas_sped
from app.sped.utils_geral import dec_br
from app.services.revisao_override_m_service import (
    buscar_override_bloco_m,
    extrair_credito_total_do_bloco_m,
)
from app.sped.blocoM.blocoM import construir_bloco_m_v3
from app.sped.bloco_0.bloco_0_0900 import aplicar_0900_se_necessario
from app.sped.bloco_1.builder import (
     montar_bloco_1_1100_1500_cumulativo, extrair_creditos_mes_bloco_m
)
from app.db.models import EfdVersao, EfdArquivo, EfdRegistro
from app.services.versao_overlay_service import carregar_linhas_logicas_com_revisoes
from app.sped.blocoM.m_utils import caminho_sped_corrigido, nome_sped_corrigido, _cst_norm, sanitizar_bloco_m, \
    carregar_ajustes_m
from app.sped.parser import parse_sped_from_lines
from app.sped.writer import gerar_sped
import app.sped.writer as writer_module
from app.sped.layouts.c170 import LAYOUT_C170
from app.sped.blocoC.c170_utils import _parse_linha_sped_to_reg_dados
from app.sped.logic.consolidador import obter_conteudo_final, eh_pf_por_c100
from app.sped.bloco_1.utils_1500 import montar_bloco_1_1500_cumulativo, yyyymm_to_mmyyyy
from app.services.revisao_override_base_service import buscar_override_base_por_cst




def exportar_sped(
    *,
    versao_id: int,
    caminho_saida: Optional[str] = None,
    db: Session,
    valor_utilizado_mes: float = 0.0
) -> str:
    versao = db.get(EfdVersao, int(versao_id))
    if not versao:
        raise ValueError("Versão não encontrada")

    retifica_de = getattr(versao, "retifica_de_versao_id", None)

    # 🔒 TRAVA: não exporta original
    if not retifica_de:
        raise ValueError(
            "Exportação bloqueada: esta é uma versão ORIGINAL. "
            "Primeiro confirme a revisão para materializar a versão revisada e só então exporte."
        )

    versao_origem_id = int(retifica_de)
    versao_final_id = int(versao.id)

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

    nome_arquivo = nome_sped_corrigido(arquivo, versao)

    if not caminho_saida:
        caminho_saida = caminho_sped_corrigido(nome_arquivo)

    final_path = Path(caminho_saida)
    final_path.parent.mkdir(parents=True, exist_ok=True)

    line_ending = getattr(arquivo, "line_ending", "CRLF")
    newline = "\r\n" if str(line_ending).upper() == "CRLF" else "\n"

    # ✅ CSTs de crédito (começa assim; depois vocês refinam com apontamentos)
    CST_CREDITO = {"50", "51", "52", "53", "54", "55", "56"}

    try:
        # 3) soma C170 por CST (layout-driven) - já com overlay aplicado
        base_por_cst: Dict[str, Decimal] = {}
        qtd_itens = 0
        qtd_pf = 0

        # DEBUG (sem dict): conta quantas linhas suspeitas entraram na soma
        dbg_cfop_fora = 0
        dbg_nao_entrada = 0
        dbg_sem_pai = 0

        # whitelist do café (mesmo guard-rail do autocorrigível/foto)
        CFOPS_CAFE = {"1101", "1102", "2101", "2102", "3101", "3102"}

        # helper inline (sem criar função global): tenta achar IND_OPER do C100 pai
        def _ind_oper_pai_c100(_rid: int) -> str:
            try:
                r170 = db.get(EfdRegistro, int(_rid))
                if not r170 or not getattr(r170, "pai_id", None):
                    return ""
                r100 = db.get(EfdRegistro, int(r170.pai_id))
                if not r100 or getattr(r100, "reg", "") != "C100":
                    return ""
                cj = getattr(r100, "conteudo_json", None) or {}
                dados100 = cj.get("dados") if isinstance(cj, dict) else None
                if not isinstance(dados100, list) or len(dados100) < 1:
                    return ""
                return str(dados100[0] or "").strip()  # "0" entrada / "1" saída
            except Exception:
                return ""

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

            # CST PIS
            if len(dados) <= LAYOUT_C170.idx_cst_pis:
                continue

            cst_pis = _cst_norm(dados[LAYOUT_C170.idx_cst_pis])
            if cst_pis not in CST_CREDITO:
                continue

            # trava PF
            registro_id = getattr(ln, "registro_id", None)
            rid_int = int(registro_id) if registro_id else 0

            if rid_int and eh_pf_por_c100(db, versao_origem_id, rid_int):
                qtd_pf += 1
                continue

            # VL_ITEM
            if len(dados) <= LAYOUT_C170.idx_vl_item:
                continue

            vl_item = dec_br(dados[LAYOUT_C170.idx_vl_item])
            if vl_item <= 0:
                continue

            vl_desc = dec_br(dados[LAYOUT_C170.idx_vl_desc]) if len(dados) > LAYOUT_C170.idx_vl_desc else Decimal(
                "0.00")
            vl_icms = dec_br(dados[LAYOUT_C170.idx_vl_icms]) if len(dados) > LAYOUT_C170.idx_vl_icms else Decimal(
                "0.00")

            base_liquida = vl_item - vl_desc - vl_icms
            if base_liquida <= 0:
                continue

            # ---------------------------
            # CFOP + ENTRADA (guard-rails)
            # ---------------------------
            cfop = ""
            try:
                if len(dados) > LAYOUT_C170.idx_cfop:
                    cfop = str(dados[LAYOUT_C170.idx_cfop] or "").strip()
            except Exception:
                cfop = ""

            # 1) Barra CFOP fora do escopo do café
            if cfop and cfop not in CFOPS_CAFE:
                dbg_cfop_fora += 1
                print(f"⛔ BASE_SKIP_CFOP> cfop={cfop} rid={rid_int or '?'} cst={cst_pis} vl_item={vl_item}")
                continue

            # 2) Só ENTRADAS (IND_OPER do C100 pai == "0")
            ind_oper = ""
            if rid_int:
                ind_oper = _ind_oper_pai_c100(rid_int)

            # Conservador: se não achou pai/ind_oper, NÃO soma
            if not ind_oper:
                dbg_sem_pai += 1
                print(f"⛔ BASE_SKIP_SEM_PAI> rid={rid_int or '?'} cfop={cfop or '?'} cst={cst_pis} vl_item={vl_item}")
                continue

            if ind_oper != "0":
                dbg_nao_entrada += 1
                print(
                    f"⛔ BASE_SKIP_SAIDA> ind_oper={ind_oper} cfop={cfop or '?'} rid={rid_int or '?'} cst={cst_pis} vl_item={vl_item}")
                continue

            # ---------------------------
            # Soma (agora já filtrado)
            # ---------------------------
            base_por_cst[cst_pis] = base_por_cst.get(cst_pis, Decimal("0.00")) + base_liquida
            qtd_itens += 1

        base_total = sum(base_por_cst.values(), Decimal("0.00")).quantize(Decimal("0.01"), ROUND_HALF_UP)
        credito_pis = (base_total * Decimal("0.0165")).quantize(Decimal("0.01"), ROUND_HALF_UP)
        credito_cofins = (base_total * Decimal("0.0760")).quantize(Decimal("0.01"), ROUND_HALF_UP)
        credito_total_calc = (credito_pis + credito_cofins).quantize(Decimal("0.01"), ROUND_HALF_UP)

        valor_utilizado_mes_dec = Decimal(str(valor_utilizado_mes or 0)).quantize(Decimal("0.01"))

        print(
            f"🔥 BASE_TOTAL={base_total} | "
            f"PIS={credito_pis} | COFINS={credito_cofins} | CRED_TOTAL={credito_total_calc} "
            f"C170_creditaveis={qtd_itens} | PF_bloqueados={qtd_pf} | "
            f"DBG(cfop_fora={dbg_cfop_fora}, nao_entrada={dbg_nao_entrada}, sem_pai={dbg_sem_pai}) | "
            f"base_por_cst={ {k: str(v) for k, v in base_por_cst.items()} }"
        )

        # 4) remove COMPLETAMENTE M* do conteúdo (por linha, não por reg do objeto)
        conteudo_linhas = [(obter_conteudo_final(l) or "") for l in linhas]
        conteudo_sem_m = [ln for ln in conteudo_linhas if not (ln or "").lstrip().startswith("|M")]

        # 5) histórico (1100/1500) + captura CNPJ/período do 0000
        cnpj_empresa, periodo_0000 = extrair_cnpj_periodo_do_0000(conteudo_sem_m)

        if not cnpj_empresa:
            print("⚠️ HISTÓRICO> CNPJ não encontrado no 0000. Histórico desativado.")
            linhas_prev = []
        else:
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

        # >>> Bloco 0900 (layout PVA real) <<<
        # aplica ANTES do parse final, porque o 0900 mexe no bloco 0 e recalcula 0990
        conteudo_sem_m = aplicar_0900_se_necessario(
            linhas_sped=conteudo_sem_m,
            periodo_yyyymm=int(periodo_0000) if periodo_0000 else None,
        )

        # Agora sim, parse do conteúdo final (já com 0900 se inserido)
        parsed = parse_sped_from_lines(conteudo_sem_m)

        # carregar ajustes para M zerado
        ajustes_m = carregar_ajustes_m(db, versao_id=versao_final_id)

        if ajustes_m:
            print(f"✅ AJUSTE_M> carregados={len(ajustes_m)} | tipos_top={[a.get('tipo') for a in ajustes_m[:3]]}")
        else:
            print("ℹ️ AJUSTE_M> nenhum ajuste encontrado")

        # 6) Override de BASE por CST
        override_base = buscar_override_base_por_cst(
            db,
            versao_origem_id=versao_origem_id,
            versao_final_id=versao_final_id,
        )

        # ✅ regra: OVERRIDE_BASE_POR_CST só entra se NÃO houver AJUSTE_M de exportação
        # (porque exportação é delta e já está em AJUSTE_M; evitar "replace" e evitar duplicar)
        tem_ajuste_export = any(
            isinstance(a, dict)
            and isinstance(a.get("meta"), dict)
            and str(a["meta"].get("tipo") or "").strip().upper() == "EXPORTACAO_RESSARCIMENTO"
            for a in (ajustes_m or [])
        )

        if override_base is not None and not tem_ajuste_export:
            print("✅ BASE_POR_CST> usando OVERRIDE do banco (EfdRevisao)")
            base_por_cst = override_base
        elif override_base is not None and tem_ajuste_export:
            print("⚠️ BASE_POR_CST> override ignorado (exportação via AJUSTE_M) para manter base original")

        # 6a) Bloco M: override do banco > fallback construir_bloco_m_v3
        override_db = buscar_override_bloco_m(
            db,
            versao_origem_id=versao_origem_id,
            versao_final_id=versao_final_id,
        )

        if override_db is not None:
            print("✅ BLOCO M> usando OVERRIDE do banco (EfdRevisao)")
            bloco_m_override = override_db
            # se override existir, crédito do 1100 deve refletir ele (não o cálculo)
            credito_total_1100 = extrair_credito_total_do_bloco_m(bloco_m_override)
        else:
            print("ℹ️ BLOCO M> sem override, usando construir_bloco_m_v3 (por CST)")
            print("M_BASE> base_por_cst=", {k: str(v) for k, v in (base_por_cst or {}).items()}, flush=True)
            print("M_BASE> base_total_norm=", str(sum((v for v in (base_por_cst or {}).values()), Decimal("0"))),

                  flush=True)
            print("M_BASE> tem_51?", "51" in (base_por_cst or {}), "vl_51=", str((base_por_cst or {}).get("51")),
                  flush=True)
            print("M_BASE> tem_73?", "73" in (base_por_cst or {}), "vl_73=", str((base_por_cst or {}).get("73")),
                  flush=True)
            bloco_m_override = construir_bloco_m_v3(
                linhas_sped=conteudo_sem_m,
                parsed=parsed,
                base_por_cst=base_por_cst,
                cod_cred="201",
                nat_bc="01",
                ajustes_m=ajustes_m,
            )

            credito_total_1100 = extrair_credito_total_do_bloco_m(bloco_m_override)

        # 7) Bloco 1 (1100/1500)
        periodo_atual = getattr(arquivo, "periodo", None)
        if not periodo_atual:
            raise ValueError("EfdArquivo.periodo não preenchido (YYYYMM).")

        periodo_atual_mmaaaa = yyyymm_to_mmyyyy(str(periodo_atual))

        bloco_1500_override = []
        if valor_utilizado_mes_dec > 0:
            bloco_1500_override = montar_bloco_1_1500_cumulativo(
                linhas_sped=linhas_prev,
                periodo_atual=periodo_atual_mmaaaa,
                cod_cont="201",
                valor_utilizado_mes=valor_utilizado_mes_dec,
            )

        credito_pis_mes, credito_cofins_mes = extrair_creditos_mes_bloco_m(bloco_m_override)

        bloco_1_override = montar_bloco_1_1100_1500_cumulativo(
            linhas_sped=linhas_prev,
            periodo_atual=periodo_atual_mmaaaa,
            cod_cont="201",
            credito_pis_mes=credito_pis_mes,
            credito_cofins_mes=credito_cofins_mes,
        )

        if bloco_1_override and bloco_1_override[-1].startswith("|1990|"):
            bloco_1_override = bloco_1_override[:-1]
        bloco_1_override += bloco_1500_override
        bloco_1_override.append(f"|1990|{len(bloco_1_override) + 1}|")

        print("DEBUG> bloco_m_override linhas:", len(bloco_m_override or []))
        if bloco_m_override:
            print("DEBUG> primeira:", bloco_m_override[0])
            print("DEBUG> contém M100:", any(l.startswith("|M100|") for l in bloco_m_override))
            print("DEBUG> contém M500:", any(l.startswith("|M500|") for l in bloco_m_override))
        # --------------------

        # 8) escreve
        print(f"📂 Writer: {writer_module.__file__}")
        gerar_sped(
            conteudo_sem_m,
            str(final_path),
            newline=newline,
            bloco_m_override=bloco_m_override,
            bloco_1_override=bloco_1_override,
        )

        # status/caminho
        try:
            versao.status = "EXPORTADA"
            if hasattr(versao, "caminho_exportado"):
                versao.caminho_exportado = str(final_path)
            db.add(versao)
            db.commit()
        except Exception as _e:
            db.rollback()
            print("⚠️ Não consegui persistir status/caminho_exportado:", repr(_e))

        print("🏁 Exportação concluída com sucesso.")
        return str(final_path)

    except Exception as e:
        print(f"❌ ERRO ao construir/exportar SPED: {e}")
        traceback.print_exc()

        raise  # <-- sem fallback durante debug