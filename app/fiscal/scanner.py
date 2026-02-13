from __future__ import annotations

from sqlalchemy import text
from collections import defaultdict
from decimal import Decimal , InvalidOperation
from typing import Any, Dict, List, Optional, Tuple, Union
from sqlalchemy.orm import Session
from app.db.models import EfdRegistro, EfdApontamento
from app.fiscal.dto import RegistroFiscalDTO
from app.fiscal.scanners.c190 import montar_c190_agg
from app.db.models.efd_versao import EfdVersao
from app.db.models.efd_arquivo import EfdArquivo
from app.fiscal.scanners.c100_entrada import montar_c100_entrada_relevante_agg
from app.fiscal.ent_cat_fiscal import carregar_catalogo_fiscal
from app.fiscal.regras.base_regras import RegraBase
from app.fiscal.scanners.exportacao import montar_c190_export_agg, montar_c170_export_agg, montar_c190_ind_torrado_agg, \
    montar_c170_ind_torrado_agg
from app.fiscal.varredura import executar_varredura
from app.fiscal.regras.achado import Achado
from app.fiscal.scanners.exportacao import montar_meta_fiscal
from app.services.versao_overlay_service import carregar_linhas_logicas_com_revisoes
from app.sped.blocoC.c170_utils import linhas_para_rows_like
from app.fiscal.contexto import set_fiscal_context
import re
import logging

from app.sped.utils_geral import consolidar_achados_c170_insumo_v2

logger = logging.getLogger(__name__)

class FiscalScanner:
    @staticmethod
    def scan_versao(
        db: Session,
        *,
        versao_id: int,
        empresa_id: int,
        preservar_resolvidos: bool = True,
        aplicar_revisoes: bool = True,
    ) -> Dict[str, Any]:
        """
        Scanner fiscal:
        - Carrega registros da versão
        - (Opcional) aplica revisões overlay
        - Expurga registros vinculados a participante PF (CPF 11 dígitos) para regras/agrupadores
        - Monta DTOs com contexto (versao_id/empresa_id)
        - Injeta META_FISCAL e agregadores
        - Executa motor fiscal
        - Persiste apontamentos preservando resolvidos (merge por chave lógica)
        """

        # -----------------------------
        # 0) Normalizações e contexto
        # -----------------------------
        versao_id = int(versao_id)

        if empresa_id is None:
            versao = db.get(EfdVersao, versao_id)
            empresa_id = getattr(versao, "empresa_id", None)
            if empresa_id is None and versao and getattr(versao, "arquivo_id", None):
                arquivo = db.get(EfdArquivo, int(versao.arquivo_id))
                empresa_id = getattr(arquivo, "empresa_id", None)

        empresa_id_ctx = int(empresa_id) if empresa_id is not None else None
        if empresa_id_ctx is None:
            raise ValueError("empresa_id não pôde ser resolvido para scan_versao")

        set_fiscal_context(db, empresa_id_ctx)

        # -----------------------------
        # 1) Buscar registros originais (Fonte da Verdade)
        # -----------------------------
        rows: List[EfdRegistro] = (
            db.query(EfdRegistro)
            .filter(EfdRegistro.versao_id == versao_id)
            .order_by(EfdRegistro.linha.asc())
            .all()
        )
        linha_to_registro_id = {int(r.linha): int(r.id) for r in rows}

        # --- [OTIMIZAÇÃO DE PERFORMANCE: MAPAS EM MEMÓRIA] ---
        # 1.1) Mapa de Participantes: Classifica quem é PF (CPF com 11 dígitos)
        participantes_pf: dict[str, bool] = {}
        for r in rows:
            if (r.reg or "").strip() == "0150":
                dados_0150 = (r.conteudo_json or {}).get("dados") or []
                if len(dados_0150) > 4:
                    cod_part = str(dados_0150[0]).strip()
                    cpf_limpo = "".join(filter(str.isdigit, str(dados_0150[4] or "")))
                    participantes_pf[cod_part] = (len(cpf_limpo) == 11)

        # 1.2) Mapa de Hierarquia: ID do C100 -> COD_PART
        mapa_pai_participante: dict[str, str] = {}
        for r in rows:
            if (r.reg or "").strip() == "C100":
                dados_c100 = (r.conteudo_json or {}).get("dados") or []
                if len(dados_c100) > 2:
                    mapa_pai_participante[str(r.id)] = str(dados_c100[2]).strip()

        # 2) Aplicar Revisões (Merge em memória)
        if aplicar_revisoes:

            linhas = carregar_linhas_logicas_com_revisoes(db, versao_origem_id=int(versao_id))
            rows_agg = linhas_para_rows_like(linhas)  # para agregadores
            fonte_base = linhas  # para DTOs
        else:
            rows_agg = rows
            fonte_base = rows

        # --- 2.1) FILTRAGEM DE SEGURANÇA (O CORTE PF) ---
        rows_limpas: list[Any] = []
        ids_pf_detectados: set[int] = set()

        for r in rows_agg:
            reg_nome = str(getattr(r, "reg", "")).strip().upper()
            is_pf = False

            # Identificação de Pessoa Física
            if reg_nome == "C100":
                dados = (getattr(r, "conteudo_json", None) or {}).get("dados") or []
                cod_part = str(dados[2]).strip() if len(dados) > 2 else None
                is_pf = participantes_pf.get(cod_part, False)

            elif reg_nome == "C170":
                id_pai = str(getattr(r, "pai_id", ""))
                cod_part = mapa_pai_participante.get(id_pai)
                is_pf = participantes_pf.get(cod_part, False)

            if is_pf:
                rid_pf = int(getattr(r, "registro_id", 0) or getattr(r, "id", 0) or 0)
                if rid_pf:
                    ids_pf_detectados.add(rid_pf)
                continue  # expurgo

            rows_limpas.append(r)
        # --- 2.2) ENRIQUECER rows_like com registro_id real (quando aplicar_revisoes=True) ---
        # Isso garante anchor_registro_id e evita registro_id=0 nos AGGs.
        for r in rows_limpas:
            try:
                linha_r = int(getattr(r, "linha", 0) or 0)
                if linha_r <= 0:
                    continue

                rid = int(getattr(r, "registro_id", 0) or 0)
                if rid <= 0:
                    rid = int(linha_to_registro_id.get(linha_r, 0) or 0)
                    if rid > 0:
                        setattr(r, "registro_id", rid)

                # opcional: alguns lugares ainda usam r.id
                rid_id = int(getattr(r, "id", 0) or 0)
                if rid_id <= 0 and rid > 0:
                    setattr(r, "id", rid)

            except Exception:
                pass

        # -------------------------------------------------
        # 2.3) Mapa 0200 (COD_ITEM -> NCM) usando fonte_base
        # -------------------------------------------------
        item_to_ncm: dict[str, str] = {}

        for l in fonte_base:
            reg_nome_l = str(getattr(l, "reg", "")).strip().upper()
            if reg_nome_l != "0200":
                continue

            raw_dados_0200 = getattr(l, "dados", None)

            if not raw_dados_0200:
                cj = getattr(l, "conteudo_json", None) or {}
                if isinstance(cj, dict):
                    raw_dados_0200 = cj.get("dados")

            if not raw_dados_0200:
                rj = getattr(l, "revisao_json", None) or {}
                if isinstance(rj, dict):
                    raw_dados_0200 = rj.get("dados")

            dados_0200 = list(raw_dados_0200 or [])
            cod_item = str(dados_0200[0]).strip() if len(dados_0200) > 0 else ""
            ncm_raw = str(dados_0200[6] or "") if len(dados_0200) > 6 else ""
            ncm = "".join(filter(str.isdigit, ncm_raw))

            if cod_item and ncm:
                item_to_ncm[cod_item] = ncm

        # 3) Converter para DTO (LIMPO E ROBUSTO COM REVISÕES)
        dtos: List[RegistroFiscalDTO] = []

        for l in fonte_base:
            rid_real = int(getattr(l, "registro_id", 0) or 0)
            if rid_real <= 0:
                rid_real = linha_to_registro_id.get(int(getattr(l, "linha", 0) or 0), 0)

            reg_nome = str(getattr(l, "reg", "")).strip().upper()

            is_pessoa_fisica = False
            if reg_nome in ("C100", "C170"):
                pai_id = int(getattr(l, "pai_id", 0) or 0)
                if rid_real in ids_pf_detectados or pai_id in ids_pf_detectados:
                    is_pessoa_fisica = True

            # ✅ DADOS robusto (EfdRegistro e LinhaLogica)
            raw_dados = getattr(l, "dados", None)

            if not raw_dados:
                cj = getattr(l, "conteudo_json", None) or {}
                if isinstance(cj, dict):
                    raw_dados = cj.get("dados")

            if not raw_dados:
                rj = getattr(l, "revisao_json", None) or {}
                if isinstance(rj, dict):
                    raw_dados = rj.get("dados")

            dados_list = list(raw_dados or [])

            meta: dict[str, Any] = {}

            if reg_nome == "C170":
                cod_item = str(dados_list[1]).strip() if len(dados_list) > 1 else ""
                ncm = item_to_ncm.get(cod_item)
                if ncm:
                    meta["ncm"] = ncm

                if cod_item:
                    meta["cod_item"] = cod_item

            # opcional: se quiser rastrear origem
            meta["fonte_base"] = "overlay" if aplicar_revisoes else "original"

            dtos.append(
                RegistroFiscalDTO(
                    id=int(rid_real),
                    reg=reg_nome,
                    linha=int(getattr(l, "linha", 0) or 0),
                    dados=dados_list,
                    is_pf=is_pessoa_fisica,
                    versao_id=int(versao_id),
                    empresa_id=int(empresa_id),
                    meta=meta,
                )
            )

        # -----------------------------
        # 4) Injetar META_FISCAL e agregadores (sempre usando rows_limpas)
        # -----------------------------
        cat = None
        try:
            # já existe set_fiscal_context(db, empresa_id_ctx) antes, então:
            cat = carregar_catalogo_fiscal(db, empresa_id=empresa_id_ctx)
        except Exception:
            cat = None

        meta_fiscal = montar_meta_fiscal(rows_limpas, catalogo=cat, debug=False)

        if meta_fiscal:
            # garantir contexto também no meta (se DTO suportar)
            try:
                meta_fiscal.versao_id = versao_id
                meta_fiscal.empresa_id = empresa_id_ctx
            except Exception:
                pass
            dtos.append(meta_fiscal)

        c100_ent = montar_c100_entrada_relevante_agg(rows_limpas)

        if c100_ent:
            try:
                c100_ent.versao_id = versao_id
                c100_ent.empresa_id = empresa_id_ctx
            except Exception:
                pass
            dtos.append(c100_ent)

        c190_exp = montar_c190_export_agg(rows_limpas)
        if c190_exp:
            try:
                c190_exp.versao_id = versao_id
                c190_exp.empresa_id = empresa_id_ctx
            except Exception:
                pass
            dtos.append(c190_exp)
        else:
            c170_exp = montar_c170_export_agg(rows_limpas)
            if c170_exp:
                try:
                    c170_exp.versao_id = versao_id
                    c170_exp.empresa_id = empresa_id_ctx
                except Exception:
                    pass
                dtos.append(c170_exp)

        c190_ind = montar_c190_ind_torrado_agg(rows_limpas)
        if c190_ind:
            try:
                c190_ind.versao_id = versao_id
                c190_ind.empresa_id = empresa_id_ctx
            except Exception:
                pass
            dtos.append(c190_ind)
        else:
            c170_ind = montar_c170_ind_torrado_agg(rows_limpas)
            if c170_ind:
                try:
                    c170_ind.versao_id = versao_id
                    c170_ind.empresa_id = empresa_id_ctx
                except Exception:
                    pass
                dtos.append(c170_ind)

        c190_agg = montar_c190_agg(rows_limpas)
        if c190_agg:
            try:
                c190_agg.versao_id = versao_id
                c190_agg.empresa_id = empresa_id_ctx
            except Exception:
                pass
            dtos.append(c190_agg)

        # -----------------------------
        # 5) Executar motor fiscal
        # -----------------------------
        for d in dtos:
            if (d.reg or "").strip() == "META_FISCAL":
                print("[META_FISCAL DTO] dados=", d.dados, flush=True)
                break

        result = executar_varredura(dtos, capturar_erros=True)

        # -----------------------------
        # Helpers
        # -----------------------------
        Number = Union[int, float, Decimal]

        def _norm_codigo(c: Optional[str]) -> str:
            return (str(c).strip() if c is not None else "").strip()

        def _prioridade_por_impacto(impacto) -> Optional[str]:
            if impacto is None:
                return None
            try:
                val = Decimal(str(impacto))
            except (InvalidOperation, ValueError, TypeError):
                return None
            if val <= 0:
                return None
            if val >= Decimal("5000"):
                return "ALTA"
            if val >= Decimal("1000"):
                return "MEDIA"
            return "BAIXA"

        def _norm_prioridade(p) -> Optional[str]:
            if p is None:
                return None
            if not isinstance(p, str):
                p = str(p)
            p = p.strip().upper()
            if p == "MÉDIA":
                p = "MEDIA"
            return p if p in ("ALTA", "MEDIA", "BAIXA") else None

        def _safe_float(x) -> Optional[float]:
            if x is None:
                return None
            try:
                return float(x)
            except Exception:
                try:
                    return float(str(x).replace(",", "."))
                except Exception:
                    return None

        def _key(registro_id: Optional[int], tipo: str, codigo: Optional[str]) -> Tuple[int, str, str]:
            rid = int(registro_id) if registro_id is not None else 0
            return (rid, str(tipo), _norm_codigo(codigo))

        # -----------------------------
        # 6) Limpeza profissional (apaga pendentes e preserva resolvidos se solicitado)
        # -----------------------------
        q_del = db.query(EfdApontamento).filter(EfdApontamento.versao_id == versao_id)
        if preservar_resolvidos:
            q_del = q_del.filter(EfdApontamento.resolvido == False)  # noqa: E712
        q_del.delete(synchronize_session=False)

        existing_resolved: Dict[Tuple[int, str, str], EfdApontamento] = {}
        if preservar_resolvidos:
            resolved_rows: List[EfdApontamento] = (
                db.query(EfdApontamento)
                .filter(
                    EfdApontamento.versao_id == versao_id,
                    EfdApontamento.resolvido == True,  # noqa: E712
                )
                .all()
            )
            for ap in resolved_rows:
                existing_resolved[_key(ap.registro_id, ap.tipo, ap.codigo)] = ap

        # -----------------------------
        # REFINAMENTO 1 — C100 não compete com C190
        # -----------------------------
        tem_sum_c190 = any(_norm_codigo(a.codigo) == "C190-ENT" for a in result.apontamentos)

        # -----------------------------
        # REFINAMENTO 2 — Agregar C190 por (CFOP, CST) => cria C190-SUM
        # -----------------------------
        grupos = defaultdict(lambda: {"total": Decimal("0"), "qtd": 0, "repr_registro_id": None})

        consolidar_achados_c170_insumo_v2(result)

        for a in result.apontamentos:
            if _norm_codigo(getattr(a, "codigo", None)) != "C190-ENT":
                continue

            raw_meta = getattr(a, "meta", None) or {}
            meta = dict(raw_meta) if isinstance(raw_meta, dict) else {}

            cfop = meta.get("cfop")
            cst = meta.get("cst_icms")
            if not cfop or not cst:
                continue

            key = (str(cfop), str(cst))

            if grupos[key]["repr_registro_id"] is None:
                try:
                    grupos[key]["repr_registro_id"] = int(a.registro_id)
                except Exception:
                    grupos[key]["repr_registro_id"] = None

            imp = getattr(a, "impacto_financeiro", None)
            try:
                imp_dec = Decimal(str(imp or "0"))
            except Exception:
                imp_dec = Decimal("0")

            grupos[key]["total"] += imp_dec
            grupos[key]["qtd"] += 1

        for (cfop, cst), g in grupos.items():
            if g["qtd"] >= 2 and g["repr_registro_id"]:
                total_dec: Decimal = g["total"]
                result.apontamentos.append(
                    Achado(
                        registro_id=int(g["repr_registro_id"]),
                        tipo="OPORTUNIDADE",
                        codigo="C190-SUM",
                        descricao=(
                            f"C190 agregado: CFOP={cfop} CST={cst} — "
                            f"{g['qtd']} operação(ões) — impacto est. {total_dec.quantize(Decimal('0.01'))}"
                        ),
                        impacto_financeiro=float(total_dec),
                        regra="Resumo C190 por CFOP/CST",
                        meta={
                            "cfop": cfop,
                            "cst_icms": cst,
                            "qtd": int(g["qtd"]),
                            "impacto_total": str(total_dec),
                        },
                    )
                )


        # -----------------------------
        # 7) Inserir/atualizar apontamentos
        # -----------------------------
        tem_cafe = any(_norm_codigo(x.codigo) == "CAFE_C190_V1" for x in result.apontamentos)

        to_insert: List[EfdApontamento] = []
        to_update_mappings: List[dict] = []

        descartados_sem_fk = 0

        for a in result.apontamentos:
            rid = None
            if getattr(a, "registro_id", None) is not None:
                try:
                    rid = int(a.registro_id)
                except Exception:
                    rid = None

            raw_meta = getattr(a, "meta", None) or {}
            meta = dict(raw_meta) if isinstance(raw_meta, dict) else {}

            tipo = str(getattr(a, "tipo", "") or "").strip() or "OPORTUNIDADE"
            codigo_norm = _norm_codigo(getattr(a, "codigo", None)) or None
            descricao = str(getattr(a, "descricao", "") or "").strip()
            impacto = getattr(a, "impacto_financeiro", None)

            # fallback: tenta resolver pelo meta["linha"]
            if not rid or rid <= 0:
                linha_meta = meta.get("linha") or meta.get("linha_num") or meta.get("linha_referencia")
                try:
                    linha_i = int(linha_meta) if linha_meta is not None else None
                except Exception:
                    linha_i = None
                if linha_i is not None:
                    rid = linha_to_registro_id.get(linha_i)

            # fallback: ancora no primeiro C170 da versão se tiver fonte_base
            if not rid:
                fonte_base = meta.get("fonte_base") or meta.get("fonte")
                if fonte_base:
                    rid = next((int(r.id) for r in rows if (r.reg or "").strip().upper() == "C170"), None)

            if not rid:
                descartados_sem_fk += 1
                logger.warning(
                    "DESCARTADO sem FK | codigo=%s | tipo=%s | fonte=%s",
                    codigo_norm,
                    tipo,
                    meta.get("fonte_base") or meta.get("fonte"),
                )
                continue

            prio_regra = _norm_prioridade(getattr(a, "prioridade", None))
            prioridade = prio_regra or _prioridade_por_impacto(impacto) or "BAIXA"

            if tem_sum_c190 and codigo_norm == "C100-ENT":
                prioridade = "BAIXA"

            if tem_cafe and codigo_norm == "C190-ENT":
                prioridade = "BAIXA"
                if "Consolidado disponível" not in descricao:
                    descricao += " (Consolidado disponível em CAFE_C190_V1.)"

            k = _key(rid, tipo, codigo_norm)

            if preservar_resolvidos and k in existing_resolved:
                ap_exist = existing_resolved[k]
                to_update_mappings.append(
                    {
                        "id": ap_exist.id,
                        "descricao": descricao,
                        "impacto_financeiro": _safe_float(impacto),
                        "prioridade": prioridade,
                        "meta_json": meta,
                    }
                )
                continue

            to_insert.append(
                EfdApontamento(
                    versao_id=versao_id,
                    registro_id=rid,
                    tipo=tipo,
                    codigo=codigo_norm,
                    descricao=descricao,
                    impacto_financeiro=_safe_float(impacto),
                    prioridade=prioridade,
                    resolvido=False,
                    meta_json=meta,
                )
            )

        if to_update_mappings:
            db.bulk_update_mappings(EfdApontamento, to_update_mappings)

        if to_insert:
            db.bulk_save_objects(to_insert)



        logger.warning("SCAN FINAL | versao_id=%s | descartados_sem_fk=%s", versao_id, descartados_sem_fk)

        # -----------------------------
        # retorno final (telemetria)
        # -----------------------------
        total_c170_processados = len([d for d in dtos if d.reg == "C170" and not getattr(d, "is_pf", False)])

        return {
            "apontamentos_gerados": len(to_insert),
            "erros_regras": result.erros,
            "atualizados_preservados": len(to_update_mappings),
            "descartados_sem_fk": int(descartados_sem_fk),
            "total_c170_processados": int(total_c170_processados),
        }