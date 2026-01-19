from __future__ import annotations
from sqlalchemy import text
from collections import defaultdict
from decimal import Decimal , InvalidOperation
from typing import Any, Dict, List, Optional, Tuple, Union
from sqlalchemy.orm import Session
from app.db.models import EfdRegistro, EfdApontamento
from app.fiscal.dto import RegistroFiscalDTO
from app.fiscal.scanners.c190 import montar_c190_agg
from app.fiscal.scanners.exportacao import montar_c190_export_agg, montar_c170_export_agg
from app.fiscal.varredura import executar_varredura
from app.fiscal.regras.achado import Achado
from app.fiscal.scanners.exportacao import montar_meta_fiscal



class FiscalScanner:
    @staticmethod
    def scan_versao(
        db: Session,
        *,
        versao_id: int,
        preservar_resolvidos: bool = True,
    ) -> Dict[str, Any]:

        # 1) Buscar registros
        rows: List[EfdRegistro] = (
            db.query(EfdRegistro)
            .filter(EfdRegistro.versao_id == versao_id)
            .order_by(EfdRegistro.linha.asc())
            .all()
        )

        # 2) Converter para DTO
        dtos: List[RegistroFiscalDTO] = []
        for r in rows:
            conteudo = r.conteudo_json or {}
            dados = conteudo.get("dados") or []
            dtos.append(
                RegistroFiscalDTO(
                    id=int(r.id),
                    reg=str(r.reg),
                    linha=int(r.linha),
                    dados=dados,
                )
            )

        # 2.3) Injetar META_FISCAL (SEMPRE)
        meta_fiscal = montar_meta_fiscal(rows)
        if meta_fiscal:
            dtos.append(meta_fiscal)

        # 2.4) Injetar DTO agregador (Exportação)
        c190_exp = montar_c190_export_agg(rows)
        if c190_exp:
            dtos.append(c190_exp)
        else:
            c170_exp = montar_c170_export_agg(rows)
            if c170_exp:
                dtos.append(c170_exp)

        # 2.5) Injetar DTO agregador (C190_AGG)
        c190_agg = montar_c190_agg(rows)
        if c190_agg:
            dtos.append(c190_agg)

         # 3) Executar motor fiscal
        result = executar_varredura(dtos, capturar_erros=True)

        # DEBUG: conferir se META_FISCAL foi injetado
        meta_count = sum(1 for d in dtos if (d.reg or "").strip() == "META_FISCAL")
        print("DEBUG META_FISCAL dtos:", meta_count)

        # DEBUG: se existir, mostrar o meta
        for d in dtos:
            if (d.reg or "").strip() == "META_FISCAL":
                print("DEBUG META_FISCAL dados[0]:", (d.dados or [None])[0])
                break

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

        # --- chave lógica do apontamento (evita duplicar resolvidos) ---
        def _key(registro_id: Optional[int], tipo: str, codigo: Optional[str]) -> Tuple[int, str, str]:
            rid = int(registro_id) if registro_id is not None else 0
            return (rid, str(tipo), _norm_codigo(codigo))

        # -----------------------------
        # 4) Limpeza profissional
        # -----------------------------
        q_del = db.query(EfdApontamento).filter(EfdApontamento.versao_id == versao_id)
        if preservar_resolvidos:
            q_del = q_del.filter(EfdApontamento.resolvido == False)  # noqa: E712
        q_del.delete(synchronize_session=False)

        # 4.1) Se preservar resolvidos: carregar resolvidos existentes para fazer merge
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
        #   - usa registro_id representativo (primeiro do grupo) para não quebrar FK
        #   - só cria se qtd >= 2 (evita “resumo” inútil)
        # -----------------------------
        grupos = defaultdict(lambda: {"total": Decimal("0"), "qtd": 0, "repr_registro_id": None})

        for a in result.apontamentos:
            if _norm_codigo(a.codigo) != "C190-ENT":
                continue

            raw_meta = getattr(a, "meta", None) or {}
            meta = dict(raw_meta) if isinstance(raw_meta, dict) else {}

            cfop = meta.get("cfop")
            cst = meta.get("cst_icms")

            if not cfop or not cst:
                continue

            key = (str(cfop), str(cst))

            # registro representativo (para FK)
            if grupos[key]["repr_registro_id"] is None:
                try:
                    grupos[key]["repr_registro_id"] = int(a.registro_id)
                except Exception:
                    grupos[key]["repr_registro_id"] = None

            # soma impacto com segurança
            imp = getattr(a, "impacto_financeiro", None)
            try:
                imp_dec = Decimal(str(imp or "0"))
            except Exception:
                imp_dec = Decimal("0")

            grupos[key]["total"] += imp_dec
            grupos[key]["qtd"] += 1

        # adiciona apontamentos SUM ao resultado (não mexe nos individuais aqui)
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
        # 5) Inserir/atualizar apontamentos
        # -----------------------------
        tem_cafe = any(_norm_codigo(x.codigo) == "CAFE_C190_V1" for x in result.apontamentos)

        to_insert: List[EfdApontamento] = []
        to_update_mappings: List[dict] = []

        for a in result.apontamentos:
            rid = int(a.registro_id) if a.registro_id is not None else 0
            tipo = str(a.tipo)
            codigo_norm = _norm_codigo(getattr(a, "codigo", None)) or None

            # ✅ se SUM será via SQL, ignore aqui
            if codigo_norm == "C190-SUM":
                continue

            impacto = getattr(a, "impacto_financeiro", None)

            prio_regra = _norm_prioridade(getattr(a, "prioridade", None))
            prioridade = prio_regra or _prioridade_por_impacto(impacto) or "BAIXA"

            descricao = str(getattr(a, "descricao", "") or "").strip()

            # ✅ meta tem que nascer antes de qualquer branch
            raw_meta = getattr(a, "meta", None) or {}
            meta = dict(raw_meta) if isinstance(raw_meta, dict) else {}

            # refinamentos de prioridade
            if tem_sum_c190 and codigo_norm == "C100-ENT":
                prioridade = "BAIXA"

            if tem_cafe and codigo_norm == "C190-ENT":
                prioridade = "BAIXA"
                if "Detalhamento" not in descricao:
                    descricao += " (Consolidado disponível em CAFE_C190_V1.)"

            k = _key(rid, tipo, codigo_norm)

            if preservar_resolvidos and k in existing_resolved:
                ap_exist = existing_resolved[k]
                to_update_mappings.append({
                    "id": ap_exist.id,
                    "descricao": descricao,
                    "impacto_financeiro": impacto,
                    "prioridade": prioridade,
                    "meta_json": meta,  # ✅ agora existe
                })
                continue

            to_insert.append(
                EfdApontamento(
                    versao_id=int(versao_id),
                    registro_id=rid,
                    tipo=tipo,
                    codigo=codigo_norm,
                    descricao=descricao,
                    impacto_financeiro=impacto,
                    prioridade=prioridade,
                    resolvido=False,
                    meta_json=meta,
                )
            )

        if to_update_mappings:
            db.bulk_update_mappings(EfdApontamento, to_update_mappings)

        if to_insert:
            db.bulk_save_objects(to_insert)

        # --- C190-SUM no banco (robusto, sem depender de meta) ---
        # OBS: roda SEMPRE, porque o SUM depende do que existe no banco (inclui preservados)
        # 1) apaga SUM antigo
        db.execute(
            text("""
                 DELETE
                 FROM efd_apontamento
                 WHERE versao_id = :vid
                   AND codigo = 'C190-SUM'
                 """),
            {"vid": int(versao_id)}
        )

        # 2) insere SUM novo via subquery (ONLY_FULL_GROUP_BY safe)
        db.execute(
            text("""
                 INSERT INTO efd_apontamento
                 (versao_id, registro_id, tipo, codigo, descricao, impacto_financeiro, prioridade, resolvido)
                 SELECT t.versao_id,
                        t.registro_id,
                        'OPORTUNIDADE'            AS tipo,
                        'C190-SUM'                AS codigo,
                        CONCAT(
                                'C190 agregado: CFOP=', t.cfop,
                                ' CST=', t.cst_icms,
                                ' — ', t.qtd,
                                ' operação(ões) — impacto est. ', ROUND(t.impacto_total, 2)
                        )                         AS descricao,
                        ROUND(t.impacto_total, 2) AS impacto_financeiro,
                        'ALTA'                    AS prioridade,
                        0                         AS resolvido
                 FROM (SELECT a.versao_id                                               AS versao_id,
                              MIN(a.registro_id)                                        AS registro_id,
                              JSON_UNQUOTE(JSON_EXTRACT(r.conteudo_json, '$.dados[1]')) AS cfop,
                              JSON_UNQUOTE(JSON_EXTRACT(r.conteudo_json, '$.dados[0]')) AS cst_icms,
                              COUNT(*)                                                  AS qtd,
                              SUM(
                                      CAST(
                                              REPLACE(
                                                      JSON_UNQUOTE(JSON_EXTRACT(r.conteudo_json, '$.dados[3]')),
                                                      ',', '.'
                                              ) AS DECIMAL(15, 2)
                                      )
                              ) * 0.0925                                                AS impacto_total
                       FROM efd_apontamento a
                                JOIN efd_registro r ON r.id = a.registro_id
                       WHERE a.versao_id = :vid
                         AND a.codigo = 'C190-ENT'
                       GROUP BY a.versao_id,
                                JSON_UNQUOTE(JSON_EXTRACT(r.conteudo_json, '$.dados[1]')),
                                JSON_UNQUOTE(JSON_EXTRACT(r.conteudo_json, '$.dados[0]'))
                       HAVING COUNT(*) >= 2) t
                 """),
            {"vid": int(versao_id)}
        )

        # 3) rebaixa C190-ENT se existe SUM
        sum_qtd = db.execute(
            text("""
                 SELECT COUNT(*)
                 FROM efd_apontamento
                 WHERE versao_id = :vid
                   AND codigo = 'C190-SUM'
                 """),
            {"vid": int(versao_id)}
        ).scalar() or 0

        if int(sum_qtd) > 0:
            db.execute(
                text("""
                     UPDATE efd_apontamento
                     SET prioridade = 'BAIXA'
                     WHERE versao_id = :vid
                       AND codigo = 'C190-ENT'
                     """),
                {"vid": int(versao_id)}
            )

        return {
            "apontamentos_gerados": len(to_insert),
            "erros_regras": result.erros,
            "atualizados_preservados": len(to_update_mappings),
        }
