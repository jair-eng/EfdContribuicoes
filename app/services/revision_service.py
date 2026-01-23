from __future__ import annotations
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple
from sqlalchemy.orm import Session
from app.db.models import EfdVersao, EfdRegistro, EfdApontamento
from app.db.models.efd_revisao import EfdRevisao
from sqlalchemy import func
from app.sped.renderer import render_from_registro, _sha1

class RevisionService:
    """
    Fluxo:
      - versão original: retifica_de_versao_id = NULL (imutável)
      - versão revisada: retifica_de_versao_id = <id da original>
      - efd_revisao: alterações aplicadas sobre registros da original
    """

    @staticmethod
    def get_or_create_versao_revisada(
        db: Session,
        *,
        versao_origem_id: int,
        status_revisada: str = "EM_REVISAO",
    ) -> EfdVersao:
        origem = db.get(EfdVersao, int(versao_origem_id))
        if not origem:
            raise ValueError("Versão de origem não encontrada")

        # Se a origem já for revisada, normaliza para a raiz (original)
        raiz_id = int(origem.retifica_de_versao_id) if origem.retifica_de_versao_id else int(origem.id)

        # Já existe revisada para essa raiz?
        revisada = (
            db.query(EfdVersao)
            .filter(EfdVersao.retifica_de_versao_id == raiz_id)
            .order_by(EfdVersao.id.desc())
            .first()
        )
        if revisada:
            return revisada

        # cria nova revisada
        nova = EfdVersao(
            arquivo_id=int(origem.arquivo_id),
            numero=int(origem.numero) + 1,  # simples; se você já controla sequência de outro jeito, ajusta
            data_geracao=datetime.utcnow(),
            status=status_revisada,
            retifica_de_versao_id=raiz_id,
        )
        db.add(nova)
        db.flush()  # pega id

        return nova

    @staticmethod
    def criar_revisao_replace_line(
            db: Session,
            *,
            versao_id: int,
            apontamento_id: int,
            linha_nova: str,
            motivo_codigo: Optional[str] = None,
    ):
        ap = db.get(EfdApontamento, int(apontamento_id))
        if not ap:
            raise ValueError("Apontamento não encontrado")

        # 🔒 valida contexto
        if int(ap.versao_id) != int(versao_id):
            raise ValueError(
                f"Apontamento {apontamento_id} não pertence à versão {versao_id}"
            )

        regrow = db.get(EfdRegistro, int(ap.registro_id))
        if not regrow:
            raise ValueError("Registro do apontamento não encontrado")

        versao_revisada = RevisionService.get_or_create_versao_revisada(
            db, versao_origem_id=int(versao_id)
        )

        rev = EfdRevisao(
            versao_origem_id=int(versao_id),
            versao_revisada_id=None,  # opção 2
            registro_id=int(regrow.id),  # ok manter (origem)
            reg=str(regrow.reg),
            acao="REPLACE_LINE",
            revisao_json={
                "linha_referencia": int(regrow.linha),
                "linha_nova": linha_nova,
            },
            motivo_codigo=motivo_codigo or ap.codigo,
            apontamento_id=int(ap.id),
        )

        ap.resolvido = True

        db.add(rev)  # o apontamento (ap) já está no session
        db.flush()

        return rev



def _parse_sped_line_to_reg_dados(linha_nova: str) -> Tuple[str, List[str]]:
    """
    |C190|051|1102|...|  ->  ("C190", ["051","1102",...])
    """
    s = (linha_nova or "").strip()
    if not s:
        raise ValueError("linha_nova vazia")

    if not s.startswith("|"):
        s = "|" + s
    if not s.endswith("|"):
        s = s + "|"

    partes = s.split("|")
    if len(partes) < 3 or not partes[1].strip():
        raise ValueError("linha_nova inválida (REG ausente)")

    reg = partes[1].strip()
    dados = partes[2:-1]  # remove o último "" do pipe final
    return reg, dados


def materializar_versao_revisada(*, db: Session, versao_origem_id: int) -> int:
    origem = db.get(EfdVersao, int(versao_origem_id))
    if not origem:
        raise ValueError("Versão origem não encontrada")

    # 1) pega/cria revisada (derivada)
    revisada = (
        db.query(EfdVersao)
        .filter(EfdVersao.retifica_de_versao_id == int(versao_origem_id))
        .order_by(EfdVersao.id.desc())
        .first()
    )

    if revisada is None:
        max_num = (
            db.query(func.max(EfdVersao.numero))
            .filter(EfdVersao.arquivo_id == origem.arquivo_id)
            .scalar()
        ) or 0

        revisada = EfdVersao(
            arquivo_id=int(origem.arquivo_id),
            numero=int(max_num) + 1,
            status="EM_REVISAO",
            retifica_de_versao_id=int(versao_origem_id),
            observacao="Versão revisada (materializada)",
        )
        db.add(revisada)
        db.flush()

    versao_revisada_id = int(revisada.id)

    db.query(EfdRevisao).filter(
        EfdRevisao.versao_origem_id == int(versao_origem_id),
        EfdRevisao.versao_revisada_id.is_(None),
    ).update(
        {EfdRevisao.versao_revisada_id: versao_revisada_id},
        synchronize_session=False,
    )
    db.flush()

    # 2) copia registros da origem para revisada (uma vez)
    ja_tem = (
        db.query(func.count(EfdRegistro.id))
        .filter(EfdRegistro.versao_id == versao_revisada_id)
        .scalar()
    ) or 0

    if int(ja_tem) == 0:
        regs_origem: List[EfdRegistro] = (
            db.query(EfdRegistro)
            .filter(EfdRegistro.versao_id == int(versao_origem_id))
            .order_by(EfdRegistro.linha.asc())
            .all()
        )
        if not regs_origem:
            raise ValueError("Versão origem não possui registros")

        objs: List[EfdRegistro] = []
        for r in regs_origem:
            objs.append(
                EfdRegistro(
                    versao_id=versao_revisada_id,
                    linha=int(r.linha),
                    reg=str(r.reg),
                    conteudo_json=dict(r.conteudo_json or {}),
                    alterado=False,  # começa igual; revisão marca True
                    base_credito=getattr(r, "base_credito", None),
                    valor_credito=getattr(r, "valor_credito", None),
                    tipo_credito=getattr(r, "tipo_credito", None),
                )
            )

        db.bulk_save_objects(objs)
        db.flush()

    # 3) aplica revisões REPLACE_LINE na revisada
    revisoes: List[EfdRevisao] = (
        db.query(EfdRevisao)
        .filter(EfdRevisao.versao_origem_id == int(versao_origem_id))
        .filter(EfdRevisao.versao_revisada_id == versao_revisada_id)
        .order_by(EfdRevisao.id.asc())
        .all()
    )

    if revisoes:
        # mapa linha -> registro revisado (id)
        revisada_linha_to_id: Dict[int, int] = dict(
            db.query(EfdRegistro.linha, EfdRegistro.id)
            .filter(EfdRegistro.versao_id == versao_revisada_id)
            .all()
        )

        # mapa registro_origem_id -> linha
        origem_reg_to_linha: Dict[int, int] = dict(
            db.query(EfdRegistro.id, EfdRegistro.linha)
            .filter(EfdRegistro.versao_id == int(versao_origem_id))
            .all()
        )

        for rev in revisoes:
            payload = dict(rev.revisao_json or {})
            linha_nova = (payload.get("linha_nova") or "").strip()
            if not linha_nova:
                continue

            # resolve linha-alvo:
            linha = payload.get("linha_referencia")
            linha = int(linha) if linha is not None else None

            if linha is None and rev.registro_id is not None:
                linha = origem_reg_to_linha.get(int(rev.registro_id))

            if linha is None:
                continue

            acao = str(rev.acao)

            if acao == "REPLACE_LINE":
                registro_revisado_id = revisada_linha_to_id.get(int(linha))
                if not registro_revisado_id:
                    continue

                payload_antes = (payload.get("linha_antes") or "").strip()
                payload_hash = (payload.get("linha_hash") or "").strip()

                # estado atual do registro revisado
                reg_atual_obj = db.get(EfdRegistro, int(registro_revisado_id))
                if not reg_atual_obj:
                    continue

                linha_atual = render_from_registro(reg_atual_obj).strip()
                linha_nova_norm = linha_nova.strip()

                # ✅ idempotência: se já está como queremos, não faz nada
                if linha_atual == linha_nova_norm:
                    continue

                # ✅ conflito: se foi fornecido linha_antes, ela precisa bater com o estado atual
                if payload_antes and linha_atual != payload_antes:
                    # conflito: não aplica
                    continue

                # ✅ conflito alternativo por hash (se não tiver linha_antes)
                if (not payload_antes) and payload_hash:
                    if _sha1(linha_atual) != payload_hash:
                        # conflito: não aplica
                        continue

                # ok, aplica
                reg_novo, dados_novos = _parse_sped_line_to_reg_dados(linha_nova_norm)

                db.query(EfdRegistro).filter(EfdRegistro.id == int(registro_revisado_id)).update(
                    {
                        EfdRegistro.reg: reg_novo,
                        EfdRegistro.conteudo_json: {"dados": dados_novos},
                        EfdRegistro.alterado: True,
                    },
                    synchronize_session=False,
                )

            elif acao == "INSERT_AFTER":
                # 1) abre espaço: tudo que vem depois desloca +1
                db.query(EfdRegistro).filter(
                    EfdRegistro.versao_id == versao_revisada_id,
                    EfdRegistro.linha > int(linha),
                ).update(
                    {EfdRegistro.linha: EfdRegistro.linha + 1},
                    synchronize_session=False,
                )

                # 2) insere o novo registro na linha+1
                reg_novo, dados_novos = _parse_sped_line_to_reg_dados(linha_nova)

                db.add(EfdRegistro(
                    versao_id=versao_revisada_id,
                    linha=int(linha) + 1,
                    reg=reg_novo,
                    conteudo_json={"dados": dados_novos},
                    alterado=True,
                ))
                db.flush()

                # 3) atualiza mapa linha->id para inserções subsequentes na mesma materialização
                # (recalcula só o necessário)
                revisada_linha_to_id = dict(
                    db.query(EfdRegistro.linha, EfdRegistro.id)
                    .filter(EfdRegistro.versao_id == versao_revisada_id)
                    .all()
                )

    return versao_revisada_id