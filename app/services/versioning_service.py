from __future__ import annotations
from copy import deepcopy
from datetime import datetime
from typing import Any, Dict, List
from sqlalchemy.orm import Session
from app.db.models import EfdArquivo, EfdRegistro, EfdVersao
from app.fiscal.scanner import FiscalScanner


class VersioningService:
    @staticmethod
    def criar_nova_versao(
        db: Session,
        *,
        arquivo_id: int,
        versao_origem_id: int,
        alteracoes: List[Dict[str, Any]],
        observacao: str = "Correção manual",
    ) -> int:
        try:
            versao_origem = db.query(EfdVersao).get(versao_origem_id)
            if not versao_origem:
                raise ValueError("Versão de origem não encontrada")

            if versao_origem.arquivo_id != arquivo_id:
                raise ValueError("Versão origem não pertence ao arquivo informado")

            if versao_origem.status in ("VALIDADA", "EXPORTADA"):
                raise ValueError("Não é permitido editar versões VALIDADA ou EXPORTADA")

            arquivo = db.query(EfdArquivo).get(arquivo_id)
            if not arquivo:
                raise ValueError("Arquivo não encontrado")

            # 1) cria nova versão
            nova_versao = EfdVersao(
                arquivo_id=arquivo_id,
                numero=(versao_origem.numero or 1) + 1,
                data_geracao=datetime.utcnow(),
                observacao=observacao,
                status="GERADA",
            )
            db.add(nova_versao)
            db.flush()

            # 2) carrega registros da versão origem
            registros_origem = (
                db.query(EfdRegistro)
                .filter(EfdRegistro.versao_id == versao_origem_id)
                .order_by(EfdRegistro.linha)
                .all()
            )
            if not registros_origem:
                raise ValueError("Versão de origem não possui registros")

            # 3) mapa de alterações (por registro_id da origem)
            mapa_alteracoes = {
                int(a["registro_id"]): a["novos_dados"]
                for a in alteracoes
                if "registro_id" in a and "novos_dados" in a
            }

            # 4) snapshot + aplica alterações
            novos_registros: List[EfdRegistro] = []
            for r in registros_origem:
                novo_conteudo = deepcopy(r.conteudo_json)
                alterado = False

                if int(r.id) in mapa_alteracoes:
                    novo_conteudo["dados"] = mapa_alteracoes[int(r.id)]
                    alterado = True

                novos_registros.append(
                    EfdRegistro(
                        versao_id=nova_versao.id,
                        linha=r.linha,
                        reg=r.reg,
                        conteudo_json=novo_conteudo,
                        alterado=alterado,
                        base_credito=r.base_credito,
                        valor_credito=r.valor_credito,
                        tipo_credito=r.tipo_credito,
                    )
                )

            db.bulk_save_objects(novos_registros)
            db.flush()

            # 5) reprocessa apontamentos da nova versão
            scan = FiscalScanner.scan_versao(db, nova_versao.id, capturar_erros=True)
            FiscalScanner.salvar_resultado(db, nova_versao.id, scan)

            # 6) marca arquivo como em revisão
            arquivo.status = "EM_REVISAO"

            db.commit()
            return int(nova_versao.id)

        except Exception:
            db.rollback()
            raise
