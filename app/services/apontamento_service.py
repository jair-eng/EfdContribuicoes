from dataclasses import dataclass
from sqlalchemy.orm import Session
from app.db.models import EfdApontamento, EfdVersao, EfdArquivo
from typing import List
from app.services.c170_service import aplicar_correcao_ind_cafe_cst51, aplicar_correcao_ind_agro_cst51


@dataclass(frozen=True)
class ResolverTodosResult:
    versao_id: int
    updated_total: int
    pendentes_restantes: int


class ApontamentoService:
    @staticmethod
    def resolver_todos_pendentes_por_versao(db: Session, *, versao_id: int) -> ResolverTodosResult:
        versao_id = int(versao_id)

        # 0) Descobre códigos pendentes antes de resolver
        codigos_pendentes: List[str] = [
            str(x[0])
            for x in (
                db.query(EfdApontamento.codigo)
                .filter(EfdApontamento.versao_id == versao_id)
                .filter(EfdApontamento.resolvido.is_(False))
                .distinct()
                .all()
            )
            if x and x[0]
        ]
        print("[RESOLVER_TODOS] versao_id=", versao_id, "codigos_pendentes=", codigos_pendentes)

        # 1) AUTO-FIX — CAFÉ (prioridade) + AGRO (quando você quiser habilitar)
        total_alterado_fix = 0

        # 1.1) CAFÉ
        if "IND_CAFE_V1" in codigos_pendentes:
            print("[RESOLVER_TODOS] AUTO-FIX IND_CAFE_V1: INICIO")

            res_fix = aplicar_correcao_ind_cafe_cst51(
                db,
                versao_origem_id=versao_id,
                incluir_revenda=True,  # café: pode ser agressivo mesmo
                csts_origem=["70", "73", "75", "98", "99", "06", "07", "08"],
                apontamento_id=None,  # resolve-todos (batch)
            )
            db.flush()

            if str(res_fix.get("status")) == "erro":
                raise ValueError(f"AUTO-FIX IND_CAFE falhou: {res_fix.get('msg')}")

            alterados = int(res_fix.get("total_alterado") or 0)
            total_alterado_fix += alterados

            print("[RESOLVER_TODOS] AUTO-FIX IND_CAFE_V1: FIM alterados=", alterados, "res=", res_fix)

            # ✅ guard-rail: não “resolve tudo” se não aplicou nada
            if alterados <= 0:
                raise ValueError(
                    "AUTO-FIX IND_CAFE não alterou nenhum registro. "
                    "Não vou marcar apontamentos como resolvidos."
                )

        # 1.2) AGRO
        # ✅ aqui você decide: por enquanto pode deixar DESLIGADO (comentado),
        # ou deixar ligado mas conservador (sem revenda).
        if "IND_AGRO_V1" in codigos_pendentes:
            print("[RESOLVER_TODOS] AUTO-FIX IND_AGRO_V1: INICIO")

            res_fix = aplicar_correcao_ind_agro_cst51(
                db,
                versao_origem_id=versao_id,
                incluir_revenda=False,  # ✅ conservador: só 1101/2101/3101
                # opcional: se quiser travar por NCM via catálogo, passe prefixos:
                # ncm_prefixos_permitidos=["0901", "1001", "1201", "1208"],  # exemplo
                ncm_prefixos_permitidos=None,
                csts_origem=["70", "73", "75", "98", "99", "06", "07", "08"],
                apontamento_id=None,
                motivo_codigo="IND_AGRO_V1",
            )
            db.flush()

            if str(res_fix.get("status")) == "erro":
                raise ValueError(f"AUTO-FIX IND_AGRO falhou: {res_fix.get('msg')}")

            alterados = int(res_fix.get("total_alterado") or 0)
            total_alterado_fix += alterados

            print("[RESOLVER_TODOS] AUTO-FIX IND_AGRO_V1: FIM alterados=", alterados, "res=", res_fix)

            # ✅ guard-rail: se a regra existe como pendente mas não alterou nada, não marca tudo resolvido
            if alterados <= 0:
                raise ValueError(
                    "AUTO-FIX IND_AGRO não alterou nenhum registro. "
                    "Não vou marcar apontamentos como resolvidos."
                )

        # 2) Marca tudo como resolvido (comportamento atual)
        updated = (
            db.query(EfdApontamento)
            .filter(EfdApontamento.versao_id == versao_id)
            .filter(EfdApontamento.resolvido.is_(False))
            .update({EfdApontamento.resolvido: True}, synchronize_session=False)
        )

        pendentes_restantes = (
            db.query(EfdApontamento)
            .filter(EfdApontamento.versao_id == versao_id)
            .filter(EfdApontamento.resolvido.is_(False))
            .count()
        )

        print(
            "[RESOLVER_TODOS] updated_total=", int(updated or 0),
            "pendentes_restantes=", int(pendentes_restantes or 0),
            "total_alterado_fix=", int(total_alterado_fix),
        )

        return ResolverTodosResult(
            versao_id=versao_id,
            updated_total=int(updated or 0),
            pendentes_restantes=int(pendentes_restantes or 0),
        )