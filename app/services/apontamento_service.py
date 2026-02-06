from dataclasses import dataclass
from sqlalchemy.orm import Session
from app.db.models import EfdApontamento  # ajuste o import
from app.services.c170_service import aplicar_correcao_ind_torrado_cst51


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
        codigos_pendentes = [
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

        # 1) AUTO-FIX (agressivo) — IND_TORRADO
        if "IND_TORRADO_V1" in codigos_pendentes:
            print("[RESOLVER_TODOS] AUTO-FIX IND_TORRADO_V1: INICIO")

            res_fix = aplicar_correcao_ind_torrado_cst51(
                db,
                versao_origem_id=versao_id,
                incluir_1102=True,  # agressivo
                csts_origem=["70", "73", "75", "98", "99", "06", "07", "08"],  # agressivo
                apontamento_id=None,  # resolve-todos (batch)
            )
            db.flush()
            print("[RESOLVER_TODOS] flush após auto-fix IND_TORRADO")

            print("[RESOLVER_TODOS] AUTO-FIX IND_TORRADO_V1: FIM res=", res_fix)

            # Se deu erro, aborta a transação (melhor falhar do que "resolver" sem aplicar)
            if str(res_fix.get("status")) == "erro":
                raise ValueError(f"AUTO-FIX IND_TORRADO falhou: {res_fix.get('msg')}")

        # 2) Agora sim: marca tudo como resolvido (comportamento atual)
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

        print("[RESOLVER_TODOS] updated_total=", int(updated or 0), "pendentes_restantes=", int(pendentes_restantes or 0))

        return ResolverTodosResult(
            versao_id=versao_id,
            updated_total=int(updated or 0),
            pendentes_restantes=int(pendentes_restantes or 0),
        )
