from dataclasses import dataclass
from sqlalchemy.orm import Session
from app.db.models import EfdApontamento  # ajuste o import

@dataclass(frozen=True)
class ResolverTodosResult:
    versao_id: int
    updated_total: int
    pendentes_restantes: int


class ApontamentoService:
    @staticmethod
    def resolver_todos_pendentes_por_versao(db: Session, *, versao_id: int) -> ResolverTodosResult:
        # Tudo em 1 transação controlada pelo caller (router com db.begin())
        updated = (
            db.query(EfdApontamento)
            .filter(EfdApontamento.versao_id == int(versao_id))
            .filter(EfdApontamento.resolvido.is_(False))
            .update({EfdApontamento.resolvido: True}, synchronize_session=False)
        )

        pendentes_restantes = (
            db.query(EfdApontamento)
            .filter(EfdApontamento.versao_id == int(versao_id))
            .filter(EfdApontamento.resolvido.is_(False))
            .count()
        )

        return ResolverTodosResult(
            versao_id=int(versao_id),
            updated_total=int(updated or 0),
            pendentes_restantes=int(pendentes_restantes or 0),
        )
