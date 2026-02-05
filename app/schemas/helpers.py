from app.db.models import EfdRegistro


def extrair_credito_total(ap) -> str:
    """
    Extrai o crédito total (string pt-BR) a partir de um apontamento.
    """
    val = getattr(ap, "impacto_financeiro", None)
    if val is None:
        return "0,00"
    return str(val)



def carregar_linhas_sped(db, versao) -> list[str]:
    regs = (
        db.query(EfdRegistro)
        .filter(EfdRegistro.versao_id == int(versao.id))
        .order_by(EfdRegistro.linha.asc())
        .all()
    )

    # reconstroi linha inteira: |REG|dados...|
    linhas = []
    for r in regs:
        dados = (r.conteudo_json or {}).get("dados") or []
        # garante string e mantém pipes
        linha = "|" + str(r.reg).strip() + "|" + "|".join(map(str, dados)) + "|"
        linhas.append(linha)

    return linhas
