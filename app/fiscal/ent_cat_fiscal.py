from collections import defaultdict
from sqlalchemy import text
from app.fiscal.cat_fiscal import CatalogoFiscal
import time

def carregar_catalogo_fiscal(db, empresa_id: int | None = None) -> CatalogoFiscal:
    if empresa_id is None:
        sql = """
            SELECT g.slug, i.codigo
            FROM fiscal_grupo g
            JOIN fiscal_grupo_item i ON i.grupo_id = g.id
            WHERE g.ativo = 1
              AND i.ativo = 1
              AND i.empresa_id IS NULL
        """
        params = {}
    else:
        sql = """
            SELECT g.slug, i.codigo
            FROM fiscal_grupo g
            JOIN fiscal_grupo_item i ON i.grupo_id = g.id
            WHERE g.ativo = 1
              AND i.ativo = 1
              AND (i.empresa_id IS NULL OR i.empresa_id = :empresa_id)
        """
        params = {"empresa_id": int(empresa_id)}

    rows = db.execute(text(sql), params).fetchall()

    grupos = defaultdict(set)
    for slug, codigo in rows:
        slug = (str(slug).strip() if slug else "")
        codigo = (str(codigo).strip() if codigo else "")
        if slug and codigo:
            grupos[slug].add(codigo)

    return CatalogoFiscal(grupos=dict(grupos))
