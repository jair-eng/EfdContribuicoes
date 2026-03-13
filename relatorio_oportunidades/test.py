from app.db.session import SessionLocal
from app.fiscal.regras.Autocorrigivel.cafe import aplicar_correcao_ind_cafe_icms_match_cst51


def test_():
    db = SessionLocal()
    try:
        res = aplicar_correcao_ind_cafe_icms_match_cst51(
            db,
            versao_origem_id=57,
            empresa_id=1,
            periodo=None,
            incluir_revenda=True,

        )

        print(res)

        assert "status" in res
        assert res["status"] in {"ok", "vazio", "skip"}
    finally:
        db.close()