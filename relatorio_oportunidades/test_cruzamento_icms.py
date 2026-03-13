from app.db.session import SessionLocal
from app.icms_ipi.icms_ipi_cruzamento_service import cruzar_versao_com_icms_ipi


def test_cruzamento_icms():
    db = SessionLocal()

    res = cruzar_versao_com_icms_ipi(
        db=db,
        versao_origem_id=57,   # ajuste aqui
        empresa_id=1
    )

    print("DOCS:", res["total_docs"])
    print("DOCS VINCULADOS:", res["total_docs_vinculados"])
    print("ITENS:", res["total_itens_c170"])
    print("MATCH:", res["total_match_item"])
    print("SEM MATCH:", res["total_sem_match_item"])

    assert res["ok"] is True