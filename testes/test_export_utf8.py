from fastapi.testclient import TestClient
from app.main import app

client = TestClient(app)

def test_export_retorna_utf8():
    versao_revisada_id = 63  # depois a gente pega dinamicamente
    resp = client.get(f"/export/versao/{versao_revisada_id}")
    assert resp.status_code == 200
    resp.content.decode("utf-8")  # se falhar, explode
