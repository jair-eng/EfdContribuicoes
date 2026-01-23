from app.sped.c170_utils import patch_c170_campos
from app.sped.formatter import formatar_linha


def test_patch_c170_muda_apenas_cfop_e_csts():
    # dados reais (copiados do JSON)
    dados = [
        "1","3","",
        "50,00000","KG","858,33","0,00","1","051",
        "5102",          # idx 9 CFOP
        "",
        "0,00","0,00","0,00","0,00","0,00","0,00",
        "","","","","","",
        "06",            # idx 23 CST PIS
        "0,00","0,0000",
        "","","0,00",
        "06",            # idx 29 CST COFINS
        "0,00","0,0000",
        "","","0,00",
        "4.1.10.100.3"
    ]

    novos = patch_c170_campos(
        dados,
        cfop="7101",
        cst_pis="50",
        cst_cofins="50",
    )

    # 🔒 só os índices esperados mudam
    assert novos[9] == "7101"
    assert novos[23] == "50"
    assert novos[29] == "50"

    # 🔒 todo o resto permanece igual
    for i, (antes, depois) in enumerate(zip(dados, novos)):
        if i not in (9, 23, 29):
            assert antes == depois

    # 🔒 render final da linha
    linha = formatar_linha("C170", novos)

    assert linha.startswith("|C170|")
    assert linha.endswith("|")
    assert "|7101|" in linha
