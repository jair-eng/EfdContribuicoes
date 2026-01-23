from decimal import Decimal

# ajuste os imports conforme seu projeto
from app.config.settings import ALIQUOTA_PIS, ALIQUOTA_COFINS, ALIQUOTA_TOTAL
from app.fiscal.regras.base_regras import RegraBase
from app.fiscal.regras.regra_exportacao import RegraExportacaoRessarcimentoV1
from app.fiscal.dto import RegistroFiscalDTO


def main():
    # 1) valida settings
    assert (ALIQUOTA_PIS + ALIQUOTA_COFINS) == ALIQUOTA_TOTAL
    print("OK settings aliquotas:", RegraBase.pct(ALIQUOTA_TOTAL))

    # 2) valida helpers
    assert RegraBase.br_num(Decimal("1234.56")) == "1234,56"
    assert RegraBase.pct(Decimal("0.0925")) == "9,25%"
    assert RegraBase.q2(Decimal("1.239")) == Decimal("1.24")
    print("OK helpers q2/br_num/pct")

    # 3) valida regra EXP_RESSARC (caso básico)
    regra = RegraExportacaoRessarcimentoV1()

    registro = RegistroFiscalDTO(
        linha=1,
        id=1,
        reg="C190_EXP_AGG",
        dados=[
            {"_meta": {
                "tem_1200": False,
                "tem_1210": False,
                "tem_1700": False,
                "perfil_monofasico": False,
                "score_monofasico": "10",
                "tem_apuracao_m": False,
                "bloco_m_zerado": False,
                "fonte": "C190",
            }},
            {"cfop": "7101", "vl_opr": "1000,00"},
            {"cfop": "7101", "vl_opr": "2000,00"},
        ],
    )

    achado = regra.aplicar(registro)
    assert achado is not None
    print("OK achado gerado:", achado.codigo, achado.prioridade)

    # checagens pt-BR
    assert achado.meta["aliquota_total"] == "9,25%"
    assert "," in achado.meta["base_exportacao"]
    assert "," in achado.meta["impacto_consolidado"]
    assert achado.meta["metodo"].find("9,25%") != -1
    print("OK meta pt-BR e metodo dinâmico")

    print("\n✅ SMOKE TEST PASSOU")


if __name__ == "__main__":
    main()
