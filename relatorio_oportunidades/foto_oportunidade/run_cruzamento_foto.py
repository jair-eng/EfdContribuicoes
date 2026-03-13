from pathlib import Path

from app.icms_ipi.icms_ipi_funcoes import normalizar_itens_preview_icms_ipi
from app.icms_ipi.parser_sped_icms import parse_sped_icms_ipi_preview
from relatorio_oportunidades.foto_oportunidade.cruzar_icms_contribuicoes import cruzar_icms_ipi_com_efd_contrib, resumir_cruzamento
from relatorio_oportunidades.foto_oportunidade.exportar_cruzamento_xlsx import exportar_cruzamento_xlsx
from relatorio_oportunidades.foto_oportunidade.normalizar_efd_contribuicoes import normalizar_efd_contribuicoes_para_cruzamento

PASTA_ICMS = Path(r"C:\Sped\ICMS_IPI")
PASTA_CONTRIB = Path(r"C:\Sped\CONTRIB")

linhas_icms = []
linhas_contrib = []

print("\n=== LENDO ICMS/IPI ===")
for arq in sorted(PASTA_ICMS.glob("*.txt")):
    print("Arquivo ICMS:", arq.name)
    parsed = parse_sped_icms_ipi_preview(arq)
    linhas = normalizar_itens_preview_icms_ipi(parsed)
    linhas_icms.extend(linhas)

print("Total linhas ICMS/IPI:", len(linhas_icms))

print("\n=== LENDO EFD CONTRIBUIÇÕES ===")
for arq in sorted(PASTA_CONTRIB.glob("*.txt")):
    print("Arquivo Contrib:", arq.name)
    linhas = normalizar_efd_contribuicoes_para_cruzamento(arq)
    linhas_contrib.extend(linhas)

print("Total linhas Contribuições:", len(linhas_contrib))

print("\n=== CRUZANDO DADOS ===")
linhas_cruzadas = cruzar_icms_ipi_com_efd_contrib(linhas_icms, linhas_contrib)
resumo = resumir_cruzamento(linhas_cruzadas)

saida_xlsx = Path(r"C:\Sped\saida\foto_recuperacao_cruzada.xlsx")
saida_xlsx.parent.mkdir(parents=True, exist_ok=True)

exportar_cruzamento_xlsx(
    saida_xlsx,
    linhas_cruzadas=linhas_cruzadas,
    resumo=resumo,
)

print(f"\nXLSX gerado em: {saida_xlsx}")

print("Total cruzadas:", len(linhas_cruzadas))
print("Total match:", resumo["total_match"])
print("Total sem match:", resumo["total_sem_match"])
print("Total elegíveis:", resumo["total_elegiveis"])
print("Base simulada:", resumo["total_base_simulada"])
print("PIS simulado:", resumo["total_pis_simulado"])
print("COFINS simulado:", resumo["total_cofins_simulado"])
print("Crédito simulado:", resumo["total_credito_simulado"])

print("\n=== AMOSTRA ===")
for row in linhas_cruzadas[:10]:
    print(
        "chave=", row["chave"],
        "| numero=", row["numero"],
        "| cod_item=", row["cod_item"],
        "| cfop=", row["cfop"],
        "| cst_pis=", row["cst_pis"],
        "| cst_simulado=", row["cst_simulado"],
        "| base_simulada=", row["base_simulada"],
        "| credito_simulado=", row["credito_simulado"],
        "| tipo_match=", row["tipo_match"],
        "| regra=", row["regra_simulada"],
    )