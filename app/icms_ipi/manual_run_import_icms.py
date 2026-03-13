from pathlib import Path
from sqlalchemy.orm import Session

from app.db.session import SessionLocal
from app.icms_ipi.icms_ipi_funcoes import normalizar_itens_preview_icms_ipi
from app.icms_ipi.service_icms_import import (
    gerar_preview_sped_icms,
    importar_sped_icms,
)

PASTA_SPEDS = Path(r"C:\Sped\ICMS_IPI")
EMPRESA_ID = 3


def main():
    db: Session = SessionLocal()

    try:
        if not PASTA_SPEDS.exists():
            print(f"❌ Pasta não encontrada: {PASTA_SPEDS}")
            return

        if not PASTA_SPEDS.is_dir():
            print(f"❌ Caminho não é pasta: {PASTA_SPEDS}")
            return

        arquivos = sorted(PASTA_SPEDS.glob("*.txt"))
        if not arquivos:
            print(f"⚠️ Nenhum arquivo .txt encontrado em: {PASTA_SPEDS}")
            return

        print("\n=== PREVIEW LOTE SPED ICMS/IPI ===\n")
        print(f"Pasta: {PASTA_SPEDS}")
        print(f"Arquivos encontrados: {len(arquivos)}\n")

        previews = []
        total_notas = 0
        total_vl_doc = 0
        total_vl_icms = 0

        for arquivo in arquivos:
            try:
                preview = gerar_preview_sped_icms(
                    db=db,
                    arquivo_path=str(arquivo),
                    empresa_id=EMPRESA_ID,
                )

                linhas = normalizar_itens_preview_icms_ipi(preview)
                preview["linhas_normalizadas"] = linhas

                previews.append(preview)

                total_notas += int(preview["total_notas"] or 0)
                total_vl_doc += float(preview["total_vl_doc"] or 0)
                total_vl_icms += float(preview["total_vl_icms"] or 0)

                print(f"Arquivo: {preview['arquivo']}")
                print(f"Periodo: {preview['periodo']}")
                print(f"Total notas: {preview['total_notas']}")
                print(f"Total VL_DOC: {preview['total_vl_doc']}")
                print(f"Total ICMS: {preview['total_vl_icms']}")
                print(f"Total linhas normalizadas: {len(linhas)}")

                if linhas:
                    print("Amostra normalizada:")
                    for row in linhas[:3]:
                        print(
                            "empresa=", row["empresa"],
                            "| participante=", row["participante"],
                            "| data=", row["data"],
                            "| chave=", row["chave"],
                            "| numero=", row["numero"],
                            "| cod_item=", row["cod_item"],
                            "| descricao=", row["descricao"],
                            "| ncm=", row["ncm"],
                            "| cfop=", row["cfop"],
                            "| valor_item=", row["valor_item"],
                            "| valor_icms=", row["valor_icms"],
                            "| origem_item=", row["origem_item"],
                        )

                print("-" * 60)

            except Exception as e:
                print(f"❌ Erro no preview do arquivo {arquivo.name}: {e}")
                print("-" * 60)

        if not previews:
            print("❌ Nenhum arquivo válido para importação.")
            return

        print("\n=== CONSOLIDADO DO LOTE ===\n")
        print(f"Arquivos válidos: {len(previews)}")
        print(f"Total notas: {total_notas}")
        print(f"Total VL_DOC: {total_vl_doc}")
        print(f"Total ICMS: {total_vl_icms}")
        print()

        print("=== AMOSTRA DAS PRIMEIRAS NOTAS ===")
        for preview in previews[:3]:
            print(f"\nArquivo: {preview['arquivo']}")
            for item in preview["notas_preview"][:5]:
                print(
                    "num_doc=", item["num_doc"],
                    "| chave=", item["chave_nfe"],
                    "| dt_doc=", item["dt_doc"],
                    "| vl_doc=", item["vl_doc"],
                    "| vl_icms=", item["vl_icms"],
                )
        print()

        confirmar = input("Deseja importar todos os arquivos válidos? (s/n): ").strip().lower()
        if confirmar != "s":
            print("Importação cancelada.")
            return

        print("\n=== IMPORTANDO LOTE ===\n")

        total_inseridas = 0
        total_atualizadas = 0
        total_ignoradas = 0
        total_lidas = 0
        total_itens_lidos = 0
        total_itens_inseridos = 0
        total_itens_removidos = 0

        for preview in previews:
            arquivo_path = str(PASTA_SPEDS / preview["arquivo"])
            try:
                res = importar_sped_icms(
                    db=db,
                    arquivo_path=arquivo_path,
                    empresa_id=EMPRESA_ID,
                    sobrescrever_existentes=True,
                )

                total_lidas += int(res.get("total_lido_notas") or 0)
                total_itens_lidos += int(res.get("total_lido_itens") or 0)
                total_inseridas += int(res.get("inseridas") or 0)
                total_atualizadas += int(res.get("atualizadas") or 0)
                total_ignoradas += int(res.get("ignoradas") or 0)
                total_itens_inseridos += int(res.get("itens_inseridos") or 0)
                total_itens_removidos += int(res.get("itens_removidos") or 0)

                print(f"✅ {preview['arquivo']}")
                print(res)
                print("-" * 60)

            except Exception as e:
                print(f"❌ Erro ao importar {preview['arquivo']}: {e}")
                print("-" * 60)

        print("\n=== RESULTADO FINAL DO LOTE ===\n")
        print(f"Total lido (notas): {total_lidas}")
        print(f"Total lido (itens): {total_itens_lidos}")
        print(f"Total inseridas: {total_inseridas}")
        print(f"Total atualizadas: {total_atualizadas}")
        print(f"Total ignoradas: {total_ignoradas}")
        print(f"Total itens inseridos: {total_itens_inseridos}")
        print(f"Total itens removidos: {total_itens_removidos}")
    finally:
        db.close()


if __name__ == "__main__":
    main()