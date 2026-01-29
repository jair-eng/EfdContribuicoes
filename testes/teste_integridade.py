from app.db.session import SessionLocal
from app.db.models import EfdRegistro, EfdRevisao
from app.sped.logic.consolidador import _get_dados, calcular_totais_filhos


def testar_vinculo_revisao(versao_id, registro_id_c170):
    db = SessionLocal()
    try:
        # 1. Verificar se o C170 existe e tem pai_id
        filho = db.query(EfdRegistro).filter(EfdRegistro.id == registro_id_c170).first()
        if not filho:
            print("❌ ERRO: Registro C170 não encontrado no banco.")
            return

        print(f"✅ C170 encontrado. Linha: {filho.linha} | Pai ID: {filho.pai_id}")

        # 2. Verificar se existe revisão para este C170
        rev = db.query(EfdRevisao).filter(
            EfdRevisao.registro_id == registro_id_c170,
            EfdRevisao.versao_origem_id == versao_id
        ).first()

        if not rev:
            print("❌ ERRO: Nenhuma revisão encontrada para este C170 na EfdRevisao.")
        else:
            print(f"✅ Revisão encontrada! Ação: {rev.acao} | Conteúdo: {rev.revisao_json.get('linha_nova')[:50]}...")

        # 3. Testar a soma do Pai (C100)
        if filho.pai_id:
            print(f"\n--- Testando Consolidação do Pai ({filho.pai_id}) ---")
            pis, cofins = calcular_totais_filhos(db, versao_id, filho.pai_id)
            print(f"📊 Soma calculada pela função: PIS={pis} | COFINS={cofins}")

            if pis == 0:
                print(
                    "⚠️ ALERTA: A soma deu ZERO. A função 'calcular_totais_filhos' pode estar ignorando a tabela de revisões.")

            # 4. Verificar se o C100 tem uma revisão automática gerada
            rev_pai = db.query(EfdRevisao).filter(
                EfdRevisao.registro_id == filho.pai_id,
                EfdRevisao.reg == "C100"
            ).first()

            if not rev_pai:
                print("❌ ERRO: O C100 não possui revisão gravada. Por isso o PVA vê 0,00.")
            else:
                print(f"✅ Revisão do C100 existe no banco: {rev_pai.revisao_json.get('linha_nova')}")

    finally:
        db.close()

# EXECUTE AQUI com os IDs que você vê na sua tela

# testar_vinculo_revisao(versao_id=69, registro_id_c170=46525)