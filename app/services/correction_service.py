def corrigir_registro(registro, novos_dados, db):
    registro.conteudo_json["dados"] = novos_dados
    registro.alterado = True
    # NÃO commita aqui: o caller (versioning/workflow) decide
    db.add(registro)
