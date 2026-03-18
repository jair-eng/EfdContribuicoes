from app.fiscal.dto import RegistroFiscalDTO


def aplicar_contexto(dto, versao_id, empresa_id):
    if isinstance(dto, RegistroFiscalDTO):
        dto.versao_id = versao_id
        dto.empresa_id = empresa_id