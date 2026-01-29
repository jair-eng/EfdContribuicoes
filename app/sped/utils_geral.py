

# --- ETAPA 2: Reconstrução da Ordem do Arquivo Writer (Com Pesos Explícitos) ---
def get_peso(registro: str) -> int:
    if not registro:
        return 999

    bloco = registro[0].upper()

    ordem = {
        '0': 0,
        'A': 5,
        'C': 10,
        'D': 20,
        'E': 30,
        'F': 40,
        'G': 50,
        'H': 60,
        'I': 70,
        '1': 75,
        'M': 80,
        'P': 90,
        '9': 100,
    }

    return ordem.get(bloco, 95)
