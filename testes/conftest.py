from pathlib import Path
import pytest



@pytest.fixture
def sped_path(tmp_path: Path, sped_original_text: str) -> Path:
    """
    Cria um arquivo SPED temporário para testes de smoke (bloco9).
    """
    p = tmp_path / "sped_smoke.txt"
    p.write_text(sped_original_text, encoding="utf-8", newline="\n")
    return p

@pytest.fixture
def sped_original_lines():
    return [
        "|0000|...|",
        "|M001|0|",
        "|M100|01|0,00|0,00|0,00|0,00|0,00|0,00|0,00|0,00|0,00|",
        "|M200|0,00|0,00|0,00|0,00|",
        "|9999|5|",
    ]


@pytest.fixture
def sped_revisado_lines():
    # versão "corrigida" esperada (ajuste os valores conforme o teste)
    return [
        "|0000|...|",
        "|M001|0|",
        "|M100|01|0,00|0,00|0,00|0,00|3500,00|0,00|0,00|0,00|3500,00|",
        "|M200|3500,00|0,00|0,00|0,00|",
        "|9999|5|",
    ]


@pytest.fixture
def sped_original_text(sped_original_lines):
    return "\n".join(sped_original_lines) + "\n"


@pytest.fixture
def sped_revisado_text(sped_revisado_lines):
    return "\n".join(sped_revisado_lines) + "\n"
