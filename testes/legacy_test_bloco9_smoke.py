from collections import Counter
from pathlib import Path


BLOCO9_REGS = {"9001", "9900", "9990", "9999"}


def _split_reg(linha: str) -> str:
    if not linha.startswith("|"):
        return ""
    parts = linha.split("|")
    return parts[1] if len(parts) > 1 else ""


def read_lines(path: str | Path) -> list[str]:
    with open(path, encoding="latin-1", errors="ignore") as f:
        return [l.rstrip("\r\n") for l in f if l.strip()]


def test_bloco9_smoke(sped_path: str | Path):
    lines = read_lines(sped_path)

    # -------------------------
    # 1) contagem real de linhas
    # -------------------------
    total_linhas_real = len(lines)

    # -------------------------
    # 2) extrai bloco 9
    # -------------------------
    bloco9 = [l for l in lines if _split_reg(l) in BLOCO9_REGS]

    assert bloco9, "Bloco 9 não encontrado"

    regs_b9 = [_split_reg(l) for l in bloco9]
    counter_b9 = Counter(regs_b9)

    # -------------------------
    # 3) valida 9001
    # -------------------------
    assert counter_b9["9001"] == 1, f"Esperado 1x 9001, encontrado {counter_b9['9001']}"

    # -------------------------
    # 4) valida 9990
    # -------------------------
    linha_9990 = next(l for l in bloco9 if l.startswith("|9990|"))
    qtd_9990 = int(linha_9990.split("|")[2])
    assert qtd_9990 == len(bloco9), (
        f"9990 inconsistente: declarado={qtd_9990}, real={len(bloco9)}"
    )

    # -------------------------
    # 5) valida 9999
    # -------------------------
    linha_9999 = next(l for l in bloco9 if l.startswith("|9999|"))
    qtd_9999 = int(linha_9999.split("|")[2])
    assert qtd_9999 == total_linhas_real, (
        f"9999 inconsistente: declarado={qtd_9999}, real={total_linhas_real}"
    )

    # -------------------------
    # 6) valida 9900 (contagem por registro)
    # -------------------------
    # base real sem bloco 9
    base = [l for l in lines if _split_reg(l) not in BLOCO9_REGS]
    base_regs = [_split_reg(l) for l in base]

    real_counter = Counter(base_regs)

    linhas_9900 = [l for l in bloco9 if l.startswith("|9900|")]
    for l in linhas_9900:
        parts = l.split("|")
        if len(parts) < 4:
            continue
        reg = parts[2]
        qtd = int(parts[3])

        if reg in BLOCO9_REGS:
            # registros do próprio bloco 9 → não comparamos com base
            continue

        real = real_counter.get(reg, 0)
        assert qtd == real, (
            f"9900 inconsistente para {reg}: declarado={qtd}, real={real}"
        )
if __name__ == "__main__":
    test_bloco9_smoke(r"C:\Users\jcbn1\Downloads\sped_corrigido_v2_versao_59.txt")
    print("✅ Bloco 9 OK")