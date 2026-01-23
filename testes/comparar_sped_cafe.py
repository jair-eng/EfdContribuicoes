from pathlib import Path
import difflib
from collections import Counter

BASE = Path(r"C:\Users\jcbn1\Downloads\cafe")

orig_path = BASE / "original.txt"
corr_path = BASE / "corrigido.txt"

def read_lines(p: Path):
    with p.open("r", encoding="latin-1", errors="replace") as f:
        return [line.rstrip("\r\n") for line in f]

orig = read_lines(orig_path)
corr = read_lines(corr_path)

print("=== RESUMO ===")
print(f"Linhas original : {len(orig)}")
print(f"Linhas corrigido: {len(corr)}")
print()

# --- diff simples ---
diff = list(difflib.unified_diff(
    orig, corr,
    fromfile="original",
    tofile="corrigido",
    lineterm=""
))

if not diff:
    print("✅ Arquivos idênticos (linha a linha).")
else:
    print(f"❌ Diferenças encontradas: {len(diff)} linhas de diff")
    print("\n--- PRIMEIRAS 40 LINHAS DE DIFF ---")
    for line in diff[:40]:
        print(line)

print("\n=== REGISTROS ALTERADOS (TOP) ===")

def reg_of(line: str) -> str:
    if line.startswith("|"):
        parts = line.split("|")
        if len(parts) > 2:
            return parts[1]
    return "?"

regs_orig = Counter(reg_of(l) for l in orig if l.startswith("|"))
regs_corr = Counter(reg_of(l) for l in corr if l.startswith("|"))

todos = sorted(set(regs_orig) | set(regs_corr))
mudaram = []

for r in todos:
    if regs_orig[r] != regs_corr[r]:
        mudaram.append((r, regs_orig[r], regs_corr[r]))

if not mudaram:
    print("Nenhuma mudança na contagem de registros.")
else:
    for r, o, c in mudaram[:20]:
        print(f"{r}: original={o} corrigido={c}")
