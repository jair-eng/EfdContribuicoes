from pathlib import Path
import difflib

FIXTURES_DIR = Path(__file__).parent / "fixtures"


def _load_lines_file(p: Path) -> list[str]:
    raw = p.read_bytes()

    # tenta UTF-8, depois fallback (SPED real pode vir “ANSI”)
    try:
        txt = raw.decode("utf-8")
    except UnicodeDecodeError:
        try:
            txt = raw.decode("utf-8-sig")
        except UnicodeDecodeError:
            txt = raw.decode("latin-1")  # nunca falha

    txt = txt.replace("\r\n", "\n").replace("\r", "\n")
    lines = [l for l in txt.split("\n") if l != ""]
    return lines


def _assert_utf8_file(p: Path) -> None:
    raw = p.read_bytes()
    raw.decode("utf-8")  # explode se não for UTF-8


def _reg_of(line: str) -> str:
    if not line.startswith("|"):
        return ""
    parts = line.split("|")
    return parts[1] if len(parts) > 1 else ""


def _changed_regs(orig: list[str], rev: list[str]) -> set[str]:
    changed: set[str] = set()
    sm = difflib.SequenceMatcher(a=orig, b=rev)
    for tag, i1, i2, j1, j2 in sm.get_opcodes():
        if tag == "equal":
            continue
        for i in range(i1, i2):
            changed.add(_reg_of(orig[i]))
        for j in range(j1, j2):
            changed.add(_reg_of(rev[j]))
    changed.discard("")
    return changed


def test_diff_apenas_m100_m200_e_bloco9():
    original = FIXTURES_DIR / "sped_original.txt"
    corrigido = FIXTURES_DIR / "sped_corrigido.txt"

    assert original.exists(), f"Não achei: {original}"
    assert corrigido.exists(), f"Não achei: {corrigido}"

    # (opcional) por enquanto, eu deixaria LIGADO: força corrigido UTF-8
   # _assert_utf8_file(corrigido)

    orig_lines = _load_lines_file(original)
    corr_lines = _load_lines_file(corrigido)

    # sanidade
    assert any(l.startswith("|0000|") for l in orig_lines), "Original sem 0000"
    assert any(l.startswith("|0000|") for l in corr_lines), "Corrigido sem 0000"

    # bloco 9 existe no corrigido
    assert any(l.startswith("|9001|") for l in corr_lines), "Corrigido sem 9001"
    assert any(l.startswith("|9999|") for l in corr_lines), "Corrigido sem 9999"

    changed = _changed_regs(orig_lines, corr_lines)

    allowed = {"M100", "M200", "9001", "9900", "9990", "9999"}
    extras = changed - allowed

    assert not extras, f"Alterou regs além do esperado: {sorted(extras)}"
