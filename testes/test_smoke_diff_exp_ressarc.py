from comparar_sped import comparar_sped


def test_exp_ressarc_so_muda_m100_m200(
    sped_original_lines,
    sped_revisado_lines,
):
    report = comparar_sped(
        sped_original_lines,
        sped_revisado_lines,
        allowed_regs_to_change={"M100", "M200"},
        ignore_bloco9=True,
    )

    assert not report.diffs_fora_permitido, (
        "Diferenças fora do permitido:\n" +
        "\n".join(
            f"L{d.linha} {d.tipo} {d.reg_orig}->{d.reg_rev}"
            for d in report.diffs_fora_permitido[:10]
        )
    )
