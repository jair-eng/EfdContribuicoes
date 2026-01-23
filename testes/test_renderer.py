from app.sped.renderer import render_sped_line


def test_render_sped_line_simples():
    linha = render_sped_line(
        "M200",
        ["3500,00", "0,00", "0,00", "0,00"]
    )

    assert linha == "|M200|3500,00|0,00|0,00|0,00|"


def test_render_sped_line_com_none():
    linha = render_sped_line(
        "M100",
        ["01", None, "0,00"]
    )

    assert linha == "|M100|01||0,00|"
