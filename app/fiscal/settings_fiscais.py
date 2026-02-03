from __future__ import annotations


# CSTs que obrigam M400/M800 (receita CST 04/06/07/08/09)
CSTS_RECEITA_M = {"04", "06", "07", "08", "09"}

# Receita tributada não cumulativa (RECEITA, não crédito!)
CSTS_RECEITA_NCUM = {"01", "02", "03"}

# CSTs que o 0900 considera "excluídas" de receita bruta (alíquota zero/isenta/suspensa)
CSTS_EXCL_AZ_ISENT_SUSP = {"06", "07", "08", "09"}

# CSTs tributados não-cumulativos (crédito)
CSTS_TRIB_NCUM = {"50", "51", "52", "53", "54", "55", "56"}