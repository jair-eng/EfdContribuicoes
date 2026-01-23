import os
from decimal import Decimal
import logging

DB_USER = os.getenv("DB_USER", "sped_user")
DB_PASSWORD = os.getenv("DB_PASSWORD", "Jcbn2025#")
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_NAME = os.getenv("DB_NAME", "efd_creditos")  # ✅ alinhado com o DDL

DATABASE_URL = f"mysql+pymysql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}/{DB_NAME}"
# Impacto financeiro estimado (MVP)
ALIQUOTA_PIS = Decimal("0.0165")
ALIQUOTA_COFINS = Decimal("0.0760")
ALIQUOTA_TOTAL = ALIQUOTA_PIS + ALIQUOTA_COFINS
IND_TORRADO_ALIQUOTA_EFETIVA = Decimal("0.0736")



logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)
