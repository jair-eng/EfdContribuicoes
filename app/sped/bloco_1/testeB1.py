from decimal import Decimal

from app.sped.bloco_1.builder import montar_bloco_1
from app.sped.bloco_1.reg1100 import linha_1100


def main():
    l1100 = linha_1100(
        periodo="072022",
        cod_cont="201",
        valor=Decimal("66355.58"),
    )

    bloco1 = montar_bloco_1([l1100])

    print("=== BLOCO 1 GERADO ===")
    for l in bloco1:
        print(l)

if __name__ == "__main__":
    main()
