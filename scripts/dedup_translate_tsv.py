#!/usr/bin/env python3
"""
dedup_translate_tsv.py
──────────────────────
Скрипт має **дві дії**:

1.  extract  –  робить *де-дуп* вихідного TSV
2.  apply    –  повертає переклади з «_dedup»-файла назад у вихідний

────────────
Використання
────────────

# 1) Створити файл для перекладу без дублів
python dedup_translate_tsv.py extract path/to/names.loc.tsv

# 2) Після редагування колонки "translate" застосувати переклад
python dedup_translate_tsv.py apply   _dedup/names.loc._dedup.tsv \
                               path/to/names.loc.tsv

────────────
Формат _dedup-файла
────────────
| text | translate | keys |
|------|-----------|------|
| Alda |           | names_name_2147380140 |
| Cairo|           | att_reg_aegyptus_oxyrhynchus, … |

Колонку **translate** редагує перекладач.
Колонка **keys** потрібна скрипту ― не змінювати.
"""

from pathlib import Path
import sys, csv, pandas as pd

DEDUP_DIR = Path("_temp")          # каталог, куди кладемо _dedup-файли
DEDUP_DIR.mkdir(exist_ok=True)

def extract(src: Path) -> None:
    df = pd.read_csv(src, sep="\t", dtype=str,
                     keep_default_na=False, na_filter=False)

    # групуємо за text
    g = df.groupby("text")["key"].agg(lambda k: ",".join(sorted(k)))
    dedup_df = (
        g.reset_index()
        .rename(columns={"key": "keys"})
        .assign(translate="")
        .loc[:, ["text", "translate", "keys"]]
    )

    out = DEDUP_DIR / f"{src.stem}._dedup.tsv"
    dedup_df.to_csv(out, sep="\t", index=False, quoting=csv.QUOTE_NONE)
    try:
        shown = out.relative_to(Path.cwd())
    except ValueError:
        shown = out
    print(f"✅  Створено {shown}")

def apply(dedup_file: Path, tsv_orig: Path) -> None:
    """Переносить переклад із колонки translate у текст оригінального TSV,
       шукаючи рядки за key-ами з колонки keys."""
    dedup = pd.read_csv(dedup_file, sep="\t", dtype=str,
                        keep_default_na=False, na_filter=False)
    orig  = pd.read_csv(tsv_orig,  sep="\t", dtype=str,
                        keep_default_na=False, na_filter=False)

    # будуємо словник key → translate
    key2tr: dict[str, str] = {}
    for row in dedup.itertuples(index=False):
        if not row.translate:                 # переклад порожній — пропускаємо
            continue
        for k in map(str.strip, row.keys.split(",")):
            if k:                             # пропустити порожні елементи
                key2tr[k] = row.translate

    if not key2tr:
        print("–  У dedup-файлі немає заповненої колонки translate.")
        return

    # застосовуємо (лише для тих key, що існують у словнику)
    mask = orig["key"].isin(key2tr.keys())
    orig.loc[mask, "text"] = orig.loc[mask, "key"].map(key2tr)

    orig.to_csv(tsv_orig, sep="\t", index=False, quoting=csv.QUOTE_NONE)
    print(f"✅  Оновлено {tsv_orig.name}: перекладено {mask.sum()} рядків.")

if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("Використання:\n"
              "  extract <src.tsv>\n"
              "  apply   <_dedup.tsv> <src.tsv>")
        sys.exit(1)

    action = sys.argv[1]
    if action == "extract" and len(sys.argv) == 3:
        extract(Path(sys.argv[2]))
    elif action == "apply" and len(sys.argv) == 4:
        apply(Path(sys.argv[2]), Path(sys.argv[3]))
    else:
        print("Неправильні аргументи.")
        sys.exit(1)
