#!/usr/bin/env python3
"""
translation_report.py
• порівнює EN (_upstream/text/db) і UA (text/db)
• рахує для кожного файлу: total, translated, untranslated
• виводить компактну таблицю
"""

from pathlib import Path
import pandas as pd
from textwrap import shorten

EXCLUSIONS = ["PLACEHOLDER"]

def exclude_placeholders(df: pd.DataFrame) -> pd.DataFrame:
    return df[~df["text"].isin(EXCLUSIONS)]

SRC_DIR = Path("_upstream/en/text/db")
TRG_DIR = Path("text/db")

def load(p: Path) -> pd.DataFrame:
    return pd.read_csv(p, sep="\t", dtype=str, keep_default_na=False)

rows = []
grand_total, grand_done = 0, 0

for src_path in sorted(SRC_DIR.glob("*.loc.tsv")):
    trg_path = TRG_DIR / src_path.name

    # якщо перекладу ще немає - пишемо 0 %
    if not trg_path.exists():
        rows.append((src_path.name, 0, 0, 0))
        continue

    src = load(src_path)
    trg = load(trg_path)

    # пропускаємо порожні key
    src = src[src["key"].str.strip() != ""]
    trg = trg[trg["key"].str.strip() != ""]

    # пропускаємо ПЕРШІ ДВА службові рядки (index 0 і 1)
    src, trg = src.iloc[2:], trg.iloc[2:]

    # exclude placeholders
    src = exclude_placeholders(src)
    trg = exclude_placeholders(trg)

    # об’єднуємо по key
    df = src.merge(trg[["key", "text"]], on="key", how="left",
                   suffixes=("_en", "_ua"))

    total = len(df)
    translated = (df["text_ua"] != df["text_en"]).sum()
    untranslated = total - translated
    rows.append((src_path.name, total, translated, untranslated))

    grand_total += total
    grand_done  += translated

# вивід
col_w = max(len(name) for name, *_ in rows) + 2

print(f"{'File'.ljust(col_w)}  Total  Done  Todo  %")
for name, total, done, todo in rows:
    pct = 0 if total == 0 else round(done / total * 100)
    bar = "█" * (pct // 10)
    print(f"{name.ljust(col_w)}  {total:5}  {done:4}  {todo:4}  {pct:3}% {bar}")

# загальний підсумок
if grand_total:
    overall_pct = round(grand_done / grand_total * 100, 2)
    print("\n=== SUMMARY ===")
    print(f"Перекладено {grand_done} рядків із {grand_total} "
          f"({overall_pct}% від загальної кількості).")
else:
    print("\nНемає даних для підрахунку.")