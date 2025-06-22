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

SRC_DIR = Path("_upstream/text/db")
TRG_DIR = Path("text/db")

def load(p: Path) -> pd.DataFrame:
    return pd.read_csv(p, sep="\t", dtype=str, keep_default_na=False)

rows = []
for src_path in sorted(SRC_DIR.glob("*.loc.tsv")):
    trg_path = TRG_DIR / src_path.name

    # якщо перекладу ще немає - пишемо 0 %
    if not trg_path.exists():
        rows.append((src_path.name, 0, 0, 0))
        continue

    src = load(src_path)
    trg = load(trg_path)

    # пропускаємо порожні key
    mask = src["key"].str.strip() != ""
    src, trg = src[mask], trg[mask]

    # пропускаємо ПЕРШІ ДВА службові рядки (index 0 і 1)
    src, trg = src.iloc[2:], trg.iloc[2:]

    # об’єднуємо по key
    df = src.merge(trg[["key", "text"]], on="key", how="left",
                   suffixes=("_en", "_ua"))

    total = len(df)
    translated = (df["text_ua"] != df["text_en"]).sum()
    untranslated = total - translated
    rows.append((src_path.name, total, translated, untranslated))

# вивід
print(f"{'File':45}  Total  Done  Todo  %")
for name, total, done, todo in rows:
    pct = 0 if total == 0 else round(done / total * 100)
    bar = "█" * (pct // 10)  # грубий прогрес-бар
    print(f"{shorten(name, 43):45}  {total:5}  {done:4}  {todo:4}  {pct:3}% {bar}")
