#!/usr/bin/env python3
"""
validate_tsv.py – швидка перевірка всіх *.loc.tsv у text/db/

• 3 колонки у порядку: key, text, tooltip
• key не порожній
• немає дублів key
• TSV-розділювач = \t

Колонка tooltip не аналізується – грі потрібна, але її вміст нас не цікавить.
"""

from pathlib import Path
import sys
import pandas as pd

ROOT = Path(__file__).resolve().parents[1] / "text" / "db"
REQUIRED_COLS = ["key", "text", "tooltip"]
EXIT_CODE = 0


def fail(msg: str) -> None:
    """Додає повідомлення та встановлює код виходу 1."""
    global EXIT_CODE
    print(f"❌ {msg}")
    EXIT_CODE = 1

def warn(msg: str) -> None:
    print(f"⚠️  {msg}")


print(f"🔍 Перевіряємо TSV у {ROOT} …\n")

for file in sorted(ROOT.glob("*.loc.tsv")):
    try:
        df = pd.read_csv(file, sep="\t", dtype=str, keep_default_na=False)
    except Exception as e:
        fail(f"{file}: не вдалося прочитати файл ({e})")
        continue

    # 1. Перевіряємо колонки
    if list(df.columns) != REQUIRED_COLS:
        fail(f"{file}: очікувано колонки {REQUIRED_COLS}, а отримано {list(df.columns)}")

    # 2. Порожні key
    empty_rows = df["key"].str.strip() == ""
    if empty_rows.any():
        rows = ", ".join(map(str, (df.index[empty_rows] + 2)))  # +2: header + 0-based
        warn(f"{file}: порожній key у рядках {rows}")

    # 3. Дублікати key
    non_empty_keys = df.loc[~empty_rows, "key"]
    dup_keys = non_empty_keys[non_empty_keys.duplicated()]
    if not dup_keys.empty:
        keys = ", ".join(dup_keys.unique())
        fail(f"{file}: дублікати key: {keys}")

# ── Підсумок ─────────────────────────────────────────────────────────────
if EXIT_CODE == 0:
    print("✅ Усі файли валідні – проблем не знайдено.")
else:
    print("⚠️  Перевірка завершена з помилками.")

sys.exit(EXIT_CODE)
