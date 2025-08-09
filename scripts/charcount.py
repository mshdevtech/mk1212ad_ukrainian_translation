#!/usr/bin/env python3
"""
charcount.py
────────────
Підраховує кількість символів у текстовому файлі (.txt).

Використання
============
    # усі символи, як є
    python charcount.py file.txt

    # кілька файлів
    python charcount.py file1.txt file2.txt …

    # виключити всі «пробільні» символи (пробіл, таб, \n, \r)
    python charcount.py --no-spaces file.txt

Вивід
=====
    file.txt  —  42 187 символів
    …
    ───────────────
    Разом: 123 456
"""

import sys, argparse
from pathlib import Path
import unicodedata as ud

ap = argparse.ArgumentParser()
ap.add_argument("--no-spaces", action="store_true",
                help="не рахувати пробіли, табуляції та перенесення рядків")
ap.add_argument("files", nargs="+", help=".txt файли для підрахунку")
args = ap.parse_args()

def count_chars(path: Path, skip_spaces: bool) -> int:
    try:
        text = path.read_text(encoding="utf-8")
    except UnicodeDecodeError:
        text = path.read_text(encoding="utf-8", errors="ignore")

    if skip_spaces:
        # відкидаємо символи з Unicode-категорією Zs (Space Separator) та усі '\t\n\r'
        text = "".join(ch for ch in text if not (
                ud.category(ch) == "Zs" or ch in "\t\n\r"
        ))
    return len(text)

total = 0
for fp in args.files:
    p = Path(fp)
    if not p.exists():
        print(f"⚠️  {p} не знайдено — пропуск.")
        continue
    n = count_chars(p, args.no_spaces)
    total += n
    print(f"{p.name} — {n:,} символів".replace(",", " "))  # тонкий нерозр. пробіл

if len(args.files) > 1:
    print("───────────────")
    print(f"Разом: {total:,}".replace(",", " "))
