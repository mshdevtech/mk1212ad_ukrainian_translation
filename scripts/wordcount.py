#!/usr/bin/env python3
"""
wordcount.py
────────────
Підраховує кількість слів у текстовому файлі (.txt).

Використання
============
    python wordcount.py path/to/file.txt
    python wordcount.py file1.txt file2.txt …

• Рахує «слово» як послідовність символів, відокремлену пробілами,
  табами або переходами рядка.
• Працює з UTF-8 (і більшістю інших кодувань, якщо Python зможе їх
  визначити).

Вивід
=====
    file.txt  —  12 345 слів
    …
    ───────────────
    Разом: 87 654
"""

import sys, re
from pathlib import Path

WORD_RE = re.compile(r"\w+", re.UNICODE)

def count_words(path: Path) -> int:
    try:
        text = path.read_text(encoding="utf-8")
    except UnicodeDecodeError:
        text = path.read_text(encoding="utf-8", errors="ignore")
    return len(WORD_RE.findall(text))

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Передайте хоча б один .txt файл.")
        sys.exit(1)

    total = 0
    for fp in sys.argv[1:]:
        p = Path(fp)
        if not p.exists():
            print(f"⚠️  {p} не знайдено — пропуск.")
            continue
        n = count_words(p)
        total += n
        print(f"{p.name} — {n:,} слів".replace(",", " "))   # пробіл нерозривний

    if len(sys.argv) > 2:
        print("───────────────")
        print(f"Разом: {total:,}".replace(",", " "))
