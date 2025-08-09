#!/usr/bin/env python3
"""
unescape_quotes.py
──────────────────
• Прибирає екранування типу "" у колонці text:
    "Перекладена ""якесь слово"" строка" → Перекладена "якесь слово" строка
• Знімає зайві зовнішні лапки, якщо весь вміст поля був взятий у "…".
• Не змінює інші колонки, порядок рядків і сервісні рядки.

Використання:
    # 1) За замовчуванням пройти всі *.loc.tsv у DEFAULT_DIR
    python unescape_quotes.py

    # 2) Вказати файл(и) або директорію(ї)
    python unescape_quotes.py translation/text/db/names.loc.tsv
    python unescape_quotes.py translation/text/db  other_dir/
"""

from pathlib import Path
import sys
import pandas as pd

DEFAULT_DIR = Path("translation/text/db")  # змініть, якщо потрібно

# Повністю вимикаємо будь-яке «цитування» при читанні/записі
QUOTE_NONE = 3         # еквівалент csv.QUOTE_NONE (без імпорту csv)
QUOTECHAR  = "\x00"    # «неможливий» символ — щоб " сприймались як звичайні

def unescape_field(s: str):
    if not isinstance(s, str):
        return s
    # якщо поле повністю обгорнуте в лапки — знімаємо їх один раз
    if len(s) >= 2 and s[0] == '"' and s[-1] == '"':
        s = s[1:-1]
    # прибираємо подвоєні лапки всередині
    if '""' in s:
        # два проходи гарантують обробку послідовностей типу """" → ""
        s = s.replace('""', '"')
        while '""' in s:
            s = s.replace('""', '"')
    return s

def process_file(path: Path) -> int:
    df = pd.read_csv(
        path, sep="\t", dtype=str,
        keep_default_na=False, na_filter=False,
        engine="python", quoting=QUOTE_NONE, quotechar=QUOTECHAR
    )
    if "text" not in df.columns:
        print(f"{path.name}: колонку 'text' не знайдено — пропуск.")
        return 0

    before = df["text"].copy()
    df["text"] = df["text"].map(unescape_field)
    changed = (df["text"] != before).sum()

    if changed:
        df.to_csv(
            path, sep="\t", index=False, na_rep="",
            quoting=QUOTE_NONE, quotechar=QUOTECHAR,
        )
    print(f"{path.name}: оновлено {changed} рядків")
    return changed

def main():
    args = sys.argv[1:]
    files: list[Path] = []

    if args:
        for a in args:
            p = Path(a)
            if p.is_dir():
                files.extend(sorted(p.glob("*.loc.tsv")))
            else:
                files.append(p)
    else:
        files = sorted(DEFAULT_DIR.glob("*.loc.tsv"))

    if not files:
        print("Файлів не знайдено.")
        sys.exit(1)

    total = 0
    for f in files:
        if f.exists():
            total += process_file(f)
        else:
            print(f"{f} — не знайдено, пропуск.")
    print(f"Готово. Всього змінено рядків: {total}")

if __name__ == "__main__":
    main()
