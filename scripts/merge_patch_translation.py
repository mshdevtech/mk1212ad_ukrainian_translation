#!/usr/bin/env python3
"""
merge_patch_translation.py
──────────────────────────
• Мета: доповнити `text/db/<file>` готовими перекладами з
  `_upstream/uk/text/db/<file>`, але **тільки** там, де переклад у
  головному файлі ще не зроблено (рядок досі збігається з оригіналом
  `_upstream/en/db/<file>` або порожній).

Алгоритм для кожного *.loc.tsv:
1. читаємо
      _upstream/en/db/XXX.loc.tsv   → en
      text/db/XXX.loc.tsv           → ua_main
      _upstream/uk/text/db/XXX.loc.tsv (якщо є) → ua_patch
2. пропускаємо рядки з порожнім key та перші дві службові записи
3. для кожного key, що є в ua_main:
      якщо  (ua_main.text == en.text)   або (ua_main.text == "")
      і     key є в ua_patch
      і     ua_patch.text != en.text
      →     копіюємо ua_patch.text у ua_main.text
4. зберігаємо файл без зміни порядку рядків.
"""

import sys
from pathlib import Path
import pandas as pd

ROOT_EN     = Path("_upstream/en/text/db")
ROOT_PATCH  = Path("_upstream/uk/text/db")
ROOT_MAIN   = Path("translation/text/db")

def load(p: Path) -> pd.DataFrame:
    """Читаємо TSV, нічого не перетворюємо на NaN."""
    return pd.read_csv(
        p, sep="\t", dtype=str,
        keep_default_na=False, na_filter=False
    )

def process(file_name: str) -> None:
    path_en    = ROOT_EN   / file_name
    path_patch = ROOT_PATCH / file_name
    path_main  = ROOT_MAIN / file_name

    if not (path_en.exists() and path_main.exists() and path_patch.exists()):
        print(f"⚠️  Пропуск {file_name} — файл не знайдено у всіх трьох каталогаx.")
        return

    en    = load(path_en)
    main  = load(path_main)
    patch = load(path_patch)

    # ── фільтри, але тепер робимо *копії*, головний DF лишається повним
    sub_en    = en[  en["key"].str.strip()   != ""].iloc[1:]
    sub_main  = main[main["key"].str.strip() != ""].iloc[1:]
    sub_patch = patch[patch["key"].str.strip() != ""].iloc[1:]

    # робимо швидкий lookup по key
    en_lookup    = dict(zip(sub_en["key"],    sub_en["text"]))
    patch_lookup = dict(zip(sub_patch["key"], sub_patch["text"]))

    updated = 0
    for idx, row in main.iterrows():
        k = row["key"]
        text_main   = main.at[idx, "text"]        # беремо з ПОВНОГО DF
        text_en     = en_lookup.get(k, "")
        text_patch  = patch_lookup.get(k)

        if (
                k and text_patch and text_patch != text_en and
                (text_main == text_en or text_main == "")
        ):
            main.at[idx, "text"] = text_patch
            updated += 1

    if updated:
        main.to_csv(path_main, sep="\t", index=False, na_rep="")
        print(f"✅ {file_name}: оновлено {updated} рядків.")
    else:
        print(f"–  {file_name}: переклади не потрібні.")

if __name__ == "__main__":
    targets = sys.argv[1:] or [p.name for p in ROOT_PATCH.glob("*.loc.tsv")]
    for fname in targets:
        process(fname)
