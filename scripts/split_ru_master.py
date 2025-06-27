#!/usr/bin/env python3
"""
split_ru_master.py
──────────────────
Розкидає переклади з `_upstream/ru/text/localisation.loc.tsv`
по окремих файлах  `_upstream/ru/text/db/*.loc.tsv`.

• **Чому:** російська локалізація гри зберігається «усім скопом»
  у localisation.loc.tsv, а для зручності редагування треба мати
  ту саму структуру, що й у EN / UA (по файлах db).

• **Що робить:**
  1. Читає master-файл RU → словник key → text_ru.
  2. Перебирає ВСІ файли з `_upstream/en/text/db/*.loc.tsv`
     ─ саме вони визначають, у який RU-файл має потрапити переклад.
  3. Для кожного key, який уже є у відповідному
     `_upstream/ru/text/db/<file>` (або файл ще не існує) :
       • якщо RU-переклад існує в master
       • і у RU-файлі рядок досі англійський / порожній
       → підставляє російський текст.
  4. Якщо RU-файл ще не існує — створює копію EN-файлу й одразу
     підставляє переклади.

*Службовий рядок `#Loc;…` та порожні `key` не змінюються.*
"""

import sys
from pathlib import Path
import pandas as pd

ROOT_EN     = Path("_upstream/en/text/db")
ROOT_RU_DB  = Path("_upstream/ru/text/db")
RU_MASTER   = Path("_upstream/ru/text/localisation.loc.tsv")

def load(p: Path) -> pd.DataFrame:
    return pd.read_csv(
        p, sep="\t", dtype=str,
        keep_default_na=False, na_filter=False
    )

# ── 1. master-словник ────────────────────────────────────────────────
if not RU_MASTER.exists():
    sys.exit("⛔  _upstream/ru/text/localisation.loc.tsv не знайдено.")

master_df = load(RU_MASTER)
ru_master = dict(zip(master_df["key"], master_df["text"]))

# ── 2. список EN-файлів як еталон структури ─────────────────────────
targets = [p.name for p in ROOT_EN.glob("*.loc.tsv")]

def process(fname: str) -> None:
    path_en  = ROOT_EN   / fname
    path_ru  = ROOT_RU_DB / fname

    if not path_en.exists():
        print(f"⚠️  {fname}: немає EN-еталона, пропуск.")
        return

    en_df = load(path_en)

    # якщо RU-файлу немає — створюємо копію EN-файлу
    if path_ru.exists():
        ru_df = load(path_ru)
    else:
        path_ru.parent.mkdir(parents=True, exist_ok=True)
        ru_df = en_df.copy()

    # маска «можна редагувати»
    editable = (
            (ru_df["key"].str.strip() != "") &
            ~ru_df["key"].str.startswith("#Loc;")
    )

    updated = 0
    for idx in ru_df.index[editable]:
        k        = ru_df.at[idx, "key"]
        text_en  = en_df.at[idx, "text"]
        text_ru  = ru_master.get(k)
        text_cur = ru_df.at[idx, "text"]

        if text_ru and (text_cur == text_en or text_cur == "") and text_ru != text_en:
            ru_df.at[idx, "text"] = text_ru
            updated += 1

    if updated or not path_ru.exists():
        ru_df.to_csv(path_ru, sep="\t", index=False, na_rep="")
        print(f"✅ {fname}: записано {updated} рядків.")
    else:
        print(f"–  {fname}: оновлення не потрібне.")

if __name__ == "__main__":
    for f in sys.argv[1:] or targets:
        process(f)
