#!/usr/bin/env python3
"""
patch_lua.py
────────────────────────────
Підставляє перекладені рядки з TSV-файлів у вибрану таблицю Lua.

Аргументи (передаєте під час запуску):
  --table   REGIONS_NAMES_LOCALISATION   # назва масиву у Lua-файлі
  --prefix  factions_screen_name         # префікс TSV-ключа (додається «_» + lua-key)

Логіка заміни:
 • шукає усередині вказаної таблиці рядки виду
       key = "Text"
       ["key"] = "Text"
 • формує TSV-ключ:   full_key =  f"{prefix}_{key}"   (якщо prefix = "")
                      ⇒ бере просто `key`
 • якщо:
     – у TSV знайдено переклад для full_key
     – переклад ≠ up1_text  (якщо up1 заданий)
     – переклад ≠ up2_text
   → підставляє новий текст у Lua-рядок.

Файл *записується без бекапу* (оригінал є в _upstream).
"""

# ── КОНСТАНТИ ШЛЯХІВ (редагуйте при потребі) ─────────────────────────
LUA_FILE   = "translation/campaigns/main_attila/common/mk1212_localisation_lists.lua"
DIR_TRANSL = "translation/text/db"                   # перекладені TSV
DIR_UP1    = "_upstream/ru/text/db"      # 1-й оригінал  (може не існувати)
DIR_UP2    = "_upstream/en/text/db"      # 2-й оригінал  (EN)

import argparse, re, sys
from pathlib import Path
import pandas as pd

# ── аргументи CLI ────────────────────────────────────────────────────
ap = argparse.ArgumentParser()
ap.add_argument("--table",  required=True, help="Lua table name")
ap.add_argument("--prefix", default="",   help="TSV key prefix (без _)")
args = ap.parse_args()

PATH_LUA   = Path(LUA_FILE)
DIR_DB     = Path(DIR_TRANSL)
DIR_UP1    = Path(DIR_UP1)
DIR_UP2    = Path(DIR_UP2)

if not (PATH_LUA.exists() and DIR_DB.exists() and DIR_UP2.exists()):
    sys.exit("⛔  Вказані шляхи не існують.")

# ── читаємо TSV → словники перекладу / оригіналів ───────────────────
def load_dir(p: Path) -> dict[str, str]:
    d: dict[str,str] = {}
    for f in p.glob("*.loc.tsv"):
        df = pd.read_csv(f, sep="\t", dtype=str,
                         keep_default_na=False, na_filter=False)
        d.update(zip(df["key"], df["text"]))
    return d

tr_dict   = load_dir(DIR_DB)
up2_dict  = load_dir(DIR_UP2)
up1_dict  = load_dir(DIR_UP1) if DIR_UP1 else {}

# ── регулярки ────────────────────────────────────────────────────────
# 1) знайти потрібну таблицю цілком
TABLE_RE = re.compile(
    rf'^[ \t]*(?P<open>{re.escape(args.table)}\s*=\s*\{{)'  # відкриття
    r'(?P<body>.*?)'                                        # тіло
    r'^[ \t]*(?P<close>\})'                                 # закриття
    , re.S | re.M
)

# 2) усередині body: key / ["key"] = "Text"
ROW_RE = re.compile(
    r'(?P<lhs>(\[\s*"(?P<kq>[^"]+)"\s*\])|(?P<kp>[A-Za-z0-9_]+))\s*=\s*"(?P<txt>[^"]*)"'
)

lua_text = PATH_LUA.read_text(encoding="utf-8")

def patch_body(body: str) -> tuple[str,int]:
    """Повертає змінений body та кількість real updates."""
    updated = 0

    def repl(m: re.Match) -> str:
        nonlocal updated
        lua_key = m["kq"] or m["kp"]
        full_key = f"{args.prefix}_{lua_key}" if args.prefix else lua_key

        new = tr_dict.get(full_key)
        if not new:
            return m.group(0)

        # пропускаємо, якщо new == будь-який з оригіналів
        if new == up2_dict.get(full_key, ""):
            return m.group(0)
        if DIR_UP1 and new == up1_dict.get(full_key, ""):
            return m.group(0)

        updated += 1
        return f'{m["lhs"]} = "{new}"'

    new_body = ROW_RE.sub(repl, body)
    return new_body, updated

def patch_lua(src: str) -> tuple[str,int]:
    total_updates = 0

    def table_repl(t: re.Match) -> str:
        nonlocal total_updates
        original_body = t.group('body')
        patched_body, n = patch_body(original_body)
        total_updates += n
        # збираємо назад: open + нове тіло + close
        return f"{t.group('open')}{patched_body}{t.group('close')}"

    new_src = TABLE_RE.sub(table_repl, src, count=1)
    return new_src, total_updates

patched, n = patch_lua(lua_text)

if n:
    PATH_LUA.write_text(patched, encoding="utf-8")
    print(f"✅  {PATH_LUA.name}: замінено {n} рядків у таблиці {args.table}.")
else:
    print(f"–  {PATH_LUA.name}: жодного перекладеного рядка для {args.table} не знайдено.")
