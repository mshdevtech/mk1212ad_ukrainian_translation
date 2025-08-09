#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
sync_lua_from_upstream.py
─────────────────────────
Оновлює Lua-скрипти у каталозі `translation/` за оновленими файлами з `_upstream/`,
зберігаючи перекладені рядки.

Логіка
------
1) Знайти всі .lua в UPSTREAM_ROOT (рекурсивно).
2) Для кожного — відшукати відповідний файл у TRANS_ROOT з тією ж відносною стежкою.
3) Порівняти "нормалізований" код (усі рядкові літерали → "", усі пробіли прибрані).
   Якщо різниці немає — пропустити.
4) Якщо код відрізняється: зчитати переклади з TRANS-файла (для кожної таблиці:
   REGEX у вигляді  key = "text" або ["key"] = "text") → побудувати мапу (table, key) → text.
5) Патчити UPSTREAM-файл: у відповідних таблицях підставити текст зі збереженої мапи.
6) Записати у файл у `translation/`, залишивши існуючий тип EOL (CRLF/LF).

Запуск
------
python scripts/sync_lua_from_upstream.py
python scripts/sync_lua_from_upstream.py --dry-run   # лише звіт, без запису

За замовчуванням:
    UPSTREAM_ROOT = "_upstream"
    TRANS_ROOT    = "translation"
"""
from __future__ import annotations

import argparse
import os
import re
from pathlib import Path
from typing import Dict, Tuple, List

# ── НАЛАШТУВАННЯ (за потреби відредагуйте) ───────────────────────────
UPSTREAM_ROOT = Path("_upstream")
TRANS_ROOT    = Path("translation")

# ── Регекси ───────────────────────────────────────────────────────────
# Початок таблиці:  [local ]TableName = {
TABLE_HEAD_RE = re.compile(
    r'(?m)^[ \t]*(?:local[ \t]+)?(?P<tab>[A-Za-z_][A-Za-z0-9_]*)[ \t]*=[ \t]*\{'
)

# Пара "ключ = "значення"" всередині тіла таблиці (допускає екрановані лапки)
ROW_RE = re.compile(
    r'(?P<lhs>(?:\[\s*"(?P<kq>(?:[^"\\]|\\.)+)"\s*\])|(?P<kp>[A-Za-z0-9_]+))'
    r'\s*=\s*"(?P<txt>(?:[^"\\]|\\.)*)"',
    re.S
)

# Будь-який рядковий літерал у подвійних лапках (для нормалізації коду)
STR_RE = re.compile(r'"(?:[^"\\]|\\.)*"', re.S)

# ── Утиліти ───────────────────────────────────────────────────────────
def detect_eol(path: Path) -> str:
    """Повертає '\r\n' або '\n' залежно від наявного EOL; якщо файлу немає — системний."""
    if not path.exists():
        return os.linesep
    with open(path, "rb") as f:
        chunk = f.read(16384)
    i = chunk.find(b"\n")
    if i == -1:
        return os.linesep
    return "\r\n" if i > 0 and chunk[i - 1:i] == b"\r" else "\n"

def normalize_code(s: str) -> str:
    """Код без текстів і пробілів: рядки → "", пробіли прибрані."""
    s = STR_RE.sub('""', s)
    s = re.sub(r"\s+", "", s)
    return s

def find_matching_brace(src: str, open_idx: int) -> int:
    """Повертає позицію закривної '}' для '{' на open_idx, ігноруючи дужки всередині рядків."""
    depth = 0
    i = open_idx
    in_str = False
    escape = False
    while i < len(src):
        ch = src[i]
        if in_str:
            if escape:
                escape = False
            elif ch == "\\":
                escape = True
            elif ch == '"':
                in_str = False
        else:
            if ch == '"':
                in_str = True
            elif ch == "{":
                depth += 1
            elif ch == "}":
                depth -= 1
                if depth == 0:
                    return i
        i += 1
    raise ValueError("Не знайдено відповідної закривної дужки '}'.")

def extract_tables(src: str) -> List[Tuple[str, int, int]]:
    """
    Повертає список таблиць як кортежів:
      (table_name, body_start_index, body_end_index)
    де body — це вміст МІЖ { і відповідною }.
    """
    tables = []
    for m in TABLE_HEAD_RE.finditer(src):
        name = m.group("tab")
        open_brace = src.find("{", m.end() - 1)
        if open_brace == -1:
            continue
        close_brace = find_matching_brace(src, open_brace)
        body_start = open_brace + 1
        body_end = close_brace
        tables.append((name, body_start, body_end))
    return tables

def extract_translations_by_table(src: str) -> Dict[Tuple[str, str], str]:
    """З файлу з перекладом збирає (table, key) → text."""
    out: Dict[Tuple[str, str], str] = {}
    for name, b0, b1 in extract_tables(src):
        body = src[b0:b1]
        for m in ROW_RE.finditer(body):
            key = m.group("kq") or m.group("kp")
            # якщо key був у ["..."], приберемо екранування всередині ключа
            if key and m.group("kq"):
                key = bytes(key, "utf-8").decode("unicode_escape")
            text = m.group("txt")
            out[(name, key)] = text
    return out

def patch_upstream_with_trans(up_src: str, trans_map: Dict[Tuple[str, str], str]) -> Tuple[str, int]:
    """
    Замінює RHS у UPSTREAM-тексті згідно з trans_map[(table, key)].
    Повертає (новий_текст, кількість_заміни).
    """
    result_parts = []
    idx = 0
    replaced = 0

    for name, b0, b1 in extract_tables(up_src):
        # до таблиці — як є
        result_parts.append(up_src[idx:b0])
        body = up_src[b0:b1]

        def row_repl(m: re.Match) -> str:
            nonlocal replaced
            key = m.group("kq") or m.group("kp")
            if key and m.group("kq"):
                key = bytes(key, "utf-8").decode("unicode_escape")
            new_text = trans_map.get((name, key))
            if new_text is None:
                return m.group(0)
            if new_text == m.group("txt"):
                return m.group(0)
            replaced += 1
            return f'{m.group("lhs")} = "{new_text}"'

        patched_body = ROW_RE.sub(row_repl, body)
        result_parts.append(patched_body)
        idx = b1
    # хвіст
    result_parts.append(up_src[idx:])
    return "".join(result_parts), replaced

# ── Основна програма ─────────────────────────────────────────────────
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true", help="лише показати зміни, без запису")
    args = ap.parse_args()

    if not UPSTREAM_ROOT.exists() or not TRANS_ROOT.exists():
        raise SystemExit("⛔  Перевірте UPSTREAM_ROOT і TRANS_ROOT.")

    upstream_files = sorted(UPSTREAM_ROOT.rglob("*.lua"))
    total_changed = 0
    total_patched = 0

    for up in upstream_files:
        rel = up.relative_to(UPSTREAM_ROOT)
        tr = TRANS_ROOT / rel
        if not tr.exists():
            # немає перекладеного аналога — пропускаємо
            continue

        up_text = up.read_text(encoding="utf-8", errors="ignore")
        tr_text = tr.read_text(encoding="utf-8", errors="ignore")

        # Чи змінився код (без урахування рядкових літералів і пробілів)?
        if normalize_code(up_text) == normalize_code(tr_text):
            # код ідентичний — нічого робити
            continue

        # Є зміни в коді → переносимо переклади зі старого файла на новий upstream
        trans_map = extract_translations_by_table(tr_text)
        patched_text, replaced = patch_upstream_with_trans(up_text, trans_map)

        changed = (patched_text != tr_text)  # чи відрізняється від поточного перекладеного
        if changed:
            total_changed += 1
            total_patched += replaced
            if args.dry_run:
                print(f"• {rel}: код змінено в upstream; перенесено {replaced} переклад(ів) [dry-run]")
            else:
                eol = detect_eol(tr)  # зберігаємо наявний тип кінців рядків
                with open(tr, "w", encoding="utf-8", newline="") as f:
                    # вручну розіб'ємо по потрібному EOL, аби не змінювати CRLF/LF
                    f.write(patched_text.replace("\r\n", "\n").replace("\r", "\n").replace("\n", eol))
                print(f"✓ {rel}: оновлено код, перенесено {replaced} переклад(ів)")
        else:
            # теоретично можливо, якщо лише пробіли помінялись
            pass

    if total_changed == 0:
        print("– Змін не знайдено: усі Lua-файли в translation відповідають upstream (з точністю до рядків).")
    else:
        if args.dry_run:
            print(f"Готово (dry-run). Файлів до оновлення: {total_changed}, разом підстановок: {total_patched}.")
        else:
            print(f"Готово. Оновлено файлів: {total_changed}, разом підстановок: {total_patched}.")

if __name__ == "__main__":
    main()
