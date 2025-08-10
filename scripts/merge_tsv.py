#!/usr/bin/env python3
"""
merge_tsv.py
────────────
Скрипт для мерджу (оновлення) файлів перекладу з оригіналом

Що робить:
  - Додає нові ключі з оригіналу (EN) у відповідні файли перекладу
  - Не затирає вже перекладені рядки
  - Архівує видалені ключі у папку obsolete
  - Валідує структуру та унікальність ключів у TSV

Як запускати:
  python scripts/merge_tsv.py

Для чого потрібно:
  - Щоб переклад завжди містив усі актуальні ключі з оригіналу
  - Щоб не втрачати вже зроблений переклад
  - Для зручного оновлення після оновлення оригінальних файлів
"""

import pandas as pd, pathlib, sys
import csv

SRC_DIR = pathlib.Path("_upstream/en/text/db")
TRG_DIR = pathlib.Path("translation/text/db")
OBS_DIR = pathlib.Path("obsolete")

# - stat counters -
files_done   = 0
total_added  = 0
total_removed = 0
total_modified = 0

# ── Функції валідації ─────────────────────────────────────────────────
def validate_tsv_file(file_path: pathlib.Path) -> tuple[bool, list[str]]:
    """Валідує один TSV файл та повертає (is_valid, error_messages)."""
    errors = []
    try:
        df = pd.read_csv(file_path, sep="\t", dtype=str, keep_default_na=False, quoting=csv.QUOTE_NONE, encoding_errors='ignore')
    except Exception as e:
        errors.append(f"не вдалося прочитати файл ({e})")
        return False, errors

    # Перевіряємо колонки
    required_cols = ["key", "text", "tooltip"]
    if list(df.columns) != required_cols:
        errors.append(f"очікувано колонки {required_cols}, а отримано {list(df.columns)}")
        return False, errors

    # Порожні key
    empty_rows = df["key"].str.strip() == ""
    if empty_rows.any():
        rows = ", ".join(map(str, (df.index[empty_rows] + 2)))  # +2: header + 0-based
        errors.append(f"порожній key у рядках {rows}")

    # Дублікати key
    non_empty_keys = df.loc[~empty_rows, "key"]
    dup_keys = non_empty_keys[non_empty_keys.duplicated()]
    if not dup_keys.empty:
        keys = ", ".join(dup_keys.unique())
        errors.append(f"дублікати key: {keys}")

    return len(errors) == 0, errors

def validate_directory(dir_path: pathlib.Path, dir_name: str) -> tuple[bool, dict[str, list[str]]]:
    """Валідує всі TSV файли в директорії та повертає (is_valid, file_errors)."""
    print(f"🔍 Перевіряємо TSV у {dir_name} ({dir_path})...")
    
    if not dir_path.exists():
        print(f"⚠️  Директорія {dir_name} не існує")
        return True, {}
    
    file_errors = {}
    has_errors = False
    
    for file_path in sorted(dir_path.glob("*.loc.tsv")):
        is_valid, errors = validate_tsv_file(file_path)
        if errors:
            file_errors[file_path.name] = errors
            has_errors = True
            print(f"❌ {file_path.name}:")
            for error in errors:
                print(f"   • {error}")
        else:
            pass
    
    print()
    return not has_errors, file_errors

def ask_continue() -> bool:
    """Питає користувача чи продовжувати виконання."""
    while True:
        response = input("Продовжити мердж? (y/n): ").lower().strip()
        if response in ['y', 'yes', 'так', 'т']:
            return True
        elif response in ['n', 'no', 'ні', 'н']:
            return False
        else:
            print("Будь ласка, введіть 'y' або 'n'")

# ── Перевірка файлів перед мерджем ──────────────────────────────────
print("=== ПОПЕРЕДНЯ ПЕРЕВІРКА ФАЙЛІВ ===\n")

src_valid, src_errors = validate_directory(SRC_DIR, "SRC_DIR")
trg_valid, trg_errors = validate_directory(TRG_DIR, "TRG_DIR")

if not src_valid or not trg_valid:
    print("⚠️  ЗНАЙДЕНО ПОМИЛКИ В ФАЙЛАХ!")
    print("Скрипт може відпрацювати некоректно і краще виправити проблемні файли власноруч.")
    print()
    
    if not ask_continue():
        print("Мердж скасовано.")
        sys.exit(1)
    
    print("Продовжуємо мердж...\n")
else:
    print("✅ Всі файли валідні, продовжуємо мердж.\n")

print("=== ПОЧИНАЄМО МЕРДЖ ===\n")

files_with_changes = 0

for src_path in SRC_DIR.glob("*.loc.tsv"):
    trg_path = TRG_DIR / src_path.name
    read = lambda p: pd.read_csv(
        p, sep="\t", dtype=str, keep_default_na=False, na_filter=False, 
        quoting=csv.QUOTE_NONE, encoding_errors='ignore'
    )

    src = read(src_path)
    trg = read(trg_path) if trg_path.exists() else pd.DataFrame(columns=src.columns)

    # - Filter empty keys -
    src = src[src["key"].str.strip() != ""].copy()
    trg = trg[trg["key"].str.strip() != ""].copy()

    # - Merging -
    merged = src.merge(trg, on="key", how="left", suffixes=("", "_old"))

    # 1) if translation already exist — keep it
    # 2) if translation doesn't exist — copy original text
    merged["text"] = merged["text_old"].where(
        merged["text_old"].notna() & (merged["text_old"] != ""),        # якщо text_old НЕ NaN → залишаємо
        merged["text"]                     # інакше беремо значення з text
    )

    # - Count actually modified rows (where translation appeared or changed) -
    modified_count = 0
    if "text_old" in merged.columns:
        modified_mask = (merged["text"].notna()) & (merged["text"] != "") & (
            merged["text_old"].isna() | (merged["text_old"] == "") | (merged["text"] != merged["text_old"]))
        modified_count = modified_mask.sum()
    total_modified += modified_count

    merged = merged[src.columns]   # return column order

    # NaN → ""
    merged = merged.fillna("")

    trg_path.parent.mkdir(parents=True, exist_ok=True)
    
    # Зберігаємо з pandas to_csv, використовуючи QUOTE_NONE
    merged.to_csv(trg_path, sep='\t', index=False, quoting=csv.QUOTE_NONE, encoding='utf-8')

    # - statistic for new keys -
    new_keys = src.loc[~src["key"].isin(trg["key"])]
    total_added += len(new_keys)

    # -︎ Removed keys -
    removed = trg.loc[~trg["key"].isin(src["key"])]
    if not removed.empty:
        OBS_DIR.mkdir(parents=True, exist_ok=True)
        
        # Зберігаємо архівний файл з pandas to_csv
        archive_path = OBS_DIR / src_path.name
        removed.to_csv(archive_path, sep='\t', index=False, quoting=csv.QUOTE_NONE, encoding='utf-8')
        
        total_removed += len(removed)

    files_done += 1
    if len(new_keys) > 0 or len(removed) > 0 or modified_count > 0:
        print(f"✓ {src_path.name}: +{len(new_keys)} new, -{len(removed)} removed, ~{modified_count} modified")
        files_with_changes += 1

if files_with_changes == 0:
    print("✅ Всі файли актуальні")

print("\n=== Merge completed ===")
print(f"Processed files : {files_done}")
print(f"New keys added  : {total_added}")
print(f"Keys archived   : {total_removed}")
print(f"Rows modified   : {total_modified}")
print("Done!")

sys.exit(0)