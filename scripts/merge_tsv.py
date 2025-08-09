# This script merges translation files from a source directory (`_upstream/en/text/db`)
# with corresponding files in a target directory (`translation/text/db`).
# It performs the following steps:
#
# 1. Reads all `.loc.tsv` files from the source directory.
# 2. For each source file:
#    - Reads the corresponding target file if it exists, or creates an empty DataFrame.
#    - Filters out rows with empty or whitespace-only keys.
#    - Merges the source and target files on the `key` column:
#      - Keeps existing translations from the target file if they are non-empty.
#      - Copies the source text if no translation exists in the target file.
#    - Ensures the column order matches the source file and replaces NaN values with empty strings.
#    - Saves the merged file back to the target directory.
# 3. Tracks and logs statistics:
#    - Counts new keys added from the source file to the target file.
#    - Identifies and archives keys removed from the source file into an `obsolete` directory.
# 4. Outputs a summary of processed files, new keys added, and keys archived.
#
# Usage:
# - Place the source `.loc.tsv` files in `_upstream/en/text/db`.
# - Place the target `.loc.tsv` files (if any) in `translation/text/db`.
# - Run the script. The merged files will be saved in `translation/text/db`,
#   and removed keys will be archived in the `obsolete` directory.
import pandas as pd, pathlib, sys

SRC_DIR = pathlib.Path("_upstream/en/text/db")
TRG_DIR = pathlib.Path("translation/text/db")
OBS_DIR = pathlib.Path("obsolete")

# - stat counters -
files_done   = 0
total_added  = 0
total_removed = 0

for src_path in SRC_DIR.glob("*.loc.tsv"):
    trg_path = TRG_DIR / src_path.name
    read = lambda p: pd.read_csv(
        p, sep="\t", dtype=str, keep_default_na=False, na_filter=False
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

    merged = merged[src.columns]   # return column order

    # NaN → ""
    merged = merged.fillna("")

    trg_path.parent.mkdir(parents=True, exist_ok=True)
    merged.to_csv(trg_path, sep="\t", index=False, na_rep="")

    # - statistic for new keys -
    new_keys = src.loc[~src["key"].isin(trg["key"])]
    total_added += len(new_keys)

    # -︎ Removed keys -
    removed = trg.loc[~trg["key"].isin(src["key"])]
    if not removed.empty:
        OBS_DIR.mkdir(parents=True, exist_ok=True)
        removed.to_csv(OBS_DIR / src_path.name, sep="\t", index=False)
        total_removed += len(removed)

    files_done += 1
    print(f"✓ {src_path.name}: +{len(new_keys)} new, -{len(removed)} removed")

print("\n=== Merge completed ===")
print(f"Processed files : {files_done}")
print(f"New keys added  : {total_added}")
print(f"Keys archived   : {total_removed}")
print("Done!")

sys.exit(0)