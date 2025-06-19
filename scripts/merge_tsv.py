import pandas as pd, pathlib, sys

SRC_DIR = pathlib.Path("_upstream/text/db")
TRG_DIR = pathlib.Path("text/db")
OBS_DIR = pathlib.Path("obsolete")

# - stat counters -
files_done   = 0
total_added  = 0
total_removed = 0

for src_path in SRC_DIR.glob("*.loc.tsv"):
    trg_path = TRG_DIR / src_path.name
    read = lambda p: pd.read_csv(
        p, sep="\t", dtype=str, keep_default_na=False  # ← головне доповнення
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
        merged["text_old"].notna(),        # якщо text_old НЕ NaN → залишаємо
        merged["text"]                     # інакше беремо значення з text
    )

    merged = merged[src.columns]   # return column order
    trg_path.parent.mkdir(parents=True, exist_ok=True)
    merged.to_csv(trg_path, sep="\t", index=False)

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