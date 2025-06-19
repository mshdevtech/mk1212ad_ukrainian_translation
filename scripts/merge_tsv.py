import pandas as pd, pathlib

SRC_DIR = pathlib.Path("_upstream/text/db")
TRG_DIR = pathlib.Path("text/db")
OBS_DIR = pathlib.Path("obsolete")

for src_path in SRC_DIR.glob("*.loc.tsv"):
    trg_path = TRG_DIR / src_path.name

    # Read both (dtype=str — to not lose null's)
    src = pd.read_csv(src_path, sep="\t", dtype=str)
    trg = pd.read_csv(trg_path, sep="\t", dtype=str) if trg_path.exists() \
        else pd.DataFrame(columns=src.columns)

    # ⚙︎ Merging --------------------------------------------------------
    merged = src.merge(trg, on="key", how="left", suffixes=("", "_old"))

    # 1) if translation already exist — keep it
    # 2) if translation doesn't exist — copy original text
    merged["text"] = merged["text_old"].fillna(merged["text"])

    merged = merged[src.columns]   # return column order
    trg_path.parent.mkdir(parents=True, exist_ok=True)
    merged.to_csv(trg_path, sep="\t", index=False)

    # ⚙︎ Removed keys  ----------------------------------------------
    removed = trg.loc[~trg["key"].isin(src["key"])]
    if not removed.empty:
        OBS_DIR.mkdir(parents=True, exist_ok=True)
        removed.to_csv(OBS_DIR / src_path.name, sep="\t", index=False)
