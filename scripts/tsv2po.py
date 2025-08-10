#!/usr/bin/env python3
"""
tsv2po.py
─────────
Конвертує TSV-файли типу  key | text | tooltip  у стандартний GNU PO.

Режими роботи
=============

• **один файл**
  (вихідний PO буде поряд з output-TSV, але з розширенням .po)

      python tsv2po.py \
          --src  _upstream/en/text/db/names.loc.tsv \
          --trg  translation/text/db/names.loc.tsv

• **масово — дві папки**
  (PO-файли підуть у третю папку, структура та назви збережуться)

      python tsv2po.py \
          --srcdir _upstream/en/text/db \
          --trgdir translation/text/db \
          --outdir _tmp
"""

import argparse, csv, datetime
from pathlib import Path
import pandas as pd

# ── CLI ───────────────────────────────────────────────────────────────
ap = argparse.ArgumentParser()
ap.add_argument("--src", help="оригінальний TSV")
ap.add_argument("--trg", help="TSV з перекладом")
ap.add_argument("--srcdir", help="каталог оригіналів")
ap.add_argument("--trgdir", help="каталог перекладів")
ap.add_argument("--outdir", default="po", help="куди класти po-файли")
args = ap.parse_args()

# ── екрануємо символи ───────────────────────────────────────────────
def po_escape(txt: str) -> str:
    """Екранує символи, які ламають синтаксис PO."""
    if not txt:                       # None або ""
        return ""
    return (
        txt.replace("\\", "\\\\")   # спершу бекслеш
        .replace('"', r'\"')     # потім лапки
    )


# ── утиліта читання TSV → DataFrame ──────────────────────────────────
def read_tsv(p: Path) -> pd.DataFrame:
    return pd.read_csv(
        p, sep="\t", dtype=str,
        names=["key", "text", "tooltip"],
        header=0, keep_default_na=False, na_filter=False,
        engine="python", on_bad_lines="skip"
    )

# ── PO-заголовок ─────────────────────────────────────────────────────
def po_header(filename: str) -> str:
    today = datetime.date.today().strftime("%Y-%m-%d")
    return (
        'msgid ""\n'
        'msgstr ""\n'
        '"Project-Id-Version: TSV-to-PO\\n"\n'
        f'"POT-Creation-Date: {today}\\n"\n'
        '"Language: uk\\n"\n'
        '"Content-Type: text/plain; charset=UTF-8\\n"\n'
        f'"X-Source-File: {filename}\\n"\n\n'
    )

# ── функція конвертації DataFrame у PO-текст ─────────────────────────
def df_to_po(src_df: pd.DataFrame, trg_df: pd.DataFrame, src_name: str) -> str:
    src_map = dict(zip(src_df["key"], src_df["text"]))
    trg_map = dict(zip(trg_df["key"], trg_df["text"]))

    lines = [po_header(src_name)]
    for k, src_txt in src_map.items():
        tr_txt = trg_map.get(k, "")
        # пропускаємо службові та порожні ключі
        if not k or k.startswith("#Loc;"):
            continue

        msgid = po_escape(src_txt)
        msgstr = po_escape(trg_map.get(k, ""))

        lines.append(f'msgctxt "{k}"')
        lines.append(f'msgid "{msgid}"')
        lines.append(f'msgstr "{msgstr}"\n')
    return "\n".join(lines)

# ── конвертер одного файлу ───────────────────────────────────────────
def convert_single(src: Path, trg: Path, out: Path):
    src_df, trg_df = read_tsv(src), read_tsv(trg)
    po_text = df_to_po(src_df, trg_df, src.name)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(po_text, encoding="utf-8")
    # ― акуратний вивід шляху ―
    try:
        shown = out.relative_to(Path.cwd())
    except ValueError:
        shown = out
    print(f"✅  {shown}")

# ── головна логіка ───────────────────────────────────────────────────
if args.src and args.trg:
    out_path = Path(args.trg).with_suffix(".po")
    convert_single(Path(args.src), Path(args.trg), out_path)

elif args.srcdir and args.trgdir:
    srcdir, trgdir, outdir = map(Path, (args.srcdir, args.trgdir, args.outdir))
    if not (srcdir.exists() and trgdir.exists()):
        raise SystemExit("⛔  srcdir / trgdir не існують.")

    for src_file in srcdir.glob("*.loc.tsv"):
        trg_file = trgdir / src_file.name
        if not trg_file.exists():
            print(f"⚠️  пропущено {src_file.name} (немає перекладу)")
            continue
        out_file = outdir / f"{src_file.stem}.po"
        convert_single(src_file, trg_file, out_file)

else:
    ap.print_help()
