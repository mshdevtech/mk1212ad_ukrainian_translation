"""
sync_translation.py
───────────────────
Script for synchronizing translation from the translation folder into the target mod directory.

What it does:
  - Copies all files and folders from translation/ into the specified target folder (DST)
  - Before copying, deletes only those subfolders/files in DST that exist in translation (does not touch other files, such as .git)
  - If the target folder does not exist — displays a hint to the user

How to run:
  python scripts/sync_translation.py
  (or automatically via git pre-commit hook, see README)

Purpose:
  - To quickly update translation files in the mod folder for in-game testing
  - To avoid errors caused by outdated or unnecessary files
"""
import os
import shutil

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))

SRC = os.path.join(PROJECT_ROOT, 'translation')
ENV_FILE = os.path.join(PROJECT_ROOT, '.env')

# Load .env manually
if os.path.exists(ENV_FILE):
    with open(ENV_FILE, 'r', encoding='utf-8') as f:
        for line in f:
            if '=' in line:
                key, value = line.strip().split('=', 1)
                os.environ[key] = value

DST = os.environ.get("DST")

if not DST:
    print("[ERROR] 'DST' not set in .env file.")
    exit(1)

def clear_folder(folder, src):
    # Delete only those folders that exist in SRC
    for item in os.listdir(src):
        src_path = os.path.join(src, item)
        dst_path = os.path.join(folder, item)
        if os.path.isdir(src_path) and os.path.exists(dst_path):
            try:
                shutil.rmtree(dst_path)
                print(f'Deleted folder: {dst_path}')
            except Exception as e:
                print(f'Failed to delete {dst_path}. Reason: {e}')
        # If SRC has a file, but DST has a folder with the same name, also delete it
        elif os.path.isfile(src_path) and os.path.isdir(dst_path):
            try:
                shutil.rmtree(dst_path)
                print(f'Deleted folder (instead of file): {dst_path}')
            except Exception as e:
                print(f'Failed to delete {dst_path}. Reason: {e}')

def copytree(src, dst):
    print(f"Copying from {src} to {dst}")
    for item in os.listdir(src):
        s = os.path.join(src, item)
        d = os.path.join(dst, item)
        print(f"  Found: {s} -> {d}")
        if os.path.isdir(s):
            print("    This is a folder, copying recursively")
            try:
                shutil.copytree(s, d, dirs_exist_ok=True)
            except Exception as e:
                print(f'    [ERROR] Failed to copy folder {s} -> {d}. Reason: {e}')
        else:
            print("    This is a file, copying")
            try:
                shutil.copy2(s, d)
            except Exception as e:
                print(f'    [ERROR] Failed to copy file {s} -> {d}. Reason: {e}')

def main():
    print(f"SRC: {SRC}")
    print(f"DST: {DST}")
    if not os.path.exists(DST):
        print(f"[ERROR] Target folder does not exist: {DST}\nCreate this folder or change the path in scripts/sync_translation.py")
        exit(1)
    clear_folder(DST, SRC)
    copytree(SRC, DST)
    print(f'Synchronization completed: {SRC} -> {DST}')

if __name__ == '__main__':
    main()
