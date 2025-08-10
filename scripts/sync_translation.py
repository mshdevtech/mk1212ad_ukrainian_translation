# Скрипт для синхронізації перекладу з папки translation у цільову директорію мода
#
# Що робить:
#   - Копіює всі файли та папки з translation/ у вказану цільову папку (DST)
#   - Перед копіюванням видаляє лише ті підпапки/файли у DST, які є у translation (не чіпає інші файли, наприклад .git)
#   - Якщо цільова папка не існує — виводить підказку користувачу
#
# Як налаштувати:
#   - Вкажіть шлях до цільової папки у змінній DST (див. нижче)
#   - Якщо папка не існує — створіть її вручну
#
# Як запускати:
#   - Вручну:   python scripts/sync_translation.py
#   - Автоматично: через git pre-commit hook (див. README)
#
# Для чого потрібно:
#   - Щоб швидко оновлювати файли перекладу у папці мода для тестування в грі
#   - Щоб уникати помилок через застарілі або зайві файли
#
import os
import shutil

SRC = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'translation'))
DST = r'C:\Users\YOUR_USERNAME\TWMods\attila\YOUR_MOD_NAME'

def clear_folder(folder, src):
    # Видаляємо лише ті папки, які є у SRC
    for item in os.listdir(src):
        src_path = os.path.join(src, item)
        dst_path = os.path.join(folder, item)
        if os.path.isdir(src_path) and os.path.exists(dst_path):
            try:
                shutil.rmtree(dst_path)
                print(f'Видалено папку: {dst_path}')
            except Exception as e:
                print(f'Не вдалося видалити {dst_path}. Причина: {e}')
        # Якщо у SRC є файл, а у DST — папка з такою ж назвою, теж видалити
        elif os.path.isfile(src_path) and os.path.isdir(dst_path):
            try:
                shutil.rmtree(dst_path)
                print(f'Видалено папку (замість файлу): {dst_path}')
            except Exception as e:
                print(f'Не вдалося видалити {dst_path}. Причина: {e}')

def copytree(src, dst):
    print(f"Копіюю з {src} у {dst}")
    for item in os.listdir(src):
        s = os.path.join(src, item)
        d = os.path.join(dst, item)
        print(f"  Знайдено: {s} -> {d}")
        if os.path.isdir(s):
            print("    Це папка, копіюю рекурсивно")
            shutil.copytree(s, d, dirs_exist_ok=True)
        else:
            print("    Це файл, копіюю")
            shutil.copy2(s, d)

def main():
    print(f"SRC: {SRC}")
    print(f"DST: {DST}")
    if not os.path.exists(DST):
        print(f"[ПОМИЛКА] Цільова папка не існує: {DST}\nСтворіть цю папку або змініть шлях у scripts/sync_translation.py")
        exit(1)
    clear_folder(DST, SRC)
    copytree(SRC, DST)
    print(f'Синхронізація завершена: {SRC} → {DST}')

if __name__ == '__main__':
    main()
