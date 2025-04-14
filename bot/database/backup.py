import os
import shutil
import datetime

# Исправление пути - было "bot1.db", стало "bot.db"
DB_FILE = "database/bot.db"
BACKUP_DIR = "database/backups"

def backup_database():
    """
    Делает копию файла bot db в папку backups.
    Имя файла копии содержит дату/время.
    Возвращает путь к созданному файлу или None в случае ошибки.
    """
    if not os.path.exists(BACKUP_DIR):
        os.makedirs(BACKUP_DIR, exist_ok=True)

    if not os.path.exists(DB_FILE):
        print("Файл базы данных не найден, нечего бэкапить.")
        return None

    try:
        timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        backup_file = os.path.join(BACKUP_DIR, f"bot_backup_{timestamp}.db")

        shutil.copy2(DB_FILE, backup_file)
        print(f"Резервная копия БД сохранена: {backup_file}")
        return backup_file
    except Exception as e:
        print(f"Ошибка при создании бэкапа: {e}")
        return None


if __name__ == "__main__":
    backup_database()