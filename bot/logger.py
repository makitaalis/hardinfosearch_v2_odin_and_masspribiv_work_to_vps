import logging
import os
import shutil
from datetime import datetime
import logging.handlers
import gzip

LOGS_DIR = "logs"
ARCHIVE_DIR = os.path.join(LOGS_DIR, "archive")

# Создаём папки для логов, если их нет
os.makedirs(LOGS_DIR, exist_ok=True)
os.makedirs(ARCHIVE_DIR, exist_ok=True)

# Имя файла лога на сегодня
today_str = datetime.now().strftime("%Y-%m-%d")
log_filename = os.path.join(LOGS_DIR, f"{today_str}.log")


# Настройка логирования со встроенной ротацией
def setup_logger():
    """Настраивает логирование с ротацией по размеру и дате"""
    logger = logging.getLogger()
    logger.setLevel(logging.DEBUG)  # Изменяем на DEBUG для более подробного логгирования

    # Очистка любых существующих обработчиков
    for handler in logger.handlers[:]:
        logger.removeHandler(handler)

    # Создание обработчика с ротацией по размеру
    # 1MB = 1048576 bytes, максимум 5 бэкапов
    rotating_handler = logging.handlers.RotatingFileHandler(
        log_filename,
        maxBytes=1048576,
        backupCount=5,
        encoding='utf-8'
    )

    # Формат логов
    formatter = logging.Formatter("%(asctime)s | %(levelname)s | %(message)s",
                                  datefmt="%Y-%m-%d %H:%M:%S")
    rotating_handler.setFormatter(formatter)
    rotating_handler.setLevel(logging.INFO)  # Для файла оставляем INFO

    # Архивация старых логов (перенос .log в папку archive, кроме текущего)
    archive_old_logs()

    # Добавляем обработчик к root logger
    logger.addHandler(rotating_handler)

    # Опционально: добавляем вывод в консоль с уровнем DEBUG
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    console_handler.setLevel(logging.DEBUG)  # Для консоли ставим DEBUG
    logger.addHandler(console_handler)

    logger.info("Логирование настроено. Активный лог-файл: %s", log_filename)
    return logger


def archive_old_logs():
    """Архивирует старые логи, сжимая их"""
    for file in os.listdir(LOGS_DIR):
        if file.endswith(".log") and file != f"{today_str}.log":
            old_log_path = os.path.join(LOGS_DIR, file)
            archived_name = file.replace(".log", ".log.gz")
            archived_path = os.path.join(ARCHIVE_DIR, archived_name)

            try:
                # Сжимаем файл gzip и перемещаем в архив
                with open(old_log_path, 'rb') as f_in:
                    with gzip.open(archived_path, 'wb') as f_out:
                        shutil.copyfileobj(f_in, f_out)

                # Удаляем оригинальный файл
                os.remove(old_log_path)
                print(f"Лог-файл {file} архивирован: {archived_name}")
            except Exception as e:
                print(f"Ошибка при архивации {file}: {e}")


# Инициализация логгера
logger = setup_logger()
logging.info(f"Логирование запущено. Активный лог-файл: {log_filename}")