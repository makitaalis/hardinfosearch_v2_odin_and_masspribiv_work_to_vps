# Параметры для работы бота
import logging
import os
from dotenv import load_dotenv

# Загружаем переменные окружения
load_dotenv()

# Основные параметры бота
ADMIN_ID = int(os.getenv("ADMIN_ID")) if os.getenv("ADMIN_ID") else 0
REQUEST_COST = float(os.getenv("REQUEST_COST", "0.05"))  # Стоимость запроса по умолчанию 0.05

# Настройки веб-сессий
USE_WEB_SESSIONS = os.getenv("USE_WEB_SESSIONS", "1") == "1"  # По умолчанию используем веб
MAX_WEB_SESSIONS = int(os.getenv("MAX_WEB_SESSIONS", "50"))
SESSION_REFRESH_INTERVAL = int(os.getenv("SESSION_REFRESH_INTERVAL", "3600"))  # в секундах

# Путь к базе данных
DB_PATH = "database/bot.db"

# Путь к файлу с учетными данными
CREDENTIALS_FILE = os.getenv("CREDENTIALS_FILE", "credentials.txt")


# Функция для загрузки учетных данных
def load_credentials():
    """Загружает учетные данные из файла"""
    credentials = []

    if not os.path.exists(CREDENTIALS_FILE):
        logging.warning(f"Credentials file {CREDENTIALS_FILE} not found, using default credentials")
        # Используем дефолтные учетные данные
        return [("CipkaCapuchinka", "4uTStetepXK1p3vn")]

    try:
        with open(CREDENTIALS_FILE, 'r', encoding='utf-8') as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith('#'):
                    continue

                parts = line.split(':', 1)
                if len(parts) == 2:
                    login, password = parts
                    credentials.append((login.strip(), password.strip()))
    except Exception as e:
        logging.error(f"Error loading credentials: {str(e)}")

    if not credentials:
        logging.warning("No valid credentials found, using default credentials")
        # Используем дефолтные учетные данные
        return [("CipkaCapuchinka", "4uTStetepXK1p3vn")]

    return credentials