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
    """
    Загружает учетные данные из файла
    Improved to handle file errors more gracefully
    """
    credentials = []

    # Check if credentials file exists
    if not os.path.exists(CREDENTIALS_FILE):
        logging.warning(f"Credentials file {CREDENTIALS_FILE} not found, using default credentials")
        # Return default credentials - just one set to avoid authentication spikes
        return [("CipkaCapuchinka", "4uTStetepXK1p3vn")]

    try:
        with open(CREDENTIALS_FILE, 'r', encoding='utf-8') as f:
            for line_number, line in enumerate(f, 1):
                try:
                    line = line.strip()
                    if not line or line.startswith('#'):
                        continue

                    # Support both colon and space as separators
                    if ':' in line:
                        parts = line.split(':', 1)
                    else:
                        parts = line.split(None, 1)

                    if len(parts) == 2:
                        login, password = parts
                        credentials.append((login.strip(), password.strip()))
                    else:
                        logging.warning(f"Invalid credential format at line {line_number}, expected 'login:password'")
                except Exception as line_error:
                    logging.error(f"Error parsing credential at line {line_number}: {line_error}")
    except Exception as e:
        logging.error(f"Error loading credentials: {str(e)}")

    if not credentials:
        logging.warning("No valid credentials found, using default credentials")
        # Use default credentials as fallback
        return [("CipkaCapuchinka", "4uTStetepXK1p3vn")]

    # Log how many credentials were successfully loaded
    logging.info(f"Successfully loaded {len(credentials)} credentials")
    return credentials