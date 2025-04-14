import sqlite3
import logging
from bot.config import DB_PATH

def create_analytics_tables():
    """
    Создает минимальные таблицы для базового функционирования
    """
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    try:
        # Таблица для информации о системе
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS system_info (
                key TEXT PRIMARY KEY,
                value TEXT,
                updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
            )
        ''')

        conn.commit()
        logging.info("Базовые таблицы успешно созданы")

        return True, "Базовые таблицы успешно созданы"
    except Exception as e:
        conn.rollback()
        logging.error(f"Ошибка при создании базовых таблиц: {e}")
        return False, f"Ошибка при создании базовых таблиц: {str(e)}"
    finally:
        conn.close()

def log_request(user_id, query, request_type, source, success, execution_time, response_size):
    """
    Минимальный логгер запросов - только пишет в обычный лог, без сохранения в БД
    """
    logging.info(f"Запрос: user_id={user_id}, query={query}, тип={request_type}, источник={source}, успех={success}")

def log_financial_operation(user_id, operation_type, amount, balance_before, balance_after, admin_id=None, comment=None):
    """
    Минимальный логгер финансовых операций - только пишет в обычный лог, без сохранения в БД
    """
    logging.info(f"Финансовая операция: user_id={user_id}, тип={operation_type}, сумма={amount}, баланс до={balance_before}, баланс после={balance_after}")

def log_error(error_type, error_message, stack_trace=None, user_id=None, request_data=None):
    """
    Логирует ошибку
    """
    logging.error(f"Ошибка: {error_type}, сообщение={error_message}")
    if stack_trace:
        logging.error(f"Stack trace: {stack_trace}")
    if user_id:
        logging.error(f"От пользователя: {user_id}")

def log_user_event(user_id, event_type, event_data=None, ip_address=None):
    """
    Логирует действие пользователя
    """
    logging.info(f"Действие пользователя: user_id={user_id}, тип={event_type}")
    if event_data:
        logging.info(f"Данные события: {event_data}")