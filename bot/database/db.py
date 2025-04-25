import json
import os
import sqlite3
import logging
import time
import traceback
from datetime import datetime
from mailbox import Message
from pathlib import Path
from typing import Tuple

import bcrypt  # <-- pip install bcrypt

from bot.config import ADMIN_ID, REQUEST_COST  # Добавлен импорт REQUEST_COST
# Импорт функций для улучшенного логирования
from bot.analytics import log_financial_operation, log_user_event, log_error


DB_PATH = "database/bot.db"
db_folder = Path("database")
db_folder.mkdir(exist_ok=True)


def setup_database():
    """
    Создаёт (при необходимости) таблицы в базе данных и индексы для оптимизации.
    """
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    # Таблица для версии БД
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS db_version (
            version INTEGER PRIMARY KEY
        )
    ''')

    # Получаем текущую версию БД
    cursor.execute("SELECT version FROM db_version LIMIT 1")
    row = cursor.fetchone()
    current_version = row[0] if row else 0

    # Таблица пользователей (старая)
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS users (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            telegram_id INTEGER UNIQUE,
            login TEXT UNIQUE,
            password_hash TEXT,
            balance REAL DEFAULT 0.0,
            failed_attempts INTEGER DEFAULT 0,
            is_blocked INTEGER DEFAULT 0,
            session_active INTEGER DEFAULT 0,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP
        )
    ''')

    # === Новая таблица для параллельных сессий ===
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS active_sessions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            login TEXT,
            telegram_id INTEGER,
            is_active INTEGER DEFAULT 1
        )
    ''')

    # Таблица логов админа
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS admin_logs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            admin_id INTEGER,
            action TEXT,
            timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
        )
    ''')

    # Таблица кэша
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS cache (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER,
            query TEXT,
            response TEXT,
            timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(user_id, query)
        )
    ''')

    # Добавляем таблицу для логирования административных действий
    cursor.execute('''
            CREATE TABLE IF NOT EXISTS admin_logs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                admin_id INTEGER,
                action TEXT,
                details TEXT,
                timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
            )
        ''')

    # Добавляем индексы, если версия БД < 1
    if current_version < 1:
        # Индексы для таблицы users
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_users_telegram_id ON users(telegram_id)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_users_login ON users(login)')

        # Индексы для таблицы active_sessions
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_sessions_telegram_id ON active_sessions(telegram_id)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_sessions_login ON active_sessions(login)')

        # Индексы для таблицы cache
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_cache_user_query ON cache(user_id, query)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_cache_timestamp ON cache(timestamp)')

        # Обновляем версию БД
        if row:
            cursor.execute("UPDATE db_version SET version = 1")
        else:
            cursor.execute("INSERT INTO db_version (version) VALUES (1)")

        logging.info("База данных обновлена до версии 1 (добавлены индексы)")

    conn.commit()
    conn.close()

    # Запускаем миграции после создания таблиц
    run_migrations()

    # Исправляем структуру базы данных, если есть проблемы
    fix_database_structure()

    logging.info("База данных инициализирована (users, active_sessions, admin_logs, cache).")

# Добавление системы миграций для базы данных
def run_migrations():
    """
    Enhanced database migration system with transaction safety,
    comprehensive error handling, and performance optimizations
    """
    conn = None
    started_at = time.time()

    try:
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()

        # Enable WAL mode for better performance
        try:
            cursor.execute("PRAGMA journal_mode=WAL")
            journal_mode = cursor.fetchone()[0]
            logging.info(f"Database journal mode: {journal_mode}")
        except Exception as e:
            logging.warning(f"Could not set WAL journal mode: {e}")

        # Create version table if it doesn't exist
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS db_version (
                version INTEGER PRIMARY KEY,
                migrated_at DATETIME DEFAULT CURRENT_TIMESTAMP
            )
        """)

        # Check if description column exists in db_version table
        cursor.execute("PRAGMA table_info(db_version)")
        columns = [info[1] for info in cursor.fetchall()]

        # Add description column if it's missing
        if 'description' not in columns:
            cursor.execute("ALTER TABLE db_version ADD COLUMN description TEXT")
            logging.info("Added description column to db_version table")

        # Get current version
        cursor.execute("SELECT version FROM db_version ORDER BY version DESC LIMIT 1")
        row = cursor.fetchone()
        current_version = row[0] if row else 0

        logging.info(f"Current database version: {current_version}")

        # Start transaction for all migrations
        conn.execute("BEGIN TRANSACTION")

        try:
            # Migration 1: Add basic indexes
            if current_version < 1:
                migration_start = time.time()
                logging.info("Applying migration #1: Adding basic indexes")

                # Create indexes for users table
                cursor.execute('CREATE INDEX IF NOT EXISTS idx_users_telegram_id ON users(telegram_id)')
                cursor.execute('CREATE INDEX IF NOT EXISTS idx_users_login ON users(login)')

                # Create indexes for active_sessions table
                cursor.execute('CREATE INDEX IF NOT EXISTS idx_sessions_telegram_id ON active_sessions(telegram_id)')
                cursor.execute('CREATE INDEX IF NOT EXISTS idx_sessions_login ON active_sessions(login)')

                # Create indexes for cache table
                cursor.execute('CREATE INDEX IF NOT EXISTS idx_cache_user_query ON cache(user_id, query)')
                cursor.execute('CREATE INDEX IF NOT EXISTS idx_cache_timestamp ON cache(timestamp)')

                # Check if description column exists before using it
                cursor.execute("PRAGMA table_info(db_version)")
                columns = [info[1] for info in cursor.fetchall()]

                if 'description' in columns:
                    cursor.execute(
                        "INSERT INTO db_version (version, description) VALUES (?, ?)",
                        (1, 'Added basic indexes')
                    )
                else:
                    cursor.execute(
                        "INSERT INTO db_version (version) VALUES (?)",
                        (1,)
                    )

                migration_time = time.time() - migration_start
                logging.info(f"Migration #1 completed in {migration_time:.2f}s")

            # [Continue with migrations 2-7...]
            # For each migration, wrap the version insertion with the same check:

            # if 'description' in columns:
            #     cursor.execute(
            #         "INSERT INTO db_version (version, description) VALUES (?, ?)",
            #         (version_number, description_text)
            #     )
            # else:
            #     cursor.execute(
            #         "INSERT INTO db_version (version) VALUES (?)",
            #         (version_number,)
            #     )

            # Commit all migrations
            conn.commit()

            total_time = time.time() - started_at
            logging.info(f"Database migrations completed successfully in {total_time:.2f}s")

            # Get current version after all migrations
            cursor.execute("SELECT version FROM db_version ORDER BY version DESC LIMIT 1")
            final_version = cursor.fetchone()[0]
            logging.info(f"Current database version: {final_version}")

            return True

        except Exception as e:
            # Roll back on any error
            conn.rollback()
            error_traceback = traceback.format_exc()
            logging.error(f"Error during database migrations: {e}\n{error_traceback}")

            # Try to log the error in the database
            try:
                # Create error logging table if needed
                conn.execute("""
                    CREATE TABLE IF NOT EXISTS migration_errors (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        version INTEGER,
                        error TEXT,
                        timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
                    )
                """)

                # Log the error
                conn.execute(
                    "INSERT INTO migration_errors (version, error) VALUES (?, ?)",
                    (current_version + 1, str(e))
                )
                conn.commit()
            except:
                # If we can't even log the error, just continue
                pass

            # Log error for analytics
            log_error("MigrationError", str(e), error_traceback)
            return False

    except Exception as e:
        # Handle connection errors
        error_traceback = traceback.format_exc()
        logging.error(f"Fatal error initializing migrations: {e}\n{error_traceback}")

        # Log error for analytics
        log_error("DatabaseConnectionError", str(e), error_traceback)
        return False

    finally:
        # Always close the connection
        if conn:
            conn.close()

# ===================== Хеширование паролей (bcrypt) =====================

def _hash_password(plain_password: str) -> str:
    salt = bcrypt.gensalt()
    hashed = bcrypt.hashpw(plain_password.encode("utf-8"), salt)
    return hashed.decode("utf-8")

def _check_password(plain_password: str, stored_hash: str) -> bool:
    """
    Проверяет соответствие пароля хешу с учетом различных форматов хранения.
    """
    try:
        # Если хеш в формате bcrypt
        if isinstance(stored_hash, str) and (stored_hash.startswith("$2b$") or stored_hash.startswith("$2a$")):
            return bcrypt.checkpw(plain_password.encode("utf-8"), stored_hash.encode("utf-8"))

        # Если хеш в другом формате (SHA-256 или простой текст)
        import hashlib
        hashed_input = hashlib.sha256(plain_password.encode()).hexdigest()
        return hashed_input == stored_hash
    except Exception as e:
        logging.error(f"Ошибка при проверке пароля: {e}")
        # Логируем ошибку для аналитики
        log_error("PasswordCheckError", str(e), traceback.format_exc())
        # В случае ошибки, попробуем прямое сравнение
        return plain_password == stored_hash

# ===================== Авторизация и сессия =====================

def verify_password(login: str, password: str, user_id: int, user_info=None) -> Tuple[bool, str]:
    """
    Проверяет комбинацию логин+пароль и создаёт сессию

    :param login: Логин пользователя
    :param password: Пароль пользователя
    :param user_id: ID пользователя в Telegram
    :param user_info: Словарь с дополнительной информацией о пользователе
    :return: Кортеж (успех, сообщение)
    """
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    # Получаем данные пользователя
    cursor.execute("SELECT login, password_hash, is_blocked, failed_attempts FROM users WHERE login = ?", (login,))
    user_data = cursor.fetchone()

    if not user_data:
        conn.close()
        return False, "Пользователь не найден."

    db_login, password_hash, blocked, attempts = user_data
    logging.info(f"Получены данные пользователя: blocked={blocked}, attempts={attempts}")

    # Проверяем блокировку
    if blocked == 1:
        conn.close()
        return False, "Аккаунт заблокирован. Свяжитесь с администратором."

    # Проверяем пароль
    is_valid = bcrypt.checkpw(password.encode('utf-8'), password_hash.encode('utf-8'))
    logging.info(f"Результат проверки bcrypt пароля: {is_valid}")

    if not is_valid:
        # Обрабатываем неверный пароль
        conn.close()
        return False, "Неверный пароль."

    try:
        # Транзакция для безопасного обновления данных пользователя
        cursor.execute("BEGIN TRANSACTION")

        # Обновляем статистику входов, но НЕ меняем telegram_id
        if user_info:
            cursor.execute("""
                UPDATE users 
                SET failed_attempts = 0, last_login_at = datetime('now'),
                    login_count = login_count + 1,
                    first_name = ?, last_name = ?, username = ?
                WHERE login = ?
            """, (
                user_info.get('first_name', ''),
                user_info.get('last_name', ''),
                user_info.get('username', ''),
                login))
        else:
            cursor.execute("""
                UPDATE users 
                SET failed_attempts = 0, last_login_at = datetime('now'),
                    login_count = login_count + 1
                WHERE login = ?
            """, (login,))

        # Проверяем, существует ли уже активная сессия для этого пользователя и логина
        cursor.execute("""
            SELECT id FROM active_sessions 
            WHERE login = ? AND telegram_id = ?
        """, (login, user_id))
        existing_session = cursor.fetchone()

        if existing_session:
            # Если сессия уже существует, просто активируем ее
            cursor.execute("""
                UPDATE active_sessions 
                SET is_active = 1 
                WHERE login = ? AND telegram_id = ?
            """, (login, user_id))
        else:
            # Если сессии нет, создаем новую
            cursor.execute("""
                INSERT INTO active_sessions (login, telegram_id, is_active)
                VALUES (?, ?, 1)
            """, (login, user_id))

        cursor.execute("COMMIT")
        logging.info(f"Успешная авторизация пользователя {login} (user_id={user_id})")
        return True, "Авторизация успешна."

    except sqlite3.Error as e:
        cursor.execute("ROLLBACK")
        logging.error(f"Ошибка базы данных: {str(e)}")
        return False, f"Ошибка при авторизации: {str(e)}"
    finally:
        conn.close()

def check_active_session(user_id: int) -> bool:
    """
    Проверяет, есть ли у пользователя (Telegram ID) активная сессия:
      1) По-старому: SELECT session_active FROM users WHERE telegram_id=?
      2) Если не сработало, проверяем active_sessions (новая таблица).
    """
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    # «Старый» способ
    cursor.execute("SELECT session_active FROM users WHERE telegram_id=?", (user_id,))
    row = cursor.fetchone()

    # Проверяем также нашу новую таблицу active_sessions
    cursor.execute("""
        SELECT id FROM active_sessions
        WHERE telegram_id=? AND is_active=1
    """, (user_id,))
    row2 = cursor.fetchone()
    conn.close()

    if row2:  # если в active_sessions есть активная запись - вернуть True
        return True

    # Иначе опираемся на «старый» результат
    if not row:
        return False
    return (row[0] == 1)

def logout_user(telegram_id: int):
    """
    Сбрасывает session_active=0 в users (старый способ).
    Дополнительно деактивирует сессию в active_sessions (новая таблица).
    """
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    # Старая логика
    cursor.execute("""
        UPDATE users
        SET session_active=0
        WHERE telegram_id=?
    """, (telegram_id,))

    # Новая логика: активную сессию помечаем is_active=0
    cursor.execute("""
        UPDATE active_sessions
        SET is_active=0
        WHERE telegram_id=?
    """, (telegram_id,))

    conn.commit()
    conn.close()

    # Логируем выход пользователя
    log_user_event(
        user_id=telegram_id,
        event_type="logout",
        event_data=None
    )

# ===================== Добавляем/обновляем запись о сессии в active_sessions =====================

def _add_session_in_active_sessions(login: str, telegram_id: int):
    """
    Запоминает в таблице active_sessions, что телеграм-пользователь (telegram_id)
    вошёл под логином `login`. Если уже есть запись c (telegram_id, is_active=1),
    оставляем как есть, иначе создаём новую.
    """
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute("""
        SELECT id FROM active_sessions
        WHERE telegram_id=? AND is_active=1
    """, (telegram_id,))
    row = cursor.fetchone()

    if not row:
        # Создаём новую запись
        cursor.execute("""
            INSERT INTO active_sessions (login, telegram_id, is_active)
            VALUES (?, ?, 1)
        """, (login, telegram_id))

    conn.commit()
    conn.close()

# ===================== Баланс пользователя =====================

def get_user_balance(user_id: int):
    """
    Возвращает баланс «старым способом».
    Если запись не найдена, смотрим в active_sessions, чтобы узнать login,
    и берём баланс из users по этому логину.
    """
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    # Старая логика
    cursor.execute("""
        SELECT balance FROM users
        WHERE telegram_id=? AND session_active=1
    """, (user_id,))
    row = cursor.fetchone()
    if row:
        conn.close()
        return row[0]

    # Новая логика - если пользователь не найден «по-старому»:
    cursor.execute("""
        SELECT login FROM active_sessions
        WHERE telegram_id=? AND is_active=1
    """, (user_id,))
    row2 = cursor.fetchone()
    if not row2:
        conn.close()
        return None

    login = row2[0]
    # Берём баланс по логину
    cursor.execute("SELECT balance FROM users WHERE login=?", (login,))
    row3 = cursor.fetchone()
    conn.close()

    if not row3:
        return None
    return row3[0]

def deduct_balance(user_id: int):
    """
    «Старым способом» пытается списать средства.
    Если не удалось — проверяем active_sessions, чтобы узнать login,
    и списываем по login.
    """
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    # Старая логика
    cursor.execute("""
        SELECT balance FROM users
        WHERE telegram_id=? AND session_active=1
    """, (user_id,))
    row = cursor.fetchone()

    if row:  # пользователь есть
        balance = row[0]
        if balance < REQUEST_COST:
            conn.close()
            return False, "Недостаточно средств! Пополните баланс."

        new_balance = round(balance - REQUEST_COST, 2)
        cursor.execute("""
            UPDATE users
            SET balance=?, requests_count = requests_count + 1
            WHERE telegram_id=? AND session_active=1
        """, (new_balance, user_id))
        conn.commit()
        conn.close()

        logging.info(f"📉 Списано {REQUEST_COST} у user_id={user_id}. Баланс до: {balance}, после: {new_balance}")

        # Логируем финансовую операцию
        log_financial_operation(
            user_id=user_id,
            operation_type='deduct',
            amount=REQUEST_COST,
            balance_before=balance,
            balance_after=new_balance
        )

        return True, f"Средства списаны. Ваш новый баланс: ${new_balance:.2f}"

    # Если row=None, значит «по-старому» не нашли -> смотрим active_sessions
    cursor.execute("""
        SELECT login FROM active_sessions
        WHERE telegram_id=? AND is_active=1
    """, (user_id,))
    row2 = cursor.fetchone()
    if not row2:
        conn.close()
        return False, "Ошибка! Вы не вошли в систему."

    login = row2[0]
    # Получаем текущий баланс по login
    cursor.execute("SELECT balance FROM users WHERE login=?", (login,))
    row3 = cursor.fetchone()
    if not row3:
        conn.close()
        return False, "Ошибка! Пользователь не найден."

    balance = row3[0]
    if balance < REQUEST_COST:
        conn.close()
        return False, "Недостаточно средств! Пополните баланс."

    new_balance = round(balance - REQUEST_COST, 2)
    cursor.execute("UPDATE users SET balance=?, requests_count = requests_count + 1 WHERE login=?",
                   (new_balance, login))
    conn.commit()
    conn.close()

    logging.info(f"📉 Списано {REQUEST_COST} у TG={user_id}, (login={login}). Баланс был {balance}, стал {new_balance}")

    # Логируем финансовую операцию
    log_financial_operation(
        user_id=user_id,
        operation_type='deduct',
        amount=REQUEST_COST,
        balance_before=balance,
        balance_after=new_balance
    )

    return True, f"Средства списаны. Ваш новый баланс: ${new_balance:.2f}"

def add_balance(user_login: str, amount: float, admin_id=None):
    """
    Пополнение (не трогаем — логика старая).
    Добавлен параметр admin_id для логирования.
    """
    try:
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()

        # Получаем user_id и текущий баланс
        cursor.execute("SELECT telegram_id, balance FROM users WHERE login=?", (user_login,))
        row = cursor.fetchone()

        if row is None:
            conn.close()
            return False, "Ошибка! Пользователь не найден."

        user_id, current_balance = row
        new_balance = current_balance + amount
        cursor.execute("UPDATE users SET balance=? WHERE login=?", (new_balance, user_login))
        conn.commit()
        conn.close()

        logging.info(
            f"Админ пополнил баланс пользователя {user_login} на ${amount:.2f}. Новый баланс: ${new_balance:.2f}")

        # Логируем финансовую операцию
        log_financial_operation(
            user_id=user_id,
            operation_type='add_balance',
            amount=amount,
            balance_before=current_balance,
            balance_after=new_balance,
            admin_id=admin_id,
            comment=f"Пополнение баланса администратором"
        )

        # Логируем действие пользователя
        log_user_event(
            user_id=user_id,
            event_type="balance_increase",
            event_data=json.dumps({"amount": amount, "admin_id": admin_id})
        )

        return True, f"Баланс пользователя {user_login} пополнен на ${amount:.2f}. Новый баланс: ${new_balance:.2f}"
    except Exception as e:
        logging.error(f"Ошибка при пополнении баланса: {str(e)}")
        # Логируем ошибку для аналитики
        log_error("BalanceUpdateError", str(e), traceback.format_exc(), admin_id)
        return False, "Произошла ошибка при пополнении баланса."

# ===================== Получение списка пользователей =====================

def get_users_paginated(page=1, page_size=5):
    """
    Старая логика вывода списка (login, balance).
    """
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    offset = (page - 1) * page_size
    cursor.execute("""
        SELECT login, balance
        FROM users
        ORDER BY login ASC
        LIMIT ? OFFSET ?
    """, (page_size, offset))
    users = cursor.fetchall()

    cursor.execute("SELECT COUNT(*) FROM users")
    total = cursor.fetchone()[0]

    conn.close()
    return users, total

# ===================== Проверка баланса (предупреждение) =====================

def check_low_balance(user_id: int):
    """
    Старая логика. Если не сработало — смотрим в active_sessions и проверяем balance по login.
    """
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute("""
        SELECT balance FROM users
        WHERE telegram_id=? AND session_active=1
    """, (user_id,))
    row = cursor.fetchone()
    if row:
        balance = row[0]
        conn.close()
        if balance <= (20 * REQUEST_COST):
            return True, f"⚠ Ваш баланс низкий (${balance:.2f}). Пополните его!"
        return False, ""

    # Новая логика
    cursor.execute("""
        SELECT login FROM active_sessions
        WHERE telegram_id=? AND is_active=1
    """, (user_id,))
    row2 = cursor.fetchone()
    if not row2:
        conn.close()
        return False, ""

    login = row2[0]
    cursor.execute("SELECT balance FROM users WHERE login=?", (login,))
    row3 = cursor.fetchone()
    conn.close()

    if row3:
        balance = row3[0]
        if balance <= (20 * REQUEST_COST):
            return True, f"⚠ Ваш баланс низкий (${balance:.2f}). Пополните его!"
    return False, ""

def get_users_with_zero_balance():
    """
    Старая логика, выводим login, telegram_id там, где balance=0.
    """
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute("SELECT login, telegram_id FROM users WHERE balance=0")
    rows = cursor.fetchall()
    conn.close()
    return rows

# ===================== Кэширование =====================

def fix_cache_table_structure():
    """
    Исправляет структуру таблицы cache, добавляя столбец source если его нет
    Это решает проблему с ошибкой "no such column: source" в логах
    """
    try:
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()

        # Проверяем наличие столбца source в таблице cache
        cursor.execute("PRAGMA table_info(cache)")
        columns = [col[1] for col in cursor.fetchall()]

        if 'source' not in columns:
            # Добавляем столбец source если его нет
            logging.info("Добавление столбца 'source' в таблицу cache")
            cursor.execute("ALTER TABLE cache ADD COLUMN source TEXT DEFAULT 'system'")
            conn.commit()
            logging.info("Столбец 'source' успешно добавлен в таблицу cache")

        conn.close()
        return True
    except Exception as e:
        logging.error(f"Ошибка при исправлении таблицы cache: {e}")
        return False


def get_cached_response(user_id, query):
    """
    Получает кэшированный ответ для запроса, если он существует.
    Возвращает (найден, ответ, источник).
    Исправлена для работы даже если колонки source нет
    """
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    try:
        # Сначала проверяем, есть ли столбец source в таблице
        cursor.execute("PRAGMA table_info(cache)")
        columns = [col[1] for col in cursor.fetchall()]
        has_source_column = 'source' in columns

        if has_source_column:
            # Если есть столбец source, используем его
            cursor.execute("""
                SELECT response, source FROM cache 
                WHERE (user_id = ? OR user_id IS NULL) AND query = ? 
                ORDER BY timestamp DESC LIMIT 1
            """, (user_id, query))
        else:
            # Если нет столбца source, запрашиваем только response
            cursor.execute("""
                SELECT response FROM cache 
                WHERE (user_id = ? OR user_id IS NULL) AND query = ? 
                ORDER BY timestamp DESC LIMIT 1
            """, (user_id, query))

        result = cursor.fetchone()

        if result:
            if has_source_column:
                response, source = result
                return True, response, source
            else:
                response = result[0]
                return True, response, "система"  # Дефолтный источник

        return False, None, None
    except Exception as e:
        logging.error(f"Ошибка при получении кэша: {e}")
        return False, None, None
    finally:
        conn.close()

def get_global_cached_response(query: str):
    """
    Получает кэшированный ответ для запроса из глобального кэша (любой пользователь).
    Если несколько пользователей кэшировали один и тот же запрос, берем самый свежий.
    """
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute("""
        SELECT response FROM cache
        WHERE query = ?
        ORDER BY timestamp DESC
        LIMIT 1
    """, (query,))
    row = cursor.fetchone()
    conn.close()

    if not row or not row[0]:
        return None

    # Если кэш есть, пытаемся его декодировать
    try:
        if isinstance(row[0], str):
            return json.loads(row[0])
        elif isinstance(row[0], (list, dict)):
            return row[0]
        else:
            logging.error(f"⚠ Ошибка: кэш для query={query} имеет неожиданный тип {type(row[0])}.")
            return None
    except json.JSONDecodeError:
        logging.error(f"⚠ Ошибка декодирования JSON-кэша для query={query}.")
        return None
    except Exception as e:
        logging.error(f"⚠ Неожиданная ошибка при работе с кэшем: {e}")
        return None


def get_best_cached_response(user_id: int, query: str):
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    try:
        # Check if source column exists
        cursor.execute("PRAGMA table_info(cache)")
        columns = [col[1] for col in cursor.fetchall()]
        has_source_column = 'source' in columns

        if has_source_column:
            cursor.execute("""
                SELECT response, 
                    CASE WHEN user_id = ? THEN 'личный' ELSE 'общий' END as source 
                FROM cache
                WHERE query = ?
                ORDER BY CASE WHEN user_id = ? THEN 0 ELSE 1 END, timestamp DESC
                LIMIT 1
            """, (user_id, query, user_id))
        else:
            cursor.execute("""
                SELECT response FROM cache
                WHERE query = ?
                ORDER BY CASE WHEN user_id = ? THEN 0 ELSE 1 END, timestamp DESC
                LIMIT 1
            """, (query, user_id))

        row = cursor.fetchone()

        if not row:
            return False, None, None

        data = row[0]
        source = row[1] if has_source_column and len(row) > 1 else 'система'

        # Parse JSON if needed
        if isinstance(data, str):
            try:
                result = json.loads(data)
            except:
                result = data
        else:
            result = data

        return True, result, source
    except Exception as e:
        logging.error(f"Error in get_best_cached_response: {e}")
        return False, None, None
    finally:
        conn.close()

def save_response_to_cache(user_id: int, query: str, response, source='user'):
    """
    Стандартизированное сохранение кэша с учетом возможного отсутствия колонки source.
    """
    try:
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()

        # Проверяем наличие столбца source
        cursor.execute("PRAGMA table_info(cache)")
        columns = [col[1] for col in cursor.fetchall()]
        has_source_column = 'source' in columns

        # Определяем размер ответа для статистики
        response_size = 0

        # Стандартизация: всегда приводим к JSON строке
        if not isinstance(response, str):
            try:
                json_response = json.dumps(response, ensure_ascii=False)
                response_size = len(json_response.encode('utf-8'))
                response = json_response
            except Exception as e:
                logging.error(f"Ошибка при преобразовании в JSON: {e}")
                response = str(response)
                response_size = len(response.encode('utf-8'))
        else:
            response_size = len(response.encode('utf-8'))

        # SQL запрос зависит от наличия столбца source
        if has_source_column:
            cursor.execute("""
                INSERT INTO cache (user_id, query, response, source)
                VALUES (?, ?, ?, ?)
                ON CONFLICT(user_id, query)
                DO UPDATE SET response=excluded.response, timestamp=datetime('now'), source=excluded.source
            """, (user_id, query, response, source))
        else:
            cursor.execute("""
                INSERT INTO cache (user_id, query, response)
                VALUES (?, ?, ?)
                ON CONFLICT(user_id, query)
                DO UPDATE SET response=excluded.response, timestamp=datetime('now')
            """, (user_id, query, response))

        conn.commit()
        logging.info(f"Кэш сохранен для user_id={user_id}, query={query[:20]}...")
    except Exception as e:
        logging.error(f"Ошибка при сохранении кэша: {e}")
    finally:
        if conn:
            conn.close()

def clear_old_cache():
    """
    Удаляет из кэша записи старше 24 часов.
    """
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute("""
        DELETE FROM cache
        WHERE timestamp <= datetime('now', '-1 day')
    """)
    conn.commit()
    conn.close()

def create_user(login, password, balance=0.0):
    """
    Стандартное создание пользователя с проверкой структуры таблицы.
    """
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    cursor.execute("SELECT id FROM users WHERE login = ?", (login,))
    if cursor.fetchone():
        conn.close()
        return False, "Ошибка: этот логин уже занят."

    password_hash = _hash_password(password)

    try:
        # Проверяем наличие столбца created_at
        cursor.execute("PRAGMA table_info(users)")
        columns = [info[1] for info in cursor.fetchall()]

        if 'created_at' in columns:
            cursor.execute("""
                INSERT INTO users (login, password_hash, balance, created_at)
                VALUES (?, ?, ?, datetime('now'))
            """, (login, password_hash, balance))
        else:
            cursor.execute("""
                INSERT INTO users (login, password_hash, balance)
                VALUES (?, ?, ?)
            """, (login, password_hash, balance))

        # Получаем id нового пользователя
        cursor.execute("SELECT last_insert_rowid()")
        user_id = cursor.fetchone()[0]

        conn.commit()
        conn.close()

        logging.info(f"Создан новый пользователь: {login}, баланс: ${balance:.2f}")

        return True, f"✅ Пользователь {login} создан! Баланс: ${balance:.2f}."
    except Exception as e:
        conn.close()
        logging.error(f"Ошибка создания пользователя: {str(e)}")
        return False, f"Ошибка при создании пользователя: {str(e)}"

def delete_cached_response(user_id: int, query: str):
    """
    Удаляем кэш, если он есть.
    """
    conn = None
    try:
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        cursor.execute("""
            DELETE FROM cache
            WHERE user_id = ? AND query = ?
        """, (user_id, query))
        conn.commit()
        logging.info(f"🗑 Кэш удалён для user_id={user_id}, query={query}")
    except Exception as e:
        logging.error(f"Ошибка при удалении кэша: {e}")
    finally:
        if conn:
            conn.close()

def check_balance_for_mass_search(user_id: int, count: int):
    """
    Проверяет, достаточно ли у пользователя средств для выполнения массового пробива.
    :param user_id: ID пользователя
    :param count: Количество запросов
    :return: (достаточно, текущий_баланс, требуемая_сумма)
    """
    balance = get_user_balance(user_id)
    if balance is None:
        return False, 0, count * REQUEST_COST

    required_amount = count * REQUEST_COST
    return balance >= required_amount, balance, required_amount

def refund_balance(user_id: int):
    """
    Возвращает на баланс пользователя стоимость одного запроса.
    Используется для возврата средств, если API запрос вернул пустой результат или ошибку.
    """
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    # Старая логика
    cursor.execute("""
        SELECT balance FROM users
        WHERE telegram_id=? AND session_active=1
    """, (user_id,))
    row = cursor.fetchone()

    if row:  # пользователь есть
        balance = row[0]
        new_balance = round(balance + REQUEST_COST, 2)
        cursor.execute("""
            UPDATE users
            SET balance=?
            WHERE telegram_id=? AND session_active=1
        """, (new_balance, user_id))
        conn.commit()
        conn.close()

        logging.info(
            f"📈 Возвращено {REQUEST_COST} пользователю user_id={user_id}. Баланс до: {balance}, после: {new_balance}")

        # Логируем возврат средств
        log_financial_operation(
            user_id=user_id,
            operation_type='refund',
            amount=REQUEST_COST,
            balance_before=balance,
            balance_after=new_balance,
            comment="Возврат за пустой ответ"
        )

        return True, f"Средства возвращены из-за пустого ответа. Ваш новый баланс: ${new_balance:.2f}"

    # Если row=None, значит «по-старому» не нашли -> смотрим active_sessions
    cursor.execute("""
        SELECT login FROM active_sessions
        WHERE telegram_id=? AND is_active=1
    """, (user_id,))
    row2 = cursor.fetchone()
    if not row2:
        conn.close()
        return False, "Ошибка! Вы не вошли в систему."

    login = row2[0]
    # Получаем текущий баланс по login
    cursor.execute("SELECT balance FROM users WHERE login=?", (login,))
    row3 = cursor.fetchone()
    if not row3:
        conn.close()
        return False, "Ошибка! Пользователь не найден."

    balance = row3[0]
    new_balance = round(balance + REQUEST_COST, 2)
    cursor.execute("UPDATE users SET balance=? WHERE login=?", (new_balance, login))
    conn.commit()
    conn.close()

    logging.info(
        f"📈 Возвращено {REQUEST_COST} пользователю TG={user_id}, (login={login}). Баланс был {balance}, стал {new_balance}")

    # Логируем возврат средств
    log_financial_operation(
        user_id=user_id,
        operation_type='refund',
        amount=REQUEST_COST,
        balance_before=balance,
        balance_after=new_balance,
        comment="Возврат за пустой ответ"
    )

    return True, f"Средства возвращены из-за пустого ответа. Ваш новый баланс: ${new_balance:.2f}"


# В db.py проверить, что функция НЕ объявлена как async
def mass_refund_balance(user_id, queries_count):
    """
    Возвращает средства пользователю после неудачного массового пробива
    Возвращает кортеж (успех, сообщение)
    """
    try:
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()

        # Получаем баланс пользователя
        cursor.execute("SELECT balance, login FROM users WHERE telegram_id = ?", (user_id,))
        result = cursor.fetchone()

        if not result:
            conn.close()
            return False, "Пользователь не найден"

        current_balance, login = result
        refund_amount = queries_count * REQUEST_COST
        new_balance = current_balance + refund_amount

        # Обновляем баланс
        cursor.execute("UPDATE users SET balance = ? WHERE telegram_id = ?",
                       (new_balance, user_id))
        conn.commit()

        logging.info(f"📈 Массовый возврат: {refund_amount} пользователю TG={user_id}, "
                     f"(login={login}). Баланс был {current_balance}, стал {new_balance}")

        conn.close()
        return True, f"Средства возвращены на баланс: +${refund_amount:.2f}"
    except Exception as e:
        logging.error(f"Ошибка при возврате средств: {e}")
        return False, "Ошибка при возврате средств"

def fix_database_structure():
    """
    Принудительно добавляет отсутствующие столбцы в таблицу users.
    Вызывается один раз при запуске для исправления проблем с миграцией.
    """
    try:
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()

        # Получаем список существующих столбцов
        cursor.execute("PRAGMA table_info(users)")
        existing_columns = [info[1] for info in cursor.fetchall()]

        # Проверяем и добавляем отсутствующие столбцы
        # Обратите внимание, что для created_at мы не используем DEFAULT CURRENT_TIMESTAMP
        columns_to_add = {
            'created_at': 'DATETIME',  # Убрали DEFAULT CURRENT_TIMESTAMP
            'last_login_at': 'DATETIME',
            'login_count': 'INTEGER DEFAULT 0',
            'requests_count': 'INTEGER DEFAULT 0'
        }

        for column, data_type in columns_to_add.items():
            if column not in existing_columns:
                try:
                    cursor.execute(f"ALTER TABLE users ADD COLUMN {column} {data_type}")
                    logging.info(f"Добавлен столбец {column} в таблицу users")

                    # Если это столбец created_at, обновляем его значения на текущую дату
                    if column == 'created_at':
                        cursor.execute("""
                            UPDATE users 
                            SET created_at = datetime('now') 
                            WHERE created_at IS NULL
                        """)
                        logging.info("Установлены значения для столбца created_at")

                except Exception as e:
                    logging.error(f"Ошибка при добавлении столбца {column}: {e}")

        conn.commit()
        conn.close()
        logging.info("Структура базы данных исправлена")

        return True
    except Exception as e:
        logging.error(f"Ошибка при исправлении структуры БД: {e}")
        return False

def log_mass_search_start(user_id: int, file_path: str, valid_lines: int, total_cost: float):
    """
    Записывает информацию о начале массового пробива в журнал

    :param user_id: ID пользователя в Telegram
    :param file_path: Путь к файлу
    :param valid_lines: Количество валидных строк
    :param total_cost: Общая стоимость
    :return: ID записи в журнале
    """
    try:
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()

        # Создаем таблицу, если её нет
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS mass_search_logs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER,
                file_path TEXT,
                valid_lines INTEGER,
                total_cost REAL,
                start_time DATETIME DEFAULT CURRENT_TIMESTAMP,
                end_time DATETIME,
                status TEXT DEFAULT 'pending', -- pending, processing, completed, failed
                results_file TEXT,
                phones_found INTEGER DEFAULT 0,
                FOREIGN KEY(user_id) REFERENCES users(telegram_id)
            )
        ''')

        # Добавляем запись
        cursor.execute('''
            INSERT INTO mass_search_logs (
                user_id, file_path, valid_lines, total_cost, status
            ) VALUES (?, ?, ?, ?, 'pending')
        ''', (user_id, file_path, valid_lines, total_cost))

        # Получаем ID
        cursor.execute("SELECT last_insert_rowid()")
        log_id = cursor.fetchone()[0]

        conn.commit()

        return log_id
    except Exception as e:
        logging.error(f"Ошибка при записи данных о массовом пробиве: {e}")
        return None
    finally:
        if conn:
            conn.close()

def update_mass_search_status(log_id: int, status: str, results_file: str = None, phones_found: int = None, error_message: str = None):
    """
    Обновляет статус массового пробива

    :param log_id: ID записи в журнале
    :param status: Новый статус ('processing', 'completed', 'failed')
    :param results_file: Путь к файлу результатов (если есть)
    :param phones_found: Количество найденных телефонов (если есть)
    :param error_message: Сообщение об ошибке (если есть)
    """
    try:
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()

        update_dict = {"status": status}
        if results_file:
            update_dict["results_file"] = results_file
        if phones_found is not None:
            update_dict["phones_found"] = phones_found
        if error_message is not None:
            update_dict["error_message"] = error_message
        if status in ('completed', 'failed'):
            update_dict["end_time"] = "datetime('now')"

        # Формируем SQL запрос
        set_clauses = []
        values = []
        for key, value in update_dict.items():
            if key == "end_time":
                set_clauses.append(f"{key} = {value}")
            else:
                set_clauses.append(f"{key} = ?")
                values.append(value)

        sql = f"UPDATE mass_search_logs SET {', '.join(set_clauses)} WHERE id = ?"
        values.append(log_id)

        cursor.execute(sql, values)
        conn.commit()
    except Exception as e:
        logging.error(f"Ошибка при обновлении статуса массового пробива: {e}")
    finally:
        if conn:
            conn.close()

def get_mass_search_stats():
    """
    Получает статистику по массовым пробивам из БД

    :return: Словарь со статистикой
    """
    stats = {
        "total": 0,
        "completed": 0,
        "failed": 0,
        "phones_found": 0,
        "avg_time": None,
        "recent": []
    }

    try:
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()

        # Проверяем, существует ли таблица mass_search_logs
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='mass_search_logs'")
        if not cursor.fetchone():
            return stats

        # Общее количество пробивов
        cursor.execute("SELECT COUNT(*) FROM mass_search_logs")
        stats["total"] = cursor.fetchone()[0]

        # Количество завершенных успешно
        cursor.execute("SELECT COUNT(*) FROM mass_search_logs WHERE status='completed'")
        stats["completed"] = cursor.fetchone()[0]

        # Количество с ошибками
        cursor.execute("SELECT COUNT(*) FROM mass_search_logs WHERE status='failed'")
        stats["failed"] = cursor.fetchone()[0]

        # Общее количество найденных телефонов
        cursor.execute("SELECT SUM(phones_found) FROM mass_search_logs")
        result = cursor.fetchone()[0]
        stats["phones_found"] = result if result is not None else 0

        # Среднее время обработки
        cursor.execute("""
            SELECT AVG(CAST((julianday(completed_at) - julianday(started_at)) * 24 * 60 * 60 AS INTEGER)) 
            FROM mass_search_logs 
            WHERE status='completed' AND started_at IS NOT NULL AND completed_at IS NOT NULL
        """)
        stats["avg_time"] = cursor.fetchone()[0]

        # Последние 5 пробивов
        cursor.execute("""
            SELECT id, user_id, valid_lines, status, phones_found
            FROM mass_search_logs
            ORDER BY started_at DESC
            LIMIT 5
        """)
        stats["recent"] = [
            {
                "id": row[0],
                "user_id": row[1],
                "valid_lines": row[2],
                "status": row[3],
                "phones_found": row[4]
            }
            for row in cursor.fetchall()
        ]

        conn.close()
    except Exception as e:
        logging.error(f"Ошибка при получении статистики массовых пробивов: {e}")

    return stats

def batch_deduct_balance(user_id: int, request_count: int):
    """
    Списывает баланс для нескольких запросов одной транзакцией.
    Используется для массовых пробивов.

    Args:
        user_id: ID пользователя в Telegram
        request_count: Количество запросов

    Returns:
        (success, message, total_cost)
    """
    total_cost = round(REQUEST_COST * request_count, 2)

    conn = sqlite3.connect(DB_PATH)

    try:
        conn.execute("BEGIN TRANSACTION")
        cursor = conn.cursor()

        # Получаем текущий баланс
        cursor.execute("""
            SELECT balance FROM users
            WHERE telegram_id=? AND session_active=1
        """, (user_id,))
        row = cursor.fetchone()

        if not row:  # Пробуем через active_sessions
            cursor.execute("""
                SELECT u.balance, u.login 
                FROM users u
                JOIN active_sessions a ON u.login = a.login
                WHERE a.telegram_id=? AND a.is_active=1
            """, (user_id,))
            row = cursor.fetchone()

            if not row:
                conn.rollback()
                conn.close()
                return False, "Ошибка! Вы не вошли в систему.", 0

            balance, login = row
        else:
            balance = row[0]
            # Получаем логин для логирования
            cursor.execute("SELECT login FROM users WHERE telegram_id=?", (user_id,))
            login_row = cursor.fetchone()
            login = login_row[0] if login_row else "unknown"

        # Проверяем достаточность средств
        if balance < total_cost:
            conn.rollback()
            conn.close()
            return False, f"Недостаточно средств! Баланс: ${balance:.2f}, требуется: ${total_cost:.2f}", total_cost

        # Рассчитываем новый баланс
        new_balance = round(balance - total_cost, 2)

        # Обновляем баланс пользователя
        cursor.execute("""
            UPDATE users
            SET balance=?, requests_count = requests_count + ?
            WHERE telegram_id=? AND session_active=1
        """, (new_balance, request_count, user_id))

        if cursor.rowcount == 0:  # Если не обновилось через telegram_id, пробуем через логин
            cursor.execute("""
                UPDATE users
                SET balance=?, requests_count = requests_count + ?
                WHERE login=?
            """, (new_balance, request_count, login))

        # Завершаем транзакцию
        conn.commit()

        logging.info(
            f"📉 Списано ${total_cost:.2f} за {request_count} запросов от user_id={user_id} (login={login}). Баланс до: ${balance:.2f}, после: ${new_balance:.2f}")

        # Логируем финансовую операцию
        try:
            log_financial_operation(
                user_id=user_id,
                operation_type='batch_deduct',
                amount=total_cost,
                balance_before=balance,
                balance_after=new_balance,
                comment=f"Пакетное списание за {request_count} запросов"
            )
        except Exception as e:
            logging.error(f"Ошибка логирования финансовой операции: {e}")

        return True, f"Средства списаны за {request_count} запросов. Новый баланс: ${new_balance:.2f}", total_cost

    except Exception as e:
        conn.rollback()
        logging.error(f"Ошибка при пакетном списании баланса: {e}")
        return False, "Ошибка обработки платежа. Пожалуйста, попробуйте снова.", 0
    finally:
        conn.close()

def fix_none_user_ids():
    """
    Исправляет записи в request_logs, где user_id = NULL,
    но можно определить пользователя по query и timestamp

    :return: (количество исправленных записей, количество оставшихся NULL записей)
    """
    try:
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()

        # Проверяем, существует ли таблица request_logs
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='request_logs'")
        if not cursor.fetchone():
            return 0, 0

        # Находим количество записей с NULL user_id
        cursor.execute("SELECT COUNT(*) FROM request_logs WHERE user_id IS NULL")
        null_count_before = cursor.fetchone()[0]

        if null_count_before == 0:
            conn.close()
            return 0, 0

        # Пытаемся обновить NULL записи, используя информацию из других логов
        cursor.execute("""
            UPDATE request_logs 
            SET user_id = (
                SELECT r2.user_id 
                FROM request_logs r2 
                WHERE r2.query = request_logs.query 
                AND r2.user_id IS NOT NULL 
                ORDER BY ABS(JULIANDAY(r2.timestamp) - JULIANDAY(request_logs.timestamp)) 
                LIMIT 1
            )
            WHERE user_id IS NULL
        """)

        # Проверяем, сколько записей осталось с NULL
        cursor.execute("SELECT COUNT(*) FROM request_logs WHERE user_id IS NULL")
        null_count_after = cursor.fetchone()[0]

        fixed_count = null_count_before - null_count_after

        conn.commit()
        conn.close()

        return fixed_count, null_count_after

    except Exception as e:
        logging.error(f"Ошибка при исправлении NULL user_id: {e}")
        return 0, null_count_before

#модуля для работы с активными сессиями - центральнуя функцию для массового разлогинивания пользователей
def logout_all_users(admin_id=None):
    """
    Разлогинивает всех пользователей, кроме администратора
    Проверяет все возможные таблицы с информацией о сессиях
    """
    try:
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()

        # Список для хранения ID пользователей
        users_to_logout = []
        users_updated = 0

        # 1. Проверяем таблицу active_sessions (основная в новой структуре)
        try:
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='active_sessions'")
            if cursor.fetchone():
                # Получаем список активных пользователей
                cursor.execute("""
                    SELECT telegram_id FROM active_sessions 
                    WHERE is_active=1 AND telegram_id != ?
                """, (admin_id or 0,))

                active_users = cursor.fetchall()
                if active_users:
                    for user in active_users:
                        if user[0] and user[0] not in users_to_logout:
                            users_to_logout.append(user[0])

                    # Деактивируем сессии
                    cursor.execute("""
                        UPDATE active_sessions 
                        SET is_active = 0 
                        WHERE telegram_id != ? AND is_active = 1
                    """, (admin_id or 0,))

                    rows_affected = cursor.rowcount
                    users_updated += rows_affected
                    logging.info(f"Деактивировано {rows_affected} сессий в таблице active_sessions")
        except Exception as e:
            logging.error(f"Ошибка при работе с таблицей active_sessions: {e}")

        # 2. Проверяем поле session_active в таблице users (может использоваться в старых версиях)
        try:
            # Проверяем структуру таблицы users
            cursor.execute("PRAGMA table_info(users)")
            columns = [info[1] for info in cursor.fetchall()]

            if 'session_active' in columns:
                cursor.execute("""
                    SELECT telegram_id FROM users 
                    WHERE session_active=1 AND telegram_id != ?
                """, (admin_id or 0,))

                active_user_rows = cursor.fetchall()
                if active_user_rows:
                    for user in active_user_rows:
                        if user[0] and user[0] not in users_to_logout:
                            users_to_logout.append(user[0])

                    # Деактивируем сессии
                    cursor.execute("""
                        UPDATE users 
                        SET session_active = 0 
                        WHERE telegram_id != ? AND session_active = 1
                    """, (admin_id or 0,))

                    rows_affected = cursor.rowcount
                    users_updated += rows_affected
                    logging.info(f"Деактивировано {rows_affected} сессий в таблице users")
        except Exception as e:
            logging.error(f"Ошибка при работе с полем session_active в таблице users: {e}")

        conn.commit()
        conn.close()

        # Логируем подробную информацию для отладки
        logging.info(
            f"Функция logout_all_users: найдено {len(users_to_logout)} уникальных пользователей, обновлено {users_updated} записей")
        for user_id in users_to_logout:
            logging.info(f"Пользователь для разлогинивания: {user_id}")

        return True, users_to_logout, users_updated
    except Exception as e:
        logging.error(f"Ошибка при массовом разлогинивании: {e}", exc_info=True)
        return False, [], 0


def diagnose_database_structure():
    """
    Comprehensive database structure diagnostics with repair recommendations
    """
    try:
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        issues = []
        repair_actions = []
        warnings = []
        db_stats = {}

        # Check database file size and integrity
        try:
            db_size_mb = os.path.getsize(DB_PATH) / (1024 * 1024)
            db_stats["file_size_mb"] = round(db_size_mb, 2)

            if db_size_mb > 100:
                warnings.append(f"Database file is large ({db_size_mb:.2f} MB). Consider purging old data.")

            # Check database integrity
            cursor.execute("PRAGMA integrity_check")
            integrity_result = cursor.fetchone()[0]
            db_stats["integrity_check"] = integrity_result

            if integrity_result != "ok":
                issues.append(f"Database integrity check failed: {integrity_result}")
                repair_actions.append("Create a database backup and consider rebuilding the database")
        except Exception as e:
            issues.append(f"Error checking database file: {str(e)}")

        # Check database version
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='db_version'")
        if cursor.fetchone():
            cursor.execute("SELECT version FROM db_version LIMIT 1")
            row = cursor.fetchone()
            if row:
                db_stats["version"] = row[0]
            else:
                issues.append("db_version table exists but has no version data")
                repair_actions.append("Run database migrations to initialize version")
        else:
            issues.append("Missing db_version table")
            repair_actions.append("Run setup_database() to initialize schema")

        # Check required tables existence
        essential_tables = [
            'users', 'active_sessions', 'cache', 'admin_logs',
            'mass_search_logs'
        ]

        cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
        existing_tables = [row[0] for row in cursor.fetchall()]
        db_stats["existing_tables"] = existing_tables

        for table in essential_tables:
            if table not in existing_tables:
                issues.append(f"Missing essential table: {table}")
                repair_actions.append(f"Run setup_database() to create table {table}")

        # Check table schema for required columns
        table_schema_checks = {
            'users': ['telegram_id', 'login', 'password_hash', 'balance', 'session_active'],
            'active_sessions': ['telegram_id', 'login', 'is_active'],
            'cache': ['user_id', 'query', 'response', 'timestamp'],
            'mass_search_logs': ['user_id', 'file_path', 'status']
        }

        for table, required_columns in table_schema_checks.items():
            if table in existing_tables:
                cursor.execute(f"PRAGMA table_info({table})")
                columns = {info[1]: info for info in cursor.fetchall()}
                db_stats[f"{table}_columns"] = list(columns.keys())

                for column in required_columns:
                    if column not in columns:
                        issues.append(f"Missing required column {column} in table {table}")
                        repair_actions.append(f"Add column {column} to table {table}")

        # Check for important indexes
        index_checks = [
            ('users', 'idx_users_telegram_id', 'telegram_id'),
            ('users', 'idx_users_login', 'login'),
            ('active_sessions', 'idx_sessions_telegram_id', 'telegram_id'),
            ('cache', 'idx_cache_user_query', 'user_id,query'),
            ('cache', 'idx_cache_timestamp', 'timestamp')
        ]

        for table, index_name, columns in index_checks:
            if table in existing_tables:
                cursor.execute(
                    f"SELECT name FROM sqlite_master WHERE type='index' AND tbl_name='{table}' AND name='{index_name}'")
                if not cursor.fetchone():
                    warnings.append(f"Missing index {index_name} on {table}({columns})")
                    repair_actions.append(f"Create index {index_name} on {table}({columns})")

        # Check active users
        users_count = 0
        active_sessions_count = 0
        telegram_ids_mismatch = False

        # Check sessions in users table
        try:
            if 'users' in existing_tables:
                cursor.execute("PRAGMA table_info(users)")
                columns = {info[1]: info for info in cursor.fetchall()}

                if 'session_active' in columns and 'telegram_id' in columns:
                    cursor.execute("SELECT COUNT(*), COUNT(DISTINCT telegram_id) FROM users WHERE session_active=1")
                    row = cursor.fetchone()
                    if row:
                        users_count = row[0]
                        distinct_users = row[1]
                        db_stats["users_table_active"] = users_count
                        db_stats["users_table_distinct_active"] = distinct_users

                        if users_count != distinct_users:
                            issues.append(
                                f"Duplicate active sessions in users table: {users_count} sessions for {distinct_users} unique users")
                            repair_actions.append("Run fix_duplicate_sessions() to clean up duplicate sessions")
        except Exception as e:
            issues.append(f"Error checking users table sessions: {str(e)}")

        # Check sessions in active_sessions table
        try:
            if 'active_sessions' in existing_tables:
                cursor.execute("SELECT COUNT(*), COUNT(DISTINCT telegram_id) FROM active_sessions WHERE is_active=1")
                row = cursor.fetchone()
                if row:
                    active_sessions_count = row[0]
                    distinct_active_sessions = row[1]
                    db_stats["active_sessions_table_active"] = active_sessions_count
                    db_stats["active_sessions_table_distinct"] = distinct_active_sessions

                    if active_sessions_count != distinct_active_sessions:
                        issues.append(
                            f"Duplicate active sessions in active_sessions table: {active_sessions_count} sessions for {distinct_active_sessions} unique users")
                        repair_actions.append("Run fix_duplicate_active_sessions() to clean up duplicates")

                # Check for telegram_id consistency
                if 'users' in existing_tables:
                    cursor.execute("""
                        SELECT COUNT(*) FROM active_sessions a 
                        LEFT JOIN users u ON a.telegram_id = u.telegram_id
                        WHERE a.is_active=1 AND u.telegram_id IS NULL
                    """)
                    orphaned_sessions = cursor.fetchone()[0]
                    if orphaned_sessions > 0:
                        issues.append(f"Found {orphaned_sessions} active sessions without corresponding user records")
                        repair_actions.append("Run fix_orphaned_sessions() to clean up orphaned sessions")
        except Exception as e:
            issues.append(f"Error checking active_sessions table: {str(e)}")

        # Check for inconsistencies between users and active_sessions
        try:
            if 'users' in existing_tables and 'active_sessions' in existing_tables:
                cursor.execute("PRAGMA table_info(users)")
                columns = {info[1]: info for info in cursor.fetchall()}

                if 'session_active' in columns:
                    cursor.execute("""
                        SELECT COUNT(*) FROM users u 
                        LEFT JOIN active_sessions a ON u.telegram_id = a.telegram_id
                        WHERE u.session_active=1 AND (a.is_active IS NULL OR a.is_active=0)
                    """)
                    inconsistent_count = cursor.fetchone()[0]
                    if inconsistent_count > 0:
                        warnings.append(
                            f"Found {inconsistent_count} users marked active in users table but not in active_sessions")
                        repair_actions.append("Run synchronize_session_tables() to fix session inconsistencies")
        except Exception as e:
            issues.append(f"Error checking session consistency: {str(e)}")

        # Check cache table
        try:
            if 'cache' in existing_tables:
                cursor.execute("SELECT COUNT(*) FROM cache")
                cache_count = cursor.fetchone()[0]
                db_stats["cache_entries"] = cache_count

                if cache_count > 10000:
                    warnings.append(f"Large cache table with {cache_count} entries. Consider purging old entries.")
                    repair_actions.append("Run clear_old_cache(days=7) to remove cache older than 7 days")

                # Check if source column exists
                cursor.execute("PRAGMA table_info(cache)")
                columns = {info[1]: info for info in cursor.fetchall()}
                if 'source' not in columns:
                    warnings.append("Cache table missing 'source' column")
                    repair_actions.append("Run fix_cache_table_structure() to add missing columns")
        except Exception as e:
            issues.append(f"Error checking cache table: {str(e)}")

        # Check mass search logs
        try:
            if 'mass_search_logs' in existing_tables:
                cursor.execute("SELECT COUNT(*), COUNT(CASE WHEN status='failed' THEN 1 END) FROM mass_search_logs")
                row = cursor.fetchone()
                if row:
                    total_searches = row[0]
                    failed_searches = row[1]
                    db_stats["mass_searches_total"] = total_searches
                    db_stats["mass_searches_failed"] = failed_searches

                    if failed_searches > total_searches * 0.3 and total_searches > 10:
                        warnings.append(
                            f"High failure rate in mass searches: {failed_searches}/{total_searches} failed")
        except Exception as e:
            issues.append(f"Error checking mass_search_logs: {str(e)}")

        # Final summaries
        db_stats["active_users_count"] = users_count + active_sessions_count
        db_stats["issues_count"] = len(issues)
        db_stats["warnings_count"] = len(warnings)
        db_stats["repair_actions"] = repair_actions

        conn.close()

        # Full diagnostic result
        result = {
            "issues": issues,
            "warnings": warnings,
            "repair_actions": repair_actions,
            "stats": db_stats
        }

        # Log summary
        logging.info(
            f"Database diagnosis completed: {len(issues)} issues, {len(warnings)} warnings, "
            f"{len(repair_actions)} repair actions suggested"
        )

        return result

    except Exception as e:
        error_traceback = traceback.format_exc()
        logging.error(f"Critical error during database diagnosis: {e}\n{error_traceback}")
        return {
            "issues": [f"Critical error during database diagnosis: {str(e)}"],
            "warnings": [],
            "repair_actions": ["Check database file permissions and integrity"],
            "stats": {"error": str(e)}
        }