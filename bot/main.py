import asyncio
import os
import sqlite3
from collections import defaultdict
from datetime import datetime

from aiogram import Bot, Dispatcher
from aiogram import types
from aiogram.client.bot import DefaultBotProperties
from aiogram.filters import Command, StateFilter
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.types import (
    Message,
    CallbackQuery,
    BotCommand, BotCommandScopeChat, ErrorEvent, InlineKeyboardButton, BotCommandScopeDefault, FSInputFile,
    InlineKeyboardMarkup
)
from aiogram.utils.keyboard import InlineKeyboardBuilder
from dotenv import load_dotenv

# Добавляем импорт SessionPoolManager для работы с веб-сессиями
from bot.session_pool import SessionPoolManager

# Добавляем импорт общих переменных из common.py
from bot.common import mass_search_semaphore, MAX_USER_SEARCHES, active_user_searches, MAX_CONCURRENT_MASS_SEARCHES, \
    mass_search_queue
# Импортируем функции и данные из других модулей
from bot.database.db import (
    setup_database,
    check_active_session,
    logout_user,
    get_user_balance,
    add_balance,
    get_users_paginated,
    get_users_with_zero_balance,
    clear_old_cache,
    create_user,
    DB_PATH  # <-- добавили для удаления повреждённого кэша
)
from bot.handlers import router as handlers_router
from bot.keyboards import get_admin_users_keyboard, get_admin_menu
from bot.logger import logging
from bot.mass_search import mass_search_router
from bot.utils import (
    get_api_balance,
    setup_translation_db
)

# Increase file descriptor limits if running as root
try:
    import resource
    soft, hard = resource.getrlimit(resource.RLIMIT_NOFILE)
    new_soft = min(65536, hard)
    if new_soft > soft:
        resource.setrlimit(resource.RLIMIT_NOFILE, (new_soft, hard))
        logging.info(f"Increased file descriptor limit from {soft} to {new_soft}")
except (ImportError, PermissionError):
    pass

# Optimize event loop policy if uvloop is available
try:
    import uvloop
    asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
    logging.info("Using uvloop for improved asyncio performance")
except ImportError:
    logging.info("uvloop not available, using standard asyncio")

# Optimize TCP settings
os.environ['PYTHONASYNCIOALLDEBUG'] = '0'  # Disable asyncio debug for production

# Configure asyncio for performance
loop = asyncio.get_event_loop()
loop.set_debug(False)

# If in Linux, increase file descriptors
if os.name == 'posix':
    try:
        import resource
        resource.setrlimit(resource.RLIMIT_NOFILE, (65536, 65536))
    except (ImportError, PermissionError, ValueError):
        pass

from bot.session_manager import session_pool as _session_pool, init_session_pool

# Загружаем переменные окружения (.env)
load_dotenv()
TOKEN = os.getenv("BOT_TOKEN")
ADMIN_ID = int(os.getenv("ADMIN_ID")) if os.getenv("ADMIN_ID") else 0

# Инициализируем бота и диспетчер
bot = Bot(token=TOKEN, default=DefaultBotProperties(parse_mode="HTML"))
dp = Dispatcher()

# Добавляем глобальную переменную для пула сессий
session_pool = None

# Определяем учетные данные для сессий
CREDENTIALS = [
    ("CipkaCapuchinka", "4uTStetepXK1p3vn"),
    ("CipkaCapuchinka", "4uTStetepXK1p3vn"),
    ("CipkaCapuchinka", "4uTStetepXK1p3vn"),
    ("CipkaCapuchinka", "4uTStetepXK1p3vn"),
    ("CipkaCapuchinka", "4uTStetepXK1p3vn"),
    ("CipkaCapuchinka", "4uTStetepXK1p3vn"),
    ("CipkaCapuchinka", "4uTStetepXK1p3vn"),
    ("CipkaCapuchinka", "4uTStetepXK1p3vn"),
    ("CipkaCapuchinka", "4uTStetepXK1p3vn"),
    ("CipkaCapuchinka", "4uTStetepXK1p3vn"),
    ("CipkaCapuchinka", "4uTStetepXK1p3vn"),
    ("CipkaCapuchinka", "4uTStetepXK1p3vn"),
    ("CipkaCapuchinka", "4uTStetepXK1p3vn"),
    ("CipkaCapuchinka", "4uTStetepXK1p3vn"),
    ("CipkaCapuchinka", "4uTStetepXK1p3vn"),
    ("CipkaCapuchinka", "4uTStetepXK1p3vn"),


    # Добавьте другие учетные данные для других сессий
]

# Подключаем дополнительный router, где лежат /help, /extended_search и т. д.
# (см. handlers.py)
dp.include_router(handlers_router)
dp.include_router(mass_search_router)
# Инициализация таблицы для переводов названий (если используется)
setup_translation_db()


# ======== Фоновые задачи (очистка кэша, уведомления) ==========

async def clear_cache_daily():
    """Каждые сутки очищаем старый кэш из БД."""
    while True:
        clear_old_cache()
        logging.info("✅ Старый кэш (24+ часа) очищен.")
        await asyncio.sleep(86400)  # 24 часа


async def notify_admin_about_zero_balance():
    """Каждый час уведомляем админа о пользователях с нулевым балансом."""
    while True:
        users = get_users_with_zero_balance()
        if users and ADMIN_ID:
            msg = "⚠ У следующих пользователей закончился баланс:\n"
            for login, tg_id in users:
                msg += f"👤 {login} (ID: {tg_id})\n"
            await bot.send_message(ADMIN_ID, msg)

        await asyncio.sleep(3600)  # 1 час


# Фоновая задача для обновления сессий
async def refresh_expired_sessions():
    """Периодически обновляет неактивные сессии"""
    global session_pool
    while True:
        try:
            if session_pool is not None:
                await session_pool.refresh_expired_sessions()
                logging.info("Выполнено обновление просроченных сессий")
            else:
                logging.warning("Cannot refresh sessions: session_pool is None")
        except Exception as e:
            logging.error(f"Ошибка при обновлении сессий: {str(e)}")

        # Ждем 30 минут перед следующим обновлением
        await asyncio.sleep(1800)


# ======== Состояния для FSM (пример) ==========

class BalanceState(StatesGroup):
    waiting_for_amount = State()


# Дополнительное состояние для создания пользователя
class UserCreationState(StatesGroup):
    waiting_for_data = State()  # Ожидаем ввода данных нового пользователя


# Добавляем новый класс для массового разлогинивания
class LogoutAllState(StatesGroup):
    waiting_for_message = State()  # Ожидание сообщения для пользователей
    confirming = State()  # Ожидание подтверждения операции


# ======== Регистрация «синего меню» (slash-команды) ==========

async def register_bot_commands():
    """
    Настройка команд для разных типов пользователей:
    - Обычные пользователи видят только базовые команды
    - Администратор видит все команды включая админские
    """

    # Команды для всех пользователей
    user_commands = [
        BotCommand(command="start", description="Начало работы"),
        BotCommand(command="help", description="Справка по боту"),
        BotCommand(command="menu", description="Открыть меню бота"),  # Добавляем команду меню
        BotCommand(command="balance", description="Проверить баланс"),
        BotCommand(command="logout", description="Выйти из системы"),
        BotCommand(command="extended_search", description="Расширенный поиск")
    ]

    # Админские команды
    admin_commands = [
        BotCommand(command="admin", description="Панель администратора"),
        BotCommand(command="users", description="Список пользователей"),
        BotCommand(command="add_balance", description="Пополнить баланс"),
        BotCommand(command="create_user", description="Создать пользователя"),
        BotCommand(command="api_balance", description="Проверить баланс API"),
        BotCommand(command="db_status", description="Проверить структуру БД"),
        BotCommand(command="sessions_stats", description="Статистика веб-сессий")
    ]

    # Устанавливаем базовые команды для всех пользователей
    await bot.set_my_commands(user_commands, scope=BotCommandScopeDefault())

    # Если задан ID администратора, устанавливаем для него расширенный список команд
    if ADMIN_ID:
        full_command_list = user_commands + admin_commands
        await bot.set_my_commands(
            full_command_list,
            scope=BotCommandScopeChat(chat_id=ADMIN_ID)
        )
        logging.info(f"Установлены расширенные команды для администратора (ID: {ADMIN_ID})")


# ======== Обработчики команд ==========

@dp.message(Command("balance"))
async def cmd_balance(message: Message):
    """
    /balance: если админ, сообщаем об этом, иначе выводим баланс пользователя из БД.
    """
    user_id = message.from_user.id
    if not check_active_session(user_id):
        return await message.answer(
            "🔐 <b>Вы не вошли в систему</b>\n\n"
            "Для входа введите ваш логин и пароль одним сообщением:\n\n"
            "📌 <b>Формат:</b>\n"
            "<code>логин пароль</code>\n\n"
            "✅ <b>Пример:</b>\n"
            "<code>ivanov123 MyStrongPass2024</code>\n\n"
            "💡 <i>Возникли вопросы?</i>\n"
            "Напишите в поддержку: @Mersronada",
            parse_mode="HTML"
        )

    # Админ — условный неограниченный баланс (по логике проекта)
    if user_id == ADMIN_ID:
        return await message.answer("Вы администратор. Баланс условно не ограничен.")

    # Обычный пользователь
    bal = get_user_balance(user_id)
    if bal is None:
        await message.answer("Ошибка: ваш аккаунт не найден или вы не авторизованы.")
    else:
        await message.answer(f"Ваш текущий баланс: ${bal:.2f}")


@dp.message(Command("api_balance"))
async def cmd_api_balance(message: Message):
    """
    /api_balance: показывает баланс внешнего API (только у админа).
    """
    user_id = message.from_user.id
    if user_id != ADMIN_ID:
        return await message.answer("У вас нет доступа.")

    success, resp = get_api_balance()
    await message.answer(resp)


@dp.message(Command("sessions_stats"))
async def cmd_sessions_stats(message: Message):
    """Команда для получения статистики по сессиям (только для админа)"""
    user_id = message.from_user.id
    if user_id != ADMIN_ID:
        return await message.answer("У вас нет доступа к этой команде.")

    if not session_pool:
        return await message.answer("Пул сессий не инициализирован.")

    stats = session_pool.get_stats()

    # Формируем сообщение со статистикой
    text = (
        f"📊 <b>Статистика пула сессий</b>\n\n"
        f"Всего сессий: {stats['total_sessions']}\n"
        f"Активных сессий: {stats['active_sessions']}\n"
        f"Занятых сессий: {stats['busy_sessions']}\n\n"
        f"<b>Статистика поисков:</b>\n"
        f"Всего запросов: {stats['searches']['total_searches']}\n"
        f"Успешных: {stats['searches']['successful_searches']}\n"
        f"Неудачных: {stats['searches']['failed_searches']}\n"
        f"Повторных авторизаций: {stats['searches']['reauth_count']}\n\n"
    )

    await message.answer(text, parse_mode="HTML")


@dp.message(Command("add_balance"))
async def cmd_add_balance(message: Message):
    """
    /add_balance логин сумма — пополняем баланс пользователя (админ).
    Пример: /add_balance testuser 15
    """
    user_id = message.from_user.id
    if user_id != ADMIN_ID:
        return await message.answer("У вас нет доступа.")

    args = message.text.strip().split()
    if len(args) != 3:
        return await message.answer("Использование: /add_balance логин сумма")

    _, login, amount_str = args
    try:
        amount = float(amount_str)
    except ValueError:
        return await message.answer("Сумма должна быть числом.")

    success, info = add_balance(login, amount)
    await message.answer(info)


@dp.message(Command("users"))
async def cmd_users(message: Message):
    """
    /users: показать список всех пользователей, 5 на страницу.
    """
    user_id = message.from_user.id
    if user_id != ADMIN_ID:
        return await message.answer("У вас нет доступа.")

    page = 1
    users, total = get_users_paginated(page=page, page_size=5)
    if not users:
        return await message.answer("В системе пока нет пользователей.")

    resp = f"📋 Список пользователей (Страница {page}):\n\n"
    for login, bal in users:
        resp += f"👤 {login} — ${bal:.2f}\n"
    has_next = (page * 5) < total
    await message.answer(resp, reply_markup=get_admin_users_keyboard(page, users, has_next))


@dp.message(Command("create_user"))
async def cmd_create_user(message: Message):
    """
    /create_user логин пароль баланс
    Создаёт нового пользователя (админ).
    """
    user_id = message.from_user.id
    if user_id != ADMIN_ID:
        return await message.answer("У вас нет доступа.")

    parts = message.text.strip().split()
    if len(parts) != 4:
        return await message.answer("Использование: /create_user логин пароль баланс")

    _, login, password, bal_str = parts
    try:
        bal = float(bal_str)
    except ValueError:
        return await message.answer("Баланс должен быть числом.")

    ok, info = create_user(login, password, bal)
    await message.answer(info)


@dp.message(Command("admin"))
async def cmd_admin_panel(message: Message):
    """/admin: показывает админ-меню (ReplyKeyboardMarkup)."""
    if message.from_user.id != ADMIN_ID:
        return await message.answer("У вас нет доступа к админ-меню.")

    await message.answer("🔧 Админ-панель:", reply_markup=get_admin_menu())


@dp.message(Command("db_status"))
async def cmd_db_status(message: Message):
    """
    /db_status - проверка структуры базы данных и диагностика проблем
    (только для администратора)
    """
    user_id = message.from_user.id
    if user_id != ADMIN_ID:
        return await message.answer("У вас нет доступа к этой команде.")

    from bot.database.db import diagnose_database_structure

    status_message = await message.answer("⏳ Диагностика структуры базы данных...")

    # Получаем результаты диагностики
    result = diagnose_database_structure()
    issues = result.get("issues", [])

    # Формируем ответ
    response = f"📊 <b>Диагностика базы данных</b>\n\n"

    if issues:
        response += "⚠️ <b>Обнаружены проблемы:</b>\n"
        for i, issue in enumerate(issues, 1):
            response += f"{i}. {issue}\n"
        response += "\n"
    else:
        response += "✅ <b>Проблем не обнаружено</b>\n\n"

    response += f"👥 <b>Активных пользователей:</b> {result.get('active_users_count', 0)}\n"
    response += f"• В таблице users: {result.get('users_table_active', 0)}\n"
    response += f"• В таблице active_sessions: {result.get('active_sessions_table_active', 0)}\n"

    await status_message.edit_text(response, parse_mode="HTML")


# ======= Колбэк-хендлеры для пагинации / пополнения баланса =======

@dp.callback_query(lambda c: c.data.startswith("prev_page_") or c.data.startswith("next_page_"))
async def cb_paginate(callback: CallbackQuery, state: FSMContext):
    """Обработка кнопок пагинации пользователей (админ)."""
    user_id = callback.from_user.id
    if user_id != ADMIN_ID:
        return await callback.answer("Нет доступа.", show_alert=True)

    try:
        page_str = callback.data.split("_")[-1]
        page = int(page_str)
        if page < 1:
            page = 1

        users, total = get_users_paginated(page=page, page_size=5)
        if not users:
            await callback.message.edit_text("Нет пользователей на этой странице.")
            return await callback.answer()

        resp = f"📋 Список пользователей (Страница {page}):\n\n"
        for login, bal in users:
            resp += f"👤 {login} — ${bal:.2f}\n"
        has_next = (page * 5) < total

        await callback.message.edit_text(resp, reply_markup=get_admin_users_keyboard(page, users, has_next))
        await callback.answer()
    except ValueError:
        logging.error(f"Ошибка при разборе номера страницы: {callback.data}")
        await callback.answer("Ошибка при переходе на страницу", show_alert=True)
    except Exception as e:
        logging.error(f"Ошибка при обработке пагинации: {e}")
        await callback.answer("Произошла ошибка. Попробуйте еще раз.", show_alert=True)


@dp.callback_query(lambda c: c.data.startswith("add_balance_"))
async def cb_add_balance_user(callback: CallbackQuery, state: FSMContext):
    """Кнопка "Пополнить" напротив конкретного пользователя."""
    user_id = callback.from_user.id
    if user_id != ADMIN_ID:
        return await callback.answer("Нет доступа.", show_alert=True)

    try:
        login = callback.data.split("_", 2)[-1]
        if not login:
            raise ValueError("Логин не найден")

        # Проверяем существование пользователя
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        cursor.execute("SELECT login FROM users WHERE login = ?", (login,))
        if not cursor.fetchone():
            conn.close()
            await callback.answer("Пользователь не найден", show_alert=True)
            return
        conn.close()

        await state.update_data(user_login=login)
        await state.set_state(BalanceState.waiting_for_amount)
        await callback.message.answer(f"Введите сумму для пополнения баланса пользователя `{login}`:")
        await callback.answer()
    except Exception as e:
        logging.error(f"Ошибка при обработке callback add_balance: {e}")
        await callback.answer("Произошла ошибка. Попробуйте еще раз.", show_alert=True)


@dp.message(BalanceState.waiting_for_amount)
async def process_balance_amount(message: Message, state: FSMContext):
    """Обработка ввода суммы для пополнения (админ)."""
    user_id = message.from_user.id
    if user_id != ADMIN_ID:
        return await message.answer("У вас нет доступа.")

    data = await state.get_data()
    login = data.get("user_login")
    try:
        amount = float(message.text)
        if amount <= 0:
            raise ValueError
    except ValueError:
        return await message.answer("Введите корректную сумму (число > 0).")

    success, info = add_balance(login, amount)
    await message.answer(info)
    await state.clear()


# ======= Обработчики кнопок админ-меню =======

@dp.message(lambda msg: msg.text == "📋 Список пользователей")
async def admin_list_users(message: Message):
    """Обработчик кнопки '📋 Список пользователей' из админ-меню"""
    user_id = message.from_user.id
    if user_id != ADMIN_ID:
        return await message.answer("У вас нет доступа к этой функции.")

    # Используем существующую функцию для списка пользователей
    page = 1
    users, total = get_users_paginated(page=page, page_size=5)
    if not users:
        return await message.answer("В системе пока нет пользователей.")

    resp = f"📋 Список пользователей (Страница {page}):\n\n"
    for login, bal in users:
        resp += f"👤 {login} — ${bal:.2f}\n"
    has_next = (page * 5) < total
    await message.answer(resp, reply_markup=get_admin_users_keyboard(page, users, has_next))


@dp.message(lambda msg: msg.text == "💰 Пополнить баланс")
async def admin_add_balance_prompt(message: Message):
    """Обработчик кнопки '💰 Пополнить баланс' из админ-меню"""
    user_id = message.from_user.id
    if user_id != ADMIN_ID:
        return await message.answer("У вас нет доступа к этой функции.")

    await message.answer(
        "Введите команду в формате:\n"
        "`/add_balance логин сумма`\n\n"
        "Например: `/add_balance user123 25.50`"
    )


@dp.message(lambda msg: msg.text == "📊 Баланс API")
async def admin_api_balance(message: Message):
    """Обработчик кнопки '📊 Баланс API' из админ-меню"""
    user_id = message.from_user.id
    if user_id != ADMIN_ID:
        return await message.answer("У вас нет доступа к этой функции.")

    # Используем существующую функцию для проверки баланса API
    success, resp = get_api_balance()
    await message.answer(resp)


@dp.message(lambda msg: msg.text == "⚙️ Настройки")
async def admin_settings(message: Message):
    """Обработчик кнопки '⚙️ Настройки' из админ-меню"""
    user_id = message.from_user.id
    if user_id != ADMIN_ID:
        return await message.answer("У вас нет доступа к этой функции.")

    # Создаем инлайн-клавиатуру для настроек
    builder = InlineKeyboardBuilder()
    builder.add(InlineKeyboardButton(text="🔄 Очистить кэш", callback_data="clear_cache"))
    builder.add(InlineKeyboardButton(text="📤 Бэкап базы данных", callback_data="backup_db"))
    builder.row(InlineKeyboardButton(text="🔧 Изменить стоимость запроса", callback_data="change_cost"))

    await message.answer(
        "⚙️ <b>Настройки системы</b>\n\n"
        "Выберите действие:",
        reply_markup=builder.as_markup(),
        parse_mode="HTML"
    )


@dp.message(lambda msg: msg.text == "🚪 Выйти")
async def admin_logout(message: Message):
    """Обработчик кнопки '🚪 Выйти' из админ-меню"""
    user_id = message.from_user.id

    # Используем существующую функцию для выхода
    logout_user(user_id)
    logging.info(f"Пользователь {user_id} вышел из системы через меню.")

    # Убираем меню
    await message.answer(
        "Вы вышли из системы.",
        reply_markup=types.ReplyKeyboardRemove()
    )


@dp.callback_query(lambda c: c.data == "clear_cache")
async def cb_clear_cache(callback: CallbackQuery):
    """Очистка кэша из меню настроек"""
    user_id = callback.from_user.id
    if user_id != ADMIN_ID:
        return await callback.answer("У вас нет доступа к этой функции.", show_alert=True)

    try:
        # Очищаем кэш и считаем сколько записей удалено
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM cache")
        before_count = cursor.fetchone()[0]

        cursor.execute("DELETE FROM cache")
        conn.commit()
        conn.close()

        await callback.message.answer(f"✅ Кэш очищен. Удалено {before_count} записей.")
        await callback.answer()
    except Exception as e:
        logging.error(f"Ошибка при очистке кэша: {e}")
        await callback.answer("Произошла ошибка при очистке кэша", show_alert=True)


@dp.callback_query(lambda c: c.data == "backup_db")
async def cb_backup_db(callback: CallbackQuery):
    """Создание резервной копии базы данных"""
    user_id = callback.from_user.id
    if user_id != ADMIN_ID:
        return await callback.answer("У вас нет доступа к этой функции.", show_alert=True)

    try:
        from bot.database.backup import backup_database
        backup_file = backup_database()

        if backup_file:
            await callback.message.answer(f"✅ Резервная копия создана: {backup_file}")
        else:
            await callback.message.answer("❌ Не удалось создать резервную копию.")

        await callback.answer()
    except Exception as e:
        logging.error(f"Ошибка при создании бэкапа: {e}")
        await callback.answer("Произошла ошибка при создании бэкапа", show_alert=True)


@dp.callback_query(lambda c: c.data == "change_cost")
async def cb_change_cost(callback: CallbackQuery, state: FSMContext):
    """Запрос на изменение стоимости запроса"""
    user_id = callback.from_user.id
    if user_id != ADMIN_ID:
        return await callback.answer("У вас нет доступа к этой функции.", show_alert=True)

    # Определяем новое состояние для ожидания новой стоимости
    class CostState(StatesGroup):
        waiting_for_cost = State()

    await state.set_state(CostState.waiting_for_cost)

    # Импортируем REQUEST_COST из config.py
    from bot.config import REQUEST_COST
    await callback.message.answer(f"Текущая стоимость запроса: ${REQUEST_COST:.2f}\nВведите новую стоимость:")
    await callback.answer()


@dp.message(lambda msg: msg.text == "⚠️ Разлогинить всех")
async def admin_logout_all_users(message: Message, state: FSMContext):
    """Обработчик кнопки 'Разлогинить всех' из админ-меню"""
    user_id = message.from_user.id
    if user_id != ADMIN_ID:
        return await message.answer("У вас нет доступа к этой функции.")

    # Получаем количество активных пользователей
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    try:
        # Проверяем наличие поля session_active в таблице users
        cursor.execute("PRAGMA table_info(users)")
        columns = [info[1] for info in cursor.fetchall()]

        if 'session_active' in columns:
            cursor.execute("SELECT COUNT(*) FROM users WHERE session_active=1 AND telegram_id != ?", (ADMIN_ID,))
        else:
            # Если поля session_active нет, проверяем таблицу active_sessions
            cursor.execute("SELECT COUNT(*) FROM active_sessions WHERE is_active=1 AND telegram_id != ?", (ADMIN_ID,))

        active_count = cursor.fetchone()[0]
    except sqlite3.OperationalError as e:
        # Если таблица не существует или у нее нет нужных полей
        logging.error(f"Ошибка при проверке активных пользователей: {e}")
        active_count = 0
    finally:
        conn.close()

    # Устанавливаем состояние для ожидания сообщения
    await state.set_state(LogoutAllState.waiting_for_message)

    # Создаем клавиатуру с кнопкой отмены
    cancel_kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="❌ Отменить операцию", callback_data="cancel_logout_all")]
    ])

    # Запрашиваем подтверждение и сообщение
    await message.answer(
        f"⚠️ <b>ВНИМАНИЕ!</b> Вы собираетесь разлогинить <b>всех пользователей</b> ({active_count} активных).\n\n"
        "Введите сообщение, которое будет отправлено пользователям перед разлогиниванием, "
        "чтобы они знали, что им нужно будет заново войти в систему:\n\n"
        "<i>Например: Уважаемые пользователи! В связи с техническим обновлением, "
        "пожалуйста, войдите в систему заново. Приносим извинения за неудобства.</i>\n\n"
        "Для отмены операции нажмите кнопку ниже или напишите 'отмена'.",
        parse_mode="HTML",
        reply_markup=cancel_kb
    )


@dp.callback_query(lambda c: c.data == "cancel_logout_all")
async def cancel_logout_all(callback: CallbackQuery, state: FSMContext):
    """Обработчик кнопки отмены массового разлогинивания"""
    user_id = callback.from_user.id
    if user_id != ADMIN_ID:
        return await callback.answer("У вас нет доступа к этой функции.", show_alert=True)

    current_state = await state.get_state()
    if current_state in ["LogoutAllState:waiting_for_message", "LogoutAllState:confirming"]:
        await state.clear()
        await callback.message.edit_text(
            "❌ Операция массового разлогинивания отменена.",
            reply_markup=None
        )
    else:
        await callback.answer("Нет активной операции разлогинивания.", show_alert=True)


@dp.message(StateFilter(LogoutAllState.waiting_for_message))
async def process_logout_message(message: Message, state: FSMContext):
    """Обработчик сообщения для пользователей и запрос подтверждения"""
    user_id = message.from_user.id
    if user_id != ADMIN_ID:
        await state.clear()
        return await message.answer("У вас нет доступа к этой функции.")

    # Проверка на отмену
    if message.text.lower() in ["отмена", "cancel", "отменить"]:
        await state.clear()
        return await message.answer("❌ Операция массового разлогинивания отменена.")

    # Сохраняем текст сообщения в состоянии
    await state.update_data(notification_text=message.text)

    # Запрашиваем финальное подтверждение
    confirm_kb = InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="✅ Подтвердить", callback_data="confirm_logout_all"),
            InlineKeyboardButton(text="❌ Отменить", callback_data="cancel_logout_all")
        ]
    ])

    await state.set_state(LogoutAllState.confirming)

    await message.answer(
        "⚠️ <b>Последнее предупреждение!</b>\n\n"
        "Вы уверены, что хотите разлогинить ВСЕХ пользователей?\n"
        "Пользователям будет отправлено следующее сообщение:\n\n"
        f"<code>{message.text}</code>\n\n"
        "Подтвердите действие:",
        parse_mode="HTML",
        reply_markup=confirm_kb
    )


@dp.callback_query(lambda c: c.data == "confirm_logout_all", StateFilter(LogoutAllState.confirming))
async def confirm_logout_all(callback: CallbackQuery, state: FSMContext):
    """Выполнение массового разлогинивания после подтверждения"""
    user_id = callback.from_user.id
    if user_id != ADMIN_ID:
        await state.clear()
        return await callback.answer("У вас нет доступа к этой функции.", show_alert=True)

    # Получаем сохраненное сообщение
    data = await state.get_data()
    notification_text = data.get("notification_text", "Системное уведомление: вам необходимо заново войти в систему.")

    # Отправляем сообщение о начале процесса
    await callback.message.edit_text(
        "🔄 <b>Начинаю процесс разлогинивания пользователей...</b>",
        parse_mode="HTML",
        reply_markup=None
    )

    # Выполняем массовое разлогинивание
    from bot.database.db import logout_all_users
    logging.info("Начинаем массовое разлогинивание пользователей")

    # Обратите внимание, что функция logout_all_users теперь возвращает 3 значения
    # (успех, список пользователей, количество обновленных записей)
    success, users, users_updated = logout_all_users(admin_id=ADMIN_ID)

    logging.info(
        f"Результат разлогинивания: success={success}, ID пользователей={users}, обновлено записей={users_updated}")

    # Если произошла ошибка
    if not success:
        await callback.message.edit_text(
            "❌ <b>Произошла ошибка при разлогинивании пользователей.</b>\n\n"
            "Проверьте журнал ошибок для получения подробной информации.",
            parse_mode="HTML"
        )
        await state.clear()
        return

    # Если не найдено сессий для обновления
    if users_updated == 0:
        await callback.message.edit_text(
            "ℹ️ <b>В системе не найдено активных пользователей для разлогинивания.</b>\n\n"
            "Пользователи уже разлогинены или используется другой метод хранения сессий.",
            parse_mode="HTML"
        )
        await state.clear()
        return

    # Отправляем сообщения пользователям
    message_count = 0
    success_count = 0
    error_count = 0

    progress_message = await callback.message.edit_text(
        f"🔄 <b>Выполняется отправка уведомлений пользователям...</b>\n\n"
        f"• Разлогинено пользователей: <b>{users_updated}</b>\n"
        f"• Пользователей для уведомления: <b>{len(users)}</b>\n"
        f"• Отправлено уведомлений: <b>0/{len(users)}</b>",
        parse_mode="HTML"
    )

    # Проверяем, есть ли пользователи для отправки уведомлений
    if not users:
        await progress_message.edit_text(
            f"✅ <b>Операция частично завершена</b>\n\n"
            f"• Разлогинено пользователей: <b>{users_updated}</b>\n"
            f"• <i>Не найдены ID пользователей для отправки уведомлений</i>\n\n"
            f"<i>Пользователи разлогинены, но уведомления не были отправлены.</i>",
            parse_mode="HTML"
        )
        await state.clear()
        return

    # Последовательно отправляем сообщения с обновлением индикатора прогресса
    for idx, user_telegram_id in enumerate(users):
        # Дополнительная проверка на валидность ID
        if not user_telegram_id or not isinstance(user_telegram_id, int) or user_telegram_id <= 0:
            logging.warning(f"Пропуск невалидного ID пользователя: {user_telegram_id}")
            error_count += 1
            continue

        try:
            # Более надежная отправка сообщения с таймаутом
            try:
                await asyncio.wait_for(
                    bot.send_message(
                        chat_id=user_telegram_id,
                        text=f"🔔 <b>Системное уведомление</b>\n\n{notification_text}",
                        parse_mode="HTML"
                    ),
                    timeout=5.0  # 5 секунд таймаут
                )
                message_count += 1
                success_count += 1
                logging.info(f"Отправлено уведомление пользователю {user_telegram_id}")
            except asyncio.TimeoutError:
                error_count += 1
                logging.error(f"Превышено время ожидания при отправке сообщения пользователю {user_telegram_id}")

            # Обновляем индикатор прогресса каждые 5 пользователей или в конце
            if (idx + 1) % 5 == 0 or idx == len(users) - 1:
                await progress_message.edit_text(
                    f"🔄 <b>Выполняется отправка уведомлений пользователям...</b>\n\n"
                    f"• Разлогинено пользователей: <b>{users_updated}</b>\n"
                    f"• Отправлено уведомлений: <b>{message_count}/{len(users)}</b>\n"
                    f"• Прогресс: <b>{(idx + 1) * 100 // len(users)}%</b>",
                    parse_mode="HTML"
                )

            # Небольшая пауза между отправками
            await asyncio.sleep(0.3)
        except Exception as e:
            error_count += 1
            logging.error(f"Ошибка при отправке сообщения пользователю {user_telegram_id}: {e}", exc_info=True)

    # Очищаем состояние
    await state.clear()

    # Отправляем финальный отчет
    await progress_message.edit_text(
        f"✅ <b>Операция успешно завершена</b>\n\n"
        f"• Разлогинено пользователей: <b>{users_updated}</b>\n"
        f"• Отправлено уведомлений: <b>{success_count}/{len(users)}</b>\n"
        f"• Успешно: <b>{success_count}</b>\n"
        f"• Ошибок: <b>{error_count}</b>\n\n"
        f"<i>Примечание: вы не были разлогинены из системы.</i>",
        parse_mode="HTML"
    )


@dp.message(lambda msg: msg.text == "📊 Статистика очереди")
async def admin_queue_stats(message: Message):
    """Обработчик кнопки статистики очереди"""
    user_id = message.from_user.id
    if user_id != ADMIN_ID:
        return await message.answer("У вас нет доступа к этой функции.")

    # Получаем статистику очереди
    from bot.common import mass_search_queue
    queue_status = await mass_search_queue.get_queue_status()

    # Получаем статистику из базы данных
    from bot.database.db import get_mass_search_stats
    db_stats = get_mass_search_stats()

    # Формируем сообщение
    text = (
        f"📊 <b>Статистика массового пробива</b>\n\n"
        f"<b>Текущая очередь:</b>\n"
        f"• Всего в очереди: {queue_status['total']}\n"
        f"• Активных обработок: {queue_status['processing']}/{queue_status['capacity']}\n"
        f"• Ожидают: {queue_status['waiting']}\n\n"

        f"<b>Общая статистика:</b>\n"
        f"• Всего пробивов: {db_stats['total']}\n"
        f"• Завершено успешно: {db_stats['completed']}\n"
        f"• Завершено с ошибкой: {db_stats['failed']}\n"
        f"• Найдено телефонов: {db_stats['phones_found']}\n"
        f"• Среднее время: {int(db_stats['avg_time'] or 0)} сек\n\n"
    )

    # Показываем текущие активные запросы
    active_items = await mass_search_queue.get_all_items()
    processing_items = [item for item in active_items if item.processing]

    if processing_items:
        text += "<b>Активные запросы:</b>\n"
        for i, item in enumerate(processing_items, 1):
            text += f"{i}. ID: {item.user_id}, строк: {item.valid_lines}\n"

    # Показываем ожидающие запросы
    waiting_items = [item for item in active_items if not item.processing]
    if waiting_items:
        text += "\n<b>Ожидающие запросы:</b>\n"
        for i, item in enumerate(waiting_items, 1):
            text += f"{i}. ID: {item.user_id}, строк: {item.valid_lines}\n"

    # Показываем последние 5 пробивов
    if db_stats["recent"]:
        text += "\n<b>Последние пробивы:</b>\n"
        for i, item in enumerate(db_stats["recent"], 1):
            status_emoji = {
                "pending": "⏳",
                "processing": "🔄",
                "completed": "✅",
                "failed": "❌"
            }.get(item["status"], "⚪️")

            text += (
                f"{i}. {status_emoji} ID: {item['user_id']}, "
                f"строк: {item['valid_lines']}, "
                f"телефонов: {item['phones_found'] or 0}\n"
            )

    # Добавляем время обновления
    text += f"\n🕒 <b>Обновлено:</b> {datetime.now().strftime('%d.%m.%Y %H:%M:%S')}"

    await message.answer(text, parse_mode="HTML")


# ======= Запуск бота =======
async def on_startup():
    """Функция, вызываемая при старте бота: регистрируем команды и фоновые задачи."""
    # Проверяем соединение с Telegram API
    if not await test_telegram_connection():
        logging.warning("⚠️ Проблемы с соединением к Telegram API могут повлиять на работу бота")

    # Инициализируем пул сессий
    from bot.config import load_credentials
    credentials = load_credentials()
    logging.info(f"Loaded {len(credentials)} credentials for session pool")

    # Инициализируем пул сессий
    global session_pool
    from bot.session_manager import init_session_pool
    session_pool = init_session_pool(credentials, max_sessions=50)

    if session_pool is None:
        logging.error("Failed to initialize session pool")
    else:
        # Запускаем инициализацию всех сессий
        # Temporary workaround for missing initialize_sessions method
        if hasattr(session_pool, 'initialize_sessions'):
            success_count, fail_count = await session_pool.initialize_sessions()
        else:
            logging.warning("initialize_sessions method not found, skipping session initialization")
            success_count, fail_count = 0, 0
        logging.info(f"Session pool initialized: {success_count} successful, {fail_count} failed")

    # Запускаем фоновые задачи
    asyncio.create_task(clear_cache_daily())
    asyncio.create_task(notify_admin_about_zero_balance())

    # Запускаем фоновую задачу обновления сессий
    asyncio.create_task(refresh_expired_sessions())

    # Добавляем задачу обработки очереди
    from bot.mass_search import process_mass_search_queue
    asyncio.create_task(process_mass_search_queue(bot))

    logging.info("Фоновые задачи запущены.")

    # Регистрируем «синее меню» (список /команд)
    await register_bot_commands()


# Добавляем новую функцию для обновления сессий
async def refresh_expired_sessions():
    """Периодически обновляет неактивные сессии"""
    from bot.session_manager import session_pool
    while True:
        try:
            await session_pool.refresh_expired_sessions()
            logging.info("Выполнено обновление просроченных сессий")
        except Exception as e:
            logging.error(f"Ошибка при обновлении сессий: {str(e)}")

        # Ждем 30 минут перед следующим обновлением
        await asyncio.sleep(1800)


# Добавить в on_startup функцию проверки соединения с Telegram API
async def test_telegram_connection():
    """Проверяет соединение с Telegram API"""
    try:
        me = await bot.get_me()
        logging.info(f"Успешное соединение с Telegram API. Бот {me.username} запущен.")
        return True
    except Exception as e:
        logging.error(f"Ошибка соединения с Telegram API: {e}")
        return False


async def error_handler(event: ErrorEvent):
    """
    Централизованный обработчик ошибок.
    Логирует все исключения и отвечает пользователю.
    """
    exception = event.exception
    update = event.update

    # Логгирование всех ошибок
    logging.error(f"Необработанное исключение: {exception}", exc_info=True)

    try:
        # Если это обновление с сообщением, отвечаем пользователю
        if update and hasattr(update, 'message') and update.message:
            user_id = update.message.from_user.id
            await bot.send_message(
                user_id,
                "Произошла техническая ошибка. Пожалуйста, попробуйте позже или обратитесь в поддержку."
            )

            # Если ошибка критическая, уведомляем админа
            if ADMIN_ID and isinstance(exception, (ImportError, SyntaxError, KeyError, AttributeError)):
                error_msg = f"⚠️ Критическая ошибка:\n{str(exception)[:100]}...\n\n"
                error_msg += f"От пользователя: {user_id}\n"
                error_msg += f"Сообщение: {update.message.text[:50]}"
                await bot.send_message(ADMIN_ID, error_msg)
    except Exception as e:
        logging.error(f"Ошибка в обработчике ошибок: {e}")


# Обработчик кнопки "Добавить пользователя"
@dp.message(lambda msg: msg.text == "➕ Добавить пользователя")
async def cmd_add_user_menu(message: Message, state: FSMContext):
    """При нажатии в админ-меню на "➕ Добавить пользователя"."""
    if message.from_user.id != ADMIN_ID:
        return await message.answer("У вас нет доступа.")

    # Установим состояние ожидания данных пользователя
    await state.set_state(UserCreationState.waiting_for_data)
    await message.answer("Введите данные нового пользователя в формате:\n`логин пароль баланс`")


# Обработчик для состояния ввода данных нового пользователя
@dp.message(StateFilter(UserCreationState.waiting_for_data))
async def process_user_creation(message: Message, state: FSMContext):
    """Обработка ввода данных для создания нового пользователя"""
    if message.from_user.id != ADMIN_ID:
        await state.clear()
        return await message.answer("У вас нет доступа.")

    parts = message.text.strip().split()
    if len(parts) != 3:
        return await message.answer("Неверный формат. Используйте: `логин пароль баланс`")

    login, password, bal_str = parts
    try:
        bal = float(bal_str)
    except ValueError:
        return await message.answer("Баланс должен быть числом.")

    # Создаем пользователя
    ok, info = create_user(login, password, bal)
    await message.answer(info)

    # Сбрасываем состояние
    await state.clear()


# Добавляем обработчик для получения новой стоимости
@dp.message(StateFilter("CostState:waiting_for_cost"))
async def process_new_cost(message: Message, state: FSMContext):
    """Обработчик для получения новой стоимости запроса"""
    current_state = await state.get_state()
    if current_state != "CostState:waiting_for_cost":
        return

    user_id = message.from_user.id
    if user_id != ADMIN_ID:
        await state.clear()
        return

    try:
        new_cost = float(message.text)
        if new_cost <= 0:
            await message.answer("Стоимость должна быть больше нуля. Попробуйте снова:")
            return

        # Обновляем глобальную переменную
        global REQUEST_COST
        from bot.config import REQUEST_COST
        old_cost = REQUEST_COST

        # Импортируем для обновления значения
        import bot.config
        bot.config.REQUEST_COST = new_cost

        # Сохраняем новое значение в .env файл
        try:
            import os
            import re

            # Путь к .env файлу
            env_path = ".env"

            # Читаем содержимое файла
            if os.path.exists(env_path):
                with open(env_path, 'r') as file:
                    content = file.read()

                # Проверяем, есть ли строка с REQUEST_COST
                if re.search(r'^REQUEST_COST=', content, re.MULTILINE):
                    # Обновляем существующее значение
                    content = re.sub(r'^REQUEST_COST=.*$', f'REQUEST_COST={new_cost}', content, flags=re.MULTILINE)
                else:
                    # Добавляем новую строку
                    content += f'\nREQUEST_COST={new_cost}'

                # Записываем обновленное содержимое
                with open(env_path, 'w') as file:
                    file.write(content)

                await message.answer(
                    f"✅ Стоимость запроса изменена с ${old_cost:.2f} на ${new_cost:.2f} и сохранена в конфигурации.")
            else:
                # Если файла нет, создаем его
                with open(env_path, 'w') as file:
                    file.write(f'REQUEST_COST={new_cost}')
                await message.answer(
                    f"✅ Стоимость запроса изменена с ${old_cost:.2f} на ${new_cost:.2f} и сохранена в новом файле конфигурации.")

        except Exception as e:
            logging.error(f"Ошибка при сохранении настроек: {e}")
            await message.answer(
                f"✅ Стоимость запроса изменена с ${old_cost:.2f} на ${new_cost:.2f}, но не удалось сохранить в конфигурацию.")

        await state.clear()
    except ValueError:
        await message.answer("Введите корректное число. Например: 0.05")


async def main():
    """Точка входа для запуска бота (asyncio.run(main()))."""
    # Регистрируем глобальный обработчик ошибок диспетчера
    dp.errors.register(error_handler)

    setup_database()
    logging.info("База данных инициализирована.")
    await on_startup()
    logging.info("Бот запущен. Ожидаем сообщения...")
    await dp.start_polling(bot)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except (KeyboardInterrupt, SystemExit):
        logging.info("Бот остановлен вручную.")