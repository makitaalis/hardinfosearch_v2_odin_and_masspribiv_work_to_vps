import json
import logging
import os

from aiogram import Router
from aiogram.filters import Command
from aiogram.fsm.context import FSMContext
from aiogram.types import Message, FSInputFile, CallbackQuery

from bot import session_pool
from bot.config import ADMIN_ID
from bot.database.db import check_active_session, verify_password, get_cached_response, delete_cached_response, \
    deduct_balance, save_response_to_cache, check_low_balance, refund_balance
from bot.database.db import (
    get_user_balance
)
from bot.database.db import logout_user
from bot.keyboards import get_user_menu, get_admin_menu
from bot.utils import normalize_query, validate_query, filter_unique_data, send_api_request, format_api_response, \
    save_response_as_html, send_web_request
# Импортируем необходимые функции и модули
from bot.utils import (
    send_extended_api_request
)

from bot.database.db import check_active_session, verify_password, get_best_cached_response, delete_cached_response, \
    deduct_balance, save_response_to_cache, check_low_balance

from bot.session_manager import session_pool

router = Router()


@router.message(Command("start"))
async def cmd_start(message: Message):
    """
    Обработчик команды /start.
    Если пользователь уже вошёл, показываем меню.
    Иначе предлагаем ввести логин/пароль.
    """
    user_id = message.from_user.id

    # Проверяем входил ли пользователь
    if check_active_session(user_id):
        # Для администратора отдельное сообщение
        if user_id == ADMIN_ID:
            await message.answer(
                "👋 Приветствую, администратор!\n"
                "Выберите действие из меню ниже или используйте команды.",
                reply_markup=get_admin_menu()
            )
        else:
            # Для обычного пользователя показываем инлайн-меню
            await message.answer(
                "👋 Вы уже вошли в систему!\n"
                "Выберите действие из меню ниже:",
                reply_markup=get_user_menu()
            )
    else:
        await message.answer(
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

@router.message(Command("help"))
async def cmd_help(message: Message):
    """
    Обработчик команды /help.
    Показывает список доступных команд и краткую справку.
    """
    help_text = (
        "📌 *Доступные команды:*\n\n"
        "• `/balance` – узнать баланс\n"
        "• `/extended_search [запрос]` – расширенный поиск\n"
        "• `/logout` – выйти из системы\n\n"
        "Просто отправьте запрос в нужном формате (ФИО, номер авто, телефон, почта и т.д.), "
        "и бот выполнит поиск."
    )
    await message.answer(help_text, parse_mode="Markdown")


@router.message(Command("logout"))
async def cmd_logout(message: Message):
    """
    Команда /logout: выход из системы.
    """
    user_id = message.from_user.id
    if not check_active_session(user_id):
        await message.answer("Вы не вошли в систему.")
        return

    logout_user(user_id)
    logging.info(f"Пользователь {user_id} вышел из системы.")
    await message.answer(
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


@router.message(Command("extended_search"))
async def cmd_extended_search(message: Message):
    """
    Обработчик команды /extended_search [запрос].
    Использует веб-интерфейс вместо API.
    """
    user_id = message.from_user.id
    if not check_active_session(user_id):
        await message.answer("Вы не вошли в систему. Сначала введите логин и пароль.")
        return

    parts = message.text.strip().split(" ", 1)
    if len(parts) < 2:
        await message.answer("Использование: /extended_search [запрос]")
        return

    query = parts[1].strip()
    cache_key = "extended__" + query

    # 1. Проверяем кэш
    cached_found, cached_response, cache_source = get_best_cached_response(user_id, cache_key)
    if cached_found:
        formatted_text = format_api_response(cached_response)
        await message.answer(
            f"💾 Результат из кэша ({cache_source}):\n\n{formatted_text}",
            parse_mode="Markdown"
        )

        # Получаем HTML-файл из кэша или генерируем его
        html_path = await save_response_as_html(user_id, cache_key, cached_response)
        if html_path and os.path.exists(html_path):
            await message.answer_document(FSInputFile(html_path))

        return

    # 2. Списываем баланс
    success, response_text = deduct_balance(user_id)
    if not success:
        await message.answer(response_text)
        return

    # Информируем пользователя о начале поиска
    status_message = await message.answer("🔍 Выполняю расширенный поиск, пожалуйста, подождите...")

    # 3. Запрос через веб-интерфейс
    if session_pool is None:
        await status_message.edit_text("Ошибка: система веб-поиска не инициализирована")
        # Возвращаем средства
        refund_success, refund_message = refund_balance(user_id)
        return

    # Используем расширенный поиск (важно передать флаг extended=True)
    success, api_resp = await send_web_request(query, session_pool)

    # Удаляем сообщение о статусе
    await status_message.delete()

    if not success:
        # Возвращаем средства при ошибке
        refund_success, refund_message = refund_balance(user_id)
        await message.answer(f"{api_resp}\n\n{refund_message}")
        return

    # Сохраняем результат в кэш с префиксом extended__
    save_response_to_cache(user_id, cache_key, api_resp)

    # Форматируем и отправляем результат
    formatted_response = format_api_response(api_resp)
    await message.answer(formatted_response, parse_mode="Markdown")

    # Генерация HTML
    html_path = await save_response_as_html(user_id, cache_key, api_resp)
    if html_path and os.path.exists(html_path):
        await message.answer_document(FSInputFile(html_path))
    else:
        await message.answer("⚠ Ошибка при создании HTML-файла.")


# Обработчик для вызова меню пользователя
@router.message(Command("menu"))
async def cmd_user_menu(message: Message):
    """Показывает пользовательское меню с кнопками"""
    user_id = message.from_user.id
    if not check_active_session(user_id):
        await message.answer("Вы не вошли в систему. Сначала введите логин и пароль.")
        return

    await message.answer(
        "Выберите действие из меню:",
        reply_markup=get_user_menu()
    )


# Обработчики для кнопок инлайн-меню пользователя
@router.callback_query(lambda c: c.data == "user_balance")
async def cb_user_balance(callback: CallbackQuery):
    """Обработка кнопки 'Мой баланс'"""
    user_id = callback.from_user.id
    if not check_active_session(user_id):
        await callback.answer("Вы не вошли в систему", show_alert=True)
        return

    balance = get_user_balance(user_id)
    if balance is not None:
        await callback.message.answer(f"Ваш текущий баланс: ${balance:.2f}")
    else:
        await callback.message.answer("Не удалось получить информацию о балансе")
    await callback.answer()


@router.callback_query(lambda c: c.data == "search_help")
async def cb_search_help(callback: CallbackQuery):
    """Обработка кнопки 'Поиск'"""
    await callback.message.answer(
        "🔍 <b>Поиск информации</b>\n\n"
        "Отправьте один из следующих типов запросов:\n"
        "• ФИО + Дата рождения (Иванов Иван 01.01.1990)\n"
        "• Номер телефона (79001234567)\n"
        "• Номер паспорта (1234 567890)\n"
        "• VIN автомобиля (XTA210990Y1234567)\n"
        "• Госномер автомобиля (А123БВ77)\n"
        "• Почта (user@example.com)\n"
        "• ИНН (1234567890)\n"
        "• СНИЛС (12345678901)"
    )
    await callback.answer()


@router.callback_query(lambda c: c.data == "extended_search_info")
async def cb_extended_search_info(callback: CallbackQuery):
    """Обработка кнопки 'Расширенный поиск'"""
    await callback.message.answer(
        "🔎 <b>Расширенный поиск</b>\n\n"
        "Для выполнения расширенного поиска используйте команду:\n"
        "<code>/extended_search запрос</code>\n\n"
        "Расширенный поиск позволяет получить более подробную информацию "
        "и обрабатывает дополнительные источники данных."
    )
    await callback.answer()


@router.callback_query(lambda c: c.data == "logout")
async def cb_logout(callback: CallbackQuery):
    """Обработка кнопки 'Выйти'"""
    user_id = callback.from_user.id
    logout_user(user_id)
    await callback.message.answer("Вы вышли из системы.")
    await callback.answer()



# ======= Универсальный обработчик (поисковые запросы) =======
# Модифицируем universal_message_handler для использования веб-запросов
@router.message(lambda message: message.text is not None and not message.text.startswith('/'))
async def universal_message_handler(message: Message, state: FSMContext):
    """
    Обрабатывает входящие сообщения:
    1. Проверка авторизации.
    2. Если не авторизован – попытка считать их как логин+пароль.
    3. Проверка кэша, если запрос валиден.
    4. Списание средств, запрос к сайту, отправка HTML-отчёта.
    5. Возврат средств, если API вернуло None или ошибку.
    """

    from bot.config import ADMIN_ID
    from bot.database.db import refund_balance
    from bot.analytics import log_request
    from bot.session_manager import session_pool
    import time
    import json
    import os

    # Проверяем, находится ли пользователь в каком-то состоянии
    current_state = await state.get_state()
    if current_state:
        return

    user_id = message.from_user.id
    query_text = message.text.strip()

    # 1. Проверка авторизации
    if not check_active_session(user_id):
        # Пытаемся распознать формат "логин пароль"
        parts = query_text.split(maxsplit=1)
        if len(parts) == 2:
            login, password = parts
            success, info = verify_password(login, password, user_id, {
                'first_name': message.from_user.first_name,
                'last_name': message.from_user.last_name,
                'username': message.from_user.username
            })
            await message.answer(info)
            if success:
                await message.answer(
                    f"👋 Добро пожаловать!\nДля поиска информации отправьте ФИО, номер телефона, email или другие данные.",
                    reply_markup=get_user_menu()
                )
        else:
            await message.answer(
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
        return

    # 2. Проверяем формат запроса
    query_text = normalize_query(query_text)
    valid, formatted_text = validate_query(query_text)
    if not valid:
        await message.answer(formatted_text)
        return
    query_text = formatted_text

    # 3. Проверка кэша
    cached_found, cached_response, cache_source = get_best_cached_response(user_id, query_text)
    if cached_found:
        # Форматируем ответ из кэша без HTML-тегов для удобного чтения
        formatted_text = format_api_response(cached_response, use_html=False)
        await message.answer(
            f"💾 Результат из кэша ({cache_source}):\n\n{formatted_text}"
        )

        # Получаем HTML-файл из кэша или генерируем его
        html_path = await save_response_as_html(user_id, query_text, cached_response)
        if html_path and os.path.exists(html_path):
            await message.answer_document(FSInputFile(html_path))

        return

    # 4. Проверяем баланс перед отправкой запроса
    logging.info(f"🚀 Попытка списания баланса для user_id={user_id}")
    success, balance_message = deduct_balance(user_id)
    logging.info(f"🎯 Результат списания: {success}, {balance_message}")
    if not success:
        await message.answer(balance_message)
        return

    # 5. Отправляем запрос через веб-интерфейс вместо API
    start_time = time.time()

    # Информируем пользователя о начале поиска
    status_message = await message.answer("🔍 Выполняю поиск, пожалуйста, подождите...")

    # Проверяем наличие пула сессий
    if session_pool is None:
        logging.error("Пул сессий не инициализирован при обработке запроса")
        await status_message.edit_text("Ошибка: система веб-поиска не инициализирована")
        # Возвращаем средства
        refund_success, refund_message = refund_balance(user_id)
        return

    # Проверяем состояние пула сессий
    pool_stats = session_pool.get_stats()
    logging.info(f"Статистика пула сессий: активных {pool_stats['active_sessions']} из {pool_stats['total_sessions']}")

    # Отправляем запрос через веб-интерфейс
    from bot.utils import send_web_request
    success, api_response = await send_web_request(query_text)
    execution_time = time.time() - start_time

    # Логируем запрос
    try:
        response_size = len(json.dumps(api_response).encode('utf-8')) if api_response else 0
    except:
        response_size = 0

    log_request(
        user_id=user_id,
        query=query_text,
        request_type='web',
        source='web',
        success=(success and api_response is not None),
        execution_time=execution_time,
        response_size=response_size
    )

    # Удаляем сообщение о статусе
    await status_message.delete()

    # Проверяем результат и возвращаем средства при необходимости
    if not success or api_response is None or (isinstance(api_response, list) and len(api_response) == 0):
        refund_success, refund_message = refund_balance(user_id)
        logging.info(f"💰 Результат возврата средств: {refund_success}, {refund_message}")

        await message.answer(
            "ℹ <b>Информация в базах не найдена.</b>\n\n"
            "📌 <i>Обратите внимание:</i> Введенные данные могут отличаться от записей в базе. "
            "Рекомендуем проверить корректность запроса и попробовать снова.\n\n"
            f"💰 {refund_message}",
            parse_mode="HTML"
        )
        return

    # 6. Форматируем и отправляем результат
    try:
        filtered_response = filter_unique_data(api_response)

        # Форматируем результат БЕЗ HTML-тегов для удобного чтения в чате
        formatted_text = format_api_response(filtered_response, use_html=False)
        await message.answer(formatted_text)
    except Exception as e:
        logging.error(f"❌ Ошибка при форматировании ответа: {str(e)}")
        await message.answer("⚠ Ошибка при обработке данных.")

        # Возвращаем средства при ошибке обработки
        refund_success, refund_message = refund_balance(user_id)
        await message.answer(f"💰 {refund_message}")
        return

    # 7. Сохраняем в кэш
    try:
        save_response_to_cache(user_id, query_text, api_response)
    except Exception as e:
        logging.error(f"❌ Ошибка при сохранении в кэш: {str(e)}")

    # 8. Генерация HTML и отправка файла
    logging.info(f"📄 Генерация HTML-отчета для user_id={user_id}, query={query_text}")
    html_path = await save_response_as_html(user_id, query_text, api_response)
    if html_path and os.path.exists(html_path) and os.path.getsize(html_path) > 0:
        await message.answer_document(document=FSInputFile(html_path))
    else:
        logging.error(f"❌ Ошибка: HTML-файл {html_path} не создан или пуст.")
        await message.answer("⚠ Ошибка при создании HTML-файла.")

    # 9. Проверяем баланс пользователя и отправляем предупреждение, если он низкий
    low_balance, warning_message = check_low_balance(user_id)
    if low_balance:
        await message.answer(warning_message)