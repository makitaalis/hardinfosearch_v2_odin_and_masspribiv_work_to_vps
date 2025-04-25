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
    Обрабатывает входящие сообщения с улучшенной обработкой ошибок и режимом отказоустойчивости.
    """
    # Check if user is in a state (like waiting for file upload)
    current_state = await state.get_state()
    if current_state:
        return

    user_id = message.from_user.id
    query_text = message.text.strip()

    # 1. Authentication check
    if not check_active_session(user_id):
        # Try to recognize login/password format
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

    # 2. Query validation
    query_text = normalize_query(query_text)
    valid, formatted_text = validate_query(query_text)
    if not valid:
        await message.answer(formatted_text)
        return
    query_text = formatted_text

    # 3. Process search request using the unified handler
    # This function contains all the search logic with fallback handling
    try:
        from bot.utils import handle_search_request
        await handle_search_request(message, query_text, state)
    except Exception as e:
        # Catch-all exception handler to prevent bot crashes
        logging.error(f"Error in handle_search_request: {e}", exc_info=True)

        # Send error message to user
        await message.answer(
            "⚠️ <b>Произошла ошибка при обработке запроса</b>\n\n"
            "Пожалуйста, попробуйте снова позже или обратитесь в поддержку.",
            parse_mode="HTML"
        )

        # Return balance in case of error
        try:
            refund_success, refund_message = refund_balance(user_id)
            if refund_success:
                await message.answer(f"💰 {refund_message}")
        except Exception as refund_error:
            logging.error(f"Error refunding balance: {refund_error}")


# Also update the extended_search command handler to use fallback mechanism
@router.message(Command("extended_search"))
async def cmd_extended_search(message: Message):
    """
    Обработчик команды /extended_search [запрос] с улучшенной обработкой ошибок.
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

    # Check if web service is available
    try:
        from bot.utils import check_web_service_available
        web_available = await check_web_service_available()

        if not web_available:
            await message.answer(
                "⚠️ <b>Расширенный поиск временно недоступен</b>\n\n"
                "Извините за неудобства, но в настоящее время наш поисковый сервис недоступен. "
                "Попробуйте использовать обычный поиск или повторите попытку позже.",
                parse_mode="HTML"
            )
            return
    except Exception as e:
        logging.error(f"Error checking web service availability: {e}")
        # Continue anyway - we'll check again when sending the request

    # Add prefix to distinguish extended search in cache
    cache_key = "extended__" + query

    # Check cache
    cached_found, cached_response, cache_source = get_best_cached_response(user_id, cache_key)
    if cached_found:
        formatted_text = format_api_response(cached_response)
        await message.answer(
            f"💾 Результат из кэша ({cache_source}):\n\n{formatted_text}",
            parse_mode="Markdown"
        )

        # Get HTML-file from cache or generate it
        html_path = await save_response_as_html(user_id, cache_key, cached_response)
        if html_path and os.path.exists(html_path):
            await message.answer_document(FSInputFile(html_path))
        return

    # Deduct balance
    success, response_text = deduct_balance(user_id)
    if not success:
        await message.answer(response_text)
        return

    # Start search
    status_message = await message.answer("🔍 Выполняю расширенный поиск, пожалуйста, подождите...")

    # Check session pool
    from bot.session_manager import session_pool
    if session_pool is None:
        await status_message.edit_text("Ошибка: система веб-поиска не инициализирована")
        refund_success, refund_message = refund_balance(user_id)
        await message.answer(refund_message)
        return

    # Use extended search
    success, api_resp = await send_web_request(query, session_pool)

    # Remove status message
    await status_message.delete()

    if not success:
        # Refund on error
        refund_success, refund_message = refund_balance(user_id)
        await message.answer(f"{api_resp}\n\n{refund_message}")
        return

    # Save to cache
    save_response_to_cache(user_id, cache_key, api_resp)

    # Format and send response
    formatted_response = format_api_response(api_resp)
    await message.answer(formatted_response, parse_mode="Markdown")

    # Generate HTML
    html_path = await save_response_as_html(user_id, cache_key, api_resp)
    if html_path and os.path.exists(html_path):
        await message.answer_document(FSInputFile(html_path))
    else:
        await message.answer("⚠ Ошибка при создании HTML-файла.")