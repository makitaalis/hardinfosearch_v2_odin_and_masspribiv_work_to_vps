import asyncio
import html as html_escape
import json
import logging
import os
import re
import time
import traceback
from datetime import datetime

import requests
from aiogram.types import FSInputFile

from bot.analytics import log_request
from bot.database.db import check_low_balance, save_response_to_cache, refund_balance, get_best_cached_response, \
    deduct_balance

# Убираем циклический импорт
# НЕ использовать: from bot.utils import send_web_request

# Глобальные переменные для rate limiting
LAST_REQUEST_TIME = 0
MIN_REQUEST_INTERVAL = 0.5  # секунды между запросами

API_BASE_URL = "https://infosearch54321.xyz"

# Global flag to indicate whether the web service is available
WEB_SERVICE_AVAILABLE = False
LAST_SERVICE_CHECK = 0
SERVICE_CHECK_INTERVAL = 300  # 5 minutes

# Загружаем токен для доступа к API из переменных окружения (файл .env)
API_TOKEN = os.getenv("API_TOKEN")

# Регулярные выражения для проверки запроса
PATTERNS = {
    "ФИО": r"^[А-ЯЁA-Z][а-яёa-z]+\s[А-ЯЁa-z][а-яёa-z]+(\s[А-ЯЁA-Z][а-яёa-z]+)?(\s\d{2}\.\d{2}(\.\d{4})?)?$",
    "Авто (гос. номер)": r"^[А-ЯЁA-Z]\d{3}[А-ЯЁa-zA-Z]{2}\d{2,3}$",
    "Авто (VIN)": r"^[A-HJ-NPR-Z0-9]{17}$",
    "Телефон": r"^(7|3)\d{10}$",
    "Почта": r"^[a-zA-Z0-9_.+\-]+@[a-zA-Z0-9\-]+\.[a-zA-Z0-9.\-]+$",
    "Логин": r"^\w{3,20}$",
    "Паспорт": r"^\d{4}\s?\d{6}$",
    "ИНН": r"^\d{10,12}$",
    "СНИЛС": r"^\d{11}$",
    "ОГРН": r"^\d{13}$"
}

# Перевод базы данных на русский язык (используется в translate_database_entry, если нужно)
DATABASE_TRANSLATIONS = {}


async def send_web_request(query: str, session_pool_instance=None):
    """
    Significantly improved web request function that gracefully handles authentication and session failures.

    :param query: Строка запроса
    :param session_pool_instance: Экземпляр пула сессий для выполнения запроса
    :return: (success, data_or_error)
    """
    logging.info(f"Web request for query: {query}")

    # Get session pool
    if session_pool_instance is None:
        # Try to get global session pool
        try:
            from bot.session_manager import session_pool
            session_pool_instance = session_pool
        except Exception as e:
            logging.error(f"Error importing session pool: {e}")
            return False, "Ошибка: не удалось получить доступ к пулу сессий"

    if session_pool_instance is None:
        logging.error("Session pool is not initialized")
        return False, "Ошибка: система веб-поиска не инициализирована"

    # Normalize query
    normalized_query = normalize_query(query)

    # Try up to 3 times with different sessions
    max_attempts = 3
    last_error = None

    for attempt in range(max_attempts):
        try:
            # If not the first attempt, add some delay
            if attempt > 0:
                delay = 1.0 * (attempt + 1)
                logging.info(f"Retrying web request (attempt {attempt + 1}/{max_attempts}), waiting {delay:.1f}s")
                await asyncio.sleep(delay)

            # Try to use perform_search
            try:
                success, result = await session_pool_instance.perform_search(normalized_query)

                if success:
                    logging.info(f"Web request succeeded for query: {query}")
                    return True, result
                else:
                    logging.warning(f"Web request attempt {attempt + 1} failed: {result}")
                    last_error = result
            except AttributeError as e:
                # If perform_search is missing, try a direct approach
                logging.warning(f"perform_search not available in session pool: {e}")
                last_error = "Метод perform_search недоступен"

                # Try an alternative approach - get and use a session directly
                try:
                    session = None
                    session = await session_pool_instance.get_available_session(timeout=20)

                    if session and hasattr(session, 'search'):
                        # Ensure the session is authenticated
                        if not session.is_authenticated:
                            auth_success = await session.authenticate()
                            if not auth_success:
                                logging.error("Session authentication failed")
                                continue

                        # Now try to search
                        success, result = await session.search(normalized_query)

                        if success:
                            logging.info(f"Direct search succeeded for query: {query}")
                            return True, result
                        else:
                            logging.warning(f"Direct search failed: {result}")
                            last_error = result
                    else:
                        logging.error("Got invalid session object or session without search method")
                        last_error = "Недопустимый объект сессии"
                finally:
                    # Release the session if we got one
                    if session and hasattr(session_pool_instance, 'release_session'):
                        await session_pool_instance.release_session(session)

        except Exception as e:
            logging.error(f"Web request exception on attempt {attempt + 1}: {e}")
            last_error = str(e)

    # If we get here, all attempts failed
    error_message = last_error if last_error else "Неизвестная ошибка"
    logging.error(f"Web request failed after {max_attempts} attempts: {error_message}")
    return False, f"Ошибка поиска: {error_message}"


async def send_api_request(query: str):
    """
    Отправляет запрос через веб-интерфейс сайта вместо API.

    Возвращает кортеж (success, data_or_error):
      success = True  => data_or_error = результат (list или dict)
      success = False => data_or_error = сообщение об ошибке (str)
    """
    # Импортируем здесь, чтобы избежать циклических зависимостей
    from bot.main import session_pool

    if not query:
        return False, "Пустой запрос"

    logging.info(f"Отправка запроса через веб-интерфейс: {query}")
    start_time = time.time()

    try:
        # Проверяем, что пул сессий инициализирован
        if session_pool is None:
            return False, "Ошибка: пул сессий не инициализирован"

        # Вызываем функцию поиска через веб-интерфейс
        success, result = await send_web_request(query, session_pool)

        end_time = time.time()
        execution_time = round(end_time - start_time, 2)

        if success:
            logging.info(
                f"Запрос '{query}' выполнен успешно за {execution_time} сек. Получено {len(result) if isinstance(result, list) else 0} записей")
            return True, result
        else:
            error_message = result if isinstance(result, str) else "Неизвестная ошибка"
            logging.error(f"Ошибка веб-поиска: {error_message}")
            return False, error_message

    except Exception as e:
        end_time = time.time()
        execution_time = round(end_time - start_time, 2)

        logging.error(f"Исключение при выполнении веб-запроса '{query}' ({execution_time} сек): {e}")
        return False, f"Ошибка при запросе: {str(e)}"


1


def validate_query(query: str):
    """
    Улучшенная проверка запроса с дополнительными проверками для ФИО+дата
    """
    # Форматируем ввод перед проверкой
    query = format_fio_and_date(query)

    # Проверка запроса на соответствие одному из шаблонов
    for pattern_name, pattern in PATTERNS.items():
        if re.match(pattern, query):
            return True, query  # query уже исправлен

    # Специальная проверка для ФИО + дата
    fio_date_pattern = r"^[А-ЯЁA-Z][а-яёa-z]+(\s[А-ЯЁA-Z][а-яёa-z]+){1,2}\s\d{2}\.\d{2}\.\d{4}$"
    if re.match(fio_date_pattern, query):
        return True, query

    # Особая обработка для потенциальных ФИО+дата с ошибками
    words = query.strip().split()
    if len(words) >= 3:  # Минимум: Фамилия Имя ДД.ММ.ГГГГ
        # Проверяем наличие даты в любой части запроса
        date_pattern = r"(\d{1,2})[.\-/](\d{1,2})[.\-/](\d{2,4})"

        for word in words:
            match = re.match(date_pattern, word)
            if match:
                # Форматируем части ФИО и дату
                fio_parts = [w.capitalize() for w in words if not re.match(date_pattern, w)]

                day, month, year = match.groups()
                day = day.zfill(2)
                month = month.zfill(2)

                if len(year) == 2:
                    year_prefix = "20" if int(year) < 30 else "19"
                    year = year_prefix + year

                date = f"{day}.{month}.{year}"

                formatted_query = " ".join(fio_parts) + " " + date
                return True, formatted_query

    # Стандартные сообщения об ошибке форматирования
    help_text = (
        "❗ Неверный формат запроса. Используйте примеры:\n\n"
        "📌 Поиск по ФИО: `Иванов Иван 01.01.2000`\n"
        "📌 Поиск по авто (гос. номер): `А001АА77`\n"
        "📌 Поиск по VIN: `XTA212130T1186583`\n"
        "📌 Поиск по телефону: `79221110500`\n"
        "📌 Поиск по почте: `ivanov@mail.ru`\n"
        "📌 Поиск по паспорту: `4616 233456`\n"
        "📌 Поиск по ИНН: `7707083893`\n"
        "📌 Поиск по СНИЛС: `00461487830`\n"
        "📌 Поиск по ОГРН: `1027739099772`\n"
    )
    return False, help_text


# Изменения в функции format_api_response в файле utils.py

def format_api_response(api_response, limit_length=True, max_length=3800, use_html=True):
    """
    Улучшенное оформление с эмодзи и короткими заголовками.
    Форматирует данные в удобном для чтения виде, с валидацией телефонных номеров,
    сортировкой записей и улучшенным представлением задолженностей.

    :param api_response: Ответ API для форматирования
    :param limit_length: Ограничивать ли длину сообщения
    :param max_length: Максимальная длина сообщения
    :param use_html: Использовать ли HTML-теги для форматирования
    :return: Отформатированное сообщение
    """
    if not api_response:
        return "❌ Данные не найдены."

    # Названия категорий -> эмодзи
    categories = {
        "ЗАПИСАН В БАЗАХ": "👤",
        "ДАТА РОЖДЕНИЯ": "🎂",
        "ТЕЛЕФОН": "📞",
        "ПОЧТА": "📧",
        "ИНН": "💳",
        "ПАСПОРТ": "🆔",
        "СНИЛС": "📑"
    }

    # Порядок вывода категорий (приоритет)
    category_order = [
        "ЗАПИСАН В БАЗАХ",
        "ДАТА РОЖДЕНИЯ",
        "ТЕЛЕФОН",
        "ПОЧТА",
        "ИНН",
        "ПАСПОРТ",
        "СНИЛС"
    ]

    data_store = {cat: [] for cat in categories}
    unique_databases = set()
    total_records = 0

    # Для данных FSSP
    fssp_data = []
    total_debt = 0  # Общая сумма задолженности

    # Проходим по всем записям, собираем значения
    for record in api_response:
        if not isinstance(record, dict):
            continue
        total_records += 1

        # Если нужен учёт баз
        db_name = record.get("database", "").lower()
        if db_name:
            unique_databases.add(db_name)

            # Проверяем, если это база FSSP или содержит информацию о задолженностях
            is_fssp = False
            if 'fssp' in db_name or 'пристав' in db_name or 'исполнит' in db_name:
                is_fssp = True
            else:
                # Проверяем по ключевым полям, даже если база не упоминает FSSP в названии
                for key in record.keys():
                    upper_key = key.upper() if isinstance(key, str) else ""
                    if "ЗАДОЛЖЕННОСТЬ" in upper_key or "ДОЛГ" in upper_key or "ИП" in upper_key or "ПРИСТАВ" in upper_key:
                        is_fssp = True
                        break

            if is_fssp:
                fssp_item = {}

                # Копируем все поля в верхнем регистре для унификации
                for key, value in record.items():
                    if isinstance(key, str):
                        fssp_item[key.upper()] = value

                # Обработка синонимов полей
                if "ДОЛГ" in fssp_item and "ЗАДОЛЖЕННОСТЬ" not in fssp_item:
                    fssp_item["ЗАДОЛЖЕННОСТЬ"] = fssp_item["ДОЛГ"]
                if "ОСП" in fssp_item and "КОММЕНТАРИЙ" not in fssp_item:
                    fssp_item["КОММЕНТАРИЙ"] = fssp_item["ОСП"]

                # Если есть сумма задолженности, преобразуем в число для подсчета общей суммы
                if "ЗАДОЛЖЕННОСТЬ" in fssp_item:
                    try:
                        debt_amount = fssp_item["ЗАДОЛЖЕННОСТЬ"]
                        if isinstance(debt_amount, str):
                            # Удаляем нечисловые символы, кроме точки и запятой
                            debt_str = ''.join(c for c in debt_amount if c.isdigit() or c in '.,')
                            debt_str = debt_str.replace(',', '.')
                            debt_value = float(debt_str)
                            total_debt += debt_value
                            # Обновляем строковое представление для вывода
                            fssp_item["ЗАДОЛЖЕННОСТЬ_ЧИСЛО"] = debt_value
                    except (ValueError, TypeError):
                        pass  # Если не удалось преобразовать в число, просто пропускаем

                # Убедимся, что запись не пустая
                if fssp_item:
                    fssp_data.append(fssp_item)

        # Основные данные
        for key, value in record.items():
            if not isinstance(key, str):
                continue

            key_upper = key.upper()

            # ФИО
            if key_upper == "ФИО" and value and value not in data_store["ЗАПИСАН В БАЗАХ"]:
                data_store["ЗАПИСАН В БАЗАХ"].append(value)

            # ДАТА РОЖДЕНИЯ
            elif key_upper == "ДАТА РОЖДЕНИЯ" and value and value not in data_store["ДАТА РОЖДЕНИЯ"]:
                # Проверяем и форматируем дату (если это строка формата ДД.ММ.ГГГГ)
                data_store["ДАТА РОЖДЕНИЯ"].append(value)

            # ТЕЛЕФОН
            elif key_upper == "ТЕЛЕФОН":
                if value:
                    if isinstance(value, list):
                        for p in value:
                            if p and p not in data_store["ТЕЛЕФОН"]:
                                # Проверяем валидность телефона
                                phone = format_phone_number(p)
                                if phone and phone not in data_store["ТЕЛЕФОН"]:
                                    data_store["ТЕЛЕФОН"].append(phone)
                    else:
                        # Проверяем валидность телефона
                        phone = format_phone_number(value)
                        if phone and phone not in data_store["ТЕЛЕФОН"]:
                            data_store["ТЕЛЕФОН"].append(phone)

            # ПОЧТА
            elif key_upper == "ПОЧТА":
                if value:
                    if isinstance(value, list):
                        for em in value:
                            if em and em not in data_store["ПОЧТА"] and isinstance(em, str) and "@" in em:
                                data_store["ПОЧТА"].append(em)
                    else:
                        if value not in data_store["ПОЧТА"] and isinstance(value, str) and "@" in value:
                            data_store["ПОЧТА"].append(value)

            # ИНН
            elif key_upper == "ИНН" and value and value not in data_store["ИНН"]:
                # Проверяем валидность ИНН (10 или 12 цифр)
                if isinstance(value, str) and value.isdigit() and len(value) in (10, 12):
                    data_store["ИНН"].append(value)

            # ПАСПОРТ
            elif key_upper == "ПАСПОРТ":
                if value:
                    if isinstance(value, list):
                        for pas in value:
                            if pas and pas not in data_store["ПАСПОРТ"]:
                                data_store["ПАСПОРТ"].append(pas)
                    else:
                        if value not in data_store["ПАСПОРТ"]:
                            data_store["ПАСПОРТ"].append(value)

            # СНИЛС
            elif key_upper == "СНИЛС" and value and value not in data_store["СНИЛС"]:
                # Форматируем СНИЛС, если это строка цифр
                if isinstance(value, str):
                    # Удаляем нецифровые символы
                    snils_digits = ''.join(c for c in value if c.isdigit())
                    if len(snils_digits) == 11:
                        # Форматируем: XXX-XXX-XXX XX
                        formatted_snils = f"{snils_digits[:3]}-{snils_digits[3:6]}-{snils_digits[6:9]} {snils_digits[9:]}"
                        data_store["СНИЛС"].append(formatted_snils)
                    else:
                        data_store["СНИЛС"].append(value)
                else:
                    data_store["СНИЛС"].append(value)

    # Формируем текст для вывода
    lines = []

    # Если есть данные FSSP, добавляем специальный "сигнальный" блок
    if fssp_data:
        # Сортируем задолженности по сумме (от большей к меньшей)
        fssp_data_sorted = sorted(
            fssp_data,
            key=lambda x: x.get("ЗАДОЛЖЕННОСТЬ_ЧИСЛО", 0) if "ЗАДОЛЖЕННОСТЬ_ЧИСЛО" in x else 0,
            reverse=True
        )

        # Выделенный блок с задолженностями
        if use_html:
            lines.append("<b>🚨 ВНИМАНИЕ! НАЙДЕНЫ ИСПОЛНИТЕЛЬНЫЕ ПРОИЗВОДСТВА 🚨</b>")
            lines.append(f"<b>Количество записей: {len(fssp_data)}</b>")
            if total_debt > 0:
                lines.append(f"<b>Общая сумма задолженности: <code>{total_debt:.2f}₽</code></b>")
        else:
            lines.append("🚨 ВНИМАНИЕ! НАЙДЕНЫ ИСПОЛНИТЕЛЬНЫЕ ПРОИЗВОДСТВА 🚨")
            lines.append(f"Количество записей: {len(fssp_data)}")
            if total_debt > 0:
                lines.append(f"Общая сумма задолженности: {total_debt:.2f}₽")
        lines.append("")  # Пустая строка для разделения

    if use_html:
        lines.append("<b>Найденные данные:</b>\n")
    else:
        lines.append("Найденные данные:\n")

    # Если есть данные FSSP, показываем сводную информацию о задолженностях
    if fssp_data:
        if use_html:
            lines.append("⚖️ <b>ДАННЫЕ ИСПОЛНИТЕЛЬНЫХ ПРОИЗВОДСТВ:</b>")
        else:
            lines.append("⚖️ ДАННЫЕ ИСПОЛНИТЕЛЬНЫХ ПРОИЗВОДСТВ:")

        # Показываем информацию о каждой задолженности (до 10 задолженностей)
        max_fssp_to_show = min(10, len(fssp_data_sorted))
        for i, entry in enumerate(fssp_data_sorted[:max_fssp_to_show], 1):
            # Всегда начинаем с порядкового номера и суммы
            if use_html:
                base_info = f"<b>Задолженность {i}</b>"
                if "ЗАДОЛЖЕННОСТЬ_ЧИСЛО" in entry:
                    base_info += f" - <b><code>{entry['ЗАДОЛЖЕННОСТЬ_ЧИСЛО']:.2f}₽</code></b>"
                elif "ЗАДОЛЖЕННОСТЬ" in entry:
                    base_info += f" - <b><code>{entry['ЗАДОЛЖЕННОСТЬ']}₽</code></b>"
                elif "ДОЛГ" in entry:
                    base_info += f" - <b><code>{entry['ДОЛГ']}₽</code></b>"
            else:
                base_info = f"Задолженность {i}"
                if "ЗАДОЛЖЕННОСТЬ_ЧИСЛО" in entry:
                    base_info += f" - {entry['ЗАДОЛЖЕННОСТЬ_ЧИСЛО']:.2f}₽"
                elif "ЗАДОЛЖЕННОСТЬ" in entry:
                    base_info += f" - {entry['ЗАДОЛЖЕННОСТЬ']}₽"
                elif "ДОЛГ" in entry:
                    base_info += f" - {entry['ДОЛГ']}₽"

            # Добавляем основную строку
            lines.append(f"  • {base_info}")

            # Формируем вторую строку с дополнительной информацией
            details = []

            # Дата ИП
            if "ДАТА ИП" in entry:
                if use_html:
                    details.append(f"Дата: <code>{entry['ДАТА ИП']}</code>")
                else:
                    details.append(f"Дата: {entry['ДАТА ИП']}")

            # Номер ИП
            if "НОМЕР ИП" in entry:
                if use_html:
                    details.append(f"№ИП: <code>{entry['НОМЕР ИП']}</code>")
                else:
                    details.append(f"№ИП: {entry['НОМЕР ИП']}")

            # Комментарий/ОСП
            if "КОММЕНТАРИЙ" in entry:
                comment = entry["КОММЕНТАРИЙ"]
                if len(comment) > 30:  # Если комментарий слишком длинный, сокращаем
                    comment = comment[:27] + "..."
                if use_html:
                    details.append(f"ОСП: <code>{comment}</code>")
                else:
                    details.append(f"ОСП: {comment}")

            # Адрес
            if "АДРЕС" in entry:
                address = entry["АДРЕС"]
                if len(address) > 40:  # Если адрес слишком длинный, сокращаем
                    address = address[:37] + "..."
                if use_html:
                    details.append(f"Адрес: <code>{address}</code>")
                else:
                    details.append(f"Адрес: {address}")

            # Если есть детали, добавляем их на следующей строке с отступом
            if details:
                lines.append(f"    {' | '.join(details)}")

        # Если есть еще задолженности, добавляем примечание
        if len(fssp_data) > max_fssp_to_show:
            lines.append(f"  • ... и еще {len(fssp_data) - max_fssp_to_show} записей")

        lines.append("")  # Пустая строка для разделения

    # Максимальное количество элементов для отображения
    max_items_per_category = 15 if limit_length else float('inf')

    # Пробегаем по категориям в заданном порядке
    for cat_name in category_order:
        items = data_store[cat_name]
        if not items:
            continue

        emoji = categories[cat_name]
        if use_html:
            lines.append(f"{emoji} <b>{cat_name}:</b>")
        else:
            lines.append(f"{emoji} {cat_name}:")

        # Сортируем данные для лучшей читаемости
        sorted_items = sorted_items_by_category(cat_name, items)

        # Показываем элементы с ограничением
        items_to_show = sorted_items[:max_items_per_category] if limit_length else sorted_items

        for val in items_to_show:
            # Оборачиваем значения в <code> для возможности копирования в Telegram (только если use_html=True)
            if use_html:
                lines.append(f"  • <code>{val}</code>")
            else:
                lines.append(f"  • {val}")

        # Если показаны не все элементы, добавляем примечание
        if limit_length and len(sorted_items) > max_items_per_category:
            lines.append(f"  • ... и еще {len(sorted_items) - max_items_per_category} записей")

        lines.append("")  # Пустая строка между блоками

    # В конце — информация о базах и записях
    bases_count = len(unique_databases)
    lines.append(f"Количество баз: {bases_count}")
    lines.append(f"Количество записей: {total_records}")

    # Если есть задолженности, повторяем это в конце для привлечения внимания
    if fssp_data:
        if use_html:
            lines.append(f"<b>Обнаружено исполнительных производств: {len(fssp_data)}</b>")
        else:
            lines.append(f"Обнаружено исполнительных производств: {len(fssp_data)}")

    lines.append("\nПолная информация доступна в файле ниже 📂")

    # Склеиваем в одну строку
    full_message = "\n".join(lines)

    # Проверяем длину и при необходимости урезаем
    if limit_length and len(full_message) > max_length:
        # Создаем краткое сообщение
        if use_html:
            short_message = "<b>Найденные данные (сокращенный вид):</b>\n\n"
        else:
            short_message = "Найденные данные (сокращенный вид):\n\n"

        # Если есть задолженности, обязательно показываем это в кратком сообщении
        if fssp_data:
            if use_html:
                short_message += "<b>🚨 ВНИМАНИЕ! НАЙДЕНЫ ИСПОЛНИТЕЛЬНЫЕ ПРОИЗВОДСТВА 🚨</b>\n"
                short_message += f"<b>Количество: {len(fssp_data)}</b>\n"
                if total_debt > 0:
                    short_message += f"<b>Общая сумма: <code>{total_debt:.2f}₽</code></b>\n\n"
            else:
                short_message += "🚨 ВНИМАНИЕ! НАЙДЕНЫ ИСПОЛНИТЕЛЬНЫЕ ПРОИЗВОДСТВА 🚨\n"
                short_message += f"Количество: {len(fssp_data)}\n"
                if total_debt > 0:
                    short_message += f"Общая сумма: {total_debt:.2f}₽\n\n"

            # Добавляем 1-3 записи с задолженностями даже в кратком виде
            if use_html:
                short_message += "⚖️ <b>ДАННЫЕ ИСПОЛНИТЕЛЬНЫХ ПРОИЗВОДСТВ:</b>\n"
            else:
                short_message += "⚖️ ДАННЫЕ ИСПОЛНИТЕЛЬНЫХ ПРОИЗВОДСТВ:\n"

            for i, entry in enumerate(fssp_data_sorted[:3], 1):
                # Формируем строку задолженности
                if use_html:
                    debt_line = f"<b>Задолженность {i}</b>"
                    # Добавляем сумму задолженности, если она есть
                    if "ЗАДОЛЖЕННОСТЬ_ЧИСЛО" in entry:
                        debt_line += f" - <b><code>{entry['ЗАДОЛЖЕННОСТЬ_ЧИСЛО']:.2f}₽</code></b>"
                    elif "ЗАДОЛЖЕННОСТЬ" in entry:
                        debt_line += f" - <b><code>{entry['ЗАДОЛЖЕННОСТЬ']}₽</code></b>"
                    elif "ДОЛГ" in entry:
                        debt_line += f" - <b><code>{entry['ДОЛГ']}₽</code></b>"
                else:
                    debt_line = f"Задолженность {i}"
                    # Добавляем сумму задолженности, если она есть
                    if "ЗАДОЛЖЕННОСТЬ_ЧИСЛО" in entry:
                        debt_line += f" - {entry['ЗАДОЛЖЕННОСТЬ_ЧИСЛО']:.2f}₽"
                    elif "ЗАДОЛЖЕННОСТЬ" in entry:
                        debt_line += f" - {entry['ЗАДОЛЖЕННОСТЬ']}₽"
                    elif "ДОЛГ" in entry:
                        debt_line += f" - {entry['ДОЛГ']}₽"

                short_message += f"  • {debt_line}\n"

            if len(fssp_data) > 3:
                short_message += f"  • ... и еще {len(fssp_data) - 3} записей\n"

            short_message += "\n"

        # Добавляем копируемые данные в краткий вид (по наиболее важным категориям)
        for cat_name in ["ЗАПИСАН В БАЗАХ", "ТЕЛЕФОН", "ПОЧТА", "ИНН", "ПАСПОРТ"]:
            items = data_store[cat_name]
            if items:
                emoji = categories[cat_name]

                # Ограничение количества элементов для краткого вида
                max_short = 5 if cat_name == "ТЕЛЕФОН" else 3
                sorted_items = sorted_items_by_category(cat_name, items)
                items_to_show = sorted_items[:max_short]

                if use_html:
                    short_message += f"{emoji} <b>{cat_name}:</b>\n"
                    for val in items_to_show:
                        short_message += f"  • <code>{val}</code>\n"
                else:
                    short_message += f"{emoji} {cat_name}:\n"
                    for val in items_to_show:
                        short_message += f"  • {val}\n"

                if len(sorted_items) > max_short:
                    short_message += f"  • ... и еще {len(sorted_items) - max_short} записей\n"

                short_message += "\n"

        short_message += (
            f"Всего баз данных: {bases_count}\n"
            f"Всего записей: {total_records}\n\n"
            "⚠️ Результат слишком большой для отображения в Telegram.\n"
            "Полная информация доступна в файле ниже 📂"
        )
        return short_message

    return full_message


def format_phone_number(phone_str):
    """
    Форматирует телефонный номер к стандартному виду или возвращает None для невалидных номеров.
    Распознает международные номера, российские мобильные и стационарные номера.

    :param phone_str: Строка с телефонным номером
    :return: Отформатированный номер или None
    """
    if not phone_str:
        return None

    # Удаляем все нецифровые символы, кроме плюса в начале
    original = str(phone_str).strip()
    has_plus = original.startswith('+')
    digits_only = ''.join(c for c in original if c.isdigit())

    # Проверка на минимальную длину (отсеиваем явно невалидные)
    if len(digits_only) < 7:  # Минимум 7 цифр для городского номера
        return None

    # 1. Обработка российских мобильных номеров
    # Мобильные номера России: +7XXX-XXX-XX-XX (11 цифр с кодом страны)
    if len(digits_only) == 10 and digits_only[0] in ['9']:
        # Если номер начинается на 9 и длина 10 - это, скорее всего, российский мобильный без кода страны
        return f"+7{digits_only}"
    elif len(digits_only) == 11 and digits_only[0] in ['7', '8']:
        # Если начинается на 7 или 8 и длина 11 - это российский номер с кодом страны
        return f"+7{digits_only[1:]}"

    # 2. Обработка стационарных номеров России
    # Стационарные номера в России обычно имеют код города (3-5 цифр) + номер (5-7 цифр)
    if 7 <= len(digits_only) <= 10:
        # Это может быть стационарный номер без кода страны
        # Городские номера часто начинаются с 2, 3, 4, 5
        if digits_only[0] in ['2', '3', '4', '5']:
            # Добавляем код России
            return f"+7{digits_only}"

    # 3. Обработка международных номеров
    # Известные коды стран
    country_codes = {
        '1': 'США/Канада',  # +1: 10 цифр после кода
        '44': 'Великобритания',  # +44: 10 цифр после кода
        '49': 'Германия',  # +49: переменная длина
        '33': 'Франция',  # +33: 9 цифр после кода
        '86': 'Китай',  # +86: переменная длина
        '81': 'Япония',  # +81: переменная длина
        '39': 'Италия',  # +39: переменная длина
        '34': 'Испания',  # +34: 9 цифр после кода
        '380': 'Украина',  # +380: 9 цифр после кода
        '375': 'Беларусь',  # +375: 9 цифр после кода
        '998': 'Узбекистан',  # +998: 9 цифр после кода
        '996': 'Киргизия',  # +996: 9 цифр после кода
        '374': 'Армения',  # +374: 8 цифр после кода
        '995': 'Грузия',  # +995: 9 цифр после кода
        '994': 'Азербайджан',  # +994: 9 цифр после кода
        '992': 'Таджикистан',  # +992: 9 цифр после кода
        '7': 'Россия/Казахстан'  # +7: 10 цифр после кода
    }

    # Проверка известных международных форматов
    if has_plus and len(digits_only) >= 8:
        # Если номер начинается с + и достаточно длинный - вернуть как есть
        return f"+{digits_only}"

    # Для номеров без + в начале, но длиной 11-15 цифр,
    # скорее всего это международный номер с кодом страны
    if len(digits_only) >= 11 and len(digits_only) <= 15:
        # Пытаемся определить код страны
        for code in sorted(country_codes.keys(), key=len, reverse=True):
            if digits_only.startswith(code):
                return f"+{digits_only}"

        # Если код страны не определен, но длина подходящая - считаем международным
        return f"+{digits_only}"

    # 4. Если не распознали формат, но длина приемлемая для телефона (7-15 цифр)
    if 7 <= len(digits_only) <= 15:
        # Для номеров, которые начинаются с 8, заменяем на +7 (российский формат)
        if digits_only[0] == '8' and len(digits_only) == 11:
            return f"+7{digits_only[1:]}"
        # Иначе просто добавляем + в начало
        return f"+{digits_only}"

    # Если не подошло ни под один формат, возвращаем исходный номер как есть
    return original


def sorted_items_by_category(category, items):
    """
    Сортирует элементы в зависимости от категории для улучшения читаемости

    :param category: Название категории
    :param items: Список элементов для сортировки
    :return: Отсортированный список
    """
    if category == "ЗАПИСАН В БАЗАХ":
        # Сортировка ФИО по алфавиту
        return sorted(items)
    elif category == "ДАТА РОЖДЕНИЯ":
        # Попытка сортировки дат (если в формате ДД.ММ.ГГГГ)
        try:
            # Сначала попробуем отсортировать как даты
            from datetime import datetime

            def parse_date(date_str):
                try:
                    if isinstance(date_str, str) and '.' in date_str:
                        parts = date_str.split('.')
                        if len(parts) == 3:
                            return datetime(int(parts[2]), int(parts[1]), int(parts[0]))
                    return datetime(1900, 1, 1)  # Значение по умолчанию
                except:
                    return datetime(1900, 1, 1)  # Если ошибка

            return sorted(items, key=parse_date)
        except:
            # Если не получилось, просто алфавитная сортировка
            return sorted(items)
    elif category == "ТЕЛЕФОН":
        # Для телефонов - сначала мобильные (+7), потом остальные
        mobile = []
        other = []

        for phone in items:
            if phone and isinstance(phone, str) and phone.startswith('+7'):
                mobile.append(phone)
            else:
                other.append(phone)

        return sorted(mobile) + sorted(other)
    else:
        # Для остальных категорий - просто алфавитная сортировка
        return sorted(items)

def translate_database_entry(database_entry: str) -> str:
    """
    Переводит названия database в человекочитаемый формат, используя словарь DATABASE_TRANSLATIONS.
    """
    words = database_entry.split()
    translated_words = [DATABASE_TRANSLATIONS.get(word.lower(), word) for word in words]
    return " ".join(translated_words)


async def save_response_as_html(user_id: int, query: str, api_response) -> str:
    """
    Generates an enhanced HTML file with a professional dark interface,
    displaying all information from api_response with improved navigation.

    :param user_id: for saving in /static/responses/<user_id>
    :param query: search query text
    :param api_response: list of dictionaries with search results
    :return: Path to created HTML file or None on error
    """
    try:
        # Categories for summary information
        CATEGORIES = {
            "Личные данные": ["ФИО", "ДАТА РОЖДЕНИЯ", "РЕГИОН", "АДРЕС", "КОД ИЗБИРАТЕЛЯ", "КАТЕГОРИЯ ЛИЦА"],
            "Контакты": ["ТЕЛЕФОН", "ТЕЛЕФОН ДОМАШНИЙ", "СВЯЗЬ", "ПОЧТА"],
            "Документы": ["ПАСПОРТ", "ИНН", "СНИЛС", "ДОКУМЕНТ", "ПОЛИС", "ДАТА ВЫДАЧИ", "ДАТА ВЫДАЧИ ПОЛИС",
                          "КЕМ ВЫДАН", "ВОДИТЕЛЬСКОЕ УДОСТОВЕРЕНИЕ"],
            "Профессиональная информация": ["ДОЛЖНОСТЬ", "МЕСТО РАБОТЫ", "НАИМЕНОВАНИЕ ОРГАНИЗАЦИИ"],
            "Финансовая информация": ["ДОЛГ", "ДАТА ИП", "НОМЕР ИП", "ОСП"],
            "Информация об автомобиле": ["ГОС НОМЕР", "МОДЕЛЬ АВТО", "ГОД ВЫПУСКА", "VIN", "ЦВЕТ АВТО"]
        }

        # Standardized path for all HTML reports
        base_dir = "static"
        user_dir = os.path.join(base_dir, "responses", str(user_id))

        # Create directories with error handling
        try:
            os.makedirs(user_dir, exist_ok=True)
        except PermissionError:
            logging.error(f"❌ No permission to create directory: {user_dir}")
            return None
        except Exception as e:
            logging.error(f"❌ Error creating directory {user_dir}: {e}")
            return None

        # Verify write permissions
        if not os.access(os.path.dirname(user_dir), os.W_OK):
            logging.error(f"❌ No write permission for directory: {os.path.dirname(user_dir)}")
            return None

        # Create filename with timestamp
        safe_query = query.replace("/", "_").replace("\\", "_")
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        file_name = f"{safe_query}_{timestamp}.html"
        file_path = os.path.join(user_dir, file_name)

        # Collect all unique data for summary information
        summary_data = {}
        for category, fields in CATEGORIES.items():
            summary_data[category] = {}

        # Process all records to collect unique data
        for record in api_response:
            if not isinstance(record, dict):
                continue

            # Process fields provided as key-value pairs
            for key, value in record.items():
                if key == "database":
                    continue

                key = key.upper()  # Standardize keys to uppercase

                # Determine category for the field
                found_category = False
                for category, fields in CATEGORIES.items():
                    if key in fields:
                        if key not in summary_data[category]:
                            summary_data[category][key] = []

                        # Handle lists and scalar values
                        if isinstance(value, list):
                            for v in value:
                                if v not in summary_data[category][key]:
                                    summary_data[category][key].append(v)
                        else:
                            if value not in summary_data[category][key]:
                                summary_data[category][key].append(value)
                        found_category = True
                        break

                # If category not found, add to "Другие данные"
                if not found_category:
                    if "Другие данные" not in summary_data:
                        summary_data["Другие данные"] = {}
                    if key not in summary_data["Другие данные"]:
                        summary_data["Другие данные"][key] = []

                    if isinstance(value, list):
                        for v in value:
                            if v not in summary_data["Другие данные"][key]:
                                summary_data["Другие данные"][key].append(v)
                    else:
                        if value not in summary_data["Другие данные"][key]:
                            summary_data["Другие данные"][key].append(value)

        # Find first non-empty category for active tab
        active_category = None
        for category, data in summary_data.items():
            if data:
                active_category = category
                break

        if not active_category:
            active_category = list(CATEGORIES.keys())[0]

        # Generate HTML content
        with open(file_path, "w", encoding="utf-8") as f:
            f.write(f"""<!DOCTYPE html>
<html lang="ru">
<head>
    <meta charset="UTF-8">
    <title>{html_escape.escape(query)}</title>
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <meta name="theme-color" content="#1a1d24">
    <style>
        :root {{
            /* Color scheme (dark) */
            --color-primary: #3498db;
            --color-primary-light: #214559;
            --color-secondary: #6c7a89;
            --color-accent: #e74c3c;
            --color-success: #2ecc71;
            --color-warning: #f39c12;
            --color-danger: #e74c3c;
            --color-info: #3498db;

            /* Text */
            --color-text: #ecf0f1;
            --color-text-secondary: #bdc3c7;
            --color-text-light: #7f8c8d;

            /* Backgrounds */
            --color-bg: #101214;
            --color-bg-paper: #1a1d24;
            --color-bg-card: #23272e;
            --color-bg-section: #1e222a;
            --color-bg-hover: #2a2f3a;
            --color-bg-active: #2c3e50;

            /* Borders */
            --color-border: #2c3e50;
            --color-border-light: #34495e;

            /* Sizes */
            --spacing-unit: 8px;
            --border-radius: 4px;
            --tab-height: 40px;
            --header-height: 60px;
            --sidebar-width: 260px;

            /* Shadows */
            --shadow-sm: 0 1px 3px 0 rgba(0,0,0,0.2);
            --shadow-md: 0 4px 6px -1px rgba(0,0,0,0.3), 0 2px 4px -1px rgba(0,0,0,0.2);
            --shadow-lg: 0 10px 15px -3px rgba(0,0,0,0.4), 0 4px 6px -2px rgba(0,0,0,0.3);

            /* Animations */
            --transition-speed: 0.2s;
        }}

        /* Reset styles */
        *, *::before, *::after {{
            box-sizing: border-box;
            margin: 0;
            padding: 0;
        }}

        html {{
            font-size: 16px;
            scroll-behavior: smooth;
        }}

        body {{
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Oxygen, Ubuntu, Cantarell, 'Open Sans', 'Helvetica Neue', sans-serif;
            background-color: var(--color-bg);
            color: var(--color-text);
            line-height: 1.5;
            padding-bottom: 2rem;
            overflow-x: hidden;
        }}

        a {{
            color: var(--color-primary);
            text-decoration: none;
            transition: color var(--transition-speed) ease;
        }}

        a:hover {{
            text-decoration: none;
            color: #5dade2;
        }}

        /* Utilities */
        .container {{
            max-width: 1200px;
            margin: 0 auto;
            padding: 0 var(--spacing-unit);
        }}

        .flex {{
            display: flex;
        }}

        .flex-col {{
            flex-direction: column;
        }}

        .items-center {{
            align-items: center;
        }}

        .justify-between {{
            justify-content: space-between;
        }}

        .gap-2 {{
            gap: calc(var(--spacing-unit) * 2);
        }}

        .py-2 {{
            padding-top: calc(var(--spacing-unit) * 2);
            padding-bottom: calc(var(--spacing-unit) * 2);
        }}

        .mb-2 {{
            margin-bottom: calc(var(--spacing-unit) * 2);
        }}

        .mb-4 {{
            margin-bottom: calc(var(--spacing-unit) * 4);
        }}

        /* Page header */
        .page-header {{
            background-color: var(--color-bg-paper);
            border-bottom: 1px solid var(--color-border);
            position: sticky;
            top: 0;
            z-index: 10;
            box-shadow: var(--shadow-sm);
            height: var(--header-height);
        }}

        .header-content {{
            display: flex;
            justify-content: space-between;
            align-items: center;
            height: 100%;
            padding: calc(var(--spacing-unit) * 1.5) 0;
        }}

        .page-title {{
            font-size: 1.25rem;
            font-weight: 600;
            color: var(--color-text);
            overflow: hidden;
            text-overflow: ellipsis;
            white-space: nowrap;
            max-width: 70vw;
        }}

        .page-subtitle {{
            font-size: 0.875rem;
            color: var(--color-text-secondary);
            margin-top: 4px;
        }}

        .badge {{
            background-color: var(--color-primary-light);
            color: var(--color-primary);
            font-size: 0.75rem;
            font-weight: 600;
            padding: 0.25rem 0.5rem;
            border-radius: 1rem;
            margin-left: 0.5rem;
        }}

        .header-actions {{
            display: flex;
            gap: calc(var(--spacing-unit));
        }}

        .btn {{
            display: inline-flex;
            align-items: center;
            justify-content: center;
            padding: 0.5rem 1rem;
            font-size: 0.875rem;
            font-weight: 500;
            border-radius: var(--border-radius);
            cursor: pointer;
            transition: all 0.15s ease;
            border: 1px solid transparent;
            user-select: none;
        }}

        .btn-outline {{
            background-color: transparent;
            border-color: var(--color-border);
            color: var(--color-text-secondary);
        }}

        .btn-outline:hover {{
            background-color: var(--color-bg-section);
            border-color: var(--color-text-secondary);
        }}

        .btn-primary {{
            background-color: var(--color-primary);
            color: white;
            border-color: var(--color-primary);
        }}

        .btn-primary:hover {{
            background-color: #2980b9;
            border-color: #2980b9;
        }}

        /* Main layout */
        .main-layout {{
            display: flex;
            padding-top: calc(var(--spacing-unit) * 3);
        }}

        .main-content {{
            flex: 1;
            margin-right: 0;
            padding-left: calc(var(--sidebar-width) + var(--spacing-unit) * 3);
            transition: padding-left var(--transition-speed) ease;
            width: 100%;
        }}

        .sidebar {{
            width: var(--sidebar-width);
            position: fixed;
            top: var(--header-height);
            left: 0;
            bottom: 0;
            padding: calc(var(--spacing-unit) * 3) calc(var(--spacing-unit) * 2);
            overflow-y: auto;
            background-color: var(--color-bg-paper);
            border-right: 1px solid var(--color-border);
            z-index: 5;
            transition: transform var(--transition-speed) ease;
        }}

        /* Card */
        .card {{
            background-color: var(--color-bg-card);
            border-radius: var(--border-radius);
            box-shadow: var(--shadow-sm);
            border: 1px solid var(--color-border);
            margin-bottom: calc(var(--spacing-unit) * 3);
            overflow: hidden;
            transition: box-shadow var(--transition-speed) ease, transform var(--transition-speed) ease;
        }}

        .card:target {{
            box-shadow: var(--shadow-lg);
            border-color: var(--color-primary);
        }}

        .card-header {{
            padding: calc(var(--spacing-unit) * 2);
            background-color: var(--color-bg-section);
            border-bottom: 1px solid var(--color-border);
            display: flex;
            justify-content: space-between;
            align-items: center;
        }}

        .card-title {{
            font-size: 1rem;
            font-weight: 600;
            color: var(--color-text);
        }}

        .card-body {{
            padding: calc(var(--spacing-unit) * 2);
        }}

        /* Summary card */
        .summary-card .card-body {{
            padding: 0;
        }}

        .summary-tabs {{
            display: flex;
            border-bottom: 1px solid var(--color-border);
            overflow-x: auto;
            scrollbar-width: thin;
            scrollbar-color: var(--color-border) var(--color-bg-section);
        }}

        .summary-tabs::-webkit-scrollbar {{
            height: 6px;
        }}

        .summary-tabs::-webkit-scrollbar-thumb {{
            background-color: var(--color-border);
            border-radius: 3px;
        }}

        .summary-tabs::-webkit-scrollbar-track {{
            background-color: var(--color-bg-section);
        }}

        .tab {{
            padding: 0 calc(var(--spacing-unit) * 2);
            height: var(--tab-height);
            display: flex;
            align-items: center;
            font-size: 0.875rem;
            font-weight: 500;
            cursor: pointer;
            border-bottom: 2px solid transparent;
            transition: all var(--transition-speed);
            white-space: nowrap;
            color: var(--color-text-secondary);
        }}

        .tab.active {{
            color: var(--color-primary);
            border-bottom-color: var(--color-primary);
            background-color: var(--color-bg-active);
        }}

        .tab:hover:not(.active) {{
            color: var(--color-text);
            background-color: var(--color-bg-hover);
        }}

        .tab-content {{
            display: none;
            padding: calc(var(--spacing-unit) * 2);
        }}

        .tab-content.active {{
            display: block;
        }}

        .personal-info {{
            display: grid;
            grid-template-columns: repeat(auto-fill, minmax(240px, 1fr));
            gap: calc(var(--spacing-unit) * 2);
        }}

        .info-group {{
            margin-bottom: calc(var(--spacing-unit) * 2);
        }}

        .info-label {{
            font-size: 0.75rem;
            font-weight: 600;
            text-transform: uppercase;
            color: var(--color-text-light);
            margin-bottom: calc(var(--spacing-unit) / 2);
        }}

        .info-value {{
            font-size: 0.875rem;
            word-break: break-word;
        }}

        /* Source cards */
        .source-card {{
            transition: all var(--transition-speed);
            scroll-margin-top: calc(var(--header-height) + var(--spacing-unit) * 2);
        }}

        .source-card:hover {{
            box-shadow: var(--shadow-md);
        }}

        .source-card:target {{
            border-color: var(--color-primary);
            box-shadow: var(--shadow-lg);
            position: relative;
        }}

        .source-card:target::before {{
            content: '';
            position: absolute;
            left: 0;
            top: 0;
            height: 100%;
            width: 4px;
            background-color: var(--color-primary);
        }}

        .source-header {{
            cursor: pointer;
            user-select: none;
        }}

        .source-title {{
            display: flex;
            align-items: center;
            gap: calc(var(--spacing-unit));
        }}

        .source-icon {{
            font-size: 1.125rem;
            background-color: var(--color-primary-light);
            color: var(--color-primary);
            border-radius: 50%;
            width: 28px;
            height: 28px;
            display: flex;
            align-items: center;
            justify-content: center;
            transition: transform var(--transition-speed) ease;
        }}

        .source-card:target .source-icon {{
            transform: scale(1.1);
        }}

        .source-name {{
            font-size: 0.9rem;
            font-weight: 600;
        }}

        .source-date {{
            font-size: 0.75rem;
            color: var(--color-text-light);
        }}

        .source-body {{
            max-height: none;
            overflow: visible;
        }}

        .data-table {{
            width: 100%;
            border-collapse: collapse;
        }}

        .data-table tr {{
            border-bottom: 1px solid var(--color-border-light);
        }}

        .data-table tr:last-child {{
            border-bottom: none;
        }}

        .data-table td {{
            padding: calc(var(--spacing-unit)) 0;
            font-size: 0.875rem;
            vertical-align: top;
        }}

        .data-table td:first-child {{
            width: 30%;
            color: var(--color-text-secondary);
            padding-right: calc(var(--spacing-unit) * 2);
            font-weight: 500;
        }}

        .highlight {{
            color: var(--color-primary);
            font-weight: 600;
        }}

        /* Sidebar */
        .sidebar-title {{
            padding: calc(var(--spacing-unit));
            font-size: 0.9rem;
            font-weight: 600;
            border-bottom: 1px solid var(--color-border);
            margin-bottom: calc(var(--spacing-unit));
            color: var(--color-text);
        }}

        .nav-list {{
            list-style: none;
            margin-bottom: calc(var(--spacing-unit) * 3);
        }}

        .nav-item {{
            margin-bottom: 1px;
        }}

        .nav-link {{
            display: flex;
            align-items: center;
            padding: calc(var(--spacing-unit) * 1.2);
            font-size: 0.85rem;
            color: var(--color-text-secondary);
            border-radius: var(--border-radius);
            transition: all var(--transition-speed);
            border-left: 3px solid transparent;
        }}

        .nav-link:hover {{
            background-color: var(--color-bg-hover);
            color: var(--color-text);
            text-decoration: none;
        }}

        .nav-link.active {{
            background-color: var(--color-bg-active);
            color: var(--color-primary);
            font-weight: 500;
            border-left-color: var(--color-primary);
        }}

        .nav-icon {{
            margin-right: 8px;
            opacity: 0.7;
        }}

        .nav-link.active .nav-icon {{
            opacity: 1;
        }}

        .source-tag {{
            display: inline-block;
            font-size: 0.75rem;
            padding: 0.125rem 0.5rem;
            border-radius: 1rem;
            background-color: var(--color-primary-light);
            color: var(--color-primary);
            margin-left: 0.25rem;
        }}

        .sidebar-toggle {{
            display: none;
            position: fixed;
            bottom: 20px;
            right: 20px;
            width: 50px;
            height: 50px;
            border-radius: 50%;
            background-color: var(--color-primary);
            color: white;
            align-items: center;
            justify-content: center;
            font-size: 1.5rem;
            box-shadow: var(--shadow-md);
            z-index: 100;
            cursor: pointer;
        }}

        @keyframes pulse {{
            0% {{ transform: scale(1); }}
            50% {{ transform: scale(1.05); }}
            100% {{ transform: scale(1); }}
        }}

        .source-card:target .card-header {{
            animation: pulse 1s ease-in-out;
        }}

        /* Responsive */
        @media (max-width: 992px) {{
            .main-content {{
                padding-left: 0;
            }}

            .sidebar {{
                transform: translateX(-100%);
            }}

            .sidebar.active {{
                transform: translateX(0);
            }}

            .sidebar-toggle {{
                display: flex;
            }}

            body.sidebar-open .main-content {{
                opacity: 0.5;
                pointer-events: none;
            }}
        }}

        @media (max-width: 768px) {{
            .header-content {{
                flex-direction: column;
                align-items: flex-start;
                padding: calc(var(--spacing-unit));
            }}

            .header-actions {{
                margin-top: calc(var(--spacing-unit));
                width: 100%;
                justify-content: flex-end;
            }}

            .sidebar {{
                width: 85%;
            }}

            .data-table td {{
                display: block;
                width: 100% !important;
            }}

            .data-table td:first-child {{
                padding-bottom: 0;
                border-bottom: none;
            }}

            .personal-info {{
                grid-template-columns: 1fr;
            }}

            .page-header {{
                height: auto;
            }}
        }}

        /* Print */
        @media print {{
            body {{
                background: white;
                color: black;
            }}

            .page-header {{
                position: static;
                box-shadow: none;
                background-color: white;
                color: black;
                border-bottom: 1px solid #ddd;
            }}

            .header-actions, .sidebar, .sidebar-toggle {{
                display: none !important;
            }}

            .main-content {{
                padding-left: 0;
            }}

            .card, .source-card {{
                break-inside: avoid;
                box-shadow: none;
                background-color: white;
                border: 1px solid #ddd;
                page-break-inside: avoid;
            }}

            .card-header {{
                background-color: #f5f5f5;
                color: black;
            }}

            .data-table td {{
                color: black !important;
            }}

            .data-table td:first-child {{
                color: #555 !important;
            }}

            .highlight {{
                color: #2980b9 !important;
            }}

            /* Show all tabs when printing */
            .tab-content {{
                display: block !important;
                page-break-inside: avoid;
                border-top: 1px dashed #ddd;
                padding-top: 1rem;
                margin-top: 0.5rem;
            }}

            .tab {{
                display: none;
            }}

            .tab.active {{
                display: block;
                border: none;
                padding: 0;
                margin: 1rem 0 0.5rem;
                font-weight: bold;
                font-size: 1.1rem;
            }}
        }}
    </style>
</head>
<body>
    <!-- Page header -->
    <header class="page-header">
        <div class="container">
            <div class="header-content">
                <div>
                    <h1 class="page-title">{html_escape.escape(query)}</h1>
                    <p class="page-subtitle">Результаты поиска <span class="badge">{len(api_response)} источников</span></p>
                </div>
                <div class="header-actions">
                    <button class="btn btn-outline" onclick="window.print()">
                        <span>Печать</span>
                    </button>
                    <button class="btn btn-primary" onclick="savePDF()">
                        <span>Сохранить PDF</span>
                    </button>
                </div>
            </div>
        </div>
    </header>

    <!-- Main layout with sidebar -->
    <div class="sidebar" id="sidebar">
        <div class="sidebar-title">
            <span>Навигация по источникам</span>
        </div>
        <ul class="nav-list">
""")

            # Generate navigation links for each source
            for i, record in enumerate(api_response):
                if not isinstance(record, dict):
                    continue

                db_name = record.get("database", "Неизвестная база")
                source_id = f"source{i}"
                active_class = "active" if i == 0 else ""

                f.write(f"""            <li class="nav-item">
                <a href="#{source_id}" class="nav-link {active_class}" onclick="scrollToSource('{source_id}')">
                    <span class="nav-icon">📊</span>
                    <span>{html_escape.escape(db_name)}</span>
                </a>
            </li>
""")

            f.write("""        </ul>
    </div>

    <!-- Toggle sidebar button (mobile) -->
    <div class="sidebar-toggle" id="sidebar-toggle">
        <span>≡</span>
    </div>

    <!-- Main content -->
    <div class="main-content">
        <div class="container">
            <!-- Summary information -->
            <div class="card summary-card mb-4">
                <div class="card-header">
                    <h2 class="card-title">Сводная информация</h2>
                </div>

                <div class="summary-tabs">
""")

            # Generate tabs for summary information
            for i, category in enumerate(summary_data.keys()):
                if summary_data[category]:  # Only show tabs with data
                    is_active = category == active_category
                    active_class = "active" if is_active else ""
                    category_id = category.lower().replace(' ', '_')

                    f.write(f"""                    <div class="tab {active_class}" data-tab="{category_id}">{category}</div>
""")

            f.write("""                </div>

""")

            # Generate tab content for each category
            for category, data in summary_data.items():
                if not data:  # Skip empty categories
                    continue

                is_active = category == active_category
                active_class = "active" if is_active else ""
                category_id = category.lower().replace(' ', '_')

                f.write(f"""                <div id="{category_id}" class="tab-content {active_class}">
                    <div class="personal-info">
""")

                # Show fields in this category
                for field, values in data.items():
                    if not values:
                        continue

                    f.write(f"""                        <div class="info-group">
                            <div class="info-label">{html_escape.escape(field)}</div>
                            <div class="info-value">
""")

                    # Show values for this field
                    for value in values:
                        if value:
                            safe_value = html_escape.escape(str(value))
                            f.write(f"""                                {safe_value}<br>
""")

                    f.write("""                            </div>
                        </div>
""")

                f.write("""                    </div>
                </div>
""")

            f.write("""            </div>

            <!-- Sources heading -->
            <h2 class="card-title mb-2">Источники данных</h2>
""")

            # Generate cards for each data source
            for i, record in enumerate(api_response):
                if not isinstance(record, dict):
                    continue

                db_name = record.get("database", "Неизвестная база")
                source_id = f"source{i}"

                f.write(f"""            <!-- Source {i + 1} -->
            <div class="card source-card" id="{source_id}">
                <div class="card-header source-header">
                    <div class="source-title">
                        <div class="source-icon">📊</div>
                        <div>
                            <div class="source-name">{html_escape.escape(db_name)}</div>
                        </div>
                    </div>
                </div>
                <div class="source-body">
                    <div class="card-body">
                        <table class="data-table">
                            <tbody>
""")

                # Show fields for this source
                for key, value in record.items():
                    if key == "database":
                        continue

                    if key and value:
                        key_safe = html_escape.escape(str(key).upper())

                        # Handle lists as values
                        if isinstance(value, list):
                            val_safe = "<br>".join([html_escape.escape(str(v)) for v in value if v])
                        else:
                            val_safe = html_escape.escape(str(value))

                        # Apply highlight for important fields
                        highlight_class = "highlight" if key.upper() in ["ФИО", "ТЕЛЕФОН", "ПОЧТА"] else ""

                        f.write(f"""                                <tr>
                                    <td>{key_safe}</td>
                                    <td class="{highlight_class}">{val_safe}</td>
                                </tr>
""")

                f.write("""                            </tbody>
                        </table>
                    </div>
                </div>
            </div>
""")

            # Close container and main-content divs
            f.write("""        </div>
    </div>

    <script>
        // Tab switching
        document.querySelectorAll('.tab').forEach(tab => {
            tab.addEventListener('click', function() {
                // Deactivate all tabs and contents
                document.querySelectorAll('.tab').forEach(t => t.classList.remove('active'));
                document.querySelectorAll('.tab-content').forEach(c => c.classList.remove('active'));

                // Activate selected tab and content
                this.classList.add('active');
                document.getElementById(this.dataset.tab).classList.add('active');
            });
        });

        // Smooth scrolling to source
        function scrollToSource(id) {
            const sourceCard = document.getElementById(id);
            if (!sourceCard) return;

            // Set all nav links inactive
            document.querySelectorAll('.nav-link').forEach(link => {
                link.classList.remove('active');
            });

            // Activate the clicked link
            document.querySelector(`a[href="#${id}"]`).classList.add('active');

            // Scroll to the source with smooth animation
            sourceCard.scrollIntoView({ behavior: 'smooth', block: 'start' });

            // Add highlight effect
            sourceCard.classList.add('highlight-effect');
            setTimeout(() => {
                sourceCard.classList.remove('highlight-effect');
            }, 2000);

            // Close sidebar on mobile after navigation
            if (window.innerWidth < 992) {
                toggleSidebar(false);
            }

            return false;
        }

        // Mobile sidebar toggle
        const sidebarToggle = document.getElementById('sidebar-toggle');
        const sidebar = document.getElementById('sidebar');
        const body = document.body;

        function toggleSidebar(show) {
            if (show === undefined) {
                sidebar.classList.toggle('active');
                body.classList.toggle('sidebar-open');
            } else if (show) {
                sidebar.classList.add('active');
                body.classList.add('sidebar-open');
            } else {
                sidebar.classList.remove('active');
                body.classList.remove('sidebar-open');
            }
        }

        sidebarToggle.addEventListener('click', () => toggleSidebar());

        // Close sidebar when clicking outside
        document.addEventListener('click', (e) => {
            if (window.innerWidth < 992 && 
                !sidebar.contains(e.target) && 
                !sidebarToggle.contains(e.target) && 
                sidebar.classList.contains('active')) {
                toggleSidebar(false);
            }
        });

        // PDF saving function
        function savePDF() {
            // In the basic version, just trigger print
            window.print();
        }

        // Handle active source navigation
        window.addEventListener('load', function() {
            // If URL has hash, scroll to it
            if (window.location.hash) {
                const id = window.location.hash.substring(1);
                if (document.getElementById(id)) {
                    setTimeout(() => {
                        scrollToSource(id);
                    }, 100);
                }
            }
        });

        // Process anchor links
        document.querySelectorAll('a[href^="#"]').forEach(anchor => {
            anchor.addEventListener('click', function(e) {
                const id = this.getAttribute('href').substring(1);
                if (document.getElementById(id)) {
                    e.preventDefault();
                    scrollToSource(id);
                }
            });
        });

        // Auto-resize sidebar based on screen size
        function handleResize() {
            if (window.innerWidth >= 992) {
                sidebar.classList.remove('active');
                body.classList.remove('sidebar-open');
            }
        }

        window.addEventListener('resize', handleResize);

        // Initialize
        handleResize();
    </script>
</body>
</html>
""")

        logging.info(f"✅ Enhanced HTML report saved: {file_path}")
        return file_path

    except Exception as e:
        logging.error(f"❌ Error saving HTML file: {e}")
        logging.error(traceback.format_exc())
        return None


def get_api_balance():
    """
    Возвращает (success: bool, response: str).
    Пример: (True, "Баланс API: $100.25")
    Или (False, "Ошибка 401: Недействительный токен")
    """
    if not API_TOKEN:
        return False, "Ошибка: отсутствует API_TOKEN в .env."

    url = f"{API_BASE_URL}/api/{API_TOKEN}/profile"
    try:
        resp = requests.get(url, timeout=10)
        if resp.status_code == 401:
            return False, "Ошибка 401: Недействительный API-токен."
        if resp.status_code == 500:
            return False, "Ошибка 500: Проблема на стороне API."

        data = resp.json()
        if "profile" in data:
            bal = data["profile"].get("balance", 0.0)
            return True, f"Баланс API: ${bal:.2f}"
        elif "error" in data:
            return False, f"Ошибка API: {data['error']}"
        else:
            return False, "Невозможно распознать ответ от API."
    except requests.exceptions.RequestException as e:
        return False, f"Ошибка запроса к API: {e}"


def filter_unique_data(api_response):
    """
    Убирает дубликаты для ключевых полей: ФИО, ДАТА РОЖДЕНИЯ, ТЕЛЕФОН, ПАСПОРТ, ИНН.
    Остальные поля + database остаются как есть.
    Возвращает "очищенный" список словарей.
    """
    unique_values = {
        "ФИО": set(),
        "ДАТА РОЖДЕНИЯ": set(),
        "ТЕЛЕФОН": set(),
        "ПАСПОРТ": set(),
        "ИНН": set()
    }

    filtered_data = []

    for record in api_response:
        if not isinstance(record, dict):
            continue

        filtered_record = {}

        # Проверяем дубликаты для ключевых полей
        for key in unique_values:
            value = record.get(key)
            if value and value not in unique_values[key]:
                unique_values[key].add(value)
                filtered_record[key] = value

        # Всегда добавляем database, если он есть
        if "database" in record:
            filtered_record["database"] = record["database"]

        # Если filtered_record не пуст, добавляем
        if filtered_record:
            filtered_data.append(filtered_record)

    return filtered_data


async def get_or_translate_database_name(original_name):
    """
    Возвращает оригинальное название базы данных без перевода.
    Для совместимости с существующим кодом.
    """
    return original_name

def setup_translation_db():
    """
    Функция-заглушка для совместимости.
    Больше не создается таблица для переводов.
    """
    pass


def format_fio_and_date(query: str) -> str:
    """
    Улучшенная функция форматирования ФИО и даты рождения:
    - Распознает дату в любой позиции строки
    - Правильно форматирует компоненты даты
    - Корректно обрабатывает двузначные годы
    """
    parts = query.strip().split()
    if len(parts) < 2:
        return query  # слишком короткая строка

    # Ищем часть, которая похожа на дату
    date_index = None
    date_part = None
    # Поддержка разных разделителей в дате (., -, /)
    date_pattern = r"^(\d{1,2})[.\-/](\d{1,2})[.\-/](\d{2,4})$"

    for i, part in enumerate(parts):
        match = re.match(date_pattern, part)
        if match:
            date_index = i
            day, month, year = match.groups()

            # Добавляем ведущие нули для дня и месяца
            day = day.zfill(2)
            month = month.zfill(2)

            # Приводим год к 4-значному формату, если он 2-значный
            if len(year) == 2:
                # Предполагаем, что год 20XX если < 30, иначе 19XX
                year_prefix = "20" if int(year) < 30 else "19"
                year = year_prefix + year

            date_part = f"{day}.{month}.{year}"
            break

    # Если дата найдена, форматируем строку
    if date_index is not None:
        # Форматируем части ФИО (все с заглавной буквы)
        fio_parts = [p.capitalize() for i, p in enumerate(parts) if i != date_index]

        # Собираем строку с форматированной датой
        result = " ".join(fio_parts) + " " + date_part
        return result

    # Если дата не найдена, просто форматируем как ФИО
    return " ".join(p.capitalize() for p in parts)


def normalize_query(query: str) -> str:
    """
    Приводит строку к формату:
    - ФИО (каждое слово с заглавной буквы)
    - Дата рождения (DD.MM.YYYY), если есть дата в любой позиции
    """
    # Используем улучшенную функцию форматирования
    formatted_query = format_fio_and_date(query)

    # Дополнительная проверка для специальных случаев
    date_pattern = r'(\d{1,2})[.\-/](\d{1,2})[.\-/](\d{2,4})'
    if re.search(date_pattern, formatted_query):
        # Если есть дата в строке, применяем дополнительное форматирование
        date_match = re.search(date_pattern, formatted_query)
        if date_match:
            day, month, year = date_match.groups()

            # Форматируем дату
            day = day.zfill(2)
            month = month.zfill(2)
            if len(year) == 2:
                year_prefix = "20" if int(year) < 30 else "19"
                year = year_prefix + year

            formatted_date = f"{day}.{month}.{year}"

            # Заменяем найденную дату на форматированную
            formatted_query = formatted_query.replace(date_match.group(0), formatted_date)

    return formatted_query


async def test_message_sending(bot, user_id):
    """
    Проверяет возможность отправки сообщения конкретному пользователю

    :param bot: Экземпляр бота
    :param user_id: ID пользователя
    :return: (success, error_message)
    """
    try:
        # Сначала проверяем существование чата
        try:
            chat = await bot.get_chat(user_id)
        except Exception as e:
            return False, f"Чат не найден: {str(e)}"

        # Проверяем права бота в чате
        if chat.type != 'private':
            try:
                bot_member = await bot.get_chat_member(chat.id, (await bot.get_me()).id)
                if not bot_member.can_send_messages:
                    return False, "Бот не имеет прав на отправку сообщений в чате"
            except Exception as e:
                return False, f"Ошибка при проверке прав бота: {str(e)}"

        return True, None
    except Exception as e:
        return False, str(e)


def format_phone_number(phone_str):
    """
    Форматирует телефонный номер к стандартному виду или возвращает None для невалидных номеров

    :param phone_str: Строка с телефонным номером
    :return: Отформатированный номер или None
    """
    if not phone_str:
        return None

    # Только цифры
    digits_only = ''.join(c for c in str(phone_str) if c.isdigit())

    # Проверка на минимальную длину (отсеиваем явно невалидные)
    if len(digits_only) < 10:
        return None

    # Проверка на российские мобильные форматы
    if len(digits_only) == 10 and digits_only[0] in ['9', '8', '7', '6', '5', '4', '3']:
        return f"+7{digits_only}"
    elif len(digits_only) == 11 and digits_only[0] in ['7', '8']:
        return f"+7{digits_only[1:]}"
    elif len(digits_only) > 11:
        # Проверяем международные форматы (просто добавляем +)
        return f"+{digits_only}"
    else:
        # Если не распознали формат, но длина от 10 до 11 цифр, предполагаем что это телефон
        if 10 <= len(digits_only) <= 11:
            if digits_only[0] == '8':
                return f"+7{digits_only[1:]}"
            elif digits_only[0] == '7':
                return f"+7{digits_only[1:]}"
            else:
                return f"+{digits_only}"

    # Если не подошло ни под один формат, возвращаем None
    return None


async def test_mass_search_process(bot, admin_id, test_file_path):
    """
    Тестовая функция для проверки работоспособности массового пробива

    :param bot: Экземпляр бота
    :param admin_id: ID администратора для отправки отчета
    :param test_file_path: Путь к тестовому файлу
    :return: Результат теста (успех/неудача)
    """
    try:
        from bot.mass_search import MassSearchProcessor

        # Создаем процессор и запускаем обработку тестового файла
        processor = MassSearchProcessor()
        result_file_path, stats, results_dict = await processor.process_file(test_file_path, admin_id)

        # Формируем отчет о результатах теста
        report = f"Тест массового пробива завершен:\n"
        report += f"- Найдено телефонов: {stats.get('phones_found', 0)}\n"
        report += f"- Обработано запросов: {stats.get('valid_lines', 0)}\n"
        report += f"- Время обработки: {stats.get('processing_time', 0)} сек\n"

        # Отправляем отчет администратору
        await bot.send_message(admin_id, report)

        # Если есть файл результатов, отправляем его тоже
        if result_file_path and os.path.exists(result_file_path):
            await bot.send_document(admin_id, FSInputFile(result_file_path))

        return True
    except Exception as e:
        logging.error(f"Ошибка при тестировании массового пробива: {e}", exc_info=True)
        await bot.send_message(admin_id, f"Ошибка при тестировании массового пробива: {e}")
        return False


def sorted_items_by_category(category, items):
    """
    Сортирует элементы в зависимости от категории для улучшения читаемости

    :param category: Название категории
    :param items: Список элементов для сортировки
    :return: Отсортированный список
    """
    if category == "ЗАПИСАН В БАЗАХ":
        # Сортировка ФИО по алфавиту
        return sorted(items)
    elif category == "ДАТА РОЖДЕНИЯ":
        # Попытка сортировки дат (если в формате ДД.ММ.ГГГГ)
        try:
            # Сначала попробуем отсортировать как даты
            from datetime import datetime

            def parse_date(date_str):
                try:
                    if isinstance(date_str, str) and '.' in date_str:
                        parts = date_str.split('.')
                        if len(parts) == 3:
                            return datetime(int(parts[2]), int(parts[1]), int(parts[0]))
                    return datetime(1900, 1, 1)  # Значение по умолчанию
                except:
                    return datetime(1900, 1, 1)  # Если ошибка

            return sorted(items, key=parse_date)
        except:
            # Если не получилось, просто алфавитная сортировка
            return sorted(items)
    elif category == "ТЕЛЕФОН":
        # Для телефонов - сначала мобильные (+7), потом остальные
        mobile = []
        other = []

        for phone in items:
            if phone and isinstance(phone, str) and phone.startswith('+7'):
                mobile.append(phone)
            else:
                other.append(phone)

        return sorted(mobile) + sorted(other)
    else:
        # Для остальных категорий - просто алфавитная сортировка
        return sorted(items)


async def check_web_service_available():
    """
    Check if the web service is available by trying to authenticate a session.
    Updates the global WEB_SERVICE_AVAILABLE flag.
    """
    global WEB_SERVICE_AVAILABLE, LAST_SERVICE_CHECK

    # Don't check too frequently
    current_time = time.time()
    if current_time - LAST_SERVICE_CHECK < SERVICE_CHECK_INTERVAL:
        return WEB_SERVICE_AVAILABLE

    LAST_SERVICE_CHECK = current_time

    # Try to get the session pool
    try:
        from bot.session_manager import session_pool
        if not session_pool:
            logging.warning("Session pool is not initialized, web service considered unavailable")
            WEB_SERVICE_AVAILABLE = False
            return False

        # Try to get an authenticated session
        stats = None
        if hasattr(session_pool, 'get_stats'):
            stats = session_pool.get_stats()

        if stats and isinstance(stats, dict) and stats.get('active_sessions', 0) > 0:
            logging.info("Web service available: found active sessions")
            WEB_SERVICE_AVAILABLE = True
            return True

        # Try to authenticate a session
        if hasattr(session_pool, 'initialize_sessions'):
            try:
                success_count, _ = await asyncio.wait_for(
                    session_pool.initialize_sessions(min_sessions=1),
                    timeout=30
                )

                if success_count > 0:
                    logging.info("Web service available: successfully authenticated a session")
                    WEB_SERVICE_AVAILABLE = True
                    return True
                else:
                    logging.warning("Web service unavailable: failed to authenticate any sessions")
                    WEB_SERVICE_AVAILABLE = False
                    return False
            except (asyncio.TimeoutError, Exception) as e:
                logging.error(f"Web service check failed: {e}")
                WEB_SERVICE_AVAILABLE = False
                return False
        else:
            logging.warning("Cannot check web service: initialize_sessions method not found")
            WEB_SERVICE_AVAILABLE = False
            return False
    except Exception as e:
        logging.error(f"Error checking web service availability: {e}")
        WEB_SERVICE_AVAILABLE = False
        return False


async def handle_search_request(message, query_text, state):
    """
    Unified handler for search requests with fallback handling.
    To be used in universal_message_handler.
    """
    user_id = message.from_user.id

    # Check if web service is available
    web_available = await check_web_service_available()

    if not web_available:
        # FALLBACK MODE: Inform the user about service unavailability
        await message.answer(
            "⚠️ <b>Сервис временно недоступен</b>\n\n"
            "Извините за неудобства, но в настоящее время наш поисковый сервис недоступен. "
            "Наши технические специалисты уже работают над решением проблемы.\n\n"
            "Пожалуйста, попробуйте снова через некоторое время.",
            parse_mode="HTML"
        )
        return

    # Normal flow - check cache
    cached_found, cached_response, cache_source = get_best_cached_response(user_id, query_text)
    if cached_found:
        # Format and send cached response
        formatted_text = format_api_response(cached_response, use_html=False)
        await message.answer(
            f"💾 Результат из кэша ({cache_source}):\n\n{formatted_text}"
        )

        # Get HTML-file from cache or generate it
        html_path = await save_response_as_html(user_id, query_text, cached_response)
        if html_path and os.path.exists(html_path):
            await message.answer_document(FSInputFile(html_path))
        return

    # Deduct balance
    logging.info(f"🚀 Попытка списания баланса для user_id={user_id}")
    success, balance_message = deduct_balance(user_id)
    logging.info(f"🎯 Результат списания: {success}, {balance_message}")
    if not success:
        await message.answer(balance_message)
        return

    # Show loading message
    status_message = await message.answer("🔍 Выполняю поиск, пожалуйста, подождите...")

    # Send request with improved error handling
    from bot.session_manager import session_pool
    if session_pool is None:
        logging.error("Session pool is not initialized during search request")
        await status_message.edit_text("Ошибка: система поиска не инициализирована")
        refund_success, refund_message = refund_balance(user_id)
        return

    # Perform search with retry
    start_time = time.time()
    success, api_response = await send_web_request(query_text, session_pool)
    execution_time = time.time() - start_time

    # Log request
    log_request(
        user_id=user_id,
        query=query_text,
        request_type='web',
        source='web',
        success=success and api_response is not None,
        execution_time=execution_time,
        response_size=len(json.dumps(api_response).encode('utf-8')) if api_response else 0
    )

    # Remove loading message
    await status_message.delete()

    # Handle response
    if not success or not api_response or (isinstance(api_response, list) and len(api_response) == 0):
        # Refund and inform user
        refund_success, refund_message = refund_balance(user_id)
        await message.answer(
            "ℹ <b>Информация в базах не найдена.</b>\n\n"
            "📌 <i>Обратите внимание:</i> Введенные данные могут отличаться от записей в базе. "
            "Рекомендуем проверить корректность запроса и попробовать снова.\n\n"
            f"💰 {refund_message}",
            parse_mode="HTML"
        )
        return

    # Format and send results
    try:
        filtered_response = filter_unique_data(api_response)
        formatted_text = format_api_response(filtered_response, use_html=False)
        await message.answer(formatted_text)
    except Exception as e:
        logging.error(f"❌ Ошибка при форматировании ответа: {str(e)}")
        await message.answer("⚠ Ошибка при обработке данных.")
        refund_success, refund_message = refund_balance(user_id)
        await message.answer(f"💰 {refund_message}")
        return

    # Save to cache
    try:
        save_response_to_cache(user_id, query_text, api_response)
    except Exception as e:
        logging.error(f"❌ Ошибка при сохранении в кэш: {str(e)}")

    # Generate and send HTML report
    html_path = await save_response_as_html(user_id, query_text, api_response)
    if html_path and os.path.exists(html_path) and os.path.getsize(html_path) > 0:
        await message.answer_document(document=FSInputFile(html_path))
    else:
        logging.error(f"❌ Ошибка: HTML-файл {html_path} не создан или пуст.")
        await message.answer("⚠ Ошибка при создании HTML-файла.")

    # Check balance warning
    low_balance, warning_message = check_low_balance(user_id)
    if low_balance:
        await message.answer(warning_message)