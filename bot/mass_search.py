import asyncio
import json
import logging
import os
import random
import re
import time
from collections import defaultdict
from datetime import datetime
from typing import Dict


from aiogram import F, Router
from aiogram.filters import StateFilter
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.types import Message, CallbackQuery, InlineKeyboardMarkup, InlineKeyboardButton, FSInputFile

# Правильные импорты с полным набором необходимых функций
from bot.common import mass_search_semaphore, active_user_searches, mass_search_queue
from bot.database.db import (
    check_active_session, get_user_balance, check_balance_for_mass_search, batch_deduct_balance, log_mass_search_start,
    update_mass_search_status
)
from bot.utils import normalize_query, validate_query, send_api_request, filter_unique_data, format_api_response


# Определяем состояния FSM для массового пробива
class MassSearchStates(StatesGroup):
    waiting_for_file = State()  # Ожидание загрузки файла
    confirming_search = State()  # Подтверждение пробива (после расчета стоимости)
    processing = State()  # Обработка запросов


# Создаем роутер для обработчиков массового пробива
mass_search_router = Router()


# Отладочная функция для сравнения результатов
async def debug_compare_results(user_id, query):
    """
    Отладочная функция для сравнения результатов между одиночным и массовым пробивом
    """
    # Получаем данные одиночным запросом
    success, single_response = send_api_request(query)

    if not success:
        logging.error(f"Ошибка при получении данных одиночным запросом: {single_response}")
        return

    # Логируем результаты одиночного запроса
    logging.info(f"Результаты одиночного запроса для '{query}':")
    logging.info(json.dumps(single_response, ensure_ascii=False, indent=2)[:1000] + "...")

    # Используем функции из одиночного пробива
    filtered_data = filter_unique_data(single_response)
    formatted_text = format_api_response(filtered_data, limit_length=False)

    # Логируем форматированный текст
    logging.info(f"Форматированный текст одиночного запроса:")
    logging.info(formatted_text[:1000] + "...")

    # Создаем процессор для массового пробива
    processor = MassSearchProcessor()

    # Пробуем извлечь телефоны разными методами
    phones1 = processor.extract_phones(single_response)
    phones2 = processor.extract_phones_from_text(formatted_text)

    logging.info(f"Телефоны, найденные методом extract_phones: {phones1}")
    logging.info(f"Телефоны, найденные методом extract_phones_from_text: {phones2}")


# Класс для обработки результатов
class MassSearchProcessor:
    """
    Улучшенный класс для обработки массового пробива с балансировкой нагрузки между сессиями
    и отображением прогресса в реальном времени.
    """

    def __init__(self, max_concurrent: int = 5, min_request_interval: float = 1.0, max_request_interval: float = 4.0,
                 batch_size: int = 20):
        """
        Инициализация обработчика массовых запросов.

        :param max_concurrent: Максимальное количество одновременных запросов
        :param min_request_interval: Минимальный интервал между запросами в секундах
        :param max_request_interval: Максимальный интервал между запросами в секундах
        :param batch_size: Размер пакета для групповой обработки
        """
        self.max_concurrent = max_concurrent
        self.min_request_interval = min_request_interval
        self.max_request_interval = max_request_interval
        self.batch_size = batch_size
        self.semaphore = asyncio.Semaphore(max_concurrent)
        self.results_lock = asyncio.Lock()
        self.progress_counter = 0
        self.progress_lock = asyncio.Lock()
        self.session_usage = defaultdict(int)  # Отслеживание использования сессий
        self.total_queries = 0
        self.processed_queries = 0
        self.session_rotation_counter = 0  # Счетчик для ротации сессий
        self.mass_search_id = f"mass_{int(time.time())}_{random.randint(1000, 9999)}"  # Уникальный ID этого массового пробива

        # Для отслеживания обновлений статуса
        self.bot = None  # Будет установлено при вызове process_file
        self.user_id = None
        self.status_message_id = None

        # Время последнего обновления статуса для управления частотой обновлений
        self.last_status_update = time.time()
        self.status_update_interval = 2.0  # Секунды между обновлениями статуса

        # Словарь для отслеживания статуса запросов
        self.query_status = {}

        # Статистика производительности
        self.stats = {
            "cache_hits": 0,
            "cache_misses": 0,
            "successful_requests": 0,
            "failed_requests": 0,
            "start_time": time.time(),
            "end_time": None,
            "avg_request_time": 0,
            "total_request_time": 0,
            "request_times": []
        }

    async def update_progress_message(self):
        """
        Обновляет сообщение с прогрессом выполнения.
        Использует сохраненные в объекте параметры bot, user_id и status_message_id.
        """
        # Проверяем наличие необходимых параметров
        if not hasattr(self, 'bot') or not self.bot or not hasattr(self, 'user_id') or not self.user_id or not hasattr(
                self, 'status_message_id') or not self.status_message_id:
            logging.warning("Не хватает параметров для обновления прогресса")
            return

        # Проверяем, нужно ли обновлять статус (не чаще чем раз в N секунд)
        current_time = time.time()
        if hasattr(self, 'last_status_update') and current_time - self.last_status_update < self.status_update_interval:
            return

        self.last_status_update = current_time

        # Рассчитываем процент выполнения
        if self.total_queries == 0:
            percent = 0
        else:
            percent = int((self.processed_queries / self.total_queries) * 100)

        # Создаем прогресс-бар
        progress_bar_length = 20
        filled_length = int(
            progress_bar_length * self.processed_queries // self.total_queries) if self.total_queries > 0 else 0
        progress_bar = '█' * filled_length + '░' * (progress_bar_length - filled_length)

        # Рассчитываем статистику по статусам запросов
        success_count = sum(1 for status in self.query_status.values() if status == 'success')
        error_count = sum(1 for status in self.query_status.values() if status == 'error')
        cache_count = sum(1 for status in self.query_status.values() if status == 'cache')
        processing_count = sum(1 for status in self.query_status.values() if status == 'processing')

        # Расчет примерного оставшегося времени
        elapsed_time = current_time - self.stats["start_time"]
        if self.processed_queries > 0:
            time_per_query = elapsed_time / self.processed_queries
            remaining_queries = self.total_queries - self.processed_queries
            eta_seconds = time_per_query * remaining_queries

            # Форматирование ETA
            if eta_seconds < 60:
                eta_text = f"{int(eta_seconds)} сек"
            elif eta_seconds < 3600:
                eta_text = f"{int(eta_seconds // 60)} мин {int(eta_seconds % 60)} сек"
            else:
                eta_text = f"{int(eta_seconds // 3600)} ч {int((eta_seconds % 3600) // 60)} мин"
        else:
            eta_text = "расчет..."

        # Формируем сообщение с прогрессом
        status_message = (
            f"🔍 <b>Массовый пробив в процессе...</b>\n\n"
            f"<code>[{progress_bar}] {percent}%</code>\n\n"
            f"✅ Обработано: <b>{self.processed_queries}</b> из <b>{self.total_queries}</b>\n"
            f"📊 Статистика:\n"
            f"  • Успешных запросов: <b>{success_count}</b>\n"
            f"  • Запросов из кэша: <b>{cache_count}</b>\n"
            f"  • Обрабатывается: <b>{processing_count}</b>\n"
            f"  • Ошибок: <b>{error_count}</b>\n\n"
            f"⏱ Примерное оставшееся время: <b>{eta_text}</b>\n\n"
            f"⏳ <i>Пожалуйста, ожидайте завершения...</i>"
        )

        try:
            await self.bot.edit_message_text(
                chat_id=self.user_id,
                message_id=self.status_message_id,
                text=status_message,
                parse_mode="HTML"
            )
        except Exception as e:
            logging.error(f"Ошибка при обновлении сообщения о прогрессе: {e}")

    async def get_session_for_query(self, session_pool):
        """
        Выбирает оптимальную сессию из пула для запроса с учетом ротации.

        :param session_pool: Пул сессий
        :return: Сессия для использования
        """
        async with self.semaphore:
            # Увеличиваем счетчик ротации
            self.session_rotation_counter += 1

            if session_pool is None:
                raise Exception("Пул сессий не инициализирован")

            # Получаем сессию с учетом массового пробива
            session = await session_pool.get_available_session(
                is_mass_search=True,
                mass_search_id=self.mass_search_id
            )

            if not session:
                logging.warning(
                    f"Не удалось получить сессию после нескольких попыток. MassSearch ID: {self.mass_search_id}")
                raise Exception("Не удалось получить доступную сессию")

            return session

    async def release_session(self, session, session_pool):
        """
        Освобождает сессию после использования.

        :param session: Сессия для освобождения
        :param session_pool: Пул сессий
        """
        if session:
            # Освобождаем через пул сессий с указанием массового пробива
            await session_pool.release_session(
                session,
                is_mass_search=True,
                mass_search_id=self.mass_search_id
            )

    async def process_query(self, query: str, user_id: int, session_pool, results_dict: Dict):
        """
        Асинхронно обрабатывает отдельный запрос из массового пробива.

        :param query: Строка запроса для поиска
        :param user_id: ID пользователя в Telegram
        :param session_pool: Пул сессий для запросов
        :param results_dict: Словарь для хранения результатов {query: result}
        """
        query_start_time = time.time()

        try:
            # Отмечаем запрос как в обработке
            self.query_status[query] = 'processing'

            # Создаем задержку между запросами для массового пробива
            # Это снижает нагрузку на систему и позволяет одиночным запросам выполняться параллельно
            request_interval = random.uniform(self.min_request_interval, self.max_request_interval)

            # Получаем сессию через пул с учетом массового пробива
            try:
                session = await self.get_session_for_query(session_pool)
            except Exception as e:
                logging.error(f"Ошибка при получении сессии: {e}")
                self.query_status[query] = 'error'
                results_dict[query] = []
                self.processed_queries += 1
                return

            try:
                # Запускаем поиск через сессию напрямую (не через perform_search пула)
                # Это позволяет более тонко контролировать процесс
                if not session.is_authenticated:
                    auth_success = await session.authenticate()
                    if not auth_success:
                        self.query_status[query] = 'error'
                        results_dict[query] = []
                        logging.error(f"Не удалось авторизовать сессию {session.session_id} для запроса '{query}'")
                        self.stats["failed_requests"] += 1
                        return

                # Выполняем поиск
                success, result = await session.search(query)

                # Добавляем задержку после выполнения запроса
                await asyncio.sleep(request_interval)

                # Обрабатываем результат запроса
                if success:
                    # Парсим данные из HTML
                    parsed_data = await session.parse_results(result)
                    results_dict[query] = parsed_data
                    self.query_status[query] = 'success'
                    self.stats["successful_requests"] += 1

                    # Сохраняем результат в кэш
                    try:
                        from bot.database.db import save_response_to_cache
                        save_response_to_cache(user_id, query, parsed_data)
                    except Exception as cache_error:
                        logging.error(f"Ошибка при сохранении в кэш: {cache_error}")
                else:
                    # Ошибка запроса
                    logging.warning(f"Ошибка запроса '{query}': {result}")
                    self.query_status[query] = 'error'
                    results_dict[query] = []
                    self.stats["failed_requests"] += 1
            finally:
                # Освобождаем сессию
                await self.release_session(session, session_pool)

            # Обновляем статистику времени выполнения запросов
            query_time = time.time() - query_start_time
            self.stats["request_times"].append(query_time)
            self.stats["total_request_time"] += query_time
            if self.stats["request_times"]:
                self.stats["avg_request_time"] = self.stats["total_request_time"] / len(self.stats["request_times"])

        except Exception as e:
            logging.error(f"Ошибка при обработке запроса '{query}': {e}", exc_info=True)
            self.query_status[query] = 'error'
            results_dict[query] = []
            self.stats["failed_requests"] += 1
        finally:
            # Увеличиваем счетчик обработанных запросов
            self.processed_queries += 1

            # Обновляем прогресс
            await self.update_progress_message()

    async def process_file(self, file_path: str, user_id: int, session_pool, bot=None, status_message_id=None):
        """
        Оптимизированный метод обработки файла с запросами на пробив.

        :param file_path: Путь к загруженному файлу
        :param user_id: ID пользователя в Telegram
        :param session_pool: Пул сессий для запросов
        :param bot: Экземпляр бота для обновления статуса
        :param status_message_id: ID сообщения со статусом
        :return: (путь к результирующему файлу, словарь статистики, словарь результатов)
        """
        # Сохраняем параметры для обновления прогресса
        self.bot = bot
        self.user_id = user_id
        self.status_message_id = status_message_id

        logging.info(f"Начало обработки файла {file_path} для пользователя {user_id}")

        stats = {
            "total_lines": 0,
            "valid_lines": 0,
            "cached_queries": 0,
            "api_queries": 0,
            "phones_found": 0,
            "duplicate_phones": 0,
            "total_raw_phones": 0,
            "errors": 0,
            "skipped": 0,
            "processing_time": 0
        }

        self.stats = {
            "start_time": time.time(),
            "cache_hits": 0,
            "cache_misses": 0,
            "successful_requests": 0,
            "failed_requests": 0,
            "end_time": None,
            "avg_request_time": 0,
            "total_request_time": 0,
            "request_times": []
        }

        # Проверка целостности данных перед обработкой
        if not os.path.exists(file_path) or os.path.getsize(file_path) == 0:
            logging.error(f"Файл {file_path} не существует или пуст")
            stats["errors"] += 1
            return None, stats, {}

        start_time = time.time()
        valid_queries = []
        results_dict = {}  # Словарь для хранения всех результатов: {query: result}

        # 1. Чтение и валидация файла
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                lines = f.readlines()

            stats["total_lines"] = len(lines)
            logging.info(f"Чтение файла {file_path}: найдено {len(lines)} строк")

            # Валидация и нормализация строк
            from bot.utils import normalize_query, validate_query

            for line in lines:
                line = line.strip()
                if not line:
                    continue

                # Нормализуем запрос
                normalized_query = normalize_query(line)
                valid, result = validate_query(normalized_query)

                if valid:
                    valid_queries.append(normalized_query)
                    stats["valid_lines"] += 1

            logging.info(f"Валидных строк: {stats['valid_lines']} из {stats['total_lines']}")
        except Exception as e:
            logging.error(f"Ошибка при чтении файла: {e}", exc_info=True)
            return None, stats, {}

        if not valid_queries:
            logging.warning(f"Не найдено валидных запросов в файле {file_path}")
            return None, stats, {}

        # 2. Сортируем и убираем дубликаты для оптимизации
        valid_queries = sorted(list(set(valid_queries)))
        self.total_queries = len(valid_queries)
        logging.info(f"Уникальных валидных запросов: {len(valid_queries)}")

        # 3. Создаем файл результатов
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        result_file_path = f"static/responses/{user_id}/mass_search_result_{timestamp}.txt"
        result_full_data_path = f"static/responses/{user_id}/mass_search_full_data_{timestamp}.json"
        os.makedirs(os.path.dirname(result_file_path), exist_ok=True)

        # 4. Разделяем запросы на пакеты и обрабатываем
        cache_hit_queries = []  # Запросы, которые уже есть в кэше
        cache_miss_queries = []  # Запросы, которых нет в кэше

        # 4.1 Проверяем кэш для всех запросов
        from bot.database.db import get_cached_response

        for query in valid_queries:
            try:
                cached_found, cached_response, cache_source = get_cached_response(user_id, query)
                if cached_found and cached_response:
                    cache_hit_queries.append(query)
                    results_dict[query] = cached_response
                    self.query_status[query] = 'cache'
                    self.processed_queries += 1
                    self.stats["cache_hits"] += 1
                else:
                    cache_miss_queries.append(query)
                    self.query_status[query] = 'pending'
                    self.stats["cache_misses"] += 1
            except Exception as e:
                logging.error(f"Ошибка при проверке кэша для '{query}': {e}")
                cache_miss_queries.append(query)
                self.query_status[query] = 'pending'
                self.stats["cache_misses"] += 1

        stats["cached_queries"] = len(cache_hit_queries)
        stats["api_queries"] = len(cache_miss_queries)

        logging.info(f"Найдено в кэше: {stats['cached_queries']}, требуют API-запроса: {stats['api_queries']}")

        # Обновляем сообщение со статусом
        if bot and status_message_id:
            await self.update_progress_message()

        # 4.2 Выполняем API-запросы для отсутствующих в кэше запросов
        if cache_miss_queries:
            # Разбиваем запросы на небольшие пакеты для более равномерной нагрузки
            # и лучшего отображения прогресса
            batch_size = min(self.batch_size, max(5, len(cache_miss_queries) // 10))

            logging.info(f"Разбиваем {len(cache_miss_queries)} запросов на пакеты по {batch_size}")

            for i in range(0, len(cache_miss_queries), batch_size):
                batch_queries = cache_miss_queries[i:i + batch_size]
                batch_num = i // batch_size + 1
                total_batches = (len(cache_miss_queries) + batch_size - 1) // batch_size

                logging.info(f"Обработка пакета {batch_num}/{total_batches}: {len(batch_queries)} запросов")

                # Создаем задачи для пакета
                api_tasks = []
                for query in batch_queries:
                    # Исправленный вызов с правильными аргументами
                    task = self.process_query(query, user_id, session_pool, results_dict)
                    api_tasks.append(task)

                # Запускаем пакет задач
                try:
                    await asyncio.gather(*api_tasks)
                    logging.info(f"Пакет {batch_num}/{total_batches} API-запросов выполнен")
                except Exception as e:
                    logging.error(f"Ошибка при выполнении пакета {batch_num}: {e}", exc_info=True)

                # Задержка между пакетами для снижения нагрузки
                await asyncio.sleep(2.0)

        # 5. Обрабатываем результаты и группируем телефоны по запросам
        # 5.1. Извлекаем телефоны из результатов
        query_phones_dict = defaultdict(set)
        total_raw_phones = 0

        for query, result in results_dict.items():
            phones = self.extract_phones(result)

            if phones:
                total_raw_phones += len(phones)
                query_phones_dict[query].update(phones)
                logging.debug(f"Найдено {len(phones)} телефонов для запроса '{query}'")
            elif result and isinstance(result, list) and len(result) > 0:
                # Если телефоны не найдены обычным методом, пробуем через текст
                formatted_text = self.format_result_for_phones(result)
                text_phones = self.extract_phones_from_text(formatted_text)

                if text_phones:
                    total_raw_phones += len(text_phones)
                    query_phones_dict[query].update(text_phones)
                    logging.debug(f"Найдено {len(text_phones)} телефонов через текст для '{query}'")

        # 6. Записываем результаты в файлы
        # 6.1 Основной файл с телефонами
        has_results = False

        # Подсчитываем количество уникальных телефонов
        total_unique_phones = 0
        for query, phones in query_phones_dict.items():
            total_unique_phones += len(phones)

        # Вычисляем количество дубликатов
        duplicate_phones = total_raw_phones - total_unique_phones

        try:
            with open(result_file_path, 'w', encoding='utf-8') as result_file:
                # Записываем заголовок файла
                result_file.write(f"РЕЗУЛЬТАТЫ МАССОВОГО ПРОБИВА ОТ {timestamp}\n")
                result_file.write(f"Всего запросов: {len(valid_queries)}\n")
                result_file.write(f"Найдено телефонов: {total_unique_phones}\n")
                if duplicate_phones > 0:
                    result_file.write(f"Найдено дубликатов: {duplicate_phones}\n")
                result_file.write(f"====================================\n\n")

                if total_unique_phones == 0:
                    result_file.write("Не найдено телефонов ни по одному запросу.\n")
                    result_file.write("Проверьте правильность запросов и попробуйте снова.\n")
                    result_file.write("\nВозможные причины проблемы:\n")
                    result_file.write("1. Данные отсутствуют в базе\n")
                    result_file.write("2. Формат запроса неверный\n")
                    result_file.write("3. Технические проблемы с API\n")
                else:
                    has_results = True
                    # Сортируем запросы для удобства чтения
                    sorted_queries = sorted(query_phones_dict.keys())

                    for query in sorted_queries:
                        # Получаем все телефоны для данного запроса
                        all_phones = sorted(query_phones_dict[query])

                        # Фильтруем только мобильные телефоны
                        mobile_phones = []
                        for phone in all_phones:
                            if self.is_valid_mobile_phone(phone):
                                # Форматируем телефон в стандартный вид
                                formatted_phone = self.format_phone_number(phone)
                                if formatted_phone and formatted_phone not in mobile_phones:
                                    mobile_phones.append(formatted_phone)

                        # Если найдены мобильные телефоны - выводим их
                        if mobile_phones:
                            # Записываем ФИО/запрос
                            result_file.write(f"{query}\n")

                            # Записываем только мобильные телефоны с отступом
                            for phone in mobile_phones:
                                result_file.write(f" {phone}\n")

                            # Пустая строка между разными запросами для лучшей читаемости
                            result_file.write("\n")
                        else:
                            stats["skipped"] += 1

                    # Подсчитываем и записываем статистику
                    successful_queries = len([q for q in query_phones_dict if query_phones_dict[q]])
                    success_rate = round((successful_queries / len(valid_queries)) * 100, 1) if valid_queries else 0

                    result_file.write(f"\n====================================\n")
                    result_file.write(f"СТАТИСТИКА:\n")
                    result_file.write(
                        f"Запросов обработано успешно: {successful_queries}/{len(valid_queries)} ({success_rate}%)\n")

                    # Записываем информацию о запросах без результатов
                    if stats["skipped"] > 0:
                        result_file.write(f"\nЗапросы без результатов: {stats['skipped']}\n")
                        no_results_queries = [q for q in valid_queries if
                                              q not in query_phones_dict or not query_phones_dict[q]]
                        for q in no_results_queries[:5]:  # Выводим первые 5 запросов без результатов
                            result_file.write(f" - {q}\n")
                        if len(no_results_queries) > 5:
                            result_file.write(f" ... и еще {len(no_results_queries) - 5} запросов\n")

            # 6.2 Сохраняем полные данные в JSON
            with open(result_full_data_path, 'w', encoding='utf-8') as full_data_file:
                json.dump(results_dict, full_data_file, ensure_ascii=False, indent=2)

        except Exception as e:
            logging.error(f"Ошибка при сохранении результатов: {e}", exc_info=True)
            stats["errors"] += 1

        # Завершаем массовый пробив в пуле сессий
        try:
            await session_pool.finish_mass_search(self.mass_search_id)
        except Exception as e:
            logging.error(f"Ошибка при завершении массового пробива в пуле сессий: {e}")

        # Обновляем статистику
        stats["phones_found"] = total_unique_phones
        stats["duplicate_phones"] = duplicate_phones
        stats["total_raw_phones"] = total_raw_phones
        stats["processing_time"] = round(time.time() - start_time, 2)
        stats["has_results"] = has_results

        # Завершаем статистику
        self.stats["end_time"] = time.time()

        logging.info(f"Массовый пробив завершен за {stats['processing_time']} секунд. "
                     f"Найдено {stats['phones_found']} телефонов.")

        return result_file_path, stats, results_dict

    # Методы для извлечения и форматирования телефонов
    def is_valid_mobile_phone(self, phone_str):
        """Проверяет, является ли строка действительным российским мобильным номером"""
        if not phone_str:
            return False

        # Очищаем номер от всех нецифровых символов, кроме +
        phone_clean = ''.join(c for c in str(phone_str) if c.isdigit() or c == '+')

        # Убираем + в начале, если есть
        if phone_clean.startswith('+'):
            phone_clean = phone_clean[1:]

        # Исправляем 8 на 7 в начале номера
        if phone_clean.startswith('8') and len(phone_clean) == 11:
            phone_clean = '7' + phone_clean[1:]

        # Проверяем что это российский номер длиной 11 цифр
        if len(phone_clean) == 11 and phone_clean.startswith('7'):
            # Проверяем что второй символ - код мобильной сети
            mobile_codes = ['9', '8', '7', '6', '5', '4', '3']
            return phone_clean[1] in mobile_codes

        return False

    def extract_phones(self, data, query_phones=None):
        """Улучшенная функция извлечения телефонов из данных API"""
        if query_phones is None:
            query_phones = set()

        # Если данные строка - проверяем, является ли она телефоном
        if isinstance(data, (str, int)) and str(data):
            phone_str = str(data).strip()

            # Очищаем номер от лишних символов для проверки
            digits_only = ''.join(c for c in phone_str if c.isdigit())

            # Проверяем длину и возможные телефонные форматы
            if len(digits_only) >= 7 and len(digits_only) <= 15:
                # Более гибкая проверка форматов телефонов
                if re.match(r'^\+?[\d\s\(\)\-\.]{7,20}$', phone_str):
                    # Проверяем, является ли это мобильным номером
                    if self.is_valid_mobile_phone(phone_str):
                        query_phones.add(phone_str)
                    return query_phones

                # Дополнительно пробуем найти телефоны по количеству цифр
                if 10 <= len(digits_only) <= 12:
                    # Проверяем, является ли это мобильным номером
                    if self.is_valid_mobile_phone(phone_str):
                        query_phones.add(phone_str)
                    return query_phones

        # Если это словарь
        if isinstance(data, dict):
            for key, value in data.items():
                key_upper = str(key).upper() if isinstance(key, str) else ""

                # Расширенный список ключевых слов для поиска телефонов
                phone_keys = ["ТЕЛЕФОН", "PHONE", "МОБИЛЬНЫЙ", "MOBILE", "КОНТАКТ",
                              "ТЕЛ", "TEL", "НОМЕР", "NUMBER", "CONTACT", "MOB",
                              "ТЕЛЕФОНЫ", "PHONES", "TELEPHONE"]

                # Проверяем, содержит ли ключ одно из ключевых слов
                if any(phone_key in key_upper for phone_key in phone_keys):
                    if isinstance(value, list):
                        for phone in value:
                            self.extract_phones(phone, query_phones)
                    else:
                        self.extract_phones(value, query_phones)

                # Для поля ТЕЛЕФОН с вложенными структурами
                if key_upper == "ТЕЛЕФОН" and isinstance(value, (dict, list)):
                    self.extract_phones(value, query_phones)

                # Всегда рекурсивно проверяем значения
                self.extract_phones(value, query_phones)

        # Если это список
        elif isinstance(data, list):
            for item in data:
                self.extract_phones(item, query_phones)

        return query_phones

    def extract_phones_from_text(self, text):
        """
        Ищет телефонные номера в произвольном тексте

        :param text: Текст для анализа
        :return: Множество найденных мобильных телефонов
        """
        candidate_phones = set()

        # Разные форматы телефонов
        patterns = [
            r'\+?[78][\d\s\(\)\-]{8,15}',  # +7/8 с любыми разделителями
            r'\d{3}[\s\-]?\d{3}[\s\-]?\d{4}',  # 999-999-9999
            r'\+?\d{1,4}[\s\-\(\)]+\d{3,4}[\s\-\(\)]+\d{3,4}[\s\-\(\)]*\d{0,4}',  # Международный формат
            r'(?<!\d)\d{10}(?!\d)',  # Просто 10 цифр подряд
            r'(?<!\d)\d{11}(?!\d)'  # Просто 11 цифр подряд
        ]

        for pattern in patterns:
            matches = re.findall(pattern, text)
            for match in matches:
                # Проверяем является ли этот номер мобильным
                if self.is_valid_mobile_phone(match):
                    # Форматируем в стандартный вид +7XXXXXXXXXX
                    clean_phone = ''.join(c for c in match if c.isdigit() or c == '+')
                    if clean_phone.startswith('+'):
                        clean_phone = clean_phone[1:]
                    if clean_phone.startswith('8') and len(clean_phone) == 11:
                        clean_phone = '7' + clean_phone[1:]

                    candidate_phones.add(f"+{clean_phone}")

        return candidate_phones

    def format_phone_number(self, phone_str):
        """
        Форматирует телефонный номер в стандартный формат +7XXXXXXXXXX
        Возвращает None если это не мобильный номер или номер в неверном формате

        :param phone_str: Строка с телефонным номером
        :return: Отформатированный телефонный номер или None
        """
        if not phone_str:
            return None

        # Проверяем, является ли это мобильным номером
        if not self.is_valid_mobile_phone(phone_str):
            return None

        # Удаляем все нецифровые символы
        digits_only = ''.join(c for c in str(phone_str) if c.isdigit())

        # Если это 10-значный номер и начинается с кода мобильного оператора
        if len(digits_only) == 10 and digits_only[0] in ['9', '8', '7', '6', '5', '4', '3']:
            return f"+7{digits_only}"

        # Если это 11-значный номер и начинается с 7 или 8
        elif len(digits_only) == 11 and digits_only[0] in ['7', '8']:
            # Заменяем 8 на 7 в начале номера
            return f"+7{digits_only[1:]}"

        # Если уже начинается с +7, но нужно очистить от лишних символов
        elif len(digits_only) == 11 and digits_only.startswith('7'):
            return f"+{digits_only}"

        # Другие случаи считаем невалидными
        return None

    def format_result_for_phones(self, data):
        """
        Преобразует результаты запроса в текст для поиска телефонов

        :param data: Результаты запроса (список словарей)
        :return: Текстовое представление результатов
        """
        result_text = ""

        if not isinstance(data, (list, dict)):
            return str(data)

        if isinstance(data, list):
            for item in data:
                result_text += self.format_result_for_phones(item) + "\n"
        elif isinstance(data, dict):
            for key, value in data.items():
                # Особо выделяем поля, которые могут содержать телефоны
                if key.upper() in ["ТЕЛЕФОН", "ТЕЛ", "КОНТАКТ", "PHONE", "MOBILE", "CONTACT"]:
                    result_text += f"{key}: {value}\n"
                else:
                    # Для остальных полей - просто добавляем в текст
                    if isinstance(value, (list, dict)):
                        result_text += f"{key}:\n{self.format_result_for_phones(value)}\n"
                    else:
                        result_text += f"{key}: {value}\n"

        return result_text


# Обработчик для кнопки "Массовый пробив"
@mass_search_router.callback_query(lambda c: c.data == "mass_search")
async def cb_mass_search(callback: CallbackQuery, state: FSMContext):
    """Обработка нажатия на кнопку 'Массовый пробив'"""
    user_id = callback.from_user.id
    if not check_active_session(user_id):
        await callback.answer("Вы не вошли в систему", show_alert=True)
        return

    # Проверяем баланс пользователя перед продолжением
    balance = get_user_balance(user_id)
    if balance is None or balance <= 0:
        await callback.message.answer("Недостаточно средств на балансе для выполнения массового пробива.")
        await callback.answer()
        return

    await state.set_state(MassSearchStates.waiting_for_file)
    await callback.message.answer(
        "📤 Пожалуйста, загрузите файл .txt со списком ФИО и датами рождения.\n\n"
        "Формат каждой строки: Фамилия Имя Отчество ДД.ММ.ГГГГ\n"
        "Например: Иванов Иван Иванович 01.01.1990\n\n"
        "Максимальный размер файла: 5 МБ"
    )
    await callback.answer()


# Обработчик для загрузки файла
@mass_search_router.message(StateFilter(MassSearchStates.waiting_for_file), F.document)
async def process_file_upload(message: Message, state: FSMContext):
    """Обработка загруженного файла для массового пробива"""
    user_id = message.from_user.id

    # Проверяем формат файла
    if not message.document.file_name.endswith('.txt'):
        await message.answer("❌ Пожалуйста, загрузите файл в формате .txt")
        return

    # Проверяем размер файла (5 МБ = 5 * 1024 * 1024 байт)
    if message.document.file_size > 5 * 1024 * 1024:
        await message.answer("❌ Размер файла превышает 5 МБ. Пожалуйста, загрузите файл меньшего размера.")
        return

    # Сохраняем файл
    file_id = message.document.file_id
    file_path = f"static/uploads/{user_id}_{message.document.file_name}"
    os.makedirs(os.path.dirname(file_path), exist_ok=True)

    try:
        await message.bot.download(file=file_id, destination=file_path)
    except Exception as e:
        logging.error(f"Ошибка при скачивании файла: {e}")
        await message.answer(f"❌ Произошла ошибка при загрузке файла: {str(e)}")
        await state.clear()
        return

    # Подсчитываем количество валидных строк и стоимость
    try:
        valid_lines = 0
        with open(file_path, 'r', encoding='utf-8') as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue

                normalized_query = normalize_query(line)
                valid, _ = validate_query(normalized_query)

                if valid:
                    valid_lines += 1

        if valid_lines == 0:
            await message.answer("❌ В файле не найдено ни одной валидной строки для пробива.")
            await state.clear()
            return

        # Проверяем баланс пользователя, используя функцию из db.py
        enough_balance, balance, required_amount = check_balance_for_mass_search(user_id, valid_lines)

        if not enough_balance:
            additional_needed = required_amount - balance
            await message.answer(
                f"❌ Недостаточно средств для выполнения массового пробива.\n\n"
                f"В файле найдено {valid_lines} валидных строк.\n"
                f"Стоимость обработки: ${required_amount:.2f}\n"
                f"Ваш текущий баланс: ${balance:.2f}\n\n"
                f"Необходимо пополнить баланс на сумму: ${additional_needed:.2f}"
            )
            await state.clear()
            return

        # Сохраняем информацию о файле и стоимости в FSM
        await state.update_data(
            file_path=file_path,
            valid_lines=valid_lines,
            total_cost=required_amount
        )

        # Создаем клавиатуру с кнопками для подтверждения/отмены
        confirm_keyboard = InlineKeyboardMarkup(inline_keyboard=[
            [
                InlineKeyboardButton(text="✅ Подтверждаю", callback_data="confirm_mass_search"),
                InlineKeyboardButton(text="❌ Отмена", callback_data="cancel_mass_search")
            ]
        ])

        # Просим подтверждение с помощью кнопок вместо текста
        await state.set_state(MassSearchStates.confirming_search)
        await message.answer(
            f"📊 Информация о файле:\n\n"
            f"- Валидных строк для обработки: {valid_lines}\n"
            f"- Стоимость обработки: ${required_amount:.2f}\n"
            f"- Ваш текущий баланс: ${balance:.2f}\n\n"
            f"Выберите действие:",
            reply_markup=confirm_keyboard
        )
    except Exception as e:
        logging.error(f"Ошибка при обработке файла: {e}")
        await message.answer(f"❌ Произошла ошибка при анализе файла: {str(e)}")
        await state.clear()


# Обработчик для кнопки подтверждения массового пробива
@mass_search_router.callback_query(lambda c: c.data == "confirm_mass_search",
                                   StateFilter(MassSearchStates.confirming_search))
async def process_confirm_button(callback: CallbackQuery, state: FSMContext):
    """Обработка нажатия кнопки подтверждения"""
    user_id = callback.from_user.id

    # Проверяем, не в очереди ли уже пользователь
    if await mass_search_queue.is_user_in_queue(user_id):
        position = await mass_search_queue.get_position(user_id)
        queue_status = await mass_search_queue.get_queue_status()

        if position == 0:
            await callback.answer("Ваш запрос уже обрабатывается.", show_alert=True)
        else:
            await callback.answer(
                f"Вы уже в очереди на позиции {position} из {queue_status['waiting'] + queue_status['processing']}",
                show_alert=True)
        return

    # Получаем данные из FSM
    data = await state.get_data()
    file_path = data.get("file_path")
    valid_lines = data.get("valid_lines")
    total_cost = data.get("total_cost")

    # Выполняем списание баланса
    success, message_text, _ = batch_deduct_balance(user_id, valid_lines)

    if not success:
        await callback.answer("Ошибка списания средств: " + message_text, show_alert=True)
        # Не сбрасываем состояние, чтобы пользователь мог попробовать снова
        # Добавляем дополнительное сообщение с инструкцией
        await callback.message.answer(
            "❌ Произошла ошибка при списании средств.\n"
            "Вы можете попробовать снова или нажать 'Отмена'."
        )
        return

    # Логируем начало массового пробива
    log_id = log_mass_search_start(user_id, file_path, valid_lines, total_cost)

    # Убираем инлайн-клавиатуру с сообщения
    await callback.message.edit_reply_markup(reply_markup=None)

    # Отправляем сообщение о постановке в очередь
    queue_message = await callback.message.answer(
        "🕒 <b>Ваш запрос добавляется в очередь на обработку...</b>\n\n"
        "Пожалуйста, ожидайте.",
        parse_mode="HTML"
    )

    # Добавляем в очередь
    position = await mass_search_queue.add_to_queue(
        user_id=user_id,
        message_id=queue_message.message_id,
        file_path=file_path,
        valid_lines=valid_lines,
        total_cost=total_cost
    )

    # Запускаем задачу обновления сообщения о позиции
    asyncio.create_task(
        update_queue_position_message(
            bot=callback.bot,
            user_id=user_id,
            message_id=queue_message.message_id
        )
    )

    # Отвечаем на callback и очищаем состояние
    await callback.answer(f"Вы добавлены в очередь на позицию {position}", show_alert=True)
    await state.clear()


# Обработчик для кнопки отмены массового пробива
@mass_search_router.callback_query(lambda c: c.data == "cancel_mass_search",
                                   StateFilter(MassSearchStates.confirming_search))
async def process_cancel_button(callback: CallbackQuery, state: FSMContext):
    """Обработка нажатия кнопки отмены"""
    await callback.answer()
    await callback.message.answer("❌ Массовый пробив отменен.")

    # Убираем инлайн-клавиатуру с сообщения
    await callback.message.edit_reply_markup(reply_markup=None)

    await state.clear()


async def update_queue_position_message(bot, user_id, message_id):
    """Обновляет сообщение с информацией о позиции в очереди"""
    try:
        while await mass_search_queue.is_user_in_queue(user_id):
            position = await mass_search_queue.get_position(user_id)
            queue_status = await mass_search_queue.get_queue_status()

            if position == 0 or await mass_search_queue.is_user_processing(user_id):
                # Запрос в обработке
                await bot.edit_message_text(
                    chat_id=user_id,
                    message_id=message_id,
                    text=f"⏳ <b>Ваш запрос обрабатывается...</b>\n\n"
                         f"Всего в очереди: {queue_status['waiting']} запросов\n"
                         f"Активных обработок: {queue_status['processing']}/{queue_status['capacity']}",
                    parse_mode="HTML"
                )
            else:
                # Запрос в очереди
                await bot.edit_message_text(
                    chat_id=user_id,
                    message_id=message_id,
                    text=f"🕒 <b>Вы в очереди на массовый пробив</b>\n\n"
                         f"Ваша позиция: {position} из {queue_status['waiting'] + queue_status['processing']}\n"
                         f"Активных обработок: {queue_status['processing']}/{queue_status['capacity']}\n\n"
                         f"Пожалуйста, дождитесь вашей очереди. Информация обновляется автоматически.",
                    parse_mode="HTML"
                )

            # Обновляем каждые 5 секунд
            await asyncio.sleep(5)
    except Exception as e:
        logging.error(f"Ошибка при обновлении сообщения очереди: {e}")


# Обработчик очереди массовых пробивов
async def process_mass_search_queue(bot):
    """Обработчик очереди массовых пробивов"""
    logging.info("Запущен обработчик очереди массовых пробивов")

    # Импортируем необходимые зависимости
    from bot.session_manager import session_pool

    while True:
        try:
            # Получаем следующий элемент для обработки
            queue_item = await mass_search_queue.get_next_item()

            if queue_item:
                user_id = queue_item.user_id
                file_path = queue_item.file_path
                valid_lines = queue_item.valid_lines
                total_cost = queue_item.total_cost
                message_id = queue_item.message_id
                log_id = None  # Правильная инициализация переменной

                # Проверяем существование файла
                if not os.path.exists(file_path):
                    logging.error(f"Файл не найден: {file_path}")
                    await bot.send_message(
                        user_id,
                        "❌ Ошибка: Файл для обработки не найден. Пожалуйста, загрузите файл снова."
                    )
                    # Удаляем из очереди
                    await mass_search_queue.remove_item(user_id)
                    continue

                try:
                    # Используем существующие механизмы для совместимости
                    active_user_searches[user_id] += 1
                    await mass_search_semaphore.acquire()

                    # Обновляем статус в базе данных
                    log_id = log_mass_search_start(user_id, file_path, valid_lines, total_cost)
                    if log_id:
                        update_mass_search_status(log_id, "processing")
                    else:
                        logging.warning(f"Не удалось создать запись о массовом пробиве для пользователя {user_id}")
                        await bot.send_message(
                            user_id,
                            "⚠️ Предупреждение: Не удалось создать запись о массовом пробиве. Обработка будет продолжена."
                        )

                    # Уведомляем о начале обработки
                    status_message = await bot.send_message(
                        user_id,
                        f"🔄 <b>Начинаем массовый пробив</b>\n\n"
                        f"Файл содержит {valid_lines} запросов\n"
                        f"Стоимость: ${total_cost:.2f}\n\n"
                        f"<code>[░░░░░░░░░░░░░░░░░░░░] 0%</code>\n\n"
                        f"⏳ <i>Пожалуйста, ожидайте...</i>",
                        parse_mode="HTML"
                    )

                    # Создаем процессор и выполняем обработку
                    from bot.mass_search import MassSearchProcessor

                    processor = MassSearchProcessor(
                        max_concurrent=3,  # Используем не более 3 одновременных запросов
                        min_request_interval=1.0,  # Минимальный интервал между запросами
                        max_request_interval=4.0,  # Максимальный интервал между запросами
                        batch_size=10  # Обрабатываем небольшими пакетами
                    )

                    # Добавляем обработку возможных исключений при обработке файла
                    try:
                        # Передаем бот и ID сообщения для обновления прогресса
                        result_file_path, stats, results_dict = await processor.process_file(
                            file_path,
                            user_id,
                            session_pool,
                            bot,
                            status_message.message_id
                        )
                    except Exception as file_error:
                        logging.error(f"Ошибка при обработке файла: {file_error}", exc_info=True)
                        # Возвращаем средства пользователю
                        from bot.database.db import mass_refund_balance
                        success, refund_message = mass_refund_balance(user_id, valid_lines)

                        # Информируем пользователя
                        await bot.send_message(
                            user_id,
                            f"❌ <b>Ошибка при обработке файла:</b>\n{str(file_error)}\n\n"
                            f"{refund_message}",
                            parse_mode="HTML"
                        )

                        if log_id:
                            update_mass_search_status(log_id, "failed")

                        # Освобождаем ресурсы
                        active_user_searches[user_id] -= 1
                        if active_user_searches[user_id] < 0:
                            active_user_searches[user_id] = 0
                        mass_search_semaphore.release()
                        await mass_search_queue.remove_item(user_id)
                        continue

                    # Добавить подробное логирование результатов
                    phones_found = stats.get('phones_found', 0)
                    logging.info(f"Обработка завершена для пользователя {user_id}: найдено {phones_found} телефонов")

                    # Проверка результатов
                    if phones_found == 0:
                        logging.warning(f"Не найдено телефонов для пользователя {user_id} в файле {file_path}")

                    # Обновляем статус в базе данных
                    if log_id:
                        update_mass_search_status(
                            log_id,
                            "completed",
                            results_file=result_file_path,
                            phones_found=stats.get('phones_found', 0)
                        )

                    # Обновляем последний раз сообщение о завершении
                    await bot.edit_message_text(
                        chat_id=user_id,
                        message_id=status_message.message_id,
                        text=f"✅ <b>Массовый пробив завершен!</b>\n\n"
                             f"📊 <b>Статистика:</b>\n"
                             f"• Всего строк: {stats['total_lines']}\n"
                             f"• Валидных запросов: {stats['valid_lines']}\n"
                             f"• Найдено телефонов: {stats['phones_found']}\n"
                             f"• Обработано за: {stats['processing_time']} сек\n\n"
                             f"<code>[{'█' * 20}] 100%</code>",
                        parse_mode="HTML"
                    )

                    # Проверяем результаты и отправляем файл
                    if result_file_path and os.path.exists(result_file_path) and os.path.getsize(result_file_path) > 0:
                        # Формируем сообщение в зависимости от наличия телефонов
                        if stats.get("phones_found", 0) > 0:
                            result_message = f"📎 Результаты в файле ниже:"
                        else:
                            result_message = f"📎 Отчет о проверке запросов в файле ниже:"

                        # Отправляем сообщение и файл
                        await bot.send_message(user_id, result_message, parse_mode="HTML")

                        # Добавляем обработку возможных ошибок при отправке файла
                        try:
                            await bot.send_document(user_id, FSInputFile(result_file_path))
                        except Exception as send_error:
                            logging.error(f"Ошибка при отправке файла: {send_error}")
                            await bot.send_message(
                                user_id,
                                "⚠️ Не удалось отправить файл с результатами. Пожалуйста, свяжитесь с администратором."
                            )
                    else:
                        # Если файл не создан или пустой
                        result_message = "❌ <b>Не удалось создать файл результатов.</b>"
                        await bot.send_message(user_id, result_message, parse_mode="HTML")

                        if log_id:
                            update_mass_search_status(log_id, "failed")

                except Exception as e:
                    logging.error(f"Ошибка при обработке массового пробива: {e}", exc_info=True)
                    await bot.send_message(
                        user_id,
                        f"❌ <b>Произошла ошибка при обработке:</b>\n{str(e)}",
                        parse_mode="HTML"
                    )

                    if log_id:
                        update_mass_search_status(log_id, "failed")
                except asyncio.CancelledError:
                    logging.warning(f"Задача обработки очереди для пользователя {user_id} отменена")
                    if log_id:
                        update_mass_search_status(log_id, "failed", error_message="Задача отменена")

                    # Возвращаем средства пользователю
                    from bot.database.db import mass_refund_balance
                    await mass_refund_balance(user_id, valid_lines)

                    # Уведомляем пользователя
                    try:
                        await bot.send_message(
                            user_id,
                            "❌ Задача отменена. Средства возвращены на ваш баланс."
                        )
                    except:
                        logging.error(f"Не удалось отправить уведомление пользователю {user_id}")

                finally:
                    # Освобождаем ресурсы
                    active_user_searches[user_id] -= 1
                    if active_user_searches[user_id] < 0:
                        active_user_searches[user_id] = 0  # Исправляем возможное отрицательное значение

                    mass_search_semaphore.release()

                    # Удаляем из очереди
                    await mass_search_queue.remove_item(user_id)

            # Проверяем очередь каждую секунду
            await asyncio.sleep(1)

        except Exception as e:
            logging.error(f"Ошибка в процессе обработки очереди: {e}", exc_info=True)
            await asyncio.sleep(5)  # При ошибке ждем дольше