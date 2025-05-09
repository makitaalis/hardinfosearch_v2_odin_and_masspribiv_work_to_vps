import asyncio
import json
import logging
import os
import random
import re
import time
import traceback
from collections import defaultdict, deque
from datetime import datetime
from typing import Dict, List, Tuple, Optional, Set

from aiogram import F, Router
from aiogram.filters import StateFilter
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.types import Message, CallbackQuery, InlineKeyboardMarkup, InlineKeyboardButton, FSInputFile

# Proper imports with full set of required functions
from bot.common import mass_search_semaphore, active_user_searches, mass_search_queue, MAX_CONCURRENT_MASS_SEARCHES
from bot.database.db import (
    check_active_session, get_user_balance, check_balance_for_mass_search, batch_deduct_balance, log_mass_search_start,
    update_mass_search_status
)
from bot.utils import normalize_query, validate_query, send_api_request, filter_unique_data, format_api_response


# Define FSM states for mass search
class MassSearchStates(StatesGroup):
    waiting_for_file = State()  # Waiting for file upload
    confirming_search = State()  # Confirming search (after cost calculation)
    processing = State()  # Processing requests


# Create router for mass search handlers
mass_search_router = Router()

# Class for processing results
class MassSearchProcessor:
    """
    Enhanced class for mass search processing with load balancing between sessions
    and real-time progress display.
    """

    def __init__(self, max_concurrent=20, min_request_interval=0.5, max_request_interval=2.0, batch_size=25):
        """
        Enhanced initialization with higher performance parameters
        """
        # Performance parameters
        self.max_concurrent = max_concurrent  # Increased for more parallelism
        self.min_request_interval = min_request_interval  # Reduced for faster processing
        self.max_request_interval = max_request_interval  # Reduced maximum delay
        self.batch_size = batch_size  # Larger batches for efficiency
        self.semaphore = asyncio.Semaphore(max_concurrent)

        # Adaptive pacing system
        self.adaptive_pacing = True
        self.success_rate_threshold = 0.8  # 80% success rate target
        self.current_pace_factor = 1.0  # Dynamically adjusted

        # Queue processing
        self.queue = deque()  # Local processing queue
        self.processing_users = set()  # Users with active processing
        self.lock = asyncio.Lock()  # Lock for thread safety

        # Tracking and statistics
        self.processing_stats = defaultdict(float)
        self.results_lock = asyncio.Lock()
        self.progress_counter = 0
        self.progress_lock = asyncio.Lock()
        self.session_usage = defaultdict(int)  # Track session usage
        self.total_queries = 0
        self.processed_queries = 0
        self.session_rotation_counter = 0
        self.mass_search_id = f"mass_{int(time.time())}_{random.randint(1000, 9999)}"

        # Status tracking
        self.bot = None
        self.user_id = None
        self.status_message_id = None
        self.last_status_update = time.time()
        self.status_update_interval = 1.0  # Faster status updates
        self.query_status = {}

        # Performance metrics
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

    async def process_query_batch(self, queries, user_id, session_pool, results_dict):
        """Process multiple queries efficiently with dynamic concurrency control"""
        # Determine optimal concurrent jobs based on system load
        total_jobs = len(self.active_mass_searches) if hasattr(self, 'active_mass_searches') else 1
        effective_concurrent = max(5, min(20, 100 // total_jobs))

        # Create semaphore for this batch
        batch_semaphore = asyncio.Semaphore(effective_concurrent)

        # Launch tasks with controlled concurrency
        tasks = []
        for query in queries:
            task = self._process_query_with_semaphore(query, user_id, session_pool,
                                                      results_dict, batch_semaphore)
            tasks.append(task)

        # Wait for completion
        await asyncio.gather(*tasks)

    async def _process_query_with_semaphore(self, query, user_id, session_pool,
                                            results_dict, semaphore):
        """Process single query with semaphore control"""
        async with semaphore:
            await self.process_query(query, user_id, session_pool, results_dict)

    async def update_progress_message(self):
        """
        Updates the progress message during execution.
        Uses bot, user_id and status_message_id parameters stored in the object.
        """
        # Check for required parameters
        if not hasattr(self, 'bot') or not self.bot or not hasattr(self, 'user_id') or not self.user_id or not hasattr(
                self, 'status_message_id') or not self.status_message_id:
            logging.warning("Missing parameters for progress update")
            return

        # Check if we need to update status (not more often than once every N seconds)
        current_time = time.time()
        if hasattr(self, 'last_status_update') and current_time - self.last_status_update < self.status_update_interval:
            return

        self.last_status_update = current_time

        # Calculate completion percentage
        if self.total_queries == 0:
            percent = 0
        else:
            percent = int((self.processed_queries / self.total_queries) * 100)

        # Create progress bar
        progress_bar_length = 20
        filled_length = int(
            progress_bar_length * self.processed_queries // self.total_queries) if self.total_queries > 0 else 0
        progress_bar = '█' * filled_length + '░' * (progress_bar_length - filled_length)

        # Calculate query status statistics
        success_count = sum(1 for status in self.query_status.values() if status == 'success')
        error_count = sum(1 for status in self.query_status.values() if status == 'error')
        cache_count = sum(1 for status in self.query_status.values() if status == 'cache')
        processing_count = sum(1 for status in self.query_status.values() if status == 'processing')

        # Calculate estimated remaining time
        elapsed_time = current_time - self.stats["start_time"]
        if self.processed_queries > 0:
            time_per_query = elapsed_time / self.processed_queries
            remaining_queries = self.total_queries - self.processed_queries
            eta_seconds = time_per_query * remaining_queries

            # Format ETA
            if eta_seconds < 60:
                eta_text = f"{int(eta_seconds)} сек"
            elif eta_seconds < 3600:
                eta_text = f"{int(eta_seconds // 60)} мин {int(eta_seconds % 60)} сек"
            else:
                eta_text = f"{int(eta_seconds // 3600)} ч {int((eta_seconds % 3600) // 60)} мин"
        else:
            eta_text = "расчет..."

        # Create progress message
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
            logging.error(f"Error updating progress message: {e}")

    async def get_session_for_query(self, session_pool):
        """
        Optimized session acquisition with minimal delay
        """
        async with self.semaphore:
            # Apply adaptive pacing delay
            if self.adaptive_pacing:
                request_interval = random.uniform(
                    self.min_request_interval * self.current_pace_factor,
                    self.max_request_interval * self.current_pace_factor
                )
            else:
                request_interval = random.uniform(self.min_request_interval, self.max_request_interval)

            # Smaller delay to improve throughput
            await asyncio.sleep(request_interval)

            # Increase rotation counter
            self.session_rotation_counter += 1

            if session_pool is None:
                raise Exception("Session pool is not initialized")

            # Get session with mass search ID for better allocation
            session = await session_pool.get_available_session(
                is_mass_search=True,
                mass_search_id=self.mass_search_id
            )

            if not session:
                logging.warning(f"Failed to get session after several attempts for mass search {self.mass_search_id}")
                raise Exception("Failed to get available session")

            return session

    async def release_session(self, session, session_pool):
        """
        Releases a session after use.

        :param session: Session to release
        :param session_pool: Session pool
        """
        if session:
            # Release through session pool with mass search indication
            await session_pool.release_session(
                session,
                is_mass_search=True,
                mass_search_id=self.mass_search_id
            )

    async def process_query(self, query: str, user_id: int, session_pool, results_dict: Dict):
        """
        Enhanced asynchronous processing of a single query from mass search
        """
        query_start_time = time.time()

        try:
            # Mark query as processing
            self.query_status[query] = 'processing'

            # Get session through pool considering mass search
            try:
                session = await self.get_session_for_query(session_pool)
            except Exception as e:
                logging.error(f"Error getting session: {e}")
                self.query_status[query] = 'error'
                results_dict[query] = []
                self.processed_queries += 1
                return

            try:
                # Format query before sending
                formatted_query = normalize_query(query)

                # Start search through session directly
                if not session.is_authenticated:
                    auth_success = await session.authenticate()
                    if not auth_success:
                        self.query_status[query] = 'error'
                        results_dict[query] = []
                        logging.error(f"Failed to authenticate session {session.session_id} for query '{query}'")
                        self.stats["failed_requests"] += 1
                        return

                # Perform search with formatted query
                success, result = await session.search(formatted_query)

                # Process request result
                if success:
                    # Parse data from HTML
                    parsed_data = await session.parse_results(result)
                    results_dict[query] = parsed_data
                    self.query_status[query] = 'success'
                    self.stats["successful_requests"] += 1

                    # Save result to cache
                    try:
                        from bot.database.db import save_response_to_cache
                        save_response_to_cache(user_id, query, parsed_data)
                    except Exception as cache_error:
                        logging.error(f"Error saving to cache: {cache_error}")
                else:
                    # Request error
                    logging.warning(f"Query error '{query}': {result}")
                    self.query_status[query] = 'error'
                    results_dict[query] = []
                    self.stats["failed_requests"] += 1
            finally:
                # Release session
                await self.release_session(session, session_pool)

            # Update query execution time statistics
            query_time = time.time() - query_start_time
            self.stats["request_times"].append(query_time)
            self.stats["total_request_time"] += query_time
            if self.stats["request_times"]:
                self.stats["avg_request_time"] = self.stats["total_request_time"] / len(self.stats["request_times"])

        except Exception as e:
            logging.error(f"Error processing query '{query}': {e}", exc_info=True)
            self.query_status[query] = 'error'
            results_dict[query] = []
            self.stats["failed_requests"] += 1
        finally:
            # Increment processed queries counter
            self.processed_queries += 1

            # Update progress
            await self.update_progress_message()

    async def process_file(self, file_path: str, user_id: int, session_pool, bot=None, status_message_id=None,
                           process_id=None):
        """
        Оптимизированный метод для обработки файла с поисковыми запросами.
        Модифицированный для фильтрации только российских мобильных номеров.

        :param file_path: Путь к загруженному файлу
        :param user_id: ID пользователя в Telegram
        :param session_pool: Пул сессий для запросов
        :param bot: Экземпляр бота для обновления статуса
        :param status_message_id: ID сообщения со статусом
        :param process_id: Опциональный ID процесса для отслеживания
        :return: (путь к файлу результатов, словарь статистики, словарь результатов)
        """
        # Сохраняем параметры для обновления прогресса
        self.bot = bot
        self.user_id = user_id
        self.status_message_id = status_message_id
        if process_id:
            self.mass_search_id = process_id

        logging.info(f"Starting file processing {file_path} for user {user_id}")

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
            logging.error(f"File {file_path} doesn't exist or is empty")
            stats["errors"] += 1
            return None, stats, {}

        start_time = time.time()
        valid_queries = []
        results_dict = {}  # Словарь для хранения всех результатов: {query: результат}

        # 1. Чтение и валидация файла
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                lines = f.readlines()

            stats["total_lines"] = len(lines)
            logging.info(f"Reading file {file_path}: found {len(lines)} lines")

            # Валидация и нормализация строк
            from bot.utils import normalize_query, validate_query

            for line in lines:
                line = line.strip()
                if not line:
                    continue

                # Нормализуем запрос
                normalized_query = normalize_query(line)
                valid, _ = validate_query(normalized_query)

                if valid:
                    valid_queries.append(normalized_query)
                    stats["valid_lines"] += 1

            logging.info(f"Valid lines: {stats['valid_lines']} out of {stats['total_lines']}")
        except Exception as e:
            logging.error(f"Error reading file: {e}", exc_info=True)
            return None, stats, {}

        if not valid_queries:
            logging.warning(f"No valid queries found in file {file_path}")
            return None, stats, {}

        # 2. Сортировка и удаление дубликатов для оптимизации
        valid_queries = sorted(list(set(valid_queries)))
        self.total_queries = len(valid_queries)
        logging.info(f"Unique valid queries: {len(valid_queries)}")

        # 3. Создание файла для результатов
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        result_file_path = f"static/responses/{user_id}/mass_search_result_{timestamp}.txt"
        result_full_data_path = f"static/responses/{user_id}/mass_search_full_data_{timestamp}.json"
        os.makedirs(os.path.dirname(result_file_path), exist_ok=True)

        # 4. Разделение запросов на пакеты и их обработка
        cache_hit_queries = []  # Запросы, уже имеющиеся в кэше
        cache_miss_queries = []  # Запросы, отсутствующие в кэше

        # 4.1 Проверка кэша для всех запросов
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
                logging.error(f"Error checking cache for '{query}': {e}")
                cache_miss_queries.append(query)
                self.query_status[query] = 'pending'
                self.stats["cache_misses"] += 1

        stats["cached_queries"] = len(cache_hit_queries)
        stats["api_queries"] = len(cache_miss_queries)

        logging.info(f"Found in cache: {stats['cached_queries']}, require API request: {stats['api_queries']}")

        # Обновление сообщения о статусе
        if bot and status_message_id:
            await self.update_progress_message()

        # 4.2 Эффективная обработка API-запросов для элементов, отсутствующих в кэше
        if cache_miss_queries:
            # Сначала попытаемся выделить специальные сессии для этого массового поиска
            allocated_sessions = await session_pool.allocate_sessions_for_mass_search(
                self.mass_search_id,
                requested_count=min(10, len(cache_miss_queries) // 10 + 1)
            )

            # Настройка параллельных заданий на основе доступных сессий
            effective_concurrent = min(self.max_concurrent, max(5, allocated_sessions * 2))
            self.semaphore = asyncio.Semaphore(effective_concurrent)

            # Расчет оптимального размера пакета на основе количества запросов
            optimal_batch_size = min(self.batch_size, max(5, len(cache_miss_queries) // 5))
            logging.info(
                f"Processing {len(cache_miss_queries)} queries in batches of {optimal_batch_size} with {effective_concurrent} concurrent slots")

            # Обработка по пакетам
            for i in range(0, len(cache_miss_queries), optimal_batch_size):
                batch_queries = cache_miss_queries[i:i + optimal_batch_size]
                batch_num = i // optimal_batch_size + 1
                total_batches = (len(cache_miss_queries) + optimal_batch_size - 1) // optimal_batch_size

                logging.info(f"Processing batch {batch_num}/{total_batches}: {len(batch_queries)} queries")

                # Обработка пакета с параллельным выполнением
                await self.process_query_batch(batch_queries, user_id, session_pool, results_dict)

                # Небольшая пауза между пакетами
                await asyncio.sleep(0.5)

                # Обновление сообщения о статусе
                await self.update_progress_message()

        # 5. Обработка результатов и группировка телефонов по запросам
        # 5.1. Извлечение телефонов из результатов
        query_phones_dict = defaultdict(set)
        total_raw_phones = 0

        for query, result in results_dict.items():
            phones = self.extract_phones(result)

            if phones:
                total_raw_phones += len(phones)
                query_phones_dict[query].update(phones)
                logging.debug(f"Found {len(phones)} phones for query '{query}'")
            elif result and isinstance(result, list) and len(result) > 0:
                # Если телефоны не найдены стандартным методом, попробуем через текст
                formatted_text = self.format_result_for_phones(result)
                text_phones = self.extract_phones_from_text(formatted_text)

                if text_phones:
                    total_raw_phones += len(text_phones)
                    query_phones_dict[query].update(text_phones)
                    logging.debug(f"Found {len(text_phones)} phones through text for '{query}'")

        # 6. Запись результатов в файлы
        # 6.1 Основной файл с телефонами
        has_results = False
        russian_mobile_count = 0  # Счетчик российских мобильных номеров

        try:
            with open(result_file_path, 'w', encoding='utf-8') as result_file:
                # Заголовок файла
                result_file.write(f"РЕЗУЛЬТАТЫ МАССОВОГО ПРОБИВА ОТ {timestamp}\n")
                result_file.write(f"Всего запросов: {len(valid_queries)}\n")

                # Подсчет российских мобильных номеров
                for query, phones in query_phones_dict.items():
                    for phone in phones:
                        if phone and isinstance(phone, str) and phone.startswith('+79'):
                            russian_mobile_count += 1

                result_file.write(f"Найдено российских мобильных номеров: {russian_mobile_count}\n")
                result_file.write(f"====================================\n\n")

                if russian_mobile_count == 0:
                    result_file.write("Не найдено российских мобильных номеров ни по одному запросу.\n")
                    result_file.write("Проверьте правильность запросов и попробуйте снова.\n\n")

                    # Добавляем список запросов без результатов
                    result_file.write("Список запросов:\n")
                    for i, query in enumerate(valid_queries, 1):
                        result_file.write(f"{i}. {query}\n")

                    result_file.write("\nВозможные причины отсутствия результатов:\n")
                    result_file.write("1. Данные отсутствуют в базе\n")
                    result_file.write("2. Формат запроса неверный\n")
                    result_file.write("3. У найденных пользователей нет российских мобильных номеров\n")
                else:
                    has_results = True
                    # Сортируем запросы для удобства чтения
                    sorted_queries = sorted(query_phones_dict.keys())

                    for query in sorted_queries:
                        # Получаем все телефоны для этого запроса
                        all_phones = sorted(query_phones_dict[query])

                        # Фильтруем только российские мобильные номера
                        russian_mobiles = []
                        for phone in all_phones:
                            if phone and isinstance(phone, str) and phone.startswith('+79'):
                                russian_mobiles.append(phone)

                        # Если найдены российские мобильные - выводим их
                        if russian_mobiles:
                            # Записываем запрос/ФИО
                            result_file.write(f"{query}\n")

                            # Записываем только российские мобильные с отступом
                            for phone in russian_mobiles:
                                result_file.write(f" {phone}\n")

                            # Пустая строка между разными запросами для лучшей читаемости
                            result_file.write("\n")
                        else:
                            stats["skipped"] += 1

                    # Статистика и информация
                    successful_queries = sum(1 for q in query_phones_dict if any(
                        p.startswith('+79') for p in query_phones_dict[q] if isinstance(p, str)
                    ))

                    success_rate = round((successful_queries / len(valid_queries)) * 100, 1) if valid_queries else 0

                    result_file.write(f"\n====================================\n")
                    result_file.write(f"СТАТИСТИКА:\n")
                    result_file.write(
                        f"Запросов с найденными российскими номерами: {successful_queries}/{len(valid_queries)} ({success_rate}%)\n")

                    # Информация о запросах без результатов
                    if stats["skipped"] > 0:
                        result_file.write(f"\nЗапросы без российских мобильных номеров: {stats['skipped']}\n")
                        no_results_queries = [q for q in valid_queries if
                                              q not in query_phones_dict or
                                              not any(p.startswith('+79') for p in query_phones_dict[q] if
                                                      isinstance(p, str))]

                        for q in no_results_queries[:5]:  # Показываем первые 5 запросов без результатов
                            result_file.write(f" - {q}\n")

                        if len(no_results_queries) > 5:
                            result_file.write(f" ... и еще {len(no_results_queries) - 5} запросов\n")

            # 6.2 Сохранение полных данных в JSON
            with open(result_full_data_path, 'w', encoding='utf-8') as full_data_file:
                json.dump(results_dict, full_data_file, ensure_ascii=False, indent=2)

        except Exception as e:
            logging.error(f"Error saving results: {e}", exc_info=True)
            stats["errors"] += 1

        # Завершение массового поиска в пуле сессий
        try:
            await session_pool.finish_mass_search(self.mass_search_id)
        except Exception as e:
            logging.error(f"Error finishing mass search in session pool: {e}")

        # Обновляем статистику
        stats["phones_found"] = russian_mobile_count
        stats["duplicate_phones"] = total_raw_phones - russian_mobile_count
        stats["total_raw_phones"] = total_raw_phones
        stats["processing_time"] = round(time.time() - start_time, 2)
        stats["has_results"] = has_results

        # Завершаем статистику
        self.stats["end_time"] = time.time()

        logging.info(f"Mass search completed in {stats['processing_time']} seconds. "
                     f"Found {stats['phones_found']} Russian mobile phones.")

        return result_file_path, stats, results_dict

    # Methods for extracting and formatting phones
    def extract_phones(self, data, query_phones=None):
        """
        Enhanced phone extraction with better pattern recognition
        """
        if query_phones is None:
            query_phones = set()

        # Direct string check
        if isinstance(data, (str, int)) and str(data):
            phone_str = str(data).strip()
            digits_only = ''.join(c for c in phone_str if c.isdigit())

            # More flexible length detection
            if 10 <= len(digits_only) <= 15:
                formatted_phone = self.format_phone_number(phone_str)
                if formatted_phone:
                    query_phones.add(formatted_phone)
                return query_phones

        # Dictionary processing
        if isinstance(data, dict):
            for key, value in data.items():
                key_upper = str(key).upper() if isinstance(key, str) else ""

                # Expanded list of phone-related keys
                phone_keys = [
                    "ТЕЛЕФОН", "PHONE", "МОБИЛЬНЫЙ", "MOBILE", "КОНТАКТ",
                    "ТЕЛ", "TEL", "НОМЕР", "NUMBER", "CONTACT", "MOB",
                    "ТЕЛЕФОНЫ", "PHONES", "TELEPHONE", "РАБОЧИЙ ТЕЛЕФОН",
                    "СОТОВЫЙ", "МОБИЛЬНЫЕ", "CELL", "CONTACT_NUMBER"
                ]

                # Check for phone-related fields
                if any(phone_key in key_upper for phone_key in phone_keys):
                    if isinstance(value, list):
                        for phone in value:
                            self.extract_phones(phone, query_phones)
                    else:
                        # Try direct extraction
                        phone_value = self.extract_phone_from_value(value)
                        if phone_value:
                            query_phones.add(phone_value)

                # Always process values recursively
                self.extract_phones(value, query_phones)

        # List processing
        elif isinstance(data, list):
            for item in data:
                self.extract_phones(item, query_phones)

        return query_phones

    def extract_phone_from_value(self, value):
        """
        Extracts phone from various value formats including text with embedded phones
        """
        if not value:
            return None

        value_str = str(value)

        # Try regex patterns for various phone formats
        phone_patterns = [
            r'(?<!\d)(\+?7\d{10})(?!\d)',  # +7XXXXXXXXXX or 7XXXXXXXXXX
            r'(?<!\d)(8\d{10})(?!\d)',  # 8XXXXXXXXXX
            r'(?<!\d)(\d{10})(?!\d)',  # XXXXXXXXXX (10 digits)
            r'(?<!\d)(\+?\d{1,3}[\s\-\.]?\(?\d{3,4}\)?[\s\-\.]*\d{3}[\s\-\.]*\d{2}[\s\-\.]*\d{2})(?!\d)'
            # International
        ]

        for pattern in phone_patterns:
            matches = re.findall(pattern, value_str)
            if matches:
                for match in matches:
                    formatted = self.format_phone_number(match)
                    if formatted:
                        return formatted

        # Try direct digit extraction
        digits_only = ''.join(c for c in value_str if c.isdigit())
        if 10 <= len(digits_only) <= 15:
            formatted = self.format_phone_number(digits_only)
            if formatted:
                return formatted

        return None

    def extract_phones_from_text(self, text):
        """
        Searches for phone numbers in arbitrary text

        :param text: Text to analyze
        :return: Set of found mobile phones
        """
        candidate_phones = set()

        # Different phone formats
        patterns = [
            r'\+?[78][\d\s\(\)\-]{8,15}',  # +7/8 with any separators
            r'\d{3}[\s\-]?\d{3}[\s\-]?\d{4}',  # 999-999-9999
            r'\+?\d{1,4}[\s\-\(\)]+\d{3,4}[\s\-\(\)]+\d{3,4}[\s\-\(\)]*\d{0,4}',  # International format
            r'(?<!\d)\d{10}(?!\d)',  # Just 10 digits in a row
            r'(?<!\d)\d{11}(?!\d)'  # Just 11 digits in a row
        ]

        for pattern in patterns:
            matches = re.findall(pattern, text)
            for match in matches:
                # Check if this is a mobile number
                if self.is_valid_mobile_phone(match):
                    # Format to standard form +7XXXXXXXXXX
                    clean_phone = ''.join(c for c in match if c.isdigit() or c == '+')
                    if clean_phone.startswith('+'):
                        clean_phone = clean_phone[1:]
                    if clean_phone.startswith('8') and len(clean_phone) == 11:
                        clean_phone = '7' + clean_phone[1:]

                    candidate_phones.add(f"+{clean_phone}")

        return candidate_phones

    def format_phone_number(self, phone_str):
        """
        Форматирует телефонный номер к стандартному виду

        :param phone_str: Строка с телефонным номером
        :return: Отформатированный номер или None если невалидный
        """
        if not phone_str:
            return None

        # Только цифры (и потенциально + в начале)
        has_plus = str(phone_str).strip().startswith('+')
        digits_only = ''.join(c for c in str(phone_str) if c.isdigit())

        # Проверка российского мобильного формата
        if len(digits_only) == 10 and digits_only.startswith('9'):
            # Если начинается с 9 и длина 10 - добавляем код страны
            return f"+7{digits_only}"
        elif len(digits_only) == 11 and digits_only.startswith(('79', '89')):
            # Если начинается с 79 или 89 и длина 11 - нормализуем к формату +79
            return f"+7{digits_only[1:]}"

        # Для всех других случаев возвращаем исходный номер
        # Это номера которые не являются российскими мобильными
        if has_plus:
            return f"+{digits_only}"
        return f"+{digits_only}"

    def is_valid_mobile_phone(self, phone_str):
        """
        Проверяет, является ли номер российским мобильным (+79XXXXXXXXX)

        :param phone_str: Строка с телефонным номером
        :return: True если это российский мобильный номер (начинается с +79)
        """
        if not phone_str:
            return False

        # Очищаем номер от всего, кроме цифр и знака +
        clean_phone = ''.join(c for c in str(phone_str) if c.isdigit() or c == '+')

        # Проверяем формат российского мобильного (+79XXXXXXXXX)
        return clean_phone.startswith('+79') and len(clean_phone) == 12

    def format_result_for_phones(self, data):
        """
        Converts request results to text for phone search

        :param data: Request results (list of dictionaries)
        :return: Text representation of results
        """
        result_text = ""

        if not isinstance(data, (list, dict)):
            return str(data)

        if isinstance(data, list):
            for item in data:
                result_text += self.format_result_for_phones(item) + "\n"
        elif isinstance(data, dict):
            for key, value in data.items():
                # Emphasize fields that may contain phones
                if key.upper() in ["ТЕЛЕФОН", "ТЕЛ", "КОНТАКТ", "PHONE", "MOBILE", "CONTACT"]:
                    result_text += f"{key}: {value}\n"
                else:
                    # For other fields - just add to text
                    if isinstance(value, (list, dict)):
                        result_text += f"{key}:\n{self.format_result_for_phones(value)}\n"
                    else:
                        result_text += f"{key}: {value}\n"

        return result_text


# Handler for "Mass Search" button
@mass_search_router.callback_query(lambda c: c.data == "mass_search")
async def cb_mass_search(callback: CallbackQuery, state: FSMContext):
    """Handle 'Mass Search' button click"""
    user_id = callback.from_user.id
    if not check_active_session(user_id):
        await callback.answer("Вы не вошли в систему", show_alert=True)
        return

    # Check user's balance before continuing
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


# Handler for file upload
@mass_search_router.message(StateFilter(MassSearchStates.waiting_for_file), F.document)
async def process_file_upload(message: Message, state: FSMContext):
    """Process uploaded file for mass search"""
    user_id = message.from_user.id

    # Check file format
    if not message.document.file_name.endswith('.txt'):
        await message.answer("❌ Пожалуйста, загрузите файл в формате .txt")
        return

    # Check file size (5 MB = 5 * 1024 * 1024 bytes)
    if message.document.file_size > 5 * 1024 * 1024:
        await message.answer("❌ Размер файла превышает 5 МБ. Пожалуйста, загрузите файл меньшего размера.")
        return

    # Save file
    file_id = message.document.file_id
    file_path = f"static/uploads/{user_id}_{message.document.file_name}"
    os.makedirs(os.path.dirname(file_path), exist_ok=True)

    try:
        await message.bot.download(file=file_id, destination=file_path)
    except Exception as e:
        logging.error(f"Error downloading file: {e}")
        await message.answer(f"❌ Произошла ошибка при загрузке файла: {str(e)}")
        await state.clear()
        return

    # Count valid lines and cost
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

        # Check user's balance, using function from db.py
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

        # Save file information and cost in FSM
        await state.update_data(
            file_path=file_path,
            valid_lines=valid_lines,
            total_cost=required_amount
        )

        # Create keyboard with confirm/cancel buttons
        confirm_keyboard = InlineKeyboardMarkup(inline_keyboard=[
            [
                InlineKeyboardButton(text="✅ Подтверждаю", callback_data="confirm_mass_search"),
                InlineKeyboardButton(text="❌ Отмена", callback_data="cancel_mass_search")
            ]
        ])

        # Ask for confirmation with buttons instead of text
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
        logging.error(f"Error processing file: {e}")
        await message.answer(f"❌ Произошла ошибка при анализе файла: {str(e)}")
        await state.clear()


# Handler for mass search confirmation button
@mass_search_router.callback_query(lambda c: c.data == "confirm_mass_search",
                                   StateFilter(MassSearchStates.confirming_search))
async def process_confirm_button(callback: CallbackQuery, state: FSMContext):
    """Handle confirmation button press"""
    user_id = callback.from_user.id

    # Check if user is already in queue
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

    # Get data from FSM
    data = await state.get_data()
    file_path = data.get("file_path")
    valid_lines = data.get("valid_lines")
    total_cost = data.get("total_cost")

    # Deduct balance
    success, message_text, _ = batch_deduct_balance(user_id, valid_lines)

    if not success:
        await callback.answer("Ошибка списания средств: " + message_text, show_alert=True)
        # Don't reset state so user can try again
        # Add additional instruction message
        await callback.message.answer(
            "❌ Произошла ошибка при списании средств.\n"
            "Вы можете попробовать снова или нажать 'Отмена'."
        )
        return

    # Log mass search start
    log_id = log_mass_search_start(user_id, file_path, valid_lines, total_cost)

    # Remove inline keyboard from message
    await callback.message.edit_reply_markup(reply_markup=None)

    # Send message about being added to queue
    queue_message = await callback.message.answer(
        "🕒 <b>Ваш запрос добавляется в очередь на обработку...</b>\n\n"
        "Пожалуйста, ожидайте.",
        parse_mode="HTML"
    )

    # Add to queue
    position = await mass_search_queue.add_to_queue(
        user_id=user_id,
        message_id=queue_message.message_id,
        file_path=file_path,
        valid_lines=valid_lines,
        total_cost=total_cost
    )

    # Start task to update queue position message
    asyncio.create_task(
        update_queue_position_message(
            bot=callback.bot,
            user_id=user_id,
            message_id=queue_message.message_id
        )
    )

    # Answer callback and clear state
    await callback.answer(f"Вы добавлены в очередь на позицию {position}", show_alert=True)
    await state.clear()


# Handler for mass search cancellation button
@mass_search_router.callback_query(lambda c: c.data == "cancel_mass_search",
                                   StateFilter(MassSearchStates.confirming_search))
async def process_cancel_button(callback: CallbackQuery, state: FSMContext):
    """Handle cancel button press"""
    await callback.answer()
    await callback.message.answer("❌ Массовый пробив отменен.")

    # Remove inline keyboard from message
    await callback.message.edit_reply_markup(reply_markup=None)

    await state.clear()


async def update_queue_position_message(bot, user_id, message_id):
    """Updates message with queue position information"""
    try:
        while await mass_search_queue.is_user_in_queue(user_id):
            position = await mass_search_queue.get_position(user_id)
            queue_status = await mass_search_queue.get_queue_status()

            if position == 0 or await mass_search_queue.is_user_processing(user_id):
                # Request is being processed
                await bot.edit_message_text(
                    chat_id=user_id,
                    message_id=message_id,
                    text=f"⏳ <b>Ваш запрос обрабатывается...</b>\n\n"
                         f"Всего в очереди: {queue_status['waiting']} запросов\n"
                         f"Активных обработок: {queue_status['processing']}/{queue_status['capacity']}",
                    parse_mode="HTML"
                )
            else:
                # Request is in queue
                await bot.edit_message_text(
                    chat_id=user_id,
                    message_id=message_id,
                    text=f"🕒 <b>Вы в очереди на массовый пробив</b>\n\n"
                         f"Ваша позиция: {position} из {queue_status['waiting'] + queue_status['processing']}\n"
                         f"Активных обработок: {queue_status['processing']}/{queue_status['capacity']}\n\n"
                         f"Пожалуйста, дождитесь вашей очереди. Информация обновляется автоматически.",
                    parse_mode="HTML"
                )

            # Update every 5 seconds
            await asyncio.sleep(5)
    except Exception as e:
        logging.error(f"Error updating queue message: {e}")


# Mass search queue processor
async def process_mass_search_queue(bot):
    """
    Enhanced queue processor for mass searches with adaptive concurrency and improved error handling
    """
    # Declare global variable at the beginning of the function
    global mass_search_semaphore

    logging.info("Started mass search queue processor with optimized parallel processing")

    # Import session pool (avoid circular imports)
    from bot.session_manager import session_pool
    from bot.database.db import mass_refund_balance, update_mass_search_status

    # Track performance metrics
    processing_stats = {
        "total_processed": 0,
        "successful": 0,
        "failed": 0,
        "avg_processing_time": 0,
        "total_processing_time": 0
    }

    while True:
        try:
            # Calculate optimal concurrency based on active users & system load
            queue_status = await mass_search_queue.get_queue_status()
            active_users = len(await mass_search_queue.get_all_items())

            # Adaptive concurrency based on system load (Linux specific)
            try:
                system_load = os.getloadavg()[0]  # Get 1-minute load average

                # Adjust MAX_CONCURRENT_MASS_SEARCHES dynamically
                if system_load < 2.0 and active_users > 10:
                    effective_max = min(50, active_users + 5)  # More parallel jobs on low load
                elif system_load > 5.0:
                    effective_max = max(5, 15 - int(system_load))  # Reduce on high load
                else:
                    effective_max = MAX_CONCURRENT_MASS_SEARCHES  # Default

                # Update semaphore if needed
                if effective_max != mass_search_semaphore._value:
                    # Create new semaphore with updated value
                    mass_search_semaphore = asyncio.Semaphore(effective_max)
                    logging.info(f"Adjusted mass search concurrency to {effective_max} based on load")
            except Exception as e:
                # Non-Linux systems or other errors - use default
                logging.warning(f"Could not adjust concurrency dynamically: {e}")

            # Get next queue item
            queue_item = await mass_search_queue.get_next_item()

            if queue_item:
                user_id = queue_item.user_id
                file_path = queue_item.file_path
                valid_lines = queue_item.valid_lines
                total_cost = queue_item.total_cost
                message_id = queue_item.message_id
                process_start_time = time.time()

                # Check file existence
                if not os.path.exists(file_path):
                    logging.error(f"File not found: {file_path}")
                    await bot.send_message(
                        user_id,
                        "❌ Error: The file to process was not found. Please upload the file again."
                    )
                    await mass_search_queue.remove_item(user_id, success=False)
                    continue

                # Create a unique ID for this mass search process
                process_id = f"mass_{int(time.time())}_{random.randint(1000, 9999)}"
                log_id = None

                try:
                    # Acquire resources
                    active_user_searches[user_id] += 1
                    await mass_search_semaphore.acquire()

                    # Log in database
                    log_id = log_mass_search_start(user_id, file_path, valid_lines, total_cost)
                    if log_id:
                        update_mass_search_status(log_id, "processing")

                    # Send initial progress message
                    status_message = await bot.send_message(
                        user_id,
                        f"🔄 <b>Starting mass search (ID: {process_id})</b>\n\n"
                        f"File contains {valid_lines} queries\n"
                        f"Cost: ${total_cost:.2f}\n\n"
                        f"<code>[░░░░░░░░░░░░░░░░░░░░] 0%</code>\n\n"
                        f"⏳ <i>Please wait...</i>",
                        parse_mode="HTML"
                    )

                    # Allocate dedicated sessions for this mass search
                    if session_pool:
                        allocated_sessions = await session_pool.allocate_sessions_for_mass_search(
                            process_id,
                            requested_count=min(10, max(2, valid_lines // 20))  # Adaptive session count
                        )
                        logging.info(f"Allocated {allocated_sessions} sessions for mass search {process_id}")

                    # Create optimized processor with adaptive settings
                    processor = MassSearchProcessor(
                        max_concurrent=min(20, max(5, 100 // (active_users or 1))),  # Adaptive concurrency
                        min_request_interval=0.2,  # Even faster processing
                        max_request_interval=1.0,  # Lower upper bound
                        batch_size=min(30, max(10, valid_lines // 4))  # Adaptive batch size
                    )

                    # Process file with comprehensive error handling
                    try:
                        result_file_path, stats, results_dict = await processor.process_file(
                            file_path,
                            user_id,
                            session_pool,
                            bot,
                            status_message.message_id,
                            process_id=process_id  # Pass ID for resource tracking
                        )

                        # Update processing statistics
                        processing_time = time.time() - process_start_time
                        processing_stats["total_processed"] += 1
                        processing_stats["successful"] += 1
                        processing_stats["total_processing_time"] += processing_time
                        processing_stats["avg_processing_time"] = (
                                processing_stats["total_processing_time"] /
                                processing_stats["total_processed"]
                        )

                        # Log success
                        phones_found = stats.get('phones_found', 0)
                        logging.info(
                            f"Mass search completed for user {user_id}: "
                            f"found {phones_found} phones in {processing_time:.2f}s"
                        )

                        # Update database
                        if log_id:
                            update_mass_search_status(
                                log_id,
                                "completed",
                                results_file=result_file_path,
                                phones_found=stats.get('phones_found', 0),
                                error_message=None
                            )

                        # Final progress update
                        await bot.edit_message_text(
                            chat_id=user_id,
                            message_id=status_message.message_id,
                            text=f"✅ <b>Mass search completed!</b>\n\n"
                                 f"📊 <b>Statistics:</b>\n"
                                 f"• Total lines: {stats['total_lines']}\n"
                                 f"• Valid queries: {stats['valid_lines']}\n"
                                 f"• Phones found: {stats['phones_found']}\n"
                                 f"• Processing time: {stats['processing_time']:.2f} sec\n\n"
                                 f"<code>[{'█' * 20}] 100%</code>",
                            parse_mode="HTML"
                        )

                        # Send results file if available
                        if result_file_path and os.path.exists(result_file_path) and os.path.getsize(
                                result_file_path) > 0:
                            # Choose message based on results
                            if stats.get("phones_found", 0) > 0:
                                result_message = f"📎 Results are in the file below:"
                            else:
                                result_message = f"📎 Query verification report in the file below:"

                            # Send message and file
                            await bot.send_message(user_id, result_message, parse_mode="HTML")
                            await bot.send_document(user_id, FSInputFile(result_file_path))
                        else:
                            # Report file creation issues
                            await bot.send_message(
                                user_id,
                                "⚠️ Could not create results file. Please contact support."
                            )

                            if log_id:
                                update_mass_search_status(log_id, "failed",
                                                          error_message="Results file creation failed")

                    except Exception as file_error:
                        # Comprehensive error handling
                        processing_stats["failed"] += 1
                        error_traceback = traceback.format_exc()
                        logging.error(
                            f"Error processing file for user {user_id}: {file_error}\n{error_traceback}"
                        )

                        # Refund user
                        refund_success, refund_message = await mass_refund_balance(user_id, valid_lines)

                        # Update database status
                        if log_id:
                            update_mass_search_status(
                                log_id,
                                "failed",
                                error_message=f"{str(file_error)[:200]}"
                            )

                        # Notify user with helpful error message
                        error_message = str(file_error)
                        if len(error_message) > 100:
                            error_message = error_message[:100] + "..."

                        await bot.send_message(
                            user_id,
                            f"❌ <b>Error processing your file:</b>\n{error_message}\n\n"
                            f"{refund_message}\n\n"
                            f"If this problem persists, please contact support.",
                            parse_mode="HTML"
                        )

                except asyncio.CancelledError:
                    # Handle task cancellation
                    logging.warning(f"Mass search task for user {user_id} was cancelled")
                    processing_stats["failed"] += 1

                    # Update database
                    if log_id:
                        update_mass_search_status(log_id, "failed", error_message="Task cancelled")

                    # Refund and notify user
                    refund_success, refund_message = await mass_refund_balance(user_id, valid_lines)
                    await bot.send_message(
                        user_id,
                        "❌ Task was cancelled. Funds have been returned to your balance."
                    )

                except Exception as e:
                    # Handle other unexpected errors
                    processing_stats["failed"] += 1
                    error_traceback = traceback.format_exc()
                    logging.error(
                        f"Unexpected error in mass search for user {user_id}: {e}\n{error_traceback}"
                    )

                    # Update database
                    if log_id:
                        update_mass_search_status(log_id, "failed", error_message=f"Unexpected error: {str(e)[:200]}")

                    # Refund and notify user
                    refund_success, refund_message = await mass_refund_balance(user_id, valid_lines)
                    await bot.send_message(
                        user_id,
                        f"❌ <b>An unexpected error occurred:</b>\n{str(e)}\n\n{refund_message}",
                        parse_mode="HTML"
                    )

                finally:
                    # Always release resources
                    active_user_searches[user_id] = max(0, active_user_searches[user_id] - 1)
                    mass_search_semaphore.release()

                    # Release allocated sessions
                    if session_pool:
                        await session_pool.finish_mass_search(process_id)

                    # Remove from queue
                    await mass_search_queue.remove_item(user_id)

                    # Log completion
                    logging.info(
                        f"Mass search job completed for user {user_id}. "
                        f"Queue stats: processing={queue_status['processing']}, waiting={queue_status['waiting']}"
                    )

            # Check queue every second
            await asyncio.sleep(1)

        except Exception as e:
            # Handle errors in the queue processing loop
            error_traceback = traceback.format_exc()
            logging.error(f"Error in mass search queue processor: {e}\n{error_traceback}")
            await asyncio.sleep(5)  # Wait longer on errors