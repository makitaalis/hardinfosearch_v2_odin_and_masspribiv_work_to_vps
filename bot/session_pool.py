import asyncio
import logging
import random
from datetime import datetime, timedelta
from typing import Dict, List, Tuple, Optional, Set
import time

from bot.web_session import SauronWebSession


# In session_pool.py
class SessionPoolManager:
    def __init__(self, credentials_list, max_sessions=50):
        # Increase maximum sessions and implement dynamic scaling
        self.max_sessions = max_sessions
        self.min_sessions_per_mass_search = 3  # Guarantee at least 3 sessions per mass search
        self.dynamic_scaling_enabled = True

        # Create more session slots than credentials through rotation
        expanded_credentials = []
        for i in range(3):  # Triple the effective session count
            for cred in credentials_list:
                expanded_credentials.append((cred[0], cred[1], f"{i + 1}"))

        # Initialize with expanded credentials
        self.sessions = []
        self.session_lock = asyncio.Lock()

        for i, (login, password, suffix) in enumerate(expanded_credentials):
            session_id = f"{i + 1}_{suffix}"
            session = SauronWebSession(login, password, session_id)
            self.sessions.append(session)

        self.stats = {
            "total_searches": 0,
            "successful_searches": 0,
            "failed_searches": 0,
            "reauth_count": 0,
            "mass_searches": 0,
            "single_searches": 0,
            "session_wait_time": 0,
        }

        # Ограничиваем количество сессий
        max_available = min(len(credentials_list), max_sessions)

        # Инициализируем сессии
        for i in range(max_available):
            login, password = credentials_list[i % len(credentials_list)]  # Используем доступные креды циклически
            session = SauronWebSession(login, password, i + 1)
            self.sessions.append(session)

        logging.info(f"Initialized session pool with {len(self.sessions)} sessions")

    async def initialize_sessions(self):
        """Инициализирует все сессии (авторизует их)"""
        init_tasks = []

        # Создаем задачи авторизации для каждой сессии
        for session in self.sessions:
            task = asyncio.create_task(session.authenticate())
            init_tasks.append(task)

        # Ждем завершения всех задач
        results = await asyncio.gather(*init_tasks, return_exceptions=True)

        # Считаем успешные авторизации
        success_count = sum(1 for result in results if result is True)
        fail_count = len(self.sessions) - success_count

        logging.info(f"Session pool initialization completed: {success_count} successful, {fail_count} failed")
        return success_count, fail_count

    async def get_available_session(self, is_mass_search=False, mass_search_id=None, timeout=30):
        """
        Улучшенный метод получения доступной сессии с приоритетами
        """
        start_time = time.time()

        # Если это массовый пробив, регистрируем его
        if is_mass_search and mass_search_id:
            async with self.mass_search_lock:
                self.active_mass_searches.add(mass_search_id)
                if mass_search_id not in self.mass_search_session_map:
                    self.mass_search_session_map[mass_search_id] = set()

        # Для массового пробива - добавляем задержку ПЕРЕД получением сессии
        # Это позволит одиночным запросам использовать сессии в это время
        if is_mass_search:
            delay = random.uniform(1.0, 4.0)  # Задержка 1-4 секунды как требуется
            await asyncio.sleep(delay)

        # Пытаемся получить сессию в течение указанного таймаута
        while time.time() - start_time < timeout:
            async with self.session_lock:
                # Сначала проверяем доступные авторизованные сессии
                available_sessions = [s for s in self.sessions if not s.is_busy and s.is_authenticated]

                if available_sessions:
                    # Стратегия выбора сессии зависит от типа запроса
                    if is_mass_search:
                        # Для массового пробива пытаемся сохранить афинность сессий
                        if mass_search_id in self.mass_search_session_map:
                            # Ищем сессии, которые уже использовались этим пробивом
                            used_sessions = [s for s in available_sessions
                                             if s.session_id in self.mass_search_session_map[mass_search_id]]
                            if used_sessions:
                                # Предпочитаем использовать те же сессии, но с балансировкой нагрузки
                                session = min(used_sessions, key=lambda s: s.search_count)
                                session.is_busy = True
                                self.mass_search_session_map[mass_search_id].add(session.session_id)
                                self.stats["mass_searches"] += 1
                                return session

                        # Если нет афинности или свободных сессий из афинного пула, выбираем новую
                        # Используем стратегию наименьшей загрузки + случайность для лучшего распределения
                        least_loaded = sorted(available_sessions, key=lambda s: s.search_count)
                        if len(least_loaded) > 5:
                            # Выбираем из топ-5 наименее загруженных сессий
                            session = random.choice(least_loaded[:5])
                        else:
                            session = random.choice(least_loaded)

                        session.is_busy = True

                        # Регистрируем сессию в массовом пробиве
                        if mass_search_id:
                            self.mass_search_session_map[mass_search_id].add(session.session_id)

                        self.stats["mass_searches"] += 1
                        return session
                    else:
                        # Для одиночного запроса - приоритет над массовыми пробивами
                        # Исключаем сессии, используемые массовыми пробивами, если есть другие свободные
                        mass_search_sessions = set()
                        for sessions in self.mass_search_session_map.values():
                            mass_search_sessions.update(sessions)

                        # Приоритет сессиям, не используемым массовыми пробивами
                        free_sessions = [s for s in available_sessions if s.session_id not in mass_search_sessions]

                        if free_sessions:
                            # Если есть свободные сессии, не используемые массовыми пробивами
                            session = min(free_sessions, key=lambda s: s.search_count)
                        else:
                            # Иначе используем любую доступную сессию
                            session = min(available_sessions, key=lambda s: s.search_count)

                        session.is_busy = True
                        self.stats["single_searches"] += 1
                        return session

                # Если нет авторизованных, пытаемся найти свободную неавторизованную
                available_sessions = [s for s in self.sessions if not s.is_busy]
                if available_sessions:
                    # Выбираем сессию с наименьшим количеством ошибок
                    session = min(available_sessions, key=lambda s: s.error_count)
                    session.is_busy = True
                    return session

            # Если все сессии заняты, ждем и повторяем
            wait_time = min(0.5, (timeout - (time.time() - start_time)) / 2)
            if wait_time > 0:
                self.stats["session_wait_time"] += wait_time
                await asyncio.sleep(wait_time)
            else:
                break

        # Если превышен таймаут
        logging.warning(f"Timeout waiting for session: {timeout}s exceeded")
        return None

    async def release_session(self, session, is_mass_search=False, mass_search_id=None):
        """
        Оптимизированное освобождение сессии после использования
        """
        if session not in self.sessions:
            return

        # Не добавляем задержку после запроса для массовых пробивов
        # Задержка теперь добавляется ПЕРЕД получением сессии

        # Помечаем сессию как свободную
        session.is_busy = False
        session.last_activity = datetime.now()

        # Если нужно, удаляем связь с массовым пробивом
        if is_mass_search and mass_search_id:
            async with self.mass_search_lock:
                if mass_search_id in self.mass_search_session_map:
                    if session.session_id in self.mass_search_session_map[mass_search_id]:
                        self.mass_search_session_map[mass_search_id].remove(session.session_id)

    async def finish_mass_search(self, mass_search_id):
        """
        Завершает массовый пробив и освобождает ресурсы

        :param mass_search_id: ID массового пробива
        """
        async with self.mass_search_lock:
            if mass_search_id in self.active_mass_searches:
                self.active_mass_searches.remove(mass_search_id)

            if mass_search_id in self.mass_search_session_map:
                del self.mass_search_session_map[mass_search_id]

    async def refresh_session(self, session):
        """
        Обновляет авторизацию сессии

        :param session: Объект сессии для обновления
        :return: True если успешно, иначе False
        """
        if session in self.sessions and not session.is_busy:
            session.is_busy = True
            try:
                self.stats["reauth_count"] += 1
                result = await session.authenticate()
                return result
            finally:
                session.is_busy = False
        return False

    async def refresh_expired_sessions(self, max_age_hours=2):
        """
        Обновляет все сессии, которые не использовались более указанного времени

        :param max_age_hours: Максимальный возраст сессии в часах
        """
        refresh_tasks = []
        now = datetime.now()

        async with self.session_lock:
            for session in self.sessions:
                if session.is_busy:
                    continue

                age = now - session.last_activity
                if age > timedelta(hours=max_age_hours) or not session.is_authenticated:
                    session.is_busy = True
                    refresh_tasks.append(session)

        # Обновляем сессии последовательно, чтобы не перегружать сервер
        for session in refresh_tasks:
            try:
                result = await session.authenticate()
                if result:
                    logging.info(f"Successfully refreshed session {session.session_id}")
                else:
                    logging.warning(f"Failed to refresh session {session.session_id}")
            except Exception as e:
                logging.error(f"Error refreshing session {session.session_id}: {e}")
            finally:
                session.is_busy = False

        if refresh_tasks:
            logging.info(f"Refreshed {len(refresh_tasks)} expired sessions")

    async def perform_search(self, query, is_mass_search=False, mass_search_id=None):
        """
        Выполняет поиск через доступную сессию с учетом приоритетов

        :param query: Строка запроса
        :param is_mass_search: Флаг массового пробива
        :param mass_search_id: ID массового пробива
        :return: (success, result)
        """
        self.stats["total_searches"] += 1

        # Получаем доступную сессию
        session = await self.get_available_session(is_mass_search, mass_search_id)
        if not session:
            logging.error("Failed to get available session for search")
            self.stats["failed_searches"] += 1
            return False, "Не удалось получить доступную сессию для поиска"

        try:
            # Если сессия не авторизована, авторизуем
            if not session.is_authenticated:
                auth_success = await session.authenticate()
                if not auth_success:
                    self.stats["failed_searches"] += 1
                    return False, "Не удалось авторизоваться для выполнения поиска"

            # Выполняем поиск
            success, result = await session.search(query)

            if success:
                # Если поиск успешен, парсим результаты
                parsed_data = await session.parse_results(result)
                self.stats["successful_searches"] += 1
                return True, parsed_data
            else:
                # Если ошибка связана с авторизацией, помечаем сессию как неавторизованную
                if "авториз" in result.lower() or "session" in result.lower():
                    session.is_authenticated = False

                self.stats["failed_searches"] += 1
                return False, result
        except Exception as e:
            self.stats["failed_searches"] += 1
            logging.error(f"Error during search: {str(e)}")
            return False, f"Ошибка при выполнении поиска: {str(e)}"
        finally:
            # Освобождаем сессию
            await self.release_session(session, is_mass_search, mass_search_id)

    def get_stats(self):
        """Возвращает статистику работы пула сессий"""
        sessions_stats = [session.get_stats() for session in self.sessions]

        active_sessions = sum(1 for s in self.sessions if s.is_authenticated)
        busy_sessions = sum(1 for s in self.sessions if s.is_busy)

        return {
            "total_sessions": len(self.sessions),
            "active_sessions": active_sessions,
            "busy_sessions": busy_sessions,
            "active_mass_searches": len(self.active_mass_searches),
            "searches": self.stats,
            "sessions": sessions_stats
        }