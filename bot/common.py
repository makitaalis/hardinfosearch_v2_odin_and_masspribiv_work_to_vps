# bot/common.py
import asyncio
from collections import defaultdict, deque
from dataclasses import dataclass
from datetime import datetime
import logging
from typing import Dict, Optional, List

# Существующие переменные
MAX_CONCURRENT_MASS_SEARCHES = 15  # Максимум параллельных массовых пробивов
mass_search_semaphore = asyncio.Semaphore(MAX_CONCURRENT_MASS_SEARCHES)
active_user_searches = defaultdict(int)  # Отслеживание активных пробивов по пользователю
MAX_USER_SEARCHES = 1  # Максимум одновременных пробивов для одного пользователя


# Новый класс для элемента очереди
@dataclass
class QueueItem:
    """Элемент очереди массового пробива"""
    user_id: int
    message_id: int
    queue_time: datetime
    file_path: str
    valid_lines: int
    total_cost: float
    processing: bool = False


# Класс управления очередью
class MassSearchQueue:
    """Класс управления очередью массового пробива"""

    def __init__(self):
        self.queue = deque()  # Двусторонняя очередь элементов
        self.processing_users = set()  # Множество ID пользователей в обработке
        self.lock = asyncio.Lock()  # Блокировка для потокобезопасности

    async def add_to_queue(self, user_id: int, message_id: int, file_path: str,
                           valid_lines: int, total_cost: float) -> int:
        """Добавляет запрос в очередь и возвращает позицию"""
        async with self.lock:
            # Проверяем, не в очереди ли уже пользователь
            for item in self.queue:
                if item.user_id == user_id and not item.processing:
                    return self._get_position(user_id)

            # Добавляем в очередь
            item = QueueItem(
                user_id=user_id,
                message_id=message_id,
                queue_time=datetime.now(),
                file_path=file_path,
                valid_lines=valid_lines,
                total_cost=total_cost
            )
            self.queue.append(item)
            return self._get_position(user_id)

    def _get_position(self, user_id: int) -> int:
        """Возвращает позицию в очереди (начиная с 1)"""
        position = 1
        for item in self.queue:
            if item.user_id == user_id:
                if item.processing:
                    return 0  # Обрабатывается (0 = активен)
                return position
            if not item.processing:
                position += 1
        return 0  # Не найден

    async def get_position(self, user_id: int) -> int:
        """Thread-safe получение позиции в очереди"""
        async with self.lock:
            return self._get_position(user_id)

    async def get_queue_status(self) -> Dict:
        """Возвращает статистику очереди"""
        async with self.lock:
            total = len(self.queue)
            processing = sum(1 for item in self.queue if item.processing)
            waiting = total - processing
            return {
                "total": total,
                "processing": processing,
                "waiting": waiting,
                "capacity": MAX_CONCURRENT_MASS_SEARCHES
            }

    async def get_next_item(self) -> Optional[QueueItem]:
        """Получает следующий элемент для обработки"""
        async with self.lock:
            # Проверяем количество активных обработок
            active_count = sum(1 for item in self.queue if item.processing)
            if active_count >= MAX_CONCURRENT_MASS_SEARCHES:
                return None

            # Ищем первый элемент в ожидании
            for item in self.queue:
                if not item.processing and item.user_id not in self.processing_users:
                    item.processing = True
                    self.processing_users.add(item.user_id)
                    return item

            return None  # Нет подходящих элементов

    async def remove_item(self, user_id: int) -> bool:
        """Удаляет элемент из очереди"""
        async with self.lock:
            for i, item in enumerate(list(self.queue)):
                if item.user_id == user_id:
                    self.queue.remove(item)
                    if user_id in self.processing_users:
                        self.processing_users.remove(user_id)
                    return True
            return False

    async def is_user_in_queue(self, user_id: int) -> bool:
        """Проверяет, находится ли пользователь в очереди"""
        async with self.lock:
            return any(item.user_id == user_id for item in self.queue)

    async def is_user_processing(self, user_id: int) -> bool:
        """Проверяет, обрабатывается ли запрос пользователя"""
        async with self.lock:
            return user_id in self.processing_users

    async def get_all_items(self) -> List[QueueItem]:
        """Возвращает копию всех элементов в очереди для админа"""
        async with self.lock:
            return list(self.queue)


# Создаем глобальный экземпляр очереди
mass_search_queue = MassSearchQueue()