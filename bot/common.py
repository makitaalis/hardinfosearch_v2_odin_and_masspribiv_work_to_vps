# bot/common.py
import asyncio
from collections import defaultdict, deque
from dataclasses import dataclass
from datetime import datetime
import logging
import time
from typing import Dict, Optional, List

# Existing configuration variables
MAX_CONCURRENT_MASS_SEARCHES = 50  # Maximum parallel mass searches
mass_search_semaphore = asyncio.Semaphore(MAX_CONCURRENT_MASS_SEARCHES)
active_user_searches = defaultdict(int)  # Track active searches per user
MAX_USER_SEARCHES = 1  # Maximum simultaneous searches per user


# Enhanced queue item class
@dataclass
class QueueItem:
    """Enhanced queue item with additional tracking fields"""
    user_id: int
    message_id: int
    queue_time: datetime
    file_path: str
    valid_lines: int
    total_cost: float
    processing: bool = False
    start_time: Optional[datetime] = None
    attempts: int = 0
    last_status_update: Optional[datetime] = None
    priority_boost: float = 1.0  # Multiplier for priority calculation


# Enhanced queue management class
class MassSearchQueue:
    """Enhanced queue management for mass searches with fair scheduling"""

    def __init__(self):
        self.queue = deque()  # Queue items
        self.processing_users = set()  # Users with active processing
        self.lock = asyncio.Lock()  # Thread safety lock

        # Fair scheduling enhancements
        self.last_user_processing_time = defaultdict(float)
        self.user_priority_factor = defaultdict(lambda: 1.0)
        self.user_completion_stats = defaultdict(lambda: {"total": 0, "success": 0})

        # Performance tracking
        self.stats = {
            "total_enqueued": 0,
            "total_processed": 0,
            "avg_wait_time": 0,
            "avg_processing_time": 0,
        }

    async def add_to_queue(self, user_id: int, message_id: int, file_path: str,
                           valid_lines: int, total_cost: float) -> int:
        """Enhanced queue addition with better duplicate detection"""
        async with self.lock:
            # Check if user already has a non-processing request
            for item in self.queue:
                if item.user_id == user_id and not item.processing:
                    # Update stats
                    self.stats["total_enqueued"] += 1
                    return self._get_position(user_id)

            # Add to queue with timestamp
            item = QueueItem(
                user_id=user_id,
                message_id=message_id,
                queue_time=datetime.now(),
                file_path=file_path,
                valid_lines=valid_lines,
                total_cost=total_cost
            )
            self.queue.append(item)

            # Update stats
            self.stats["total_enqueued"] += 1

            return self._get_position(user_id)

    def _get_position(self, user_id: int) -> int:
        """Get user's position in queue (1-based)"""
        position = 1
        for item in self.queue:
            if item.user_id == user_id:
                if item.processing:
                    return 0  # Processing (0 = active)
                return position
            if not item.processing:
                position += 1
        return 0  # Not found

    async def get_position(self, user_id: int) -> int:
        """Thread-safe queue position check"""
        async with self.lock:
            return self._get_position(user_id)

    async def get_queue_status(self) -> Dict:
        """Get comprehensive queue statistics"""
        async with self.lock:
            total = len(self.queue)
            processing = sum(1 for item in self.queue if item.processing)
            waiting = total - processing

            # Calculate average wait time
            current_time = datetime.now()
            if waiting > 0:
                wait_times = [(current_time - item.queue_time).total_seconds()
                              for item in self.queue if not item.processing]
                avg_wait = sum(wait_times) / len(wait_times) if wait_times else 0
            else:
                avg_wait = 0

            return {
                "total": total,
                "processing": processing,
                "waiting": waiting,
                "capacity": MAX_CONCURRENT_MASS_SEARCHES,
                "avg_wait_time": avg_wait
            }

    async def get_next_item(self):
        """Get next item using optimized fair scheduling algorithm"""
        async with self.lock:
            # Check active processing count
            active_count = sum(1 for item in self.queue if item.processing)
            if active_count >= MAX_CONCURRENT_MASS_SEARCHES:
                return None

            # Group by user to enforce fairness
            user_items = defaultdict(list)
            for item in self.queue:
                if not item.processing:
                    user_items[item.user_id].append(item)

            # No eligible items
            if not user_items:
                return None

            # Select user with highest priority fairly
            user_priorities = []
            current_time = time.time()

            for user_id, items in user_items.items():
                # Calculate wait time
                oldest_item = min(items, key=lambda x: x.queue_time.timestamp())
                wait_time = (current_time - oldest_item.queue_time.timestamp()) / 60.0

                # Last processing time factor (wait longer = higher priority)
                last_process_time = current_time - self.last_user_processing_time.get(user_id, 0)
                last_process_factor = min(5.0, max(1.0, last_process_time / 60.0))

                # Calculate user priority
                priority = (
                                   wait_time * 3.0 +
                                   last_process_factor * 2.0 +
                                   (1.0 / max(1, len(items)))  # Users with fewer items get higher priority
                           ) * self.user_priority_factor[user_id]

                user_priorities.append((user_id, priority))

            # Sort by priority (highest first)
            user_priorities.sort(key=lambda x: -x[1])
            selected_user_id = user_priorities[0][0]

            # Get user's first item
            selected_item = user_items[selected_user_id][0]
            selected_item.processing = True
            self.processing_users.add(selected_user_id)
            self.last_user_processing_time[selected_user_id] = current_time

            # Adjust priority factor
            self.user_priority_factor[selected_user_id] *= 0.9

            return selected_item

    async def remove_item(self, user_id: int, success: bool = True) -> bool:
        """Remove item with success tracking"""
        async with self.lock:
            for i, item in enumerate(list(self.queue)):
                if item.user_id == user_id:
                    # Update statistics
                    self.user_completion_stats[user_id]["total"] += 1
                    if success:
                        self.user_completion_stats[user_id]["success"] += 1

                    # Remove from queue
                    self.queue.remove(item)
                    if user_id in self.processing_users:
                        self.processing_users.remove(user_id)

                    # Reset priority factor on successful completion
                    if success:
                        self.user_priority_factor[user_id] = 1.0

                    self.stats["total_processed"] += 1
                    return True
            return False

    async def is_user_in_queue(self, user_id: int) -> bool:
        """Check if user has any items in queue"""
        async with self.lock:
            return any(item.user_id == user_id for item in self.queue)

    async def is_user_processing(self, user_id: int) -> bool:
        """Check if user has an item being processed"""
        async with self.lock:
            return user_id in self.processing_users

    async def get_all_items(self) -> List[QueueItem]:
        """Get all queue items (for admin view)"""
        async with self.lock:
            return list(self.queue)


# Global queue instance
mass_search_queue = MassSearchQueue()