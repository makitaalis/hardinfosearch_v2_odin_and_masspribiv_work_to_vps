import asyncio
import logging
import random
import time
from datetime import datetime, timedelta
from typing import Dict, List, Tuple, Optional, Set
import json

from bot.web_session import SauronWebSession


class SessionPoolManager:
    """Optimized session pool manager with improved parallelism support"""

    def __init__(self, credentials_list, max_sessions=50):
        """
        Enhanced initialization with support for more concurrent sessions
        """
        self.max_sessions = max_sessions
        self.min_sessions_per_mass_search = 3  # Guarantee minimum sessions per search
        self.dynamic_scaling_enabled = True
        self.sessions = []
        self.session_lock = asyncio.Lock()
        self.mass_search_lock = asyncio.Lock()
        self.active_mass_searches = set()
        self.mass_search_session_map = {}  # Map of mass search ID -> session IDs

        # Expand credentials to create more virtual sessions

        # Calculate sessions per credential to reach desired total
        sessions_per_credential = max(1, max_sessions // len(credentials_list))
        expanded_credentials = []
        for cred in credentials_list:
            for i in range(sessions_per_credential):
                expanded_credentials.append((cred[0], cred[1], f"{i + 1}"))

        # Initialize expanded session pool
        actual_sessions = min(max_sessions, len(expanded_credentials))
        for i in range(actual_sessions):
            login, password, suffix = expanded_credentials[i % len(expanded_credentials)]
            session_id = f"{i + 1}_{suffix}"
            session = SauronWebSession(login, password, session_id)
            self.sessions.append(session)

        logging.info(
            f"Initialized session pool with {len(self.sessions)} virtual sessions based on {len(credentials_list)} credentials")

        # Statistics initialization
        self.stats = {
            "total_searches": 0,
            "successful_searches": 0,
            "failed_searches": 0,
            "reauth_count": 0,
            "mass_searches": 0,
            "single_searches": 0,
            "session_wait_time": 0,
        }

    async def allocate_sessions_for_mass_search(self, mass_search_id, requested_count=5):
        """
        Allocates a dedicated group of sessions for a mass search job with fair distribution
        """
        async with self.session_lock:
            # Register this mass search
            async with self.mass_search_lock:
                self.active_mass_searches.add(mass_search_id)
                if mass_search_id not in self.mass_search_session_map:
                    self.mass_search_session_map[mass_search_id] = set()

            # Calculate fair allocation based on active searches
            active_search_count = len(self.active_mass_searches)
            available_sessions = [s for s in self.sessions if not s.is_busy and s.is_authenticated]

            # Ensure at least minimum sessions per search, but not more than fair share
            min_sessions = max(2, min(len(available_sessions) // max(1, active_search_count), 5))
            allocated_count = min(requested_count, min_sessions)

            # Select least used sessions
            available_sessions.sort(key=lambda s: s.search_count)
            allocated_sessions = available_sessions[:allocated_count]

            # Register sessions with this mass search
            for session in allocated_sessions:
                self.mass_search_session_map[mass_search_id].add(session.session_id)

            return len(allocated_sessions)

    async def get_available_session(self, is_mass_search=False, mass_search_id=None, timeout=30):
        """
        Enhanced session acquisition with priority for mass searches that have allocated sessions
        """
        start_time = time.time()

        # For mass search, use a shorter delay to increase throughput
        if is_mass_search:
            delay = random.uniform(0.3, 0.8)  # Reduced delay for mass searches
            await asyncio.sleep(delay)

        # Try to get a session within the timeout period
        while time.time() - start_time < timeout:
            async with self.session_lock:
                # Available authenticated sessions
                available_sessions = [s for s in self.sessions if not s.is_busy and s.is_authenticated]

                if available_sessions:
                    if is_mass_search and mass_search_id:
                        # For mass search, prefer sessions already allocated to this mass_search_id
                        allocated_sessions = []
                        if mass_search_id in self.mass_search_session_map:
                            allocated_ids = self.mass_search_session_map[mass_search_id]
                            allocated_sessions = [s for s in available_sessions if s.session_id in allocated_ids]

                        if allocated_sessions:
                            # Use allocated session with least searches
                            session = min(allocated_sessions, key=lambda s: s.search_count)
                        else:
                            # If no allocated sessions available, use any available session
                            session = min(available_sessions, key=lambda s: s.search_count)

                        session.is_busy = True
                        self.stats["mass_searches"] += 1
                        return session
                    else:
                        # For single search, prioritize sessions not used by mass searches
                        mass_search_sessions = set()
                        for sessions in self.mass_search_session_map.values():
                            mass_search_sessions.update(sessions)

                        # Try to find sessions not allocated to mass searches
                        free_sessions = [s for s in available_sessions if s.session_id not in mass_search_sessions]

                        if free_sessions:
                            session = min(free_sessions, key=lambda s: s.search_count)
                        else:
                            # If all sessions are allocated, use least loaded session
                            session = min(available_sessions, key=lambda s: s.search_count)

                        session.is_busy = True
                        self.stats["single_searches"] += 1
                        return session

                # If no authenticated sessions, try to find an unauthenticated one
                available_sessions = [s for s in self.sessions if not s.is_busy]
                if available_sessions:
                    session = min(available_sessions, key=lambda s: s.error_count)
                    session.is_busy = True
                    return session

            # Wait and retry
            wait_time = min(0.5, (timeout - (time.time() - start_time)) / 3)
            if wait_time > 0:
                self.stats["session_wait_time"] += wait_time
                await asyncio.sleep(wait_time)
            else:
                break

        logging.warning(f"Timeout waiting for session: {timeout}s exceeded")
        return None

    async def release_session(self, session, is_mass_search=False, mass_search_id=None):
        """
        Optimized session release without unnecessary delays
        """
        if session not in self.sessions:
            return

        # Mark session as not busy
        session.is_busy = False
        session.last_activity = datetime.now()

        # Update mass search session map if needed
        if is_mass_search and mass_search_id:
            async with self.mass_search_lock:
                if mass_search_id in self.mass_search_session_map:
                    # We don't remove the session from the map here
                    # This helps maintain session affinity for this mass search
                    pass

    async def finish_mass_search(self, mass_search_id):
        """
        Finalize a mass search job and release resources
        """
        async with self.mass_search_lock:
            if mass_search_id in self.active_mass_searches:
                self.active_mass_searches.remove(mass_search_id)

            if mass_search_id in self.mass_search_session_map:
                del self.mass_search_session_map[mass_search_id]

    async def refresh_session(self, session):
        """
        Refresh session authentication
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
        Refresh sessions that haven't been used recently
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

        # Refresh sessions one at a time to avoid overwhelming the server
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
        Execute a search query using an available session
        """
        self.stats["total_searches"] += 1

        # Get an available session
        session = await self.get_available_session(is_mass_search, mass_search_id)
        if not session:
            logging.error("Failed to get available session for search")
            self.stats["failed_searches"] += 1
            return False, "No available session for search"

        try:
            # Ensure session is authenticated
            if not session.is_authenticated:
                auth_success = await session.authenticate()
                if not auth_success:
                    self.stats["failed_searches"] += 1
                    return False, "Authentication failed before search"

            # Perform the search
            success, result = await session.search(query)

            if success:
                # Parse the search results
                parsed_data = await session.parse_results(result)
                self.stats["successful_searches"] += 1
                return True, parsed_data
            else:
                # Handle authentication-related errors
                if "авториз" in result.lower() or "session" in result.lower():
                    session.is_authenticated = False

                self.stats["failed_searches"] += 1
                return False, result
        except Exception as e:
            self.stats["failed_searches"] += 1
            logging.error(f"Error during search: {str(e)}")
            return False, f"Search error: {str(e)}"
        finally:
            # Release the session
            await self.release_session(session, is_mass_search, mass_search_id)

    def get_stats(self):
        """
        Return session pool statistics
        """
        active_sessions = sum(1 for s in self.sessions if s.is_authenticated)
        busy_sessions = sum(1 for s in self.sessions if s.is_busy)

        return {
            "total_sessions": len(self.sessions),
            "active_sessions": active_sessions,
            "busy_sessions": busy_sessions,
            "active_mass_searches": len(self.active_mass_searches),
            "searches": self.stats,
            "sessions": [session.get_stats() for session in self.sessions]
        }


async def initialize_sessions(self, min_sessions=5):
    """
    Initialize and authenticate a minimum number of sessions to have them ready for use.

    This prepares a batch of sessions at startup to ensure the system has authenticated
    sessions immediately available for user requests, reducing initial request latency.

    Args:
        min_sessions: Minimum number of sessions to initialize (default: 5)

    Returns:
        Tuple of (successful_authentications, failed_authentications)
    """
    logging.info(f"Initializing {min_sessions} sessions for immediate availability...")

    success_count = 0
    fail_count = 0

    # Use a semaphore to limit simultaneous authentications to avoid overwhelming the server
    auth_semaphore = asyncio.Semaphore(10)  # Max 10 simultaneous authentications

    async def init_single_session(session):
        async with auth_semaphore:
            try:
                if not session.is_authenticated:
                    # Mark session as busy before authentication
                    original_busy_state = session.is_busy
                    session.is_busy = True
                    try:
                        # Add some jitter to prevent thundering herd
                        await asyncio.sleep(random.uniform(0.1, 0.5))
                        success = await session.authenticate()
                        return success
                    finally:
                        # Restore original busy state
                        session.is_busy = original_busy_state
                return True  # Already authenticated
            except Exception as e:
                logging.error(f"Session {session.session_id} initialization error: {e}")
                return False

    # Create initialization tasks for sessions, prioritizing ones with different credentials
    init_tasks = []
    unique_creds = set()

    # First, pick one session per unique credential
    for session in self.sessions:
        cred_key = f"{session.login}:{session.password}"
        if cred_key not in unique_creds and len(init_tasks) < min_sessions:
            unique_creds.add(cred_key)
            init_tasks.append(init_single_session(session))

    # Then add more sessions up to min_sessions if needed
    remaining_slots = min_sessions - len(init_tasks)
    if remaining_slots > 0:
        # Find sessions that aren't busy and aren't already in our tasks
        available_sessions = [s for s in self.sessions if not s.is_busy
                              and not any(t is init_single_session(s) for t in init_tasks)]

        # Add up to the remaining slots
        for session in available_sessions[:remaining_slots]:
            init_tasks.append(init_single_session(session))

    # Wait for all initialization tasks to complete
    if init_tasks:
        results = await asyncio.gather(*init_tasks, return_exceptions=True)

        # Count successes and failures
        for result in results:
            if isinstance(result, Exception):
                fail_count += 1
                logging.error(f"Session initialization failed with error: {result}")
            elif result is True:
                success_count += 1
            else:
                fail_count += 1

    # Log the results
    if success_count > 0:
        logging.info(f"Successfully initialized {success_count} sessions")
    if fail_count > 0:
        logging.warning(f"Failed to initialize {fail_count} sessions")

    return success_count, fail_count