"""
Enhanced SessionPoolManager for concurrent website sessions with robust error handling,
load balancing, and diagnostic capabilities.
"""

import asyncio
import logging
import random
import time
from datetime import datetime, timedelta
from typing import Dict, List, Tuple, Optional, Set, Any
import json

from bot.web_session import SauronWebSession


class SessionPoolManager:
    """
    Optimized session pool manager with improved concurrency, load balancing,
    and self-healing capabilities.
    """

    def __init__(self, credentials_list, max_sessions=50):
        """
        Initialize session pool with multiple concurrent sessions.

        Args:
            credentials_list: List of (login, password) tuples for website authentication
            max_sessions: Maximum number of concurrent sessions to maintain
        """
        # Core pool configuration
        self.max_sessions = max_sessions
        self.min_sessions_per_mass_search = 3
        self.sessions = []

        self.playwright_initialized = False

        # Synchronization primitives
        self.session_lock = asyncio.Lock()
        self.mass_search_lock = asyncio.Lock()
        self.stats_lock = asyncio.Lock()

        # Session allocation tracking
        self.active_mass_searches = set()
        self.mass_search_session_map = {}  # Map of mass search ID -> session IDs

        # Dynamic scaling
        self.dynamic_scaling_enabled = True
        self.session_health_checks = True
        self.last_scaling_check = datetime.now()
        self.scaling_check_interval = 300  # seconds (5 minutes)

        # Circuit breaker for failing sessions
        self.circuit_breakers = {}  # {credential_key: {'failures': count, 'last_failure': timestamp}}
        self.circuit_breaker_threshold = 5  # After this many failures, circuit opens
        self.circuit_breaker_timeout = 600  # Time in seconds to keep circuit open (10 minutes)

        # Load balancing settings
        self.session_selection_strategies = ['least_used', 'random', 'sequential']
        self.current_strategy = 'least_used'
        self.strategy_rotation_interval = 100  # Switch strategy every N calls

        # Initialize expanded session pool
        self._init_sessions(credentials_list)

        # Statistics initialization
        self.stats = {
            "total_searches": 0,
            "successful_searches": 0,
            "failed_searches": 0,
            "reauth_count": 0,
            "mass_searches": 0,
            "single_searches": 0,
            "session_wait_time": 0,
            "strategy_switches": 0,
            "session_scaling_events": 0,
        }

        logging.info(
            f"Initialized session pool with {len(self.sessions)} virtual sessions "
            f"based on {len(credentials_list)} credentials"
        )

    async def initialize_playwright(self):
        """Initialize Playwright for browser automation"""
        if not self.playwright_initialized:
            # Get first available session
            for session in self.sessions:
                if hasattr(session, 'setup_playwright'):
                    await session.setup_playwright()
                    self.playwright_initialized = True
                    logging.info("Playwright initialized for session pool")
                    break

    def _init_sessions(self, credentials_list):
        """Initialize session objects based on provided credentials"""
        if not credentials_list:
            raise ValueError("No credentials provided for session pool")

        # Calculate sessions per credential to reach desired total
        sessions_per_credential = max(1, self.max_sessions // len(credentials_list))

        # Create expanded credentials list with virtual sessions
        expanded_credentials = []
        for cred in credentials_list:
            if len(cred) < 2:
                logging.warning(f"Invalid credential format: {cred}, skipping")
                continue

            for i in range(sessions_per_credential):
                # Add suffix to create virtual sessions from same credential
                expanded_credentials.append((cred[0], cred[1], f"{i + 1}"))

        # Initialize session objects
        actual_sessions = min(self.max_sessions, len(expanded_credentials))
        for i in range(actual_sessions):
            idx = i % len(expanded_credentials)
            login, password, suffix = expanded_credentials[idx]
            session_id = f"{i + 1}_{suffix}"

            try:
                session = SauronWebSession(login, password, session_id)
                self.sessions.append(session)
            except Exception as e:
                logging.error(f"Failed to initialize session {session_id}: {e}")

    async def allocate_sessions_for_mass_search(self, mass_search_id, requested_count=5):
        """
        Allocates a dedicated group of sessions for a mass search job with fair distribution.

        Args:
            mass_search_id: Unique identifier for the mass search job
            requested_count: Desired number of sessions

        Returns:
            Number of sessions successfully allocated
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

            # Use dynamic allocation based on available resources
            if active_search_count > 0:
                # Ensure at least minimum sessions per search
                min_sessions = max(
                    self.min_sessions_per_mass_search,
                    min(len(available_sessions) // max(1, active_search_count), 5)
                )
                allocated_count = min(requested_count, min_sessions)
            else:
                # If this is the only search, give it more resources
                allocated_count = min(requested_count, len(available_sessions) // 2)

            # Select least used sessions to ensure fair utilization
            available_sessions.sort(key=lambda s: s.search_count)
            allocated_sessions = available_sessions[:allocated_count]

            # Register sessions with this mass search
            for session in allocated_sessions:
                self.mass_search_session_map[mass_search_id].add(session.session_id)

            # Update stats
            async with self.stats_lock:
                self.stats["mass_searches"] += 1

            return len(allocated_sessions)

    async def get_available_session(self, is_mass_search=False, mass_search_id=None, timeout=30):
        """
        Gets an available session using adaptive selection strategy.

        Args:
            is_mass_search: Whether this is for a mass search operation
            mass_search_id: ID of the mass search job if applicable
            timeout: Maximum time to wait for an available session

        Returns:
            SauronWebSession object or None if no session is available
        """
        start_time = time.time()
        strategy_counter = random.randint(1, 10)  # Start with some randomness

        # For mass search, use a shorter delay to increase throughput
        if is_mass_search:
            delay = random.uniform(0.3, 0.8)
            await asyncio.sleep(delay)

        # Try to get a session within the timeout period
        while time.time() - start_time < timeout:
            async with self.session_lock:
                # Get available authenticated sessions
                available_sessions = [s for s in self.sessions if not s.is_busy and s.is_authenticated]

                if available_sessions:
                    session = None

                    # Strategy 1: Session allocation for mass searches
                    if is_mass_search and mass_search_id:
                        if mass_search_id in self.mass_search_session_map:
                            # Try to use pre-allocated sessions first
                            allocated_ids = self.mass_search_session_map[mass_search_id]
                            allocated_sessions = [s for s in available_sessions if s.session_id in allocated_ids]

                            if allocated_sessions:
                                # Use adaptive session selection
                                session = self._select_session(allocated_sessions, strategy_counter)

                    # Strategy 2: General session selection
                    if not session:
                        # For single search, try to avoid sessions used by mass searches
                        if not is_mass_search:
                            mass_search_sessions = set()
                            for sessions in self.mass_search_session_map.values():
                                mass_search_sessions.update(sessions)

                            free_sessions = [s for s in available_sessions if s.session_id not in mass_search_sessions]

                            if free_sessions:
                                session = self._select_session(free_sessions, strategy_counter)

                        # If still no session, use any available
                        if not session:
                            session = self._select_session(available_sessions, strategy_counter)

                    if session:
                        # Mark session as busy and update stats
                        session.is_busy = True
                        session.last_activity = datetime.now()

                        # Update stats
                        async with self.stats_lock:
                            if is_mass_search:
                                self.stats["mass_searches"] += 1
                            else:
                                self.stats["single_searches"] += 1

                            # Rotate strategy periodically
                            strategy_counter += 1
                            if strategy_counter % self.strategy_rotation_interval == 0:
                                self._rotate_selection_strategy()
                                self.stats["strategy_switches"] += 1

                        return session

                # If no authenticated sessions, try to get an unauthenticated one to authenticate
                unauthenticated_sessions = [s for s in self.sessions if not s.is_busy]
                if unauthenticated_sessions:
                    # Prioritize sessions with lower error count
                    session = min(unauthenticated_sessions, key=lambda s: s.error_count)
                    session.is_busy = True
                    return session

            # If no session available, wait and retry
            wait_time = min(0.5, (timeout - (time.time() - start_time)) / 3)
            if wait_time > 0:
                async with self.stats_lock:
                    self.stats["session_wait_time"] += wait_time
                await asyncio.sleep(wait_time)
            else:
                break

        logging.warning(f"Timeout waiting for session: {timeout}s exceeded")
        return None

    def _select_session(self, sessions, counter):
        """
        Select a session using the current strategy for load balancing.

        Args:
            sessions: List of available sessions
            counter: Current operation counter (used for some strategies)

        Returns:
            Selected SauronWebSession object
        """
        if not sessions:
            return None

        if self.current_strategy == 'least_used':
            # Use session with least searches to ensure even distribution
            return min(sessions, key=lambda s: s.search_count)
        elif self.current_strategy == 'random':
            # Random selection for unpredictable patterns
            return random.choice(sessions)
        elif self.current_strategy == 'sequential':
            # Round-robin selection based on counter
            return sessions[counter % len(sessions)]
        else:
            # Default to least used
            return min(sessions, key=lambda s: s.search_count)

    def _rotate_selection_strategy(self):
        """Rotate to next session selection strategy for adaptive load balancing"""
        current_index = self.session_selection_strategies.index(self.current_strategy)
        next_index = (current_index + 1) % len(self.session_selection_strategies)
        self.current_strategy = self.session_selection_strategies[next_index]
        logging.debug(f"Rotated session selection strategy to: {self.current_strategy}")

    async def release_session(self, session, is_mass_search=False, mass_search_id=None):
        """
        Release a session back to the pool.

        Args:
            session: The session to release
            is_mass_search: Whether this was a mass search operation
            mass_search_id: ID of the mass search if applicable
        """
        if session not in self.sessions:
            return

        # Mark session as not busy
        session.is_busy = False
        session.last_activity = datetime.now()

        # Check if we need to perform health check
        if self.session_health_checks and session.error_count > 0:
            # Schedule health check for later to avoid immediate retry that might still fail
            asyncio.create_task(self._delayed_health_check(session))

    async def _delayed_health_check(self, session, delay=5.0):
        """
        Perform a delayed health check on a session to verify it's working.

        Args:
            session: Session to check
            delay: Time in seconds to wait before checking
        """
        await asyncio.sleep(delay)

        # Only check if session is not busy
        if not session.is_busy:
            try:
                # Temporarily mark as busy during check
                session.is_busy = True

                # If session is showing errors, try to reauthenticate
                if not session.is_authenticated or session.error_count > 0:
                    logging.info(f"Performing health check on session {session.session_id}")
                    await session.authenticate()

                # Clear error count on successful auth
                if session.is_authenticated:
                    session.error_count = 0
            except Exception as e:
                logging.error(f"Health check failed for session {session.session_id}: {e}")
            finally:
                # Release the session
                session.is_busy = False

    async def finish_mass_search(self, mass_search_id):
        """
        Finalize a mass search job and release allocated resources.

        Args:
            mass_search_id: ID of the mass search to finalize
        """
        async with self.mass_search_lock:
            if mass_search_id in self.active_mass_searches:
                self.active_mass_searches.remove(mass_search_id)

            if mass_search_id in self.mass_search_session_map:
                del self.mass_search_session_map[mass_search_id]

    async def refresh_session(self, session):
        """
        Refresh session authentication.

        Args:
            session: Session to refresh

        Returns:
            bool: Whether authentication was successful
        """
        if session in self.sessions and not session.is_busy:
            session.is_busy = True
            try:
                async with self.stats_lock:
                    self.stats["reauth_count"] += 1
                result = await session.authenticate()
                return result
            finally:
                session.is_busy = False
        return False

    async def refresh_expired_sessions(self, max_age_hours=2):
        """
        Refresh sessions that haven't been used recently.

        Args:
            max_age_hours: Maximum session age in hours before refresh
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

        # Perform batch authentication with rate limiting
        for i, session in enumerate(refresh_tasks):
            try:
                # Add small delay between auth attempts to prevent rate limiting
                if i > 0:
                    await asyncio.sleep(0.5)

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
        Execute a search query using an available session.

        Args:
            query: Search query string
            is_mass_search: Whether this is a mass search operation
            mass_search_id: ID of the mass search if applicable

        Returns:
            tuple: (success, result_or_error_message)
        """
        async with self.stats_lock:
            self.stats["total_searches"] += 1

        # Get an available session
        session = await self.get_available_session(is_mass_search, mass_search_id)
        if not session:
            logging.error("Failed to get available session for search")
            async with self.stats_lock:
                self.stats["failed_searches"] += 1
            return False, "No available session for search"

        try:
            # Ensure session is authenticated
            if not session.is_authenticated:
                auth_success = await session.authenticate()
                if not auth_success:
                    async with self.stats_lock:
                        self.stats["failed_searches"] += 1
                    return False, "Authentication failed before search"

            # Perform the search
            success, result = await session.search(query)

            if success:
                # Parse the search results
                parsed_data = await session.parse_results(result)
                async with self.stats_lock:
                    self.stats["successful_searches"] += 1
                return True, parsed_data
            else:
                # Handle authentication-related errors
                if "авториз" in result.lower() or "session" in result.lower():
                    session.is_authenticated = False

                async with self.stats_lock:
                    self.stats["failed_searches"] += 1
                return False, result
        except Exception as e:
            async with self.stats_lock:
                self.stats["failed_searches"] += 1
            logging.error(f"Error during search: {str(e)}")
            return False, f"Search error: {str(e)}"
        finally:
            # Release the session
            await self.release_session(session, is_mass_search, mass_search_id)

    def get_stats(self):
        """
        Get comprehensive statistics about the session pool.

        Returns:
            dict: Collection of session pool statistics
        """
        active_sessions = sum(1 for s in self.sessions if s.is_authenticated)
        busy_sessions = sum(1 for s in self.sessions if s.is_busy)
        error_sessions = sum(1 for s in self.sessions if s.error_count > 0)

        return {
            "total_sessions": len(self.sessions),
            "active_sessions": active_sessions,
            "busy_sessions": busy_sessions,
            "error_sessions": error_sessions,
            "active_mass_searches": len(self.active_mass_searches),
            "current_strategy": self.current_strategy,
            "searches": dict(self.stats),
            "sessions": [session.get_stats() for session in self.sessions]
        }

    async def initialize_sessions(self, min_sessions=5):
        """
        Initialize and authenticate a minimum number of sessions on startup.
        Improved to handle failures gracefully and avoid deadlocks.

        Args:
            min_sessions: Minimum number of sessions to initialize

        Returns:
            tuple: (successful_authentications, failed_authentications)
        """
        logging.info(f"Initializing {min_sessions} sessions for immediate availability...")

        success_count = 0
        fail_count = 0

        # Use a semaphore to limit simultaneous authentications but allow for some concurrency
        auth_semaphore = asyncio.Semaphore(5)  # Reduced from 10 to 5 for more stability

        async def init_single_session(session):
            async with auth_semaphore:
                try:
                    if not session.is_authenticated:
                        # Mark session as busy before authentication
                        original_busy_state = session.is_busy
                        session.is_busy = True
                        try:
                            # Add some jitter to prevent thundering herd
                            await asyncio.sleep(random.uniform(0.5, 1.5))  # Increased delay
                            success = await session.authenticate()
                            return success
                        except Exception as e:
                            logging.error(f"Session {session.session_id} authentication error: {e}")
                            return False
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
                init_tasks.append(asyncio.create_task(init_single_session(session)))

        # Then add more sessions up to min_sessions if needed
        remaining_slots = min_sessions - len(init_tasks)
        if remaining_slots > 0:
            # Find sessions that aren't busy and aren't already in our tasks
            remaining_sessions = []
            for s in self.sessions:
                if s.is_busy:
                    continue

                # Skip sessions that are already being initialized
                already_initializing = False
                for task in init_tasks:
                    # Can't directly compare coroutine objects, so we use a different approach
                    if hasattr(task, '_coro') and id(s) == id(getattr(task._coro, '__self__', None)):
                        already_initializing = True
                        break

                if not already_initializing:
                    remaining_sessions.append(s)

            # Add up to the remaining slots
            for session in remaining_sessions[:remaining_slots]:
                init_tasks.append(asyncio.create_task(init_single_session(session)))

        # Wait for all initialization tasks to complete with a timeout
        if init_tasks:
            try:
                # Add a timeout to prevent hanging if some tasks get stuck
                done, pending = await asyncio.wait(init_tasks, timeout=60)

                # Cancel any pending tasks
                for task in pending:
                    task.cancel()
                    fail_count += 1

                # Process completed tasks
                results = []
                for task in done:
                    try:
                        results.append(task.result())
                    except Exception as e:
                        logging.error(f"Task error: {e}")
                        results.append(False)

                # Count successes and failures
                success_count = sum(1 for r in results if r is True)
                fail_count = len(results) - success_count + len(pending)
            except Exception as e:
                logging.error(f"Error waiting for initialization tasks: {e}")
                # Try to cancel all tasks
                for task in init_tasks:
                    try:
                        if not task.done():
                            task.cancel()
                    except Exception:
                        pass

        # Log the results
        if success_count > 0:
            logging.info(f"Successfully initialized {success_count} sessions")
        if fail_count > 0:
            logging.warning(f"Failed to initialize {fail_count} sessions")

        return success_count, fail_count

    async def cleanup(self):
        """Clean up all sessions when shutting down"""
        logging.info("Cleaning up session pool...")

        # Release all sessions
        async with self.session_lock:
            for session in self.sessions:
                session.is_busy = False

        # Call cleanup on underlying connection pool
        try:
            from bot.web_session import SauronWebSession
            await SauronWebSession.cleanup()
            logging.info("Session connection pool closed")
        except Exception as e:
            logging.error(f"Error cleaning up session connection pool: {e}")