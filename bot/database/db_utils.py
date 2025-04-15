"""
Database connection management utilities with thread safety and connection pooling.
This module should be used for all database operations to ensure proper connection handling.
"""

import asyncio
import contextlib
import logging
import time
import sqlite3
from functools import wraps
from typing import Dict, Any, Optional, Callable, List, Tuple, Union

# Global database connection pool (SQLite)
_connection_pools = {}
_locks = {}
_timeout = 30  # Connection timeout in seconds
_max_connections = 20  # Maximum concurrent connections
_semaphore = asyncio.Semaphore(_max_connections)


# ====================== SQLite Connection Management ======================

def get_connection(db_path: str = "database/bot.db"):
    """
    Gets a connection from the connection pool for the specified database.
    Creates a new pool if one doesn't exist for this database.

    Args:
        db_path: Path to SQLite database file

    Returns:
        sqlite3.Connection: Database connection
    """
    global _connection_pools, _locks

    # Create lock for this database if it doesn't exist
    if db_path not in _locks:
        _locks[db_path] = asyncio.Lock()

    # Create connection pool if it doesn't exist
    if db_path not in _connection_pools:
        _connection_pools[db_path] = []

    # Try to get connection from pool
    pool = _connection_pools[db_path]
    if pool:
        connection = pool.pop()
        try:
            # Test connection validity with a simple query
            connection.execute("SELECT 1")
            return connection
        except sqlite3.Error:
            # Connection is invalid, create a new one
            connection.close()

    # Create new connection with extended timeout and pragma settings
    connection = sqlite3.connect(db_path, timeout=_timeout)

    # Set WAL journal mode for better concurrency
    connection.execute("PRAGMA journal_mode=WAL")

    # Set busy timeout to avoid SQLITE_BUSY errors
    connection.execute(f"PRAGMA busy_timeout={_timeout * 1000}")

    # Set other pragmas for performance/safety
    connection.execute("PRAGMA foreign_keys=ON")  # Enable foreign key constraints
    connection.execute("PRAGMA synchronous=NORMAL")  # Balance between safety and speed

    # Enable connection row factory
    connection.row_factory = sqlite3.Row

    return connection


def release_connection(connection, db_path: str = "database/bot.db"):
    """
    Returns a connection to the connection pool.

    Args:
        connection: SQLite connection to release
        db_path: Path to SQLite database
    """
    global _connection_pools

    if db_path not in _connection_pools:
        _connection_pools[db_path] = []

    try:
        # Rollback any uncommitted changes
        connection.rollback()

        # Add connection back to pool if pool isn't too large
        if len(_connection_pools[db_path]) < _max_connections:
            _connection_pools[db_path].append(connection)
        else:
            # If pool is full, close the connection
            connection.close()
    except sqlite3.Error:
        # If connection has errors, close it
        connection.close()


@contextlib.contextmanager
def db_connection(db_path: str = "database/bot.db"):
    """
    Context manager for database connections.
    Automatically acquires and releases connections.

    Usage:
        with db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM users")

    Args:
        db_path: Path to SQLite database

    Yields:
        sqlite3.Connection: Database connection
    """
    connection = None
    try:
        connection = get_connection(db_path)
        yield connection
    finally:
        if connection:
            release_connection(connection, db_path)


def with_connection(db_path: str = "database/bot.db"):
    """
    Decorator for functions that need a database connection.

    Usage:
        @with_connection()
        def get_user(conn, user_id):
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM users WHERE id = ?", (user_id,))
            return cursor.fetchone()

    Args:
        db_path: Path to SQLite database

    Returns:
        Callable: Decorated function
    """

    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            with db_connection(db_path) as conn:
                return func(conn, *args, **kwargs)

        return wrapper

    return decorator


async def with_async_connection(db_path: str = "database/bot.db"):
    """
    Decorator for async functions that need a database connection.

    Usage:
        @with_async_connection()
        async def get_user(conn, user_id):
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM users WHERE id = ?", (user_id,))
            return cursor.fetchone()

    Args:
        db_path: Path to SQLite database

    Returns:
        Callable: Decorated async function
    """

    def decorator(func):
        @wraps(func)
        async def wrapper(*args, **kwargs):
            async with _semaphore:
                # Get connection from pool
                connection = get_connection(db_path)
                try:
                    # Run function with connection
                    return await func(connection, *args, **kwargs)
                finally:
                    # Release connection back to pool
                    release_connection(connection, db_path)

        return wrapper

    return decorator


# ====================== Transaction Management ======================

@contextlib.contextmanager
def transaction(db_path: str = "database/bot.db"):
    """
    Context manager for database transactions.
    Automatically handles commits and rollbacks.

    Usage:
        with transaction() as conn:
            cursor = conn.cursor()
            cursor.execute("UPDATE users SET balance = ? WHERE id = ?", (new_balance, user_id))

    Args:
        db_path: Path to SQLite database

    Yields:
        sqlite3.Connection: Database connection
    """
    with db_connection(db_path) as conn:
        try:
            yield conn
            conn.commit()
        except Exception as e:
            conn.rollback()
            raise e


def with_transaction(db_path: str = "database/bot.db"):
    """
    Decorator for functions that need a database transaction.

    Usage:
        @with_transaction()
        def update_balance(conn, user_id, amount):
            cursor = conn.cursor()
            cursor.execute("UPDATE users SET balance = balance + ? WHERE id = ?", (amount, user_id))

    Args:
        db_path: Path to SQLite database

    Returns:
        Callable: Decorated function
    """

    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            with transaction(db_path) as conn:
                return func(conn, *args, **kwargs)

        return wrapper

    return decorator


async def with_async_transaction(db_path: str = "database/bot.db", max_retries: int = 3):
    """
    Decorator for async functions that need a database transaction.
    Includes retry logic for handling concurrent access issues.

    Args:
        db_path: Path to SQLite database
        max_retries: Maximum number of retries on conflict

    Returns:
        Callable: Decorated async function
    """

    def decorator(func):
        @wraps(func)
        async def wrapper(*args, **kwargs):
            retries = 0
            last_error = None

            while retries <= max_retries:
                async with _semaphore:
                    connection = get_connection(db_path)
                    try:
                        # Start transaction
                        connection.execute("BEGIN IMMEDIATE")

                        # Run function
                        result = await func(connection, *args, **kwargs)

                        # Commit transaction
                        connection.commit()

                        # Success, return result
                        return result
                    except sqlite3.OperationalError as e:
                        # Check if database is locked
                        if "database is locked" in str(e) and retries < max_retries:
                            connection.rollback()
                            last_error = e
                            retries += 1

                            # Wait with exponential backoff
                            wait_time = 0.1 * (2 ** retries)
                            logging.warning(
                                f"Database locked, retrying after {wait_time:.2f}s (attempt {retries}/{max_retries})"
                            )
                            await asyncio.sleep(wait_time)
                        else:
                            # Other error or max retries reached
                            connection.rollback()
                            raise e
                    except Exception as e:
                        # Other exceptions - rollback and re-raise
                        connection.rollback()
                        raise e
                    finally:
                        # Always release connection
                        release_connection(connection, db_path)

            # If we get here, we've exceeded max retries
            if last_error:
                raise last_error
            else:
                raise Exception(f"Transaction failed after {max_retries} retries")

        return wrapper

    return decorator


# ====================== Query Execution Utilities ======================

def execute_query(query: str, params: tuple = (), fetchone: bool = False,
                  fetchall: bool = False, db_path: str = "database/bot.db") -> Optional[Union[Dict, List[Dict]]]:
    """
    Executes a SQL query and returns results.

    Args:
        query: SQL query string
        params: Query parameters
        fetchone: Whether to fetch a single row
        fetchall: Whether to fetch all rows
        db_path: Path to SQLite database

    Returns:
        Optional[Union[Dict, List[Dict]]]: Query results or None
    """
    with db_connection(db_path) as conn:
        cursor = conn.cursor()
        cursor.execute(query, params)

        if fetchone:
            row = cursor.fetchone()
            return dict(row) if row else None
        elif fetchall:
            rows = cursor.fetchall()
            return [dict(row) for row in rows] if rows else []
        else:
            conn.commit()
            return None


def execute_script(script: str, db_path: str = "database/bot.db") -> bool:
    """
    Executes a SQL script (multiple statements).

    Args:
        script: SQL script string
        db_path: Path to SQLite database

    Returns:
        bool: Whether execution was successful
    """
    with transaction(db_path) as conn:
        cursor = conn.cursor()
        cursor.executescript(script)
        return True


async def execute_async_query(query: str, params: tuple = (), fetchone: bool = False,
                              fetchall: bool = False, db_path: str = "database/bot.db") -> Optional[
    Union[Dict, List[Dict]]]:
    """
    Executes a SQL query asynchronously.

    Args:
        query: SQL query string
        params: Query parameters
        fetchone: Whether to fetch a single row
        fetchall: Whether to fetch all rows
        db_path: Path to SQLite database

    Returns:
        Optional[Union[Dict, List[Dict]]]: Query results or None
    """
    async with _semaphore:
        conn = get_connection(db_path)
        try:
            cursor = conn.cursor()
            cursor.execute(query, params)

            if fetchone:
                row = cursor.fetchone()
                result = dict(row) if row else None
            elif fetchall:
                rows = cursor.fetchall()
                result = [dict(row) for row in rows] if rows else []
            else:
                conn.commit()
                result = None

            return result
        finally:
            release_connection(conn, db_path)


# ====================== Connection Pool Management ======================

def close_all_connections():
    """
    Closes all database connections in all pools.
    Should be called during application shutdown.
    """
    global _connection_pools

    for db_path, pool in _connection_pools.items():
        for conn in pool:
            try:
                conn.close()
            except Exception as e:
                logging.error(f"Error closing connection to {db_path}: {e}")

        # Clear pool
        _connection_pools[db_path] = []

    logging.info("All database connections closed")


def get_connection_pool_stats():
    """
    Gets statistics about the connection pools.

    Returns:
        Dict: Connection pool statistics
    """
    global _connection_pools

    stats = {
        'total_pools': len(_connection_pools),
        'total_connections': sum(len(pool) for pool in _connection_pools.values()),
        'pools': {}
    }

    for db_path, pool in _connection_pools.items():
        stats['pools'][db_path] = len(pool)

    return stats


# PostgreSQL compatibility layer - for future use
# This creates a similar interface but uses asyncpg

try:
    import asyncpg

    # Global PostgreSQL connection pool
    _pg_pool = None
    _pg_config = None


    async def init_pg_pool(config: Dict[str, Any]):
        """
        Initializes PostgreSQL connection pool.

        Args:
            config: PostgreSQL connection configuration
        """
        global _pg_pool, _pg_config

        if _pg_pool is None:
            _pg_config = config

            # Create connection pool
            _pg_pool = await asyncpg.create_pool(
                host=config.get('host', 'localhost'),
                port=config.get('port', 5432),
                user=config.get('user', 'postgres'),
                password=config.get('password', ''),
                database=config.get('database', 'hardinfosearch'),
                min_size=5,
                max_size=20,
                command_timeout=30
            )

            logging.info("PostgreSQL connection pool initialized")


    async def close_pg_pool():
        """Closes PostgreSQL connection pool."""
        global _pg_pool

        if _pg_pool is not None:
            await _pg_pool.close()
            _pg_pool = None
            logging.info("PostgreSQL connection pool closed")


    @contextlib.asynccontextmanager
    async def pg_connection():
        """
        Async context manager for PostgreSQL connections.

        Yields:
            asyncpg.Connection: PostgreSQL connection
        """
        global _pg_pool

        if _pg_pool is None:
            raise ValueError("PostgreSQL connection pool not initialized")

        async with _pg_pool.acquire() as conn:
            yield conn


    @contextlib.asynccontextmanager
    async def pg_transaction():
        """
        Async context manager for PostgreSQL transactions.

        Yields:
            asyncpg.Connection: PostgreSQL connection with active transaction
        """
        global _pg_pool

        if _pg_pool is None:
            raise ValueError("PostgreSQL connection pool not initialized")

        async with _pg_pool.acquire() as conn:
            async with conn.transaction():
                yield conn


    async def pg_execute(query: str, *args, fetch: bool = False):
        """
        Executes a PostgreSQL query.

        Args:
            query: SQL query string
            *args: Query parameters
            fetch: Whether to fetch results

        Returns:
            Union[List[asyncpg.Record], None]: Query results or None
        """
        async with pg_connection() as conn:
            if fetch:
                return await conn.fetch(query, *args)
            else:
                return await conn.execute(query, *args)


    def with_pg_connection():
        """
        Decorator for async functions that need a PostgreSQL connection.

        Returns:
            Callable: Decorated async function
        """

        def decorator(func):
            @wraps(func)
            async def wrapper(*args, **kwargs):
                async with pg_connection() as conn:
                    return await func(conn, *args, **kwargs)

            return wrapper

        return decorator


    def with_pg_transaction():
        """
        Decorator for async functions that need a PostgreSQL transaction.

        Returns:
            Callable: Decorated async function
        """

        def decorator(func):
            @wraps(func)
            async def wrapper(*args, **kwargs):
                async with pg_transaction() as conn:
                    return await func(conn, *args, **kwargs)

            return wrapper

        return decorator
except ImportError:
    # AsyncPG not available, define stub functions
    logging.info("asyncpg not installed, PostgreSQL support disabled")