# bot/database/db_pool.py
import asyncio
import logging
import asyncpg
from bot.database.database_config import POSTGRES_CONFIG

# Global connection pool
_pool = None

async def get_pool():
    """Get or create the database connection pool"""
    global _pool
    if _pool is None:
        try:
            logging.info("Creating PostgreSQL connection pool...")
            _pool = await asyncpg.create_pool(
                host=POSTGRES_CONFIG['host'],
                port=POSTGRES_CONFIG['port'],
                database=POSTGRES_CONFIG['database'],
                user=POSTGRES_CONFIG['user'],
                password=POSTGRES_CONFIG['password'],
                min_size=5,  # Minimum connections in pool
                max_size=20,  # Maximum connections in pool
                command_timeout=60,  # Command timeout in seconds
                server_settings={
                    'application_name': 'hardinfosearch_bot'
                }
            )
            logging.info("PostgreSQL connection pool created")
        except Exception as e:
            logging.error(f"Failed to create PostgreSQL connection pool: {e}")
            raise
    return _pool

async def close_pool():
    """Close the database connection pool"""
    global _pool
    if _pool is not None:
        await _pool.close()
        _pool = None
        logging.info("PostgreSQL connection pool closed")

async def execute_query(query, *args, fetch=False):
    """Execute a PostgreSQL query with proper connection management"""
    pool = await get_pool()
    async with pool.acquire() as conn:
        try:
            if fetch:
                return await conn.fetch(query, *args)
            else:
                return await conn.execute(query, *args)
        except Exception as e:
            logging.error(f"Database query error: {e}")
            logging.error(f"Query: {query}, Args: {args}")
            raise

async def test_connection():
    """Test PostgreSQL connection and return status"""
    try:
        pool = await get_pool()
        async with pool.acquire() as conn:
            result = await conn.fetchval("SELECT 1")
            if result == 1:
                version = await conn.fetchval("SELECT version()")
                logging.info(f"PostgreSQL connection successful: {version}")
                return True, version
        return False, "Connection test failed"
    except Exception as e:
        logging.error(f"PostgreSQL connection test failed: {e}")
        return False, str(e)