"""
PostgreSQL Migration Guide for HardInfoSearch

This guide outlines how to migrate the project from SQLite to PostgreSQL
"""

# Step 1: Install required packages
# pip install asyncpg psycopg2-binary

# Step 2: Create database_config.py
# ==========================================
# database_config.py - Add this file to your project
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# PostgreSQL connection parameters
POSTGRES_CONFIG = {
    'host': os.getenv('DB_HOST', 'localhost'),
    'port': os.getenv('DB_PORT', 5432),
    'database': os.getenv('DB_NAME', 'hardinfosearch'),
    'user': os.getenv('DB_USER', 'postgres'),
    'password': os.getenv('DB_PASSWORD', 'password')
}


# Connection string format
def get_postgres_uri():
    return f"postgresql://{POSTGRES_CONFIG['user']}:{POSTGRES_CONFIG['password']}@{POSTGRES_CONFIG['host']}:{POSTGRES_CONFIG['port']}/{POSTGRES_CONFIG['database']}"


# Step 3: Create Database Pool Module
# ==========================================
# db_pool.py - Add this file to your project
import asyncpg
import logging
from .database_config import POSTGRES_CONFIG

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
                command_timeout=60  # Command timeout in seconds
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


# Example of a database query function
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


# Step 4: Database Schema Migration Script
# ==========================================
# migrate_schema.py - Run this script to create PostgreSQL tables
import asyncio
import argparse
from bot.db_pool import get_pool, close_pool


async def create_tables():
    """Create all required PostgreSQL tables"""
    pool = await get_pool()

    async with pool.acquire() as conn:
        # Start transaction
        async with conn.transaction():
            # Create version table first
            await conn.execute('''
                CREATE TABLE IF NOT EXISTS db_version (
                    version INTEGER PRIMARY KEY,
                    migrated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    description TEXT
                )
            ''')

            # Users table
            await conn.execute('''
                CREATE TABLE IF NOT EXISTS users (
                    id SERIAL PRIMARY KEY,
                    telegram_id BIGINT UNIQUE,
                    login TEXT UNIQUE,
                    password_hash TEXT,
                    balance DECIMAL(10, 2) DEFAULT 0.0,
                    failed_attempts INTEGER DEFAULT 0,
                    is_blocked BOOLEAN DEFAULT FALSE,
                    session_active BOOLEAN DEFAULT FALSE,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    last_login_at TIMESTAMP,
                    login_count INTEGER DEFAULT 0,
                    requests_count INTEGER DEFAULT 0,
                    first_name TEXT,
                    last_name TEXT,
                    username TEXT
                )
            ''')

            # Active sessions table
            await conn.execute('''
                CREATE TABLE IF NOT EXISTS active_sessions (
                    id SERIAL PRIMARY KEY,
                    login TEXT,
                    telegram_id BIGINT,
                    is_active BOOLEAN DEFAULT TRUE,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    CONSTRAINT fk_user FOREIGN KEY(telegram_id) REFERENCES users(telegram_id) ON DELETE CASCADE
                )
            ''')

            # Admin logs table
            await conn.execute('''
                CREATE TABLE IF NOT EXISTS admin_logs (
                    id SERIAL PRIMARY KEY,
                    admin_id BIGINT,
                    action TEXT,
                    details TEXT,
                    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            ''')

            # Cache table
            await conn.execute('''
                CREATE TABLE IF NOT EXISTS cache (
                    id SERIAL PRIMARY KEY,
                    user_id BIGINT,
                    query TEXT,
                    response TEXT,
                    source TEXT DEFAULT 'system',
                    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(user_id, query)
                )
            ''')

            # Mass search logs table
            await conn.execute('''
                CREATE TABLE IF NOT EXISTS mass_search_logs (
                    id SERIAL PRIMARY KEY,
                    user_id BIGINT,
                    file_path TEXT,
                    valid_lines INTEGER,
                    total_cost DECIMAL(10, 2),
                    start_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    end_time TIMESTAMP,
                    status TEXT DEFAULT 'pending',
                    results_file TEXT,
                    phones_found INTEGER DEFAULT 0,
                    error_message TEXT,
                    CONSTRAINT fk_user FOREIGN KEY(user_id) REFERENCES users(telegram_id) ON DELETE SET NULL
                )
            ''')

            # Migration errors table
            await conn.execute('''
                CREATE TABLE IF NOT EXISTS migration_errors (
                    id SERIAL PRIMARY KEY,
                    version INTEGER,
                    error TEXT,
                    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            ''')

            # Create indexes for performance
            await conn.execute('CREATE INDEX IF NOT EXISTS idx_users_telegram_id ON users(telegram_id)')
            await conn.execute('CREATE INDEX IF NOT EXISTS idx_users_login ON users(login)')
            await conn.execute('CREATE INDEX IF NOT EXISTS idx_sessions_telegram_id ON active_sessions(telegram_id)')
            await conn.execute('CREATE INDEX IF NOT EXISTS idx_sessions_login ON active_sessions(login)')
            await conn.execute('CREATE INDEX IF NOT EXISTS idx_cache_user_query ON cache(user_id, query)')
            await conn.execute('CREATE INDEX IF NOT EXISTS idx_cache_timestamp ON cache(timestamp)')

            # Insert initial version
            await conn.execute(
                "INSERT INTO db_version (version, description) VALUES ($1, $2) ON CONFLICT (version) DO NOTHING",
                1, 'Initial PostgreSQL schema'
            )

    print("Database tables created successfully!")


async def main():
    parser = argparse.ArgumentParser(description='Initialize PostgreSQL Database Schema')
    args = parser.parse_args()

    try:
        await create_tables()
    finally:
        await close_pool()


if __name__ == '__main__':
    asyncio.run(main())

# Step 5: Data Migration Script
# ==========================================
# migrate_data.py - Run this to migrate data from SQLite to PostgreSQL
import asyncio
import sqlite3
import argparse
import sys
import json
import os
from datetime import datetime
from bot.db_pool import get_pool, close_pool

# SQLite database path
SQLITE_DB_PATH = "database/bot.db"


async def migrate_data():
    """Migrate data from SQLite to PostgreSQL"""
    if not os.path.exists(SQLITE_DB_PATH):
        print(f"SQLite database not found at {SQLITE_DB_PATH}")
        return False

    # Connect to SQLite
    sqlite_conn = sqlite3.connect(SQLITE_DB_PATH)
    sqlite_conn.row_factory = sqlite3.Row
    sqlite_cursor = sqlite_conn.cursor()

    # Get PostgreSQL connection
    pg_pool = await get_pool()

    try:
        # Start transaction
        async with pg_pool.acquire() as pg_conn:
            async with pg_conn.transaction():
                # 1. Migrate users
                print("Migrating users...")
                sqlite_cursor.execute("SELECT * FROM users")
                users = sqlite_cursor.fetchall()

                for user in users:
                    # Convert SQLite data to Python dict
                    user_dict = dict(user)

                    # Handle boolean conversion
                    is_blocked = bool(user_dict.get('is_blocked', 0))
                    session_active = bool(user_dict.get('session_active', 0))

                    # Handle timestamps
                    created_at = user_dict.get('created_at')
                    if created_at is None:
                        created_at = datetime.now()

                    # Insert into PostgreSQL
                    await pg_conn.execute('''
                        INSERT INTO users (
                            telegram_id, login, password_hash, balance, 
                            failed_attempts, is_blocked, session_active, 
                            created_at, last_login_at, login_count, requests_count,
                            first_name, last_name, username
                        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14)
                        ON CONFLICT (telegram_id) DO UPDATE SET
                            login = EXCLUDED.login,
                            password_hash = EXCLUDED.password_hash,
                            balance = EXCLUDED.balance,
                            failed_attempts = EXCLUDED.failed_attempts,
                            is_blocked = EXCLUDED.is_blocked,
                            session_active = EXCLUDED.session_active,
                            created_at = EXCLUDED.created_at,
                            last_login_at = EXCLUDED.last_login_at,
                            login_count = EXCLUDED.login_count,
                            requests_count = EXCLUDED.requests_count,
                            first_name = EXCLUDED.first_name,
                            last_name = EXCLUDED.last_name,
                            username = EXCLUDED.username
                    ''',
                                          user_dict.get('telegram_id'),
                                          user_dict.get('login'),
                                          user_dict.get('password_hash'),
                                          float(user_dict.get('balance', 0.0)),
                                          user_dict.get('failed_attempts', 0),
                                          is_blocked,
                                          session_active,
                                          created_at,
                                          user_dict.get('last_login_at'),
                                          user_dict.get('login_count', 0),
                                          user_dict.get('requests_count', 0),
                                          user_dict.get('first_name'),
                                          user_dict.get('last_name'),
                                          user_dict.get('username')
                                          )

                print(f"Migrated {len(users)} users")

                # 2. Migrate active sessions
                print("Migrating active sessions...")
                sqlite_cursor.execute("SELECT * FROM active_sessions")
                sessions = sqlite_cursor.fetchall()

                for session in sessions:
                    session_dict = dict(session)
                    is_active = bool(session_dict.get('is_active', 1))

                    await pg_conn.execute('''
                        INSERT INTO active_sessions (login, telegram_id, is_active)
                        VALUES ($1, $2, $3)
                        ON CONFLICT DO NOTHING
                    ''',
                                          session_dict.get('login'),
                                          session_dict.get('telegram_id'),
                                          is_active
                                          )

                print(f"Migrated {len(sessions)} active sessions")

                # 3. Migrate cache
                print("Migrating cache...")
                sqlite_cursor.execute("SELECT * FROM cache")
                cache_entries = sqlite_cursor.fetchall()

                for entry in cache_entries:
                    entry_dict = dict(entry)

                    # Handle JSON response
                    response = entry_dict.get('response')

                    await pg_conn.execute('''
                        INSERT INTO cache (user_id, query, response, source, timestamp)
                        VALUES ($1, $2, $3, $4, $5)
                        ON CONFLICT (user_id, query) DO UPDATE SET
                            response = EXCLUDED.response,
                            source = EXCLUDED.source,
                            timestamp = EXCLUDED.timestamp
                    ''',
                                          entry_dict.get('user_id'),
                                          entry_dict.get('query'),
                                          response,
                                          entry_dict.get('source', 'system'),
                                          entry_dict.get('timestamp', datetime.now())
                                          )

                print(f"Migrated {len(cache_entries)} cache entries")

                # 4. Migrate other tables as needed...

                print("Data migration completed successfully!")
                return True
    except Exception as e:
        print(f"Error during migration: {e}")
        return False
    finally:
        sqlite_conn.close()
        await close_pool()


async def main():
    parser = argparse.ArgumentParser(description='Migrate data from SQLite to PostgreSQL')
    args = parser.parse_args()

    success = await migrate_data()
    if not success:
        sys.exit(1)


if __name__ == '__main__':
    asyncio.run(main())

# Step 6: Update main.py to use PostgreSQL
# ==========================================
# Add to your main.py startup code:

# At the top of the file, add:
from bot.db_pool import get_pool, close_pool


# In your on_startup function, add:
async def on_startup():
    # Initialize database pool
    try:
        pool = await get_pool()
        logging.info("PostgreSQL connection pool initialized")
    except Exception as e:
        logging.error(f"Failed to initialize PostgreSQL connection pool: {e}")
        raise


# In your on_shutdown function, add:
async def on_shutdown():
    # Close database pool
    try:
        await close_pool()
        logging.info("PostgreSQL connection pool closed")
    except Exception as e:
        logging.error(f"Error closing PostgreSQL connection pool: {e}")

# Step 7: Update .env file with PostgreSQL credentials
# ==========================================
# Add these lines to your .env file:
# DB_HOST=localhost
# DB_PORT=5432
# DB_NAME=hardinfosearch
# DB_USER=postgres
# DB_PASSWORD=your_password