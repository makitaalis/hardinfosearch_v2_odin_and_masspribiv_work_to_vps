# bot/database/migrate_data.py
import asyncio
import sqlite3
import argparse
import sys
import json
import os
import logging
from datetime import datetime
from bot.database.db_pool import get_pool, close_pool

# SQLite database path
SQLITE_DB_PATH = "database/bot.db"


async def migrate_data():
    """Migrate data from SQLite to PostgreSQL"""
    if not os.path.exists(SQLITE_DB_PATH):
        logging.error(f"SQLite database not found at {SQLITE_DB_PATH}")
        return False

    # Connect to SQLite
    sqlite_conn = sqlite3.connect(SQLITE_DB_PATH)
    sqlite_conn.row_factory = sqlite3.Row
    sqlite_cursor = sqlite_conn.cursor()

    try:
        # Get PostgreSQL connection
        pg_pool = await get_pool()

        # Start transaction
        async with pg_pool.acquire() as pg_conn:
            async with pg_conn.transaction():
                # 1. Migrate users
                logging.info("Migrating users...")
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

                    # Handle last_login_at
                    last_login_at = user_dict.get('last_login_at')

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
                                          last_login_at,
                                          user_dict.get('login_count', 0),
                                          user_dict.get('requests_count', 0),
                                          user_dict.get('first_name'),
                                          user_dict.get('last_name'),
                                          user_dict.get('username')
                                          )

                logging.info(f"Migrated {len(users)} users")

                # 2. Migrate active sessions
                logging.info("Migrating active sessions...")
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

                logging.info(f"Migrated {len(sessions)} active sessions")

                # 3. Migrate cache
                logging.info("Migrating cache...")
                sqlite_cursor.execute("SELECT * FROM cache")
                cache_entries = sqlite_cursor.fetchall()

                for entry in cache_entries:
                    entry_dict = dict(entry)

                    # Handle JSON response
                    response = entry_dict.get('response')

                    # Make sure we have source field
                    source = entry_dict.get('source', 'system')

                    # Get timestamp or default to now
                    timestamp = entry_dict.get('timestamp', datetime.now())

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
                                          source,
                                          timestamp
                                          )

                logging.info(f"Migrated {len(cache_entries)} cache entries")

                # 4. Migrate admin logs table
                logging.info("Migrating admin logs...")
                sqlite_cursor.execute("SELECT * FROM admin_logs")
                admin_logs = sqlite_cursor.fetchall()

                for log in admin_logs:
                    log_dict = dict(log)

                    # Insert admin log
                    await pg_conn.execute('''
                        INSERT INTO admin_logs (admin_id, action, details, timestamp)
                        VALUES ($1, $2, $3, $4)
                    ''',
                                          log_dict.get('admin_id'),
                                          log_dict.get('action'),
                                          log_dict.get('details'),
                                          log_dict.get('timestamp', datetime.now())
                                          )

                logging.info(f"Migrated {len(admin_logs)} admin logs")

                logging.info("Data migration completed successfully!")
                return True
    except Exception as e:
        logging.error(f"Error during migration: {e}")
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
    else:
        print("Data migration completed successfully!")


if __name__ == '__main__':
    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )
    asyncio.run(main())