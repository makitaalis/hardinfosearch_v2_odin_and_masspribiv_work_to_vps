# bot/database/migrate_schema.py
import asyncio
import argparse
import logging
from bot.database.db_pool import get_pool, close_pool

async def create_tables():
    """Create all required PostgreSQL tables"""
    try:
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

        logging.info("Database tables created successfully!")
        return True
    except Exception as e:
        logging.error(f"Error creating database tables: {e}")
        return False
    finally:
        await close_pool()

async def main():
    parser = argparse.ArgumentParser(description='Initialize PostgreSQL Database Schema')
    args = parser.parse_args()

    try:
        success = await create_tables()
        if success:
            print("Database schema migration completed successfully!")
        else:
            print("Database schema migration failed. Check logs for details.")
            exit(1)
    finally:
        await close_pool()

if __name__ == '__main__':
    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )
    asyncio.run(main())