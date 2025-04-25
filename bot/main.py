import asyncio
import logging
import os
import resource
import signal
import sys
import time
import traceback
from pathlib import Path

from aiogram import Bot, Dispatcher
from aiogram.client.bot import DefaultBotProperties
from aiogram.types import BotCommand, BotCommandScopeChat, BotCommandScopeDefault, ErrorEvent
from dotenv import load_dotenv

# Import shared resources
from bot.common import (
    mass_search_semaphore, MAX_USER_SEARCHES, active_user_searches,
    MAX_CONCURRENT_MASS_SEARCHES, mass_search_queue
)

# Import database functions
from bot.database.db import setup_database, fix_database_structure
from bot.logger import logging
from bot.session_manager import init_session_pool
from bot.analytics import log_error
from bot.handlers import router as handlers_router
from bot.mass_search import mass_search_router, process_mass_search_queue
from bot.admin_handlers import admin_router  # New module for admin handlers

from bot.database.db_pool import get_pool, close_pool
from bot.database.database_config import is_postgres_configured

# Optimize event loop policy if uvloop is available
try:
    import uvloop

    asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
    logging.info("Using uvloop for improved asyncio performance")
except ImportError:
    logging.info("uvloop not available, using standard asyncio")

# Optimize TCP settings
os.environ['PYTHONASYNCIOALLDEBUG'] = '0'  # Disable asyncio debug for production

# Configure asyncio for performance
loop = asyncio.get_event_loop()
loop.set_debug(False)

# If in Linux, increase file descriptors
if os.name == 'posix':
    try:
        import resource

        soft, hard = resource.getrlimit(resource.RLIMIT_NOFILE)
        new_soft = min(65536, hard)
        if new_soft > soft:
            resource.setrlimit(resource.RLIMIT_NOFILE, (new_soft, hard))
            logging.info(f"Increased file descriptor limit from {soft} to {new_soft}")
    except (ImportError, PermissionError, ValueError):
        pass

# Load environment variables from .env file
load_dotenv()
TOKEN = os.getenv("BOT_TOKEN")
ADMIN_ID = int(os.getenv("ADMIN_ID")) if os.getenv("ADMIN_ID") else 0

# Validate essential environment variables
if not TOKEN:
    logging.critical("BOT_TOKEN is not set in .env file")
    sys.exit(1)

# Initialize bot and dispatcher
bot = Bot(token=TOKEN, default=DefaultBotProperties(parse_mode="HTML"))
dp = Dispatcher()

# Global session pool
session_pool = None


# Signal handlers for graceful shutdown
def register_shutdown_handlers():
    """Register graceful shutdown handlers for various signals"""
    for sig in (signal.SIGINT, signal.SIGTERM, signal.SIGABRT):
        loop.add_signal_handler(sig, lambda s=sig: asyncio.create_task(shutdown(s)))
    logging.info("Shutdown handlers registered")


async def shutdown(signal):
    """Gracefully shut down the bot and clean up resources"""
    logging.info(f"Received shutdown signal: {signal}")

    try:
        # Cancel all tasks
        tasks = [task for task in asyncio.all_tasks() if task is not asyncio.current_task()]
        for task in tasks:
            task.cancel()

        # Wait for all tasks to complete with a timeout
        if tasks:
            logging.info(f"Waiting for {len(tasks)} tasks to complete...")
            try:
                await asyncio.wait(tasks, timeout=5)
            except asyncio.CancelledError:
                pass

        # Clean up session pool
        global session_pool
        if session_pool:
            try:
                logging.info("Cleaning up session pool...")
                await session_pool.cleanup()
            except Exception as e:
                logging.error(f"Error cleaning up session pool: {e}")

        # Close web sessions
        from bot.web_session import SauronWebSession
        try:
            await SauronWebSession.cleanup()
            logging.info("Web session connections closed")
        except Exception as e:
            logging.error(f"Error closing web session connections: {e}")

        # Stop bot polling
        await dp.stop_polling()
    finally:
        # Close event loop
        loop.stop()
        logging.info("Shutdown complete")


# Database initialization
def validate_db_structure():
    """Validate and fix database structure if needed"""
    try:
        fix_database_structure()
        return True
    except Exception as e:
        logging.error(f"Failed to fix database structure: {e}")
        return False


# Command registration
async def register_bot_commands():
    """Register bot commands for different user types"""
    # Commands for all users
    user_commands = [
        BotCommand(command="start", description="Начало работы"),
        BotCommand(command="help", description="Справка по боту"),
        BotCommand(command="menu", description="Открыть меню бота"),
        BotCommand(command="balance", description="Проверить баланс"),
        BotCommand(command="logout", description="Выйти из системы"),
        BotCommand(command="extended_search", description="Расширенный поиск")
    ]

    # Admin-only commands
    admin_commands = [
        BotCommand(command="admin", description="Панель администратора"),
        BotCommand(command="users", description="Список пользователей"),
        BotCommand(command="add_balance", description="Пополнить баланс"),
        BotCommand(command="create_user", description="Создать пользователя"),
        BotCommand(command="api_balance", description="Проверить баланс API"),
        BotCommand(command="db_status", description="Проверить структуру БД"),
        BotCommand(command="sessions_stats", description="Статистика веб-сессий")
    ]

    # Default commands for all users
    await bot.set_my_commands(user_commands, scope=BotCommandScopeDefault())

    # Extended commands for admin
    if ADMIN_ID:
        full_command_list = user_commands + admin_commands
        await bot.set_my_commands(
            full_command_list,
            scope=BotCommandScopeChat(chat_id=ADMIN_ID)
        )
        logging.info(f"Set extended commands for administrator (ID: {ADMIN_ID})")


# Background tasks
async def clear_cache_daily():
    """Daily cache cleanup"""
    while True:
        try:
            from bot.database.db import clear_old_cache
            clear_old_cache()
            logging.info("✅ Old cache (24+ hours) cleared")
        except Exception as e:
            logging.error(f"Failed to clear cache: {e}")
        await asyncio.sleep(86400)  # 24 hours


async def notify_admin_about_zero_balance():
    """Hourly notification about users with zero balance"""
    while True:
        try:
            from bot.database.db import get_users_with_zero_balance
            users = get_users_with_zero_balance()
            if users and ADMIN_ID:
                msg = "⚠ The following users have zero balance:\n"
                for login, tg_id in users:
                    msg += f"👤 {login} (ID: {tg_id})\n"
                await bot.send_message(ADMIN_ID, msg)
        except Exception as e:
            logging.error(f"Failed to notify admin about zero balance: {e}")
        await asyncio.sleep(3600)  # 1 hour


async def refresh_expired_sessions():
    """Periodically refresh expired sessions"""
    while True:
        try:
            global session_pool
            if session_pool is not None:
                await session_pool.refresh_expired_sessions()
                logging.info("Expired sessions refreshed")
            else:
                logging.warning("Cannot refresh sessions: session_pool is None")
        except Exception as e:
            logging.error(f"Error refreshing sessions: {e}")
        await asyncio.sleep(1800)  # 30 minutes


# Error handler
async def error_handler(event: ErrorEvent):
    """Global error handler for all unhandled exceptions"""
    exception = event.exception
    update = event.update

    # Log all errors
    logging.error(f"Unhandled exception: {exception}", exc_info=True)

    try:
        # Notify user if possible
        if update and hasattr(update, 'message') and update.message:
            user_id = update.message.from_user.id
            await bot.send_message(
                user_id,
                "A technical error occurred. Please try again later or contact support."
            )

            # Notify admin for critical errors
            if ADMIN_ID and isinstance(exception, (ImportError, SyntaxError, KeyError, AttributeError)):
                error_msg = f"⚠️ Critical error:\n{str(exception)[:100]}...\n\n"
                error_msg += f"From user: {user_id}\n"
                if hasattr(update.message, 'text'):
                    error_msg += f"Message: {update.message.text[:50]}"
                await bot.send_message(ADMIN_ID, error_msg)

        # Log error for analytics
        log_error(
            error_type=type(exception).__name__,
            error_message=str(exception),
            stack_trace=traceback.format_exc() if 'traceback' in sys.modules else None,
            user_id=update.message.from_user.id if update and hasattr(update, 'message') else None,
            request_data=str(update)[:200] if update else None
        )
    except Exception as e:
        logging.error(f"Error in error handler: {e}")


# Startup function
async def on_startup():
    """Initialize resources when bot starts"""
    started_at = time.monotonic()

    # Test connection to Telegram API
    try:
        me = await bot.get_me()
        logging.info(f"Successfully connected to Telegram API. Bot: {me.username}")
    except Exception as e:
        logging.critical(f"Failed to connect to Telegram API: {e}")
        sys.exit(1)

    # Validate and initialize database
    logging.info("Initializing database...")
    if not validate_db_structure():
        logging.critical("Database validation failed. Exiting.")
        sys.exit(1)
    setup_database()
    logging.info("Database initialized")

    # Initialize session pool
    global session_pool
    from bot.config import load_credentials
    credentials = load_credentials()
    logging.info(f"Loaded {len(credentials)} credentials for session pool")

    session_pool = init_session_pool(credentials, max_sessions=50)
    if session_pool is None:
        logging.error("Failed to initialize session pool")
    else:
        # Initialize sessions
        if hasattr(session_pool, 'initialize_sessions'):
            success_count, fail_count = await session_pool.initialize_sessions()
            logging.info(f"Session pool initialized: {success_count} successful, {fail_count} failed")
        else:
            logging.warning("initialize_sessions method not found, skipping initialization")

    # Register bot commands
    await register_bot_commands()

    # Start background tasks
    asyncio.create_task(clear_cache_daily())
    asyncio.create_task(notify_admin_about_zero_balance())
    asyncio.create_task(refresh_expired_sessions())
    asyncio.create_task(process_mass_search_queue(bot))

    # Register graceful shutdown handlers
    register_shutdown_handlers()

    # Measure startup time
    elapsed = time.monotonic() - started_at
    logging.info(f"Bot startup completed in {elapsed:.2f} seconds")

    # Notify admin about bot startup
    if ADMIN_ID:
        try:
            await bot.send_message(
                ADMIN_ID,
                f"✅ Bot started successfully\n"
                f"Session pool: {session_pool.get_stats()['active_sessions'] if session_pool else 0} active sessions\n"
                f"Startup time: {elapsed:.2f} seconds"
            )
        except Exception as e:
            logging.error(f"Failed to notify admin about startup: {e}")


# Main function
async def main():
    """Entry point for running the bot"""
    try:
        # Configure routers
        dp.include_router(handlers_router)
        dp.include_router(mass_search_router)
        dp.include_router(admin_router)

        # Register error handler
        dp.errors.register(error_handler)

        # Run startup procedures
        await on_startup()

        # Start polling
        logging.info("Starting bot polling...")
        await dp.start_polling(bot)
    except Exception as e:
        logging.critical(f"Fatal error during startup: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except (KeyboardInterrupt, SystemExit):
        logging.info("Bot stopped manually")
    except Exception as e:
        logging.critical(f"Unhandled exception in main process: {e}", exc_info=True)
        sys.exit(1)