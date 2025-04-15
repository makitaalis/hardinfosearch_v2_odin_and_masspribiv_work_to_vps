# bot/session_manager.py
import logging

from bot.session_pool import SessionPoolManager

# Инициализируем как None, будет установлено позже в on_startup
session_pool = None


def init_session_pool(credentials, max_sessions=50):
    """Initialize global session pool"""
    global session_pool

    # Import here to avoid circular imports
    from bot.session_pool import SessionPoolManager

    if session_pool is None:
        session_pool = SessionPoolManager(credentials, max_sessions)
        logging.info(f"Initialized session pool with {max_sessions} max sessions")
    return session_pool