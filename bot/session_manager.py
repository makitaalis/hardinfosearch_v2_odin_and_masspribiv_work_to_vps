# bot/session_manager.py
from bot.session_pool import SessionPoolManager

# Инициализируем как None, будет установлено позже в on_startup
session_pool = None

def init_session_pool(credentials, max_sessions=50):
    """Инициализирует глобальный экземпляр session_pool"""
    global session_pool
    session_pool = SessionPoolManager(credentials, max_sessions)
    return session_pool