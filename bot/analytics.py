"""
Enhanced analytics and logging module for the bot.
Provides structured logging for various events and metrics.
"""

import json
import logging
import sqlite3
import time
import traceback
from datetime import datetime
from typing import Dict, Optional, Any, Union

from bot.config import DB_PATH


def create_analytics_tables():
    """
    Creates necessary analytics tables in the database.
    """
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    try:
        # System info table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS system_info (
                key TEXT PRIMARY KEY,
                value TEXT,
                updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
            )
        ''')

        # Request logs table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS request_logs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER,
                query TEXT,
                request_type TEXT,
                source TEXT,
                success INTEGER,
                execution_time REAL,
                response_size INTEGER,
                timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY(user_id) REFERENCES users(telegram_id)
            )
        ''')

        # Financial logs table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS financial_logs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER,
                operation_type TEXT,
                amount REAL,
                balance_before REAL,
                balance_after REAL,
                admin_id INTEGER,
                comment TEXT,
                timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY(user_id) REFERENCES users(telegram_id)
            )
        ''')

        # Error logs table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS error_logs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                error_type TEXT,
                error_message TEXT,
                stack_trace TEXT,
                user_id INTEGER,
                request_data TEXT,
                timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
            )
        ''')

        # User event logs table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS user_event_logs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER,
                event_type TEXT,
                event_data TEXT,
                ip_address TEXT,
                timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY(user_id) REFERENCES users(telegram_id)
            )
        ''')

        # Admin action logs table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS admin_logs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                admin_id INTEGER,
                action_type TEXT,
                details TEXT,
                timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
            )
        ''')

        # Performance metrics table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS performance_metrics (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                metric_name TEXT,
                metric_value REAL,
                context TEXT,
                timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
            )
        ''')

        # Add indexes for better performance
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_request_logs_user_id ON request_logs(user_id)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_request_logs_timestamp ON request_logs(timestamp)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_financial_logs_user_id ON financial_logs(user_id)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_user_event_logs_user_id ON user_event_logs(user_id)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_user_event_logs_event_type ON user_event_logs(event_type)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_error_logs_error_type ON error_logs(error_type)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_performance_metrics_name ON performance_metrics(metric_name)')

        conn.commit()
        logging.info("Analytics tables successfully created")

        return True, "Analytics tables successfully created"
    except Exception as e:
        conn.rollback()
        logging.error(f"Error creating analytics tables: {e}")
        return False, f"Error creating analytics tables: {str(e)}"
    finally:
        conn.close()


def log_request(user_id: int, query: str, request_type: str, source: str,
                success: bool, execution_time: float, response_size: int):
    """
    Logs a search request for analytics.

    Args:
        user_id: User's Telegram ID
        query: Search query text
        request_type: Type of request (web, api, etc.)
        source: Source of the request (web, bot, etc.)
        success: Whether the request was successful
        execution_time: Time taken to execute the request (seconds)
        response_size: Size of the response data (bytes)
    """
    try:
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()

        cursor.execute('''
            INSERT INTO request_logs (
                user_id, query, request_type, source, success, execution_time, response_size
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
        ''', (
            user_id, query, request_type, source, 1 if success else 0, execution_time, response_size
        ))

        conn.commit()
    except Exception as e:
        logging.error(f"Error logging request: {e}")
    finally:
        if conn:
            conn.close()

    # Also log to standard log for backup
    logging.info(f"Request: user_id={user_id}, query={query}, type={request_type}, "
                 f"source={source}, success={success}, time={execution_time:.2f}s")


def log_financial_operation(user_id: int, operation_type: str, amount: float,
                            balance_before: float, balance_after: float,
                            admin_id: Optional[int] = None, comment: Optional[str] = None):
    """
    Logs a financial operation for tracking and auditing.

    Args:
        user_id: User's Telegram ID
        operation_type: Type of operation (deduct, add_balance, refund, etc.)
        amount: Amount of the transaction
        balance_before: User's balance before the operation
        balance_after: User's balance after the operation
        admin_id: Admin ID if operation was performed by an admin
        comment: Optional comment about the operation
    """
    try:
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()

        cursor.execute('''
            INSERT INTO financial_logs (
                user_id, operation_type, amount, balance_before, balance_after, admin_id, comment
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
        ''', (
            user_id, operation_type, amount, balance_before, balance_after, admin_id, comment
        ))

        conn.commit()
    except Exception as e:
        logging.error(f"Error logging financial operation: {e}")
    finally:
        if conn:
            conn.close()

    # Also log to standard log for backup
    logging.info(f"Financial operation: user_id={user_id}, type={operation_type}, "
                 f"amount={amount}, balance before={balance_before}, balance after={balance_after}")


def log_error(error_type: str, error_message: str, stack_trace: Optional[str] = None,
              user_id: Optional[int] = None, request_data: Optional[str] = None):
    """
    Logs an error for debugging and analysis.

    Args:
        error_type: Type/category of error
        error_message: Error message
        stack_trace: Optional stack trace
        user_id: User ID if error occurred during user request
        request_data: Request data that caused the error
    """
    try:
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()

        cursor.execute('''
            INSERT INTO error_logs (
                error_type, error_message, stack_trace, user_id, request_data
            ) VALUES (?, ?, ?, ?, ?)
        ''', (
            error_type, error_message, stack_trace, user_id, request_data
        ))

        conn.commit()
    except Exception as e:
        logging.error(f"Error logging error: {e}")
    finally:
        if conn:
            conn.close()

    # Also log to standard log for backup
    logging.error(f"Error: {error_type}, message={error_message}")
    if stack_trace:
        logging.error(f"Stack trace: {stack_trace}")
    if user_id:
        logging.error(f"From user: {user_id}")


def log_user_event(user_id: int, event_type: str, event_data: Optional[Union[str, Dict]] = None,
                   ip_address: Optional[str] = None):
    """
    Logs a user event for tracking user behavior.

    Args:
        user_id: User's Telegram ID
        event_type: Type of event (login, logout, etc.)
        event_data: Optional event data (as JSON string or dict)
        ip_address: User's IP address if available
    """
    try:
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()

        # Convert dict to JSON string if needed
        if isinstance(event_data, dict):
            event_data = json.dumps(event_data)

        cursor.execute('''
            INSERT INTO user_event_logs (
                user_id, event_type, event_data, ip_address
            ) VALUES (?, ?, ?, ?)
        ''', (
            user_id, event_type, event_data, ip_address
        ))

        conn.commit()
    except Exception as e:
        logging.error(f"Error logging user event: {e}")
    finally:
        if conn:
            conn.close()

    # Also log to standard log for backup
    log_msg = f"User event: user_id={user_id}, type={event_type}"
    if event_data:
        log_msg += f", data={event_data}"
    logging.info(log_msg)


def log_admin_action(admin_id: int, action_type: str, details: Optional[str] = None):
    """
    Logs an admin action for auditing and monitoring.

    Args:
        admin_id: Admin's Telegram ID
        action_type: Type of admin action
        details: Optional details about the action
    """
    try:
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()

        cursor.execute('''
            INSERT INTO admin_logs (
                admin_id, action_type, details
            ) VALUES (?, ?, ?)
        ''', (
            admin_id, action_type, details
        ))

        conn.commit()
    except Exception as e:
        logging.error(f"Error logging admin action: {e}")
    finally:
        if conn:
            conn.close()

    # Also log to standard log for backup
    logging.info(f"Admin action: admin_id={admin_id}, type={action_type}, details={details}")


def log_performance_metric(metric_name: str, metric_value: float, context: Optional[Dict[str, Any]] = None):
    """
    Logs a performance metric for monitoring system performance.

    Args:
        metric_name: Name of the metric
        metric_value: Value of the metric
        context: Optional context data
    """
    try:
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()

        context_json = json.dumps(context) if context else None

        cursor.execute('''
            INSERT INTO performance_metrics (
                metric_name, metric_value, context
            ) VALUES (?, ?, ?)
        ''', (
            metric_name, metric_value, context_json
        ))

        conn.commit()
    except Exception as e:
        logging.error(f"Error logging performance metric: {e}")
    finally:
        if conn:
            conn.close()

    # Also log to standard log
    context_str = f", context={context}" if context else ""
    logging.info(f"Performance metric: {metric_name}={metric_value}{context_str}")


def get_user_activity_stats(user_id: Optional[int] = None, days: int = 30):
    """
    Gets user activity statistics for the specified period.

    Args:
        user_id: Optional user ID to filter for a specific user
        days: Number of days to look back

    Returns:
        dict: Activity statistics
    """
    try:
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()

        # Base query condition
        condition = "WHERE timestamp >= datetime('now', '-{} days')".format(days)
        params = []

        # Add user filter if specified
        if user_id is not None:
            condition += " AND user_id = ?"
            params.append(user_id)

        # Get search request counts
        cursor.execute(f'''
            SELECT 
                COUNT(*) as total_requests,
                SUM(CASE WHEN success = 1 THEN 1 ELSE 0 END) as successful_requests,
                AVG(execution_time) as avg_execution_time
            FROM request_logs
            {condition}
        ''', params)

        request_stats = cursor.fetchone()

        # Get user event counts
        cursor.execute(f'''
            SELECT event_type, COUNT(*) as count
            FROM user_event_logs
            {condition}
            GROUP BY event_type
        ''', params)

        event_stats = {row[0]: row[1] for row in cursor.fetchall()}

        # Get financial operation totals
        cursor.execute(f'''
            SELECT 
                operation_type,
                COUNT(*) as count,
                SUM(amount) as total_amount
            FROM financial_logs
            {condition}
            GROUP BY operation_type
        ''', params)

        financial_stats = {
            row[0]: {
                'count': row[1],
                'total_amount': row[2]
            } for row in cursor.fetchall()
        }

        conn.close()

        return {
            'request_stats': {
                'total': request_stats[0] if request_stats[0] else 0,
                'successful': request_stats[1] if request_stats[1] else 0,
                'avg_execution_time': request_stats[2] if request_stats[2] else 0
            },
            'event_stats': event_stats,
            'financial_stats': financial_stats
        }
    except Exception as e:
        logging.error(f"Error getting user activity stats: {e}")
        return {
            'request_stats': {'total': 0, 'successful': 0, 'avg_execution_time': 0},
            'event_stats': {},
            'financial_stats': {}
        }


def get_system_metrics(days: int = 7):
    """
    Gets system-wide performance metrics.

    Args:
        days: Number of days to look back

    Returns:
        dict: System metrics
    """
    try:
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()

        # Get average response times by day
        cursor.execute('''
            SELECT 
                DATE(timestamp) as date,
                AVG(execution_time) as avg_time,
                COUNT(*) as request_count
            FROM request_logs
            WHERE timestamp >= datetime('now', '-{} days')
            GROUP BY DATE(timestamp)
            ORDER BY date
        '''.format(days))

        daily_performance = [
            {
                'date': row[0],
                'avg_time': row[1],
                'request_count': row[2]
            } for row in cursor.fetchall()
        ]

        # Get error counts by type
        cursor.execute('''
            SELECT 
                error_type,
                COUNT(*) as count
            FROM error_logs
            WHERE timestamp >= datetime('now', '-{} days')
            GROUP BY error_type
            ORDER BY count DESC
            LIMIT 10
        '''.format(days))

        error_counts = {row[0]: row[1] for row in cursor.fetchall()}

        # Get performance metrics averages
        cursor.execute('''
            SELECT 
                metric_name,
                AVG(metric_value) as avg_value
            FROM performance_metrics
            WHERE timestamp >= datetime('now', '-{} days')
            GROUP BY metric_name
        '''.format(days))

        performance_metrics = {row[0]: row[1] for row in cursor.fetchall()}

        conn.close()

        return {
            'daily_performance': daily_performance,
            'error_counts': error_counts,
            'performance_metrics': performance_metrics
        }
    except Exception as e:
        logging.error(f"Error getting system metrics: {e}")
        return {
            'daily_performance': [],
            'error_counts': {},
            'performance_metrics': {}
        }