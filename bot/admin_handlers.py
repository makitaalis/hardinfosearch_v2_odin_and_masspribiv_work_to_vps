"""
Administrator command handlers and functions for the bot.
Separated for better organization and maintainability.
"""

import asyncio
import json
import logging
import os
import sqlite3
import traceback
from datetime import datetime

from aiogram import Router, F
from aiogram.filters import Command, StateFilter
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.types import (
    Message, CallbackQuery, InlineKeyboardButton, InlineKeyboardMarkup,
    FSInputFile, ReplyKeyboardRemove
)
from aiogram.utils.keyboard import InlineKeyboardBuilder

from bot.config import ADMIN_ID, DB_PATH
from bot.keyboards import get_admin_menu, get_admin_users_keyboard
from bot.database.db import (
    get_users_paginated, add_balance, create_user, logout_all_users,
    get_mass_search_stats, logout_user, diagnose_database_structure
)
from bot.utils import get_api_balance
from bot.analytics import log_admin_action, log_error

# Create router for admin-only commands
admin_router = Router()


# Define FSM states for admin operations
class BalanceState(StatesGroup):
    waiting_for_amount = State()


class UserCreationState(StatesGroup):
    waiting_for_data = State()


class LogoutAllState(StatesGroup):
    waiting_for_message = State()
    confirming = State()


class CostState(StatesGroup):
    waiting_for_cost = State()


# Helper function to check admin permissions
async def check_admin(user_id, message=None, callback=None):
    """
    Check if user has admin permissions.

    Args:
        user_id: User ID to check
        message: Optional message object for direct response
        callback: Optional callback object for answer

    Returns:
        bool: Whether user is admin
    """
    if user_id != ADMIN_ID:
        if message:
            await message.answer("You don't have access to this function.")
        elif callback:
            await callback.answer("You don't have access to this function.", show_alert=True)
        return False
    return True


# Admin command handlers
@admin_router.message(Command("admin"))
async def cmd_admin_panel(message: Message):
    """Admin panel command handler - shows admin keyboard"""
    if not await check_admin(message.from_user.id, message):
        return

    await message.answer("🔧 Admin Panel:", reply_markup=get_admin_menu())

    # Log admin action
    log_admin_action(ADMIN_ID, "admin_panel_access")


@admin_router.message(Command("users"))
async def cmd_users(message: Message):
    """List all users paginated"""
    if not await check_admin(message.from_user.id, message):
        return

    page = 1
    users, total = get_users_paginated(page=page, page_size=5)
    if not users:
        return await message.answer("No users in the system yet.")

    resp = f"📋 User List (Page {page}):\n\n"
    for login, bal in users:
        resp += f"👤 {login} — ${bal:.2f}\n"

    has_next = (page * 5) < total
    await message.answer(resp, reply_markup=get_admin_users_keyboard(page, users, has_next))

    # Log admin action
    log_admin_action(ADMIN_ID, "view_users_list", f"page={page}, total={total}")


@admin_router.message(Command("add_balance"))
async def cmd_add_balance(message: Message):
    """Add balance to user: /add_balance login amount"""
    if not await check_admin(message.from_user.id, message):
        return

    args = message.text.strip().split()
    if len(args) != 3:
        return await message.answer("Usage: /add_balance login amount")

    _, login, amount_str = args
    try:
        amount = float(amount_str)
    except ValueError:
        return await message.answer("Amount must be a number.")

    success, info = add_balance(login, amount, admin_id=ADMIN_ID)
    await message.answer(info)

    # Log admin action
    log_admin_action(ADMIN_ID, "add_balance", f"login={login}, amount={amount}, success={success}")


@admin_router.message(Command("create_user"))
async def cmd_create_user(message: Message):
    """Create new user: /create_user login password balance"""
    if not await check_admin(message.from_user.id, message):
        return

    parts = message.text.strip().split()
    if len(parts) != 4:
        return await message.answer("Usage: /create_user login password balance")

    _, login, password, bal_str = parts
    try:
        bal = float(bal_str)
    except ValueError:
        return await message.answer("Balance must be a number.")

    ok, info = create_user(login, password, bal)
    await message.answer(info)

    # Log admin action
    log_admin_action(ADMIN_ID, "create_user", f"login={login}, balance={bal}, success={ok}")


@admin_router.message(Command("api_balance"))
async def cmd_api_balance(message: Message):
    """Check API balance (admin only)"""
    if not await check_admin(message.from_user.id, message):
        return

    success, resp = get_api_balance()
    await message.answer(resp)

    # Log admin action
    log_admin_action(ADMIN_ID, "check_api_balance", f"success={success}")


@admin_router.message(Command("db_status"))
async def cmd_db_status(message: Message):
    """Check database status and diagnose issues"""
    if not await check_admin(message.from_user.id, message):
        return

    status_message = await message.answer("⏳ Diagnosing database structure...")

    # Get diagnostic results
    result = diagnose_database_structure()
    issues = result.get("issues", [])
    warnings = result.get("warnings", [])
    repair_actions = result.get("repair_actions", [])

    # Format response
    response = f"📊 <b>Database Diagnostics</b>\n\n"

    if issues:
        response += "⚠️ <b>Issues Found:</b>\n"
        for i, issue in enumerate(issues, 1):
            response += f"{i}. {issue}\n"
        response += "\n"
    else:
        response += "✅ <b>No issues detected</b>\n\n"

    if warnings:
        response += "⚠️ <b>Warnings:</b>\n"
        for i, warning in enumerate(warnings, 1):
            response += f"{i}. {warning}\n"
        response += "\n"

    if repair_actions:
        response += "🔧 <b>Suggested Actions:</b>\n"
        for i, action in enumerate(repair_actions, 1):
            response += f"{i}. {action}\n"
        response += "\n"

    # Add statistics
    if "active_users_count" in result:
        response += f"👥 <b>Active users:</b> {result.get('active_users_count', 0)}\n"

    if "cache_entries" in result.get("stats", {}):
        response += f"💾 <b>Cache entries:</b> {result.get('stats', {}).get('cache_entries', 0)}\n"

    # Add database file info
    db_size_mb = result.get("stats", {}).get("file_size_mb", 0)
    if db_size_mb:
        response += f"📁 <b>Database size:</b> {db_size_mb:.2f} MB\n"

    await status_message.edit_text(response, parse_mode="HTML")

    # Log admin action
    log_admin_action(ADMIN_ID, "database_diagnostics", f"issues={len(issues)}, warnings={len(warnings)}")


@admin_router.message(Command("sessions_stats"))
async def cmd_sessions_stats(message: Message):
    """Session pool statistics (admin only)"""
    if not await check_admin(message.from_user.id, message):
        return

    # Import session pool
    from bot.session_manager import session_pool

    if not session_pool:
        return await message.answer("Session pool is not initialized.")

    # Get stats from session pool
    stats = session_pool.get_stats()

    # Format stat message
    text = (
        f"📊 <b>Session Pool Statistics</b>\n\n"
        f"<b>Sessions:</b>\n"
        f"• Total sessions: {stats['total_sessions']}\n"
        f"• Active authenticated: {stats['active_sessions']}\n"
        f"• Busy (in use): {stats['busy_sessions']}\n"
        f"• Error sessions: {stats.get('error_sessions', 0)}\n\n"

        f"<b>Mass Searches:</b>\n"
        f"• Active operations: {stats['active_mass_searches']}\n"
        f"• Load balancing: {stats.get('current_strategy', 'N/A')}\n\n"

        f"<b>Search Statistics:</b>\n"
        f"• Total searches: {stats['searches']['total_searches']}\n"
        f"• Successful: {stats['searches']['successful_searches']}\n"
        f"• Failed: {stats['searches']['failed_searches']}\n"
        f"• Re-authentications: {stats['searches']['reauth_count']}\n\n"
    )

    # Add session details if requested
    args = message.text.strip().split()
    if len(args) > 1 and args[1].lower() == "detailed":
        # Add session list (max 5 sessions)
        text += "<b>Session Details (sample):</b>\n"
        for i, session_stats in enumerate(stats['sessions'][:5]):
            text += (
                f"• {session_stats['session_id']}: "
                f"searches={session_stats['search_count']}, "
                f"errors={session_stats['error_count']}, "
                f"auth={session_stats['is_authenticated']}\n"
            )
        if len(stats['sessions']) > 5:
            text += f"<i>...and {len(stats['sessions']) - 5} more sessions</i>\n"

    await message.answer(text, parse_mode="HTML")

    # Log admin action
    log_admin_action(ADMIN_ID, "view_session_stats")


# Admin menu button handlers
@admin_router.message(F.text == "📋 Список пользователей")
async def admin_list_users(message: Message):
    """Admin menu handler for user list"""
    if not await check_admin(message.from_user.id, message):
        return

    # Use existing user list command
    await cmd_users(message)


@admin_router.message(F.text == "➕ Добавить пользователя")
async def cmd_add_user_menu(message: Message, state: FSMContext):
    """Admin menu handler for adding user"""
    if not await check_admin(message.from_user.id, message):
        return

    # Set state for waiting user data
    await state.set_state(UserCreationState.waiting_for_data)
    await message.answer("Enter new user data in format:\n`login password balance`")


@admin_router.message(F.text == "💰 Пополнить баланс")
async def admin_add_balance_prompt(message: Message):
    """Admin menu handler for adding balance"""
    if not await check_admin(message.from_user.id, message):
        return

    await message.answer(
        "Enter command in format:\n"
        "`/add_balance login amount`\n\n"
        "Example: `/add_balance user123 25.50`"
    )


@admin_router.message(F.text == "📊 Баланс API")
async def admin_api_balance(message: Message):
    """Admin menu handler for API balance"""
    if not await check_admin(message.from_user.id, message):
        return

    await cmd_api_balance(message)


@admin_router.message(F.text == "📊 Статистика очереди")
async def admin_queue_stats(message: Message):
    """Admin menu handler for queue statistics"""
    if not await check_admin(message.from_user.id, message):
        return

    # Get queue statistics
    from bot.common import mass_search_queue
    queue_status = await mass_search_queue.get_queue_status()

    # Get mass search statistics from database
    db_stats = get_mass_search_stats()

    # Format message
    text = (
        f"📊 <b>Mass Search Statistics</b>\n\n"
        f"<b>Current Queue:</b>\n"
        f"• Total in queue: {queue_status['total']}\n"
        f"• Active processing: {queue_status['processing']}/{queue_status['capacity']}\n"
        f"• Waiting: {queue_status['waiting']}\n\n"

        f"<b>Overall Statistics:</b>\n"
        f"• Total searches: {db_stats['total']}\n"
        f"• Completed successfully: {db_stats['completed']}\n"
        f"• Completed with errors: {db_stats['failed']}\n"
        f"• Phones found: {db_stats['phones_found']}\n"
        f"• Average time: {int(db_stats['avg_time'] or 0)} sec\n\n"
    )

    # Show current active requests
    active_items = await mass_search_queue.get_all_items()
    processing_items = [item for item in active_items if item.processing]

    if processing_items:
        text += "<b>Active Requests:</b>\n"
        for i, item in enumerate(processing_items, 1):
            text += f"{i}. ID: {item.user_id}, lines: {item.valid_lines}\n"

    # Show waiting requests
    waiting_items = [item for item in active_items if not item.processing]
    if waiting_items:
        text += "\n<b>Waiting Requests:</b>\n"
        for i, item in enumerate(waiting_items, 1):
            text += f"{i}. ID: {item.user_id}, lines: {item.valid_lines}\n"

    # Show recent searches
    if db_stats["recent"]:
        text += "\n<b>Recent Searches:</b>\n"
        for i, item in enumerate(db_stats["recent"], 1):
            status_emoji = {
                "pending": "⏳",
                "processing": "🔄",
                "completed": "✅",
                "failed": "❌"
            }.get(item["status"], "⚪️")

            text += (
                f"{i}. {status_emoji} ID: {item['user_id']}, "
                f"lines: {item['valid_lines']}, "
                f"phones: {item['phones_found'] or 0}\n"
            )

    # Add update timestamp
    text += f"\n🕒 <b>Updated:</b> {datetime.now().strftime('%d.%m.%Y %H:%M:%S')}"

    await message.answer(text, parse_mode="HTML")

    # Log admin action
    log_admin_action(ADMIN_ID, "view_queue_stats",
                     f"processing={queue_status['processing']}, waiting={queue_status['waiting']}")


@admin_router.message(F.text == "⚙️ Настройки")
async def admin_settings(message: Message):
    """Admin menu handler for settings"""
    if not await check_admin(message.from_user.id, message):
        return

    # Create settings keyboard
    builder = InlineKeyboardBuilder()
    builder.add(InlineKeyboardButton(text="🔄 Clear Cache", callback_data="clear_cache"))
    builder.add(InlineKeyboardButton(text="📤 Database Backup", callback_data="backup_db"))
    builder.row(InlineKeyboardButton(text="🔧 Change Request Cost", callback_data="change_cost"))
    builder.row(InlineKeyboardButton(text="🔍 Diagnose Database", callback_data="diagnose_db"))

    await message.answer(
        "⚙️ <b>System Settings</b>\n\n"
        "Choose an action:",
        reply_markup=builder.as_markup(),
        parse_mode="HTML"
    )


@admin_router.message(F.text == "🚪 Выйти")
async def admin_logout(message: Message):
    """Admin menu handler for logout"""
    if not await check_admin(message.from_user.id, message):
        return

    # Log user out
    logout_user(message.from_user.id)

    # Remove keyboard
    await message.answer(
        "You've been logged out.",
        reply_markup=ReplyKeyboardRemove()
    )

    # Log admin action
    log_admin_action(ADMIN_ID, "admin_logout")


@admin_router.message(F.text == "⚠️ Разлогинить всех")
async def admin_logout_all_users(message: Message, state: FSMContext):
    """Admin menu handler for logging out all users"""
    if not await check_admin(message.from_user.id, message):
        return

    # Get count of active users
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    try:
        # Check if session_active field exists in users table
        cursor.execute("PRAGMA table_info(users)")
        columns = [info[1] for info in cursor.fetchall()]

        if 'session_active' in columns:
            cursor.execute("SELECT COUNT(*) FROM users WHERE session_active=1 AND telegram_id != ?", (ADMIN_ID,))
        else:
            # If no session_active field, check active_sessions table
            cursor.execute("SELECT COUNT(*) FROM active_sessions WHERE is_active=1 AND telegram_id != ?", (ADMIN_ID,))

        active_count = cursor.fetchone()[0]
    except sqlite3.OperationalError as e:
        logging.error(f"Error checking active users: {e}")
        active_count = 0
    finally:
        conn.close()

    # Set state for message input
    await state.set_state(LogoutAllState.waiting_for_message)

    # Create cancel button
    cancel_kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="❌ Cancel Operation", callback_data="cancel_logout_all")]
    ])

    # Ask for confirmation message
    await message.answer(
        f"⚠️ <b>WARNING!</b> You are about to log out <b>all users</b> ({active_count} active).\n\n"
        "Enter a message to send to users before logging them out, "
        "so they know they need to log back in:\n\n"
        "<i>Example: Dear users! Due to technical updates, "
        "please log back into the system. We apologize for the inconvenience.</i>\n\n"
        "To cancel, press the button below or write 'cancel'.",
        parse_mode="HTML",
        reply_markup=cancel_kb
    )

    # Log admin action
    log_admin_action(ADMIN_ID, "mass_logout_initiated", f"active_users={active_count}")


# Admin callback handlers
@admin_router.callback_query(F.data.startswith("prev_page_") | F.data.startswith("next_page_"))
async def cb_paginate(callback: CallbackQuery, state: FSMContext):
    """Handle pagination for user list"""
    if not await check_admin(callback.from_user.id, callback=callback):
        return

    try:
        page = int(callback.data.split("_")[-1])
        if page < 1:
            page = 1

        users, total = get_users_paginated(page=page, page_size=5)
        if not users:
            await callback.message.edit_text("No users on this page.")
            return await callback.answer()

        resp = f"📋 User List (Page {page}):\n\n"
        for login, bal in users:
            resp += f"👤 {login} — ${bal:.2f}\n"
        has_next = (page * 5) < total

        await callback.message.edit_text(resp, reply_markup=get_admin_users_keyboard(page, users, has_next))
        await callback.answer()
    except ValueError:
        logging.error(f"Error parsing page number: {callback.data}")
        await callback.answer("Error navigating to page", show_alert=True)
    except Exception as e:
        logging.error(f"Error in pagination: {e}")
        await callback.answer("An error occurred. Please try again.", show_alert=True)


@admin_router.callback_query(F.data.startswith("add_balance_"))
async def cb_add_balance_user(callback: CallbackQuery, state: FSMContext):
    """Handle add balance button for specific user"""
    if not await check_admin(callback.from_user.id, callback=callback):
        return

    try:
        login = callback.data.split("_", 2)[-1]
        if not login:
            raise ValueError("Login not found")

        # Verify user exists
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        cursor.execute("SELECT login FROM users WHERE login = ?", (login,))
        if not cursor.fetchone():
            conn.close()
            await callback.answer("User not found", show_alert=True)
            return
        conn.close()

        await state.update_data(user_login=login)
        await state.set_state(BalanceState.waiting_for_amount)
        await callback.message.answer(f"Enter amount to add to user `{login}`'s balance:")
        await callback.answer()
    except Exception as e:
        logging.error(f"Error processing add_balance callback: {e}")
        await callback.answer("An error occurred. Please try again.", show_alert=True)


@admin_router.callback_query(F.data == "clear_cache")
async def cb_clear_cache(callback: CallbackQuery):
    """Handle clear cache button"""
    if not await check_admin(callback.from_user.id, callback=callback):
        return

    try:
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()

        # Get current cache count
        cursor.execute("SELECT COUNT(*) FROM cache")
        before_count = cursor.fetchone()[0]

        # Clear cache
        cursor.execute("DELETE FROM cache")
        conn.commit()
        conn.close()

        await callback.message.answer(f"✅ Cache cleared. Deleted {before_count} entries.")
        await callback.answer()

        # Log admin action
        log_admin_action(ADMIN_ID, "clear_cache", f"entries_deleted={before_count}")
    except Exception as e:
        logging.error(f"Error clearing cache: {e}")
        await callback.answer("Error clearing cache", show_alert=True)


@admin_router.callback_query(F.data == "backup_db")
async def cb_backup_db(callback: CallbackQuery):
    """Handle database backup button"""
    if not await check_admin(callback.from_user.id, callback=callback):
        return

    try:
        from bot.database.backup import backup_database
        backup_file = backup_database()

        if backup_file:
            await callback.message.answer(f"✅ Backup created: {backup_file}")

            # Send backup file if it's small enough
            if os.path.getsize(backup_file) < 50 * 1024 * 1024:  # Less than 50MB
                await callback.message.answer_document(FSInputFile(backup_file))
            else:
                await callback.message.answer("Backup file is too large to send directly. Access it on the server.")
        else:
            await callback.message.answer("❌ Failed to create backup.")

        await callback.answer()

        # Log admin action
        log_admin_action(ADMIN_ID, "database_backup", f"file={backup_file}")
    except Exception as e:
        logging.error(f"Error creating backup: {e}")
        await callback.answer("Error creating backup", show_alert=True)


@admin_router.callback_query(F.data == "change_cost")
async def cb_change_cost(callback: CallbackQuery, state: FSMContext):
    """Handle change request cost button"""
    if not await check_admin(callback.from_user.id, callback=callback):
        return

    await state.set_state(CostState.waiting_for_cost)

    # Get current request cost
    from bot.config import REQUEST_COST
    await callback.message.answer(f"Current request cost: ${REQUEST_COST:.2f}\nEnter new cost:")
    await callback.answer()


@admin_router.callback_query(F.data == "diagnose_db")
async def cb_diagnose_db(callback: CallbackQuery):
    """Handle diagnose database button"""
    if not await check_admin(callback.from_user.id, callback=callback):
        return

    # Run database diagnostics
    await callback.answer("Running diagnostics...")

    # Use the existing db_status command
    msg = Message(
        message_id=0,
        date=datetime.now(),
        chat=callback.message.chat,
        from_user=callback.from_user
    )
    await cmd_db_status(msg)


@admin_router.callback_query(F.data == "cancel_logout_all")
async def cancel_logout_all(callback: CallbackQuery, state: FSMContext):
    """Handle cancel button for mass logout"""
    if not await check_admin(callback.from_user.id, callback=callback):
        return

    current_state = await state.get_state()
    if current_state in ["LogoutAllState:waiting_for_message", "LogoutAllState:confirming"]:
        await state.clear()
        await callback.message.edit_text(
            "❌ Mass logout operation canceled.",
            reply_markup=None
        )

        # Log admin action
        log_admin_action(ADMIN_ID, "mass_logout_canceled")
    else:
        await callback.answer("No active logout operation.", show_alert=True)


@admin_router.callback_query(F.data == "confirm_logout_all", StateFilter(LogoutAllState.confirming))
async def confirm_logout_all(callback: CallbackQuery, state: FSMContext):
    """Execute mass logout after confirmation"""
    if not await check_admin(callback.from_user.id, callback=callback):
        await state.clear()
        return

    # Get notification message
    data = await state.get_data()
    notification_text = data.get("notification_text", "System notification: you need to log back into the system.")

    # Start logout process
    await callback.message.edit_text(
        "🔄 <b>Starting user logout process...</b>",
        parse_mode="HTML",
        reply_markup=None
    )

    # Execute mass logout
    logging.info("Starting mass user logout")

    success, users, users_updated = logout_all_users(admin_id=ADMIN_ID)

    logging.info(f"Logout result: success={success}, user IDs={users}, updated records={users_updated}")

    # Handle errors
    if not success:
        await callback.message.edit_text(
            "❌ <b>Error logging out users.</b>\n\n"
            "Check error log for details.",
            parse_mode="HTML"
        )
        await state.clear()
        return

    # Handle no sessions found
    if users_updated == 0:
        await callback.message.edit_text(
            "ℹ️ <b>No active users found to log out.</b>\n\n"
            "Users are already logged out or using a different session storage method.",
            parse_mode="HTML"
        )
        await state.clear()
        return

    # Send notifications to users
    message_count = 0
    success_count = 0
    error_count = 0

    progress_message = await callback.message.edit_text(
        f"🔄 <b>Sending notifications to users...</b>\n\n"
        f"• Users logged out: <b>{users_updated}</b>\n"
        f"• Users to notify: <b>{len(users)}</b>\n"
        f"• Notifications sent: <b>0/{len(users)}</b>",
        parse_mode="HTML"
    )

    # Check if there are users to notify
    if not users:
        await progress_message.edit_text(
            f"✅ <b>Operation partially complete</b>\n\n"
            f"• Users logged out: <b>{users_updated}</b>\n"
            f"• <i>No user IDs found for notifications</i>\n\n"
            f"<i>Users logged out but notifications not sent.</i>",
            parse_mode="HTML"
        )
        await state.clear()
        return

    # Send messages with progress updates
    for idx, user_telegram_id in enumerate(users):
        # Validate user ID
        if not user_telegram_id or not isinstance(user_telegram_id, int) or user_telegram_id <= 0:
            logging.warning(f"Skipping invalid user ID: {user_telegram_id}")
            error_count += 1
            continue

        try:
            # Send message with timeout
            try:
                await asyncio.wait_for(
                    callback.bot.send_message(
                        chat_id=user_telegram_id,
                        text=f"🔔 <b>System Notification</b>\n\n{notification_text}",
                        parse_mode="HTML"
                    ),
                    timeout=5.0  # 5 second timeout
                )
                message_count += 1
                success_count += 1
                logging.info(f"Notification sent to user {user_telegram_id}")
            except asyncio.TimeoutError:
                error_count += 1
                logging.error(f"Timeout sending message to user {user_telegram_id}")

            # Update progress every 5 users or at the end
            if (idx + 1) % 5 == 0 or idx == len(users) - 1:
                await progress_message.edit_text(
                    f"🔄 <b>Sending notifications to users...</b>\n\n"
                    f"• Users logged out: <b>{users_updated}</b>\n"
                    f"• Notifications sent: <b>{message_count}/{len(users)}</b>\n"
                    f"• Progress: <b>{(idx + 1) * 100 // len(users)}%</b>",
                    parse_mode="HTML"
                )

            # Small pause between sends
            await asyncio.sleep(0.3)
        except Exception as e:
            error_count += 1
            logging.error(f"Error sending message to user {user_telegram_id}: {e}", exc_info=True)

    # Clear state
    await state.clear()

    # Send final report
    await progress_message.edit_text(
        f"✅ <b>Operation successfully completed</b>\n\n"
        f"• Users logged out: <b>{users_updated}</b>\n"
        f"• Notifications sent: <b>{success_count}/{len(users)}</b>\n"
        f"• Successful: <b>{success_count}</b>\n"
        f"• Errors: <b>{error_count}</b>\n\n"
        f"<i>Note: you were not logged out.</i>",
        parse_mode="HTML"
    )

    # Log admin action
    log_admin_action(ADMIN_ID, "mass_logout_completed",
                     f"logged_out={users_updated}, notified={success_count}, errors={error_count}")


# State handlers
@admin_router.message(StateFilter(BalanceState.waiting_for_amount))
async def process_balance_amount(message: Message, state: FSMContext):
    """Process balance amount input"""
    if not await check_admin(message.from_user.id, message):
        await state.clear()
        return

    data = await state.get_data()
    login = data.get("user_login")
    try:
        amount = float(message.text)
        if amount <= 0:
            return await message.answer("Please enter a valid amount (number > 0).")
    except ValueError:
        return await message.answer("Please enter a valid amount (number).")

    success, info = add_balance(login, amount, admin_id=message.from_user.id)
    await message.answer(info)
    await state.clear()

    # Log admin action
    log_admin_action(ADMIN_ID, "add_balance_via_menu", f"login={login}, amount={amount}, success={success}")


@admin_router.message(StateFilter(UserCreationState.waiting_for_data))
async def process_user_creation(message: Message, state: FSMContext):
    """Process user creation data input"""
    if not await check_admin(message.from_user.id, message):
        await state.clear()
        return

    parts = message.text.strip().split()
    if len(parts) != 3:
        return await message.answer("Invalid format. Use: `login password balance`")

    login, password, bal_str = parts
    try:
        bal = float(bal_str)
        if bal < 0:
            return await message.answer("Balance cannot be negative.")
    except ValueError:
        return await message.answer("Balance must be a number.")

    # Create user
    ok, info = create_user(login, password, bal)
    await message.answer(info)
    await state.clear()

    # Log admin action
    log_admin_action(ADMIN_ID, "create_user_via_menu", f"login={login}, balance={bal}, success={ok}")


@admin_router.message(StateFilter(LogoutAllState.waiting_for_message))
async def process_logout_message(message: Message, state: FSMContext):
    """Process message for users before logging them out"""
    if not await check_admin(message.from_user.id, message):
        await state.clear()
        return

    # Check for cancellation
    if message.text.lower() in ["cancel", "отмена", "отменить"]:
        await state.clear()
        await message.answer("❌ Mass logout operation canceled.")

        # Log admin action
        log_admin_action(ADMIN_ID, "mass_logout_canceled")
        return

    # Save message text in state
    await state.update_data(notification_text=message.text)

    # Request final confirmation
    confirm_kb = InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="✅ Confirm", callback_data="confirm_logout_all"),
            InlineKeyboardButton(text="❌ Cancel", callback_data="cancel_logout_all")
        ]
    ])

    await state.set_state(LogoutAllState.confirming)

    await message.answer(
        "⚠️ <b>FINAL WARNING!</b>\n\n"
        "Are you sure you want to log out ALL users?\n"
        "Users will receive the following message:\n\n"
        f"<code>{message.text}</code>\n\n"
        "Confirm action:",
        parse_mode="HTML",
        reply_markup=confirm_kb
    )


@admin_router.message(StateFilter(CostState.waiting_for_cost))
async def process_new_cost(message: Message, state: FSMContext):
    """Process new request cost input"""
    if not await check_admin(message.from_user.id, message):
        await state.clear()
        return

    try:
        new_cost = float(message.text)
        if new_cost <= 0:
            await message.answer("Cost must be greater than zero. Try again:")
            return

        # Update global variable
        from bot.config import REQUEST_COST
        old_cost = REQUEST_COST

        # Import for updating value
        import bot.config
        bot.config.REQUEST_COST = new_cost

        # Save new value to .env file
        try:
            import os
            import re

            # Path to .env file
            env_path = ".env"

            # Read file content
            if os.path.exists(env_path):
                with open(env_path, 'r') as file:
                    content = file.read()

                # Check if REQUEST_COST line exists
                if re.search(r'^REQUEST_COST=', content, re.MULTILINE):
                    # Update existing value
                    content = re.sub(r'^REQUEST_COST=.*', f'REQUEST_COST={new_cost}', content, flags=re.MULTILINE)
                else:
                    # Add new line
                    content += f'\nREQUEST_COST={new_cost}'

                # Write updated content
                with open(env_path, 'w') as file:
                    file.write(content)

                await message.answer(
                    f"✅ Request cost changed from ${old_cost:.2f} to ${new_cost:.2f} and saved in configuration."
                )
            else:
                # If file doesn't exist, create it
                with open(env_path, 'w') as file:
                    file.write(f'REQUEST_COST={new_cost}')

                await message.answer(
                    f"✅ Request cost changed from ${old_cost:.2f} to ${new_cost:.2f} and saved in new configuration file."
                )

            # Log admin action
            log_admin_action(ADMIN_ID, "change_request_cost", f"old={old_cost}, new={new_cost}")

        except Exception as e:
            logging.error(f"Error saving settings: {e}")
            await message.answer(
                f"✅ Request cost changed from ${old_cost:.2f} to ${new_cost:.2f}, but failed to save in configuration."
            )

        await state.clear()
    except ValueError:
        await message.answer("Enter a valid number. Example: 0.05")