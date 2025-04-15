"""
Database backup and restoration module with compression, rotation, and validation.
"""

import gzip
import os
import shutil
import sqlite3
import time
import logging
from datetime import datetime
from pathlib import Path

# Database file path
from bot.config import DB_PATH

BACKUP_DIR = "database/backups"
MAX_BACKUPS = 10  # Maximum number of backups to keep


def backup_database(backup_dir=None, compress=True):
    """
    Creates a backup of the database file with timestamp.

    Args:
        backup_dir: Optional custom backup directory
        compress: Whether to compress the backup with gzip

    Returns:
        str: Path to the created backup file or None on failure
    """
    # Use specified backup dir or default
    backup_dir = backup_dir or BACKUP_DIR

    # Ensure backup directory exists
    Path(backup_dir).mkdir(parents=True, exist_ok=True)

    # Check if database file exists
    if not os.path.exists(DB_PATH):
        logging.error("Database file not found, nothing to back up.")
        return None

    try:
        # Generate timestamp for the filename
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

        # Determine backup file path
        backup_file = os.path.join(backup_dir, f"bot_backup_{timestamp}")
        if compress:
            backup_file += ".gz"
        else:
            backup_file += ".db"

        # Create a database backup with proper locking
        conn = sqlite3.connect(DB_PATH)

        if compress:
            # Compressed backup using gzip
            with gzip.open(backup_file, 'wb') as f_out:
                for line in conn.iterdump():
                    f_out.write(f"{line}\n".encode('utf-8'))
        else:
            # Simple file copy backup
            dest = open(backup_file, 'w')
            for line in conn.iterdump():
                dest.write(f"{line}\n")
            dest.close()

        conn.close()

        # Verify backup is valid
        if os.path.exists(backup_file) and os.path.getsize(backup_file) > 0:
            logging.info(f"Database backup created: {backup_file}")

            # Clean up old backups if we have too many
            rotate_backups(backup_dir)

            return backup_file
        else:
            logging.error("Created backup file is empty or missing")
            return None

    except Exception as e:
        logging.error(f"Error creating database backup: {e}")
        return None


def rotate_backups(backup_dir=None):
    """
    Removes oldest backups if number exceeds MAX_BACKUPS.

    Args:
        backup_dir: Optional custom backup directory
    """
    backup_dir = backup_dir or BACKUP_DIR

    try:
        # Get list of backup files
        backup_files = []
        for file in os.listdir(backup_dir):
            if file.startswith("bot_backup_") and (file.endswith(".db") or file.endswith(".gz")):
                full_path = os.path.join(backup_dir, file)
                backup_files.append((full_path, os.path.getmtime(full_path)))

        # Sort by modification time (oldest first)
        backup_files.sort(key=lambda x: x[1])

        # Remove oldest backups if we have too many
        if len(backup_files) > MAX_BACKUPS:
            for i in range(len(backup_files) - MAX_BACKUPS):
                os.remove(backup_files[i][0])
                logging.info(f"Removed old backup: {backup_files[i][0]}")
    except Exception as e:
        logging.error(f"Error rotating backups: {e}")


def restore_from_backup(backup_file, target_db=None):
    """
    Restores database from a backup file.

    Args:
        backup_file: Path to backup file
        target_db: Optional path to target database file (defaults to DB_PATH)

    Returns:
        bool: Whether restoration was successful
    """
    target_db = target_db or DB_PATH

    if not os.path.exists(backup_file):
        logging.error(f"Backup file not found: {backup_file}")
        return False

    try:
        # Create temporary path for restoration
        temp_db = f"{target_db}.restoring"

        # Remove temp DB if it exists from a previous attempt
        if os.path.exists(temp_db):
            os.remove(temp_db)

        # Create new database from backup
        conn = sqlite3.connect(temp_db)
        cursor = conn.cursor()

        if backup_file.endswith('.gz'):
            # Decompress gzip backup
            with gzip.open(backup_file, 'rt') as f:
                for line in f:
                    if line.strip():
                        cursor.execute(line)
        else:
            # Read regular backup
            with open(backup_file, 'r') as f:
                for line in f:
                    if line.strip():
                        cursor.execute(line)

        conn.commit()
        conn.close()

        # Verify temp database integrity
        verify_conn = sqlite3.connect(temp_db)
        verify_cursor = verify_conn.cursor()

        # Run integrity check
        verify_cursor.execute("PRAGMA integrity_check")
        integrity_result = verify_cursor.fetchone()[0]
        verify_conn.close()

        if integrity_result != "ok":
            logging.error(f"Integrity check failed on restored database: {integrity_result}")
            os.remove(temp_db)
            return False

        # Backup current DB before replacing
        current_backup = None
        if os.path.exists(target_db):
            current_backup = f"{target_db}.bak.{int(time.time())}"
            shutil.copy2(target_db, current_backup)
            logging.info(f"Created backup of current database: {current_backup}")

        # Replace current DB with restored one
        if os.path.exists(target_db):
            os.remove(target_db)
        os.rename(temp_db, target_db)

        logging.info(f"Successfully restored database from {backup_file}")
        return True

    except Exception as e:
        logging.error(f"Error restoring database: {e}")
        return False


def automated_backup(days=1):
    """
    Creates automated backup if last backup is older than specified days.

    Args:
        days: Minimum days between automated backups

    Returns:
        str: Path to backup file if created, None otherwise
    """
    backup_dir = BACKUP_DIR

    try:
        # Create backup directory if it doesn't exist
        Path(backup_dir).mkdir(parents=True, exist_ok=True)

        # Find most recent backup
        most_recent = None
        most_recent_time = 0

        for file in os.listdir(backup_dir):
            if file.startswith("bot_backup_") and (file.endswith(".db") or file.endswith(".gz")):
                full_path = os.path.join(backup_dir, file)
                mod_time = os.path.getmtime(full_path)

                if mod_time > most_recent_time:
                    most_recent = full_path
                    most_recent_time = mod_time

        # Check if we need a new backup
        if not most_recent or (time.time() - most_recent_time) > (days * 86400):
            logging.info("Creating automated backup")
            return backup_database(backup_dir=backup_dir, compress=True)
        else:
            logging.info(f"Recent backup exists ({most_recent}), skipping automated backup")
            return None

    except Exception as e:
        logging.error(f"Error in automated backup: {e}")
        return None


def get_backup_list(backup_dir=None):
    """
    Gets list of available backups with metadata.

    Args:
        backup_dir: Optional custom backup directory

    Returns:
        list: List of backup information dictionaries
    """
    backup_dir = backup_dir or BACKUP_DIR

    try:
        backups = []

        # Ensure directory exists
        if not os.path.exists(backup_dir):
            return backups

        for file in os.listdir(backup_dir):
            if file.startswith("bot_backup_") and (file.endswith(".db") or file.endswith(".gz")):
                full_path = os.path.join(backup_dir, file)
                file_stats = os.stat(full_path)

                backups.append({
                    'filename': file,
                    'path': full_path,
                    'size': file_stats.st_size,
                    'size_mb': round(file_stats.st_size / (1024 * 1024), 2),
                    'created': datetime.fromtimestamp(file_stats.st_ctime),
                    'compressed': file.endswith('.gz')
                })

        # Sort by creation time (newest first)
        backups.sort(key=lambda x: x['created'], reverse=True)
        return backups

    except Exception as e:
        logging.error(f"Error listing backups: {e}")
        return []


if __name__ == "__main__":
    # If run directly, perform a backup
    backup_file = backup_database()
    if backup_file:
        print(f"Backup created: {backup_file}")
    else:
        print("Backup failed!")