import sqlite3
import os


def check_database(db_path):
    if not os.path.exists(db_path):
        print(f"База данных не найдена: {db_path}")
        return

    print(f"\n--- Проверка базы данных: {db_path} ---")

    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()

        # Получаем список таблиц
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
        tables = cursor.fetchall()

        if not tables:
            print("База данных пуста, нет таблиц")
            conn.close()
            return

        print("Найденные таблицы:")
        for table in tables:
            print(f"- {table[0]}")

            # Для таблицы users показываем структуру и содержимое
            if table[0] == "users":
                # Получаем структуру таблицы
                cursor.execute(f"PRAGMA table_info(users)")
                columns = cursor.fetchall()
                print("\nСтруктура таблицы users:")
                for col in columns:
                    print(f"  - {col[1]} ({col[2]})")

                # Проверяем наличие записей
                cursor.execute("SELECT COUNT(*) FROM users")
                count = cursor.fetchone()[0]
                print(f"\nВсего пользователей: {count}")

                # Если есть пользователи, выводим несколько для примера
                if count > 0:
                    cursor.execute("SELECT * FROM users LIMIT 3")
                    users = cursor.fetchall()
                    print("\nПримеры пользователей:")
                    for user in users:
                        user_dict = {columns[i][1]: user[i] for i in range(len(columns))}
                        print(f"  - {user_dict}")

        conn.close()
    except Exception as e:
        print(f"Ошибка при проверке базы данных: {e}")


# Проверяем обе базы данных
check_database("database/bot.db")
check_database("bot/database/bot.db")