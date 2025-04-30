import sqlite3
import os


def update_user_balance():
    # Используем правильный путь к базе данных
    db_path = "bot/database/bot.db"

    if not os.path.exists(db_path):
        print(f"База данных не найдена: {db_path}")
        return

    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()

        # Выводим список пользователей
        cursor.execute("SELECT id, telegram_id, login, balance FROM users")
        users = cursor.fetchall()

        if not users:
            print("Пользователи не найдены в базе данных")
            conn.close()
            return

        print("\nСписок пользователей:")
        print("ID | Telegram ID | Логин | Баланс")
        print("-" * 50)
        for user in users:
            telegram_id = user[1] if user[1] else "Нет"
            print(f"{user[0]} | {telegram_id} | {user[2]} | ${user[3]:.2f}")

        # Запрашиваем данные для обновления
        user_login = input("\nВведите логин пользователя для обновления баланса: ")

        # Проверяем существование пользователя
        cursor.execute("SELECT login, balance FROM users WHERE login = ?", (user_login,))
        user = cursor.fetchone()

        if not user:
            print(f"Пользователь с логином '{user_login}' не найден")
            conn.close()
            return

        current_balance = user[1]
        print(f"Текущий баланс пользователя {user[0]}: ${current_balance:.2f}")

        # Запрашиваем сумму для добавления
        try:
            amount = float(input("Введите сумму для добавления (положительное число): "))
            if amount <= 0:
                raise ValueError("Сумма должна быть положительным числом")
        except ValueError as e:
            print(f"Ошибка: {e}")
            conn.close()
            return

        # Обновляем баланс
        new_balance = current_balance + amount
        cursor.execute("UPDATE users SET balance = ? WHERE login = ?", (new_balance, user_login))
        conn.commit()

        print(f"\nБаланс пользователя {user_login} успешно обновлен:")
        print(f"Предыдущий баланс: ${current_balance:.2f}")
        print(f"Новый баланс: ${new_balance:.2f}")

        conn.close()

    except Exception as e:
        print(f"Ошибка при работе с базой данных: {e}")


if __name__ == "__main__":
    update_user_balance()