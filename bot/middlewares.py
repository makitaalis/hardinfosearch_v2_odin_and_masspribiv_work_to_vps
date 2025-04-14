from aiogram import BaseMiddleware
from aiogram.types import Update
from typing import Any, Awaitable, Callable

class ExampleMiddleware(BaseMiddleware):
    """
    Пример промежуточного обработчика (middleware).
    Aiogram 3 позволяет определить логику до и после выполнения хендлера.
    """

    async def __call__(
        self,
        handler: Callable[[Update, dict[str, Any]], Awaitable[Any]],
        event: Update,
        data: dict[str, Any]
    ) -> Any:
        # Логика до выполнения хендлера:
        # Можно проверить, авторизован ли пользователь,
        # записать информацию в лог, изменить event/data и т.д.

        # Пример (логирование обновления):
        # print(f"Middleware: Пришёл апдейт: {event}")

        # Вызываем сам хендлер
        result = await handler(event, data)

        # Логика после выполнения хендлера (если нужно)
        return result


# Если пока ничего не нужно – можно оставить файл пустым.
# Или добавить хотя бы этот класс – как заготовку.

