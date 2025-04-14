from aiogram.utils.keyboard import InlineKeyboardBuilder
from aiogram.types import InlineKeyboardButton, InlineKeyboardMarkup, ReplyKeyboardMarkup, KeyboardButton

def get_admin_users_keyboard(page: int, users: list, has_next: bool) -> InlineKeyboardMarkup:
    """
    Генерирует inline-кнопки для пользователей (админ-список):
      1) Кнопка "Пополнить" для каждого пользователя (в отдельной строке).
      2) Кнопки пагинации ("⬅ Назад" / "➡ Вперед") – если нужны.
    """
    builder = InlineKeyboardBuilder()

    # Кнопка "пополнить" под каждым пользователем
    for login, balance in users:
        callback_data = f"add_balance_{login}"
        button_text = f"💰 Пополнить ({login})"
        builder.add(InlineKeyboardButton(text=button_text, callback_data=callback_data))

    # Пагинация
    pagination_buttons = []
    if page > 1:
        pagination_buttons.append(
            InlineKeyboardButton(text="⬅ Назад", callback_data=f"prev_page_{page - 1}")
        )
    if has_next:
        pagination_buttons.append(
            InlineKeyboardButton(text="➡ Вперед", callback_data=f"next_page_{page + 1}")
        )

    if pagination_buttons:
        builder.row(*pagination_buttons)

    return builder.as_markup()

def get_admin_menu() -> ReplyKeyboardMarkup:
    """
    Создаёт основное кнопочное меню для администратора.
    Это меню отображается слева от строки ввода текста (ReplyKeyboard).
    """
    keyboard = [
        [
            KeyboardButton(text="📋 Список пользователей"),
            KeyboardButton(text="➕ Добавить пользователя")
        ],
        [
            KeyboardButton(text="💰 Пополнить баланс"),
            KeyboardButton(text="📊 Баланс API")
        ],
        [
            KeyboardButton(text="📊 Статистика очереди"),
            KeyboardButton(text="⚙️ Настройки")
        ],
        [
            KeyboardButton(text="🚪 Выйти")
        ],
        # Добавляем новую кнопку на отдельной строке для важных действий
        [
            KeyboardButton(text="⚠️ Разлогинить всех")
        ]
    ]
    return ReplyKeyboardMarkup(keyboard=keyboard, resize_keyboard=True)

def get_user_menu() -> InlineKeyboardMarkup:
    """Создает инлайн-меню для обычного пользователя"""
    builder = InlineKeyboardBuilder()

    # Первый ряд кнопок
    builder.row(
        InlineKeyboardButton(text="💰 Мой баланс", callback_data="user_balance"),
        InlineKeyboardButton(text="🔍 Поиск", callback_data="search_help")
    )

    # Второй ряд кнопок
    builder.row(
        InlineKeyboardButton(text="🔎 Расширенный поиск", callback_data="extended_search_info"),
        InlineKeyboardButton(text="🚪 Выйти", callback_data="logout")
    )

    # Добавляем новую кнопку для массового пробива
    builder.row(
        InlineKeyboardButton(text="🔢 Массовый пробив", callback_data="mass_search")
    )

    return builder.as_markup()

def add_navigation_buttons(builder, back_callback=None, main_menu_callback=None):
    """
    Добавляет кнопки навигации к существующему InlineKeyboardBuilder

    :param builder: InlineKeyboardBuilder к которому добавляются кнопки
    :param back_callback: callback_data для кнопки "Назад" (если None, кнопка не добавляется)
    :param main_menu_callback: callback_data для кнопки "В главное меню" (если None, кнопка не добавляется)
    :return: обновленный builder
    """
    nav_buttons = []

    if back_callback:
        nav_buttons.append(InlineKeyboardButton(text="↩️ Назад", callback_data=back_callback))

    if main_menu_callback:
        nav_buttons.append(InlineKeyboardButton(text="🏠 В главное меню", callback_data=main_menu_callback))

    if nav_buttons:
        builder.row(*nav_buttons)

    return builder