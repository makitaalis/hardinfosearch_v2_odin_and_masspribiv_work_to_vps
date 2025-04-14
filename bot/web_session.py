import asyncio
import logging
import random
import re
import time
from datetime import datetime, timedelta
import json

import aiohttp
from bs4 import BeautifulSoup

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.107 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.1.2 Safari/605.1.15",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:90.0) Gecko/20100101 Firefox/90.0"
]

class SauronWebSession:
    """Класс для работы с веб-сессией сайта Sauron"""

    BASE_URL = "https://sauron.info"

    def __init__(self, login, password, session_id):
        self.login = login
        self.password = password
        self.session_id = session_id
        self.cookies = {}
        self.is_authenticated = False
        self.last_activity = datetime.now()
        self.is_busy = False
        self.auth_attempts = 0
        self.error_count = 0
        self.user_agent = random.choice(USER_AGENTS)

        # Статистика
        self.search_count = 0
        self.successful_searches = 0

    async def _make_request(self, method, url, data=None, headers=None, json=None, allow_redirects=True):
        """Выполняет HTTP запрос с управлением сессией"""
        if not url.startswith("http"):
            url = f"{self.BASE_URL}/{url.lstrip('/')}"

        if headers is None:
            headers = {}

        # Добавляем User-Agent если его нет
        if "User-Agent" not in headers:
            headers["User-Agent"] = self.user_agent

        # Добавляем referer если его нет
        if "Referer" not in headers and "login" not in url:
            headers["Referer"] = f"{self.BASE_URL}/dashboard"

        # Обновляем время последней активности
        self.last_activity = datetime.now()

        try:
            async with aiohttp.ClientSession(cookies=self.cookies) as session:
                async with session.request(
                        method=method,
                        url=url,
                        data=data,
                        headers=headers,
                        json=json,
                        allow_redirects=allow_redirects,
                        timeout=30
                ) as response:
                    # Сохраняем cookies
                    if response.cookies:
                        for key, cookie in response.cookies.items():
                            self.cookies[key] = cookie.value

                    # Возвращаем статус и содержимое
                    content = await response.text()
                    return response.status, content
        except Exception as e:
            logging.error(f"Session {self.session_id} request error: {str(e)}")
            self.error_count += 1
            raise

    async def authenticate(self):
        """Выполняет авторизацию на сайте"""
        self.is_busy = True
        try:
            # Увеличиваем счетчик попыток авторизации
            self.auth_attempts += 1
            logging.info(f"Session {self.session_id}: Authentication attempt {self.auth_attempts}")

            # Сначала загружаем страницу логина для получения cookie
            status, content = await self._make_request("GET", "/login")
            if status != 200:
                logging.error(f"Session {self.session_id}: Failed to load login page, status {status}")
                return False

            # Отправляем запрос на аутентификацию
            auth_data = {
                "login": self.login,
                "password": self.password
            }
            status, content = await self._make_request(
                "POST",
                "/api/v1/user/authenticateUser",
                data=auth_data
            )

            # Проверяем результат
            if status != 200:
                logging.error(f"Session {self.session_id}: Authentication failed with status {status}")
                return False

            try:
                # Пытаемся извлечь JSON ответ
                response_data = json.loads(content)

                if response_data.get('ok') == True:
                    # Проверяем нужна ли 2FA
                    if response_data.get('error_code') == 1004:
                        logging.error(f"Session {self.session_id}: 2FA required, not supported!")
                        return False

                    # Проверяем перенаправление
                    redirection = None
                    if response_data.get('result') and response_data['result'].get('redirection'):
                        redirection = response_data['result']['redirection']

                    if redirection:
                        # Делаем запрос по URL перенаправления
                        status, content = await self._make_request("GET", redirection)
                        if status != 200:
                            logging.error(f"Session {self.session_id}: Failed to follow redirection, status {status}")
                            return False

                    # Проверяем, что мы авторизованы, загрузив дашборд
                    status, dashboard = await self._make_request("GET", "/dashboard")
                    if status != 200 or "Авторизация" in dashboard:
                        logging.error(f"Session {self.session_id}: Failed to load dashboard after authentication")
                        return False

                    # Успешная авторизация
                    self.is_authenticated = True
                    self.error_count = 0
                    logging.info(f"Session {self.session_id}: Successfully authenticated")
                    return True
                else:
                    # Ошибка авторизации
                    error_message = response_data.get('description', 'Unknown error')
                    logging.error(f"Session {self.session_id}: Authentication error: {error_message}")
                    return False
            except Exception as e:
                logging.error(f"Session {self.session_id}: Error parsing authentication response: {str(e)}")
                return False
        finally:
            self.is_busy = False

    async def search(self, query):
        """Выполняет поиск по запросу"""
        if not self.is_authenticated:
            logging.warning(f"Session {self.session_id}: Attempting search without authentication")
            auth_success = await self.authenticate()
            if not auth_success:
                return False, "Не удалось авторизоваться для выполнения поиска"

        self.is_busy = True
        try:
            # Увеличиваем счетчик поисковых запросов
            self.search_count += 1
            logging.info(f"Session {self.session_id}: Performing search #{self.search_count} for query: {query}")

            # Отправляем запрос поиска
            search_data = {"query": query}
            status, content = await self._make_request(
                "POST",
                "/api/v1/search/full",
                data=search_data
            )

            if status != 200:
                logging.error(f"Session {self.session_id}: Search failed with status {status}")
                return False, f"Ошибка поиска, статус {status}"

            try:
                # Пытаемся извлечь JSON ответ
                import json
                response_data = json.loads(content)

                # Отладочная информация о JSON-ответе
                logging.info(f"Search response status: ok={response_data.get('ok')}")

                if response_data.get('ok') == True:
                    # Проверяем перенаправление
                    redirection = None
                    if response_data.get('result') and response_data['result'].get('redirection'):
                        redirection = response_data['result']['redirection']

                    if redirection:
                        logging.info(f"Following redirection to: {redirection}")
                        # Делаем запрос по URL перенаправления для получения результатов
                        status, result_page = await self._make_request("GET", redirection)
                        if status != 200:
                            logging.error(f"Session {self.session_id}: Failed to load results page, status {status}")
                            return False, "Не удалось загрузить страницу с результатами"

                        # Увеличиваем счетчик успешных поисков
                        self.successful_searches += 1

                        # Проверяем содержимое страницы
                        if "Подробный отчет" in result_page:
                            logging.info("Found search results page with 'Подробный отчет' section")
                        else:
                            logging.warning("Search results page doesn't contain 'Подробный отчет' section")

                        return True, result_page
                    else:
                        logging.error(f"Session {self.session_id}: No redirection in search response")
                        return False, "Нет перенаправления в ответе поиска"
                else:
                    # Ошибка поиска
                    error_message = response_data.get('description', 'Unknown error')
                    logging.error(f"Session {self.session_id}: Search error: {error_message}")

                    # Проверяем, не связана ли ошибка с потерей авторизации
                    if "авторизац" in error_message.lower() or "session" in error_message.lower():
                        self.is_authenticated = False
                    return False, error_message
            except Exception as e:
                logging.error(f"Session {self.session_id}: Error parsing search response: {str(e)}")
                return False, f"Ошибка при обработке ответа поиска: {str(e)}"
        finally:
            self.is_busy = False

    async def parse_results(self, html_content):
        """Парсит HTML-страницу с результатами поиска"""
        try:
            soup = BeautifulSoup(html_content, 'html.parser')

            # Проверяем, что это действительно страница с результатами
            if "Запрос:" not in html_content or "Подробный отчет" not in html_content:
                logging.error("Получена некорректная страница результатов")
                # Сохраняем первые 500 символов для анализа
                logging.debug(f"Начало страницы: {html_content[:500]}")
                return []

            # Результаты поиска
            result_data = []

            # Получаем заголовок запроса
            title_elem = soup.select_one("div.title--main")
            query = ""
            if title_elem:
                query_text = title_elem.text
                if "Запрос:" in query_text:
                    query = query_text.replace("Запрос:", "").strip()
                    logging.info(f"Parsed query: {query}")

            # Получаем блоки с результатами
            blocks = soup.select("div.simple--block.simple--result--ltd")
            logging.info(f"Found {len(blocks)} result blocks")

            for block in blocks:
                block_data = {"database": ""}

                # Получаем название базы данных
                header = block.select_one("div.simple--block--header div.title")
                if header:
                    block_data["database"] = header.text.strip()
                    logging.debug(f"Found database: {block_data['database']}")

                # Получаем поля с данными
                fields = block.select("div.column--flex--content")
                logging.debug(f"Found {len(fields)} fields in block")

                for field in fields:
                    title_elem = field.select_one("div.column--flex--title")
                    value_elem = field.select_one("div.column--flex--result")

                    if title_elem and value_elem:
                        field_name = title_elem.text.strip().rstrip(':')
                        field_value = value_elem.text.strip()

                        if field_name and field_value:
                            block_data[field_name] = field_value
                            logging.debug(f"  Field: {field_name} = {field_value[:30]}...")

                # Добавляем блок в результаты, только если есть данные
                if len(block_data) > 1:  # Более чем только "database"
                    result_data.append(block_data)

            # Если данных нет, возвращаем пустой список
            if not result_data:
                logging.warning("No data found in search results")
                return result_data

            # Единообразие ключей
            # Мапим поля к стандартным названиям
            field_mapping = {
                "ФИО": "ФИО",
                "День рождения": "ДАТА РОЖДЕНИЯ",
                "Телефон": "ТЕЛЕФОН",
                "Email": "ПОЧТА",
                "ИНН": "ИНН",
                "СНИЛС": "СНИЛС",
                "Паспорт": "ПАСПОРТ",
                "Адрес": "АДРЕС",
                "Место рождения": "МЕСТО РОЖДЕНИЯ"
            }

            # Стандартизируем ключи
            standardized_data = []
            for item in result_data:
                std_item = {"database": item.get("database", "")}
                for key, value in item.items():
                    if key == "database":
                        continue

                    if key in field_mapping:
                        std_item[field_mapping[key]] = value
                    else:
                        std_item[key] = value
                standardized_data.append(std_item)

            logging.info(f"Parsed {len(standardized_data)} result items")
            return standardized_data
        except Exception as e:
            logging.error(f"Session {self.session_id}: Error parsing results: {str(e)}")
            import traceback
            logging.error(traceback.format_exc())
            return []

    def get_stats(self):
        """Возвращает статистику сессии"""
        return {
            "session_id": self.session_id,
            "login": self.login,
            "is_authenticated": self.is_authenticated,
            "search_count": self.search_count,
            "successful_searches": self.successful_searches,
            "error_count": self.error_count,
            "auth_attempts": self.auth_attempts,
            "last_activity": self.last_activity.isoformat() if self.last_activity else None,
            "is_busy": self.is_busy
        }