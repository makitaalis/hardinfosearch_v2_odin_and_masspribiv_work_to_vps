"""
Enhanced SauronWebSession for website automation with improved reliability,
connection pooling, and error handling.

This module handles web interactions with sauron.info including:
- Authentication and session management
- Search requests with retry logic
- Connection pooling and rate limiting
- Result parsing and structuring
- Error handling and recovery
"""

import asyncio
import hashlib
import json
import logging
import random
import re
import time
import traceback
import os
from datetime import datetime, timedelta

import aiohttp
from bs4 import BeautifulSoup, Tag

# Diverse user agents for web requests
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.107 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.1.2 Safari/605.1.15",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:90.0) Gecko/20100101 Firefox/90.0",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/94.0.4606.81 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/95.0.4638.69 Safari/537.36 Edg/95.0.1020.44",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/94.0.4606.81 Safari/537.36",
    "Mozilla/5.0 (iPhone; CPU iPhone OS 15_0 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/15.0 Mobile/15E148 Safari/604.1"
]

# Enhanced headers for better site compatibility
COMMON_HEADERS = {
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.5",
    "Accept-Encoding": "gzip, deflate, br",
    "Connection": "keep-alive",
    "Upgrade-Insecure-Requests": "1",
    "Sec-Fetch-Dest": "document",
    "Sec-Fetch-Mode": "navigate",
    "Sec-Fetch-Site": "same-origin",
    "Sec-Fetch-User": "?1",
    "DNT": "1"
}


class SauronWebSession:
    """Enhanced class for web session management with improved connection handling"""

    BASE_URL = "https://sauron.info"

    # Class-level rate limiting and connection pooling
    _last_auth_time = {}  # Track last auth time per credential
    _auth_lock = asyncio.Lock()  # Synchronize authentication
    _conn_pool = None  # Shared connection pool
    _auth_rate_limit = 1.5  # seconds between auth attempts
    _auth_backoff_base = 2.0  # Base for exponential backoff
    _auth_backoff_max = 30.0  # Maximum backoff time
    _session_pool_stats = {"total_connections": 0, "active_connections": 0}

    # Result caching system to reduce duplicate requests
    _result_cache = {}  # {query_hash: (result, timestamp)}
    _result_cache_ttl = 600  # Cache TTL in seconds (10 minutes)
    _cache_lock = asyncio.Lock()

    @classmethod
    async def get_connector(cls):
        """
        Revised connector management to avoid session issues.
        Returns None instead of a shared connector, since we're using connector_owner=True
        """
        # IMPORTANT: We're going to create individual connectors for each session
        # This is less efficient but more reliable than shared connectors
        return None  # Return None to let aiohttp create a new connector each time

    def __init__(self, login, password, session_id):
        # Authentication information
        self.login = login
        self.password = password
        self.session_id = session_id

        # Session state
        self.cookies = {}
        self.is_authenticated = False
        self.last_activity = datetime.now()
        self.is_busy = False
        self.auth_attempts = 0
        self.consecutive_auth_failures = 0
        self.error_count = 0
        self.user_agent = random.choice(USER_AGENTS)
        self.session_valid_until = None
        self.last_auth_success = None
        self.last_request_time = 0

        # Performance tracking
        self.credential_key = f"{login}:{password}"
        self.search_count = 0
        self.successful_searches = 0
        self.failed_searches = 0
        self.total_request_time = 0
        self.requests_count = 0
        self.avg_response_time = 0

        # Request history for troubleshooting
        self.request_history = []  # Limited history of recent requests
        self.MAX_HISTORY = 10

    async def _make_request(self, method, url, data=None, headers=None, json_data=None,
                            allow_redirects=True, timeout=30, retry_count=0):
        """
        Completely revised HTTP request function to fix session handling issues.
        """
        # Normalize URL
        if not url.startswith("http"):
            url = f"{self.BASE_URL}/{url.lstrip('/')}"

        # Prepare headers
        request_headers = dict(COMMON_HEADERS)
        if headers:
            request_headers.update(headers)

        # Set user agent if not specified
        if "User-Agent" not in request_headers:
            request_headers["User-Agent"] = self.user_agent

        # Set referer for non-login requests
        if "Referer" not in request_headers and "login" not in url:
            request_headers["Referer"] = f"{self.BASE_URL}/dashboard"

        # Apply rate limiting for consecutive requests
        current_time = time.time()
        time_since_last_request = current_time - self.last_request_time
        if time_since_last_request < 0.5:  # 500ms minimum between requests
            await asyncio.sleep(0.5 - time_since_last_request)

        # Update last request time
        self.last_request_time = time.time()
        self.last_activity = datetime.now()

        # Add request to history
        request_info = {
            "method": method,
            "url": url,
            "timestamp": datetime.now().isoformat(),
            "retry": retry_count
        }

        if not hasattr(self, 'request_history'):
            self.request_history = []

        self.request_history.append(request_info)
        if len(self.request_history) > 10:  # Keep history limited
            self.request_history.pop(0)

        # Set up retry logic
        max_retries = 3 if retry_count == 0 else max(0, 3 - retry_count)
        retry_delay = 1.0 * (1.5 ** retry_count)
        request_start = time.time()
        last_exception = None

        for attempt in range(max_retries + 1):
            if attempt > 0:
                await asyncio.sleep(retry_delay * random.uniform(0.8, 1.2))
                logging.info(f"Retrying request to {url} (attempt {attempt}/{max_retries})")

            try:
                # CRITICAL FIX: Create a completely new session for each request
                # This avoids the "Session is closed" errors
                timeout_obj = aiohttp.ClientTimeout(total=timeout)

                # Don't use a shared connector to avoid "Session is closed" errors
                async with aiohttp.ClientSession(
                        cookies=self.cookies,
                        timeout=timeout_obj,
                        connector_owner=True,  # Important: Don't share connector
                        raise_for_status=False
                ) as session:
                    # Make the request
                    async with session.request(
                            method=method,
                            url=url,
                            data=data,
                            headers=request_headers,
                            json=json_data,
                            allow_redirects=allow_redirects,
                            ssl=False,  # Skip SSL verification
                    ) as response:
                        # Save cookies
                        if response.cookies:
                            for key, cookie in response.cookies.items():
                                self.cookies[key] = cookie.value

                        # Get content safely
                        try:
                            content = await response.text()
                        except Exception as e:
                            logging.error(f"Error reading response text: {e}")
                            content = ""

                        # Check authentication issues
                        if response.status in (401, 403) or (content and "авторизаци" in content.lower()):
                            self.is_authenticated = False

                        # Update request stats
                        request_time = time.time() - request_start
                        self.total_request_time += request_time
                        self.requests_count += 1

                        # Return the result
                        return response.status, content

            except aiohttp.ClientConnectorError as e:
                last_exception = e
                logging.error(f"Connection error on attempt {attempt + 1}: {e}")
            except aiohttp.ClientError as e:
                last_exception = e
                logging.error(f"Client error on attempt {attempt + 1}: {e}")
            except asyncio.TimeoutError:
                last_exception = asyncio.TimeoutError("Request timed out")
                logging.error(f"Timeout on attempt {attempt + 1}")
            except Exception as e:
                last_exception = e
                logging.error(f"Unexpected error on attempt {attempt + 1}: {e}")

        # If we reach here, all retries failed
        if last_exception:
            raise last_exception
        else:
            raise RuntimeError(f"Request to {url} failed after {max_retries} retries")

    async def authenticate(self):
        """
        Completely revised authentication method to fix session and JSON handling issues.
        """
        # Apply rate limiting
        async with self._auth_lock:
            current_time = time.time()
            key = f"{self.login}:{self.password}"

            if key in self._last_auth_time:
                time_since_last = current_time - self._last_auth_time[key]
                if time_since_last < 2.0:  # Increased to 2 seconds
                    wait_time = 2.0 - time_since_last + random.uniform(0, 0.5)
                    logging.info(f"Session {self.session_id}: Rate limiting auth, waiting {wait_time:.2f}s")
                    await asyncio.sleep(wait_time)

            self._last_auth_time[key] = time.time()

        self.is_busy = True
        try:
            # Count authentication attempts
            self.auth_attempts += 1
            logging.info(f"Session {self.session_id}: Authentication attempt {self.auth_attempts}")

            # Apply backoff for repeated failures
            if self.consecutive_auth_failures > 0:
                backoff = min(30, 2.0 ** self.consecutive_auth_failures)
                backoff *= random.uniform(0.8, 1.2)  # Add jitter
                logging.warning(
                    f"Session {self.session_id}: Applying auth backoff of {backoff:.2f}s after {self.consecutive_auth_failures} failures")
                await asyncio.sleep(backoff)

            # STEP 1: Load the login page to get cookies and CSRF token
            try:
                status, content = await self._make_request("GET", "/login", timeout=15)

                if status != 200 or not content:
                    self.consecutive_auth_failures += 1
                    logging.error(f"Session {self.session_id}: Failed to load login page, status {status}")
                    return False

                # Try to parse the login page for CSRF token
                csrf_token = None
                try:
                    soup = BeautifulSoup(content, 'html.parser')
                    # Look for common CSRF token fields
                    for field_name in ['_csrf_token', 'csrf_token', '_token', 'csrftoken']:
                        token_field = soup.select_one(f'input[name="{field_name}"]')
                        if token_field and 'value' in token_field.attrs:
                            csrf_token = token_field['value']
                            break
                except Exception as e:
                    logging.warning(f"Session {self.session_id}: Error parsing login page: {e}")
                    # Continue anyway - some sites don't require CSRF tokens

            except Exception as e:
                self.consecutive_auth_failures += 1
                logging.error(f"Session {self.session_id}: Error loading login page: {e}")
                return False

            # STEP 2: Send the authentication request
            auth_data = {
                "login": self.login,
                "password": self.password
            }

            if csrf_token:
                auth_data['_csrf_token'] = csrf_token

            auth_headers = {
                "Content-Type": "application/x-www-form-urlencoded",
                "Origin": self.BASE_URL,
                "Referer": f"{self.BASE_URL}/login"
            }

            try:
                status, content = await self._make_request(
                    "POST",
                    "/api/v1/user/authenticateUser",
                    data=auth_data,
                    headers=auth_headers,
                    timeout=20
                )

                if status != 200 or not content:
                    self.consecutive_auth_failures += 1
                    logging.error(f"Session {self.session_id}: Authentication request failed, status {status}")
                    return False

                # Safely parse JSON response
                try:
                    response_data = json.loads(content)
                except json.JSONDecodeError as e:
                    self.consecutive_auth_failures += 1
                    logging.error(f"Session {self.session_id}: JSON decode error: {e}")
                    logging.error(f"Response content (first 100 chars): {content[:100]}")
                    return False

                # Check authentication result
                if not response_data.get('ok'):
                    self.consecutive_auth_failures += 1
                    error_message = response_data.get('description', 'Unknown error')
                    logging.error(f"Session {self.session_id}: Authentication error: {error_message}")
                    return False

                # Check for redirection
                redirection = None
                if response_data.get('result') and response_data['result'].get('redirection'):
                    redirection = response_data['result']['redirection']

                    # Follow redirection if provided
                    if redirection:
                        try:
                            status, content = await self._make_request("GET", redirection, timeout=15)
                            if status != 200:
                                self.consecutive_auth_failures += 1
                                logging.error(
                                    f"Session {self.session_id}: Failed to follow redirection, status {status}")
                                return False
                        except Exception as e:
                            self.consecutive_auth_failures += 1
                            logging.error(f"Session {self.session_id}: Error following redirection: {e}")
                            return False

                # STEP 3: Verify authentication by loading dashboard
                try:
                    status, dashboard = await self._make_request("GET", "/dashboard", timeout=15)
                    if status != 200 or (dashboard and "Авторизация" in dashboard):
                        self.consecutive_auth_failures += 1
                        logging.error(f"Session {self.session_id}: Failed to load dashboard after authentication")
                        return False
                except Exception as e:
                    self.consecutive_auth_failures += 1
                    logging.error(f"Session {self.session_id}: Error loading dashboard: {e}")
                    return False

                # Authentication successful!
                self.is_authenticated = True
                self.consecutive_auth_failures = 0
                self.error_count = 0
                self.last_auth_success = datetime.now()
                self.session_valid_until = datetime.now() + timedelta(hours=6)

                logging.info(f"Session {self.session_id}: Successfully authenticated!")
                return True

            except Exception as e:
                self.consecutive_auth_failures += 1
                logging.error(f"Session {self.session_id}: Error during authentication: {e}")
                return False

        finally:
            self.is_busy = False

    async def search(self, query):
        """
        Выполняет поиск и сохраняет HTML перед парсингом
        для предотвращения потери исходных email-адресов.
        """
        if not self.is_authenticated:
            logging.warning(f"Session {self.session_id}: Attempting search without authentication")
            auth_success = await self.authenticate()
            if not auth_success:
                return False, "Authentication failed before search"

        # Проверяем кэш для идентичного запроса
        query_hash = hashlib.md5(query.encode()).hexdigest()

        async with self._cache_lock:
            if query_hash in self._result_cache:
                result, timestamp = self._result_cache[query_hash]
                age = (datetime.now() - timestamp).total_seconds()

                # Возвращаем кэшированный результат, если он достаточно свежий
                if age < self._result_cache_ttl:
                    logging.info(f"Session {self.session_id}: Using cached result for query '{query[:20]}...'")
                    return True, result

        self.is_busy = True
        try:
            # Увеличиваем счетчик поисков
            self.search_count += 1
            logging.info(f"Session {self.session_id}: Performing search #{self.search_count} for query: {query}")

            # Подготавливаем заголовки поиска с соответствующим content type
            search_headers = {
                "Content-Type": "application/x-www-form-urlencoded",
                "X-Requested-With": "XMLHttpRequest",  # Указываем AJAX-запрос
                "Origin": self.BASE_URL,
                "Referer": f"{self.BASE_URL}/dashboard"
            }

            # Отправляем поисковый запрос с оптимизированным timeout
            search_data = {"query": query}
            status, content = await self._make_request(
                "POST",
                "/api/v1/search/full",
                data=search_data,
                headers=search_headers,
                timeout=35  # Немного увеличенный timeout для поиска
            )

            if status != 200:
                self.failed_searches += 1
                logging.error(f"Session {self.session_id}: Search failed with status {status}")
                return False, f"Search error, status {status}"

            try:
                # Парсим JSON-ответ
                response_data = json.loads(content)
                logging.info(f"Search response status: ok={response_data.get('ok')}")

                if response_data.get('ok') == True:
                    # Проверяем на редирект
                    redirection = None
                    if response_data.get('result') and response_data['result'].get('redirection'):
                        redirection = response_data['result']['redirection']

                    if redirection:
                        logging.info(f"Following redirection to: {redirection}")

                        # ИЗМЕНЕНИЕ: Сохраняем HTML перед парсингом
                        status, result_page = await self._make_request(
                            "GET",
                            redirection,
                            timeout=40,  # Увеличенный timeout для страницы результатов
                            headers={"Referer": f"{self.BASE_URL}/api/v1/search/full"}
                        )

                        if status != 200:
                            self.failed_searches += 1
                            logging.error(
                                f"Session {self.session_id}: Failed to load results page, status {status}")
                            return False, "Failed to load results page"

                        # Увеличиваем счетчик успешных поисков
                        self.successful_searches += 1

                        # Проверяем содержимое страницы результатов
                        if "Подробный отчет" in result_page:
                            logging.info("Found search results page with 'Подробный отчет' section")
                        else:
                            logging.warning("Search results page doesn't contain 'Подробный отчет' section")

                        # НОВАЯ ЛОГИКА: Сохраняем HTML-страницу в файл перед парсингом
                        html_filename = f"temp_html_{int(time.time())}_{self.session_id}.html"
                        try:
                            os.makedirs("temp_html", exist_ok=True)
                            html_path = os.path.join("temp_html", html_filename)

                            with open(html_path, "w", encoding="utf-8") as f:
                                f.write(result_page)

                            logging.info(f"Saved HTML to {html_path} for offline parsing")

                            # Читаем файл снова для парсинга, чтобы избежать изменений при загрузке в память
                            with open(html_path, "r", encoding="utf-8") as f:
                                html_content = f.read()

                            # После отладки можно удалить файл, если нужно
                            # os.remove(html_path)
                        except Exception as save_error:
                            logging.error(f"Error saving HTML to file: {save_error}")
                            html_content = result_page

                        # Кэшируем результат
                        async with self._cache_lock:
                            self._result_cache[query_hash] = (html_content, datetime.now())

                            # Очищаем кэш, если он становится слишком большим (более 100 элементов)
                            if len(self._result_cache) > 100:
                                # Удаляем 20 самых старых элементов
                                sorted_items = sorted(self._result_cache.items(),
                                                  key=lambda x: x[1][1])  # Сортировка по timestamp
                                for key, _ in sorted_items[:20]:
                                    del self._result_cache[key]

                        return True, html_content
                    else:
                        self.failed_searches += 1
                        logging.error(f"Session {self.session_id}: No redirection in search response")
                        return False, "No redirection in search response"
                else:
                    # Ошибка поиска
                    self.failed_searches += 1
                    error_message = response_data.get('description', 'Unknown error')
                    logging.error(f"Session {self.session_id}: Search error: {error_message}")

                    # Проверяем, связана ли ошибка с аутентификацией
                    if any(term in error_message.lower() for term in ["авториз", "session", "login", "токен"]):
                        self.is_authenticated = False
                        # Принудительная повторная аутентификация при следующем поиске
                        self.session_valid_until = None

                    return False, error_message
            except Exception as e:
                self.failed_searches += 1
                logging.error(f"Session {self.session_id}: Error parsing search response: {str(e)}")
                return False, f"Error processing search response: {str(e)}"
        finally:
            self.is_busy = False

    async def parse_results(self, html_content):
        """
        Улучшенный парсер HTML для извлечения структурированных данных
        с прямым извлечением email из исходного HTML.

        Args:
            html_content: HTML строка со страницей результатов

        Returns:
            list: Список словарей с извлеченными данными
        """
        try:
            # НОВЫЙ ПОДХОД: Сначала находим все email-адреса в сыром HTML
            emails_from_raw_html = self._extract_emails_from_raw_html(html_content)
            logging.info(f"Extracted {len(emails_from_raw_html)} emails from raw HTML")

            # Теперь создаем BeautifulSoup для парсинга структуры
            soup = BeautifulSoup(html_content, 'html.parser')

            # Проверяем валидность страницы результатов
            if ("Запрос:" not in html_content and "Подробный отчет" not in html_content
                and "Общая сводка" not in html_content):
                logging.warning("Нестандартный формат страницы - попробуем извлечь данные в любом случае")

            # Извлекаем результаты поиска
            result_data = []

            # Извлекаем все блоки с данными
            blocks = soup.select("div.simple--block.simple--result--ltd")
            if not blocks:
                # Пробуем альтернативные шаблоны блоков для разных макетов страниц
                blocks = soup.select("div.simple--block")

            logging.info(f"Найдено {len(blocks)} блоков данных")

            # Если блоки не найдены, пробуем извлечь данные из основной структуры страницы
            if not blocks:
                # Создаем синтетический блок из всей страницы
                main_block = self._extract_data_from_whole_page(soup, emails_from_raw_html)
                if main_block:
                    result_data.append(main_block)
            else:
                # Обрабатываем каждый блок с использованием информации о email из сырого HTML
                for i, block in enumerate(blocks):
                    block_data = self._process_block_with_emails(block, emails_from_raw_html, i)
                    if block_data and len(block_data) > 1:  # Больше, чем просто "database"
                        result_data.append(block_data)

            # Возвращаем пустой список, если данные не найдены
            if not result_data:
                logging.warning("Данные не найдены в результатах")

                # Крайний случай: создаем базовую запись из email и других данных
                if emails_from_raw_html:
                    basic_data = {"database": "Извлеченные данные", "Email": emails_from_raw_html[0]}

                    # Ищем телефонные номера
                    phones = self._extract_phones(soup)
                    if phones:
                        basic_data["Телефон"] = phones[0]

                    # Ищем имена
                    names = self._extract_names(soup)
                    if names:
                        basic_data["ФИО"] = names[0]

                    result_data.append(basic_data)
                else:
                    return []

            # Нормализуем названия полей
            field_mapping = {
                "ФИО": "ФИО",
                "День рождения": "ДАТА РОЖДЕНИЯ",
                "Телефон": "ТЕЛЕФОН",
                "Email": "ПОЧТА",
                "E-mail": "ПОЧТА",
                "Почта": "ПОЧТА",
                "Электронная почта": "ПОЧТА",
                "ИНН": "ИНН",
                "СНИЛС": "СНИЛС",
                "Паспорт": "ПАСПОРТ",
                "Адрес": "АДРЕС",
                "Место рождения": "МЕСТО РОЖДЕНИЯ"
            }

            # Стандартизируем названия полей в результатах
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

            logging.info(f"Обработано {len(standardized_data)} элементов результата")
            return standardized_data
        except Exception as e:
            logging.error(f"Ошибка при парсинге результатов: {str(e)}")
            logging.error(traceback.format_exc())
            return []

    def _extract_emails_from_raw_html(self, html_content):
        """
        Извлекает email-адреса прямо из сырого HTML, минуя BeautifulSoup,
        чтобы избежать преобразований HTML-сущностей.

        Args:
            html_content: Сырой HTML-контент

        Returns:
            list: Список найденных email-адресов
        """
        valid_emails = []

        # 1. Очищаем HTML от тегов script, чтобы избежать JavaScript-строк
        cleaned_html = re.sub(r'<script.*?</script>', '', html_content, flags=re.DOTALL|re.IGNORECASE)

        # 2. Используем специальный регулярный шаблон для email-адресов
        # Этот шаблон исключает версии библиотек и другие ложные срабатывания
        email_pattern = r'(?<![^\s>])([a-zA-Z0-9_.+-]+)@([a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+)(?![^\s<])'

        # 3. Ищем все совпадения
        matches = re.findall(email_pattern, cleaned_html)

        # 4. Собираем полные email-адреса и фильтруем
        for match in matches:
            email = f"{match[0]}@{match[1]}"

            # Проверяем, не является ли это версией библиотеки
            if not re.search(r'\d+\.\d+\.\d+', match[1]) and self._is_valid_email(email):
                valid_emails.append(email)

        # 5. Дополнительно ищем email-адреса внутри атрибутов href="mailto:"
        mailto_pattern = r'href=["\']mailto:([^"\'>]+)["\']'
        mailto_matches = re.findall(mailto_pattern, cleaned_html)

        for email in mailto_matches:
            if '@' in email and self._is_valid_email(email):
                valid_emails.append(email)

        # 6. Ищем email в тегах SPAN и DIV с прямым вхождением
        span_email_pattern = r'<(?:span|div)[^>]*>([a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+)</(?:span|div)>'
        span_matches = re.findall(span_email_pattern, cleaned_html)

        for email in span_matches:
            if self._is_valid_email(email):
                valid_emails.append(email)

        # 7. Ищем в элементах с классом additionalsearch, которые часто содержат email
        additionalsearch_pattern = r'<a class="additionalsearch"[^>]*>\s*<span>([^<]+)</span>\s*</a>'
        for match in re.finditer(additionalsearch_pattern, cleaned_html):
            text = match.group(1)
            if '@' in text and self._is_valid_email(text):
                valid_emails.append(text)

        # 8. Специфический поиск: ищем после "Email:" или "Почта:"
        email_label_pattern = r'(?:Email|Почта|E-mail)[:]\s*(?:<[^>]*>)*\s*([a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+)'
        email_label_matches = re.findall(email_label_pattern, cleaned_html, re.IGNORECASE)

        for email in email_label_matches:
            if self._is_valid_email(email):
                valid_emails.append(email)

        # Удаляем дубликаты и отсортируем
        unique_emails = list(dict.fromkeys([email.lower() for email in valid_emails]))

        # Если ничего не найдено, попробуем еще один проход с менее строгой проверкой
        if not unique_emails:
            # Более общий шаблон
            simple_pattern = r'[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+'
            simple_matches = re.findall(simple_pattern, cleaned_html)

            for email in simple_matches:
                if self._is_valid_email(email):
                    unique_emails.append(email.lower())

        return unique_emails

    def _process_block_with_emails(self, block, emails_from_raw_html, block_index):
        """
        Обрабатывает блок данных, используя список email-адресов из сырого HTML

        Args:
            block: BeautifulSoup-элемент блока данных
            emails_from_raw_html: Список email-адресов из сырого HTML
            block_index: Индекс блока (для сопоставления email с блоками)

        Returns:
            dict: Словарь с данными из блока
        """
        block_data = {"database": ""}

        # Получаем имя базы данных
        header = block.select_one("div.simple--block--header div.title")
        if header:
            block_data["database"] = header.text.strip()
        else:
            # Пробуем альтернативные шаблоны заголовка
            header = block.select_one("div.title")
            if header:
                block_data["database"] = header.text.strip()

        # Получаем HTML блока для прямого поиска email
        block_html = str(block)

        # Ищем email в HTML блока напрямую
        block_emails = []
        for email in emails_from_raw_html:
            if email.lower() in block_html.lower():
                block_emails.append(email)

        # Если найдены email, добавляем первый в данные блока
        if block_emails:
            block_data["Email"] = block_emails[0]

        # Обрабатываем все пары поле-значение внутри блока
        field_pairs = self._extract_field_pairs(block)
        for field_name, field_value in field_pairs:
            # Специальная обработка полей email
            if field_name.lower() in ["email", "почта", "e-mail", "электронная почта"]:
                # Если это поле [email protected], заменяем его реальным email
                if field_value == "[email protected]" or "[email protected]" in field_value or '@' not in field_value:
                    # Используем найденный email, если доступен
                    if block_emails:
                        field_value = block_emails[0]
                    elif len(emails_from_raw_html) > block_index:
                        # Пробуем сопоставить email из общего списка с текущим блоком
                        field_value = emails_from_raw_html[block_index]
                    elif emails_from_raw_html:
                        # В крайнем случае берем первый email
                        field_value = emails_from_raw_html[0]

            if field_name and field_value:
                block_data[field_name] = field_value

        return block_data

    def _extract_data_from_whole_page(self, soup, emails_from_raw_html):
        """
        Извлекает данные со всей страницы, когда стандартные блоки не найдены

        Args:
            soup: BeautifulSoup объект страницы
            emails_from_raw_html: Список email-адресов из сырого HTML

        Returns:
            dict: Словарь с извлеченными данными
        """
        block_data = {"database": "Извлеченные данные"}

        # Извлекаем заголовок или название, если доступно
        title = soup.select_one('h1, h2, h3, .title, .header')
        if title:
            block_data["database"] = title.text.strip()

        # Добавляем email, если доступен
        if emails_from_raw_html:
            block_data["Email"] = emails_from_raw_html[0]

        # Извлекаем телефонные номера
        phones = self._extract_phones(soup)
        if phones:
            block_data["Телефон"] = phones[0]

        # Извлекаем имена/ФИО
        names = self._extract_names(soup)
        if names:
            block_data["ФИО"] = names[0]

        # Пробуем найти другие помеченные данные
        field_labels = soup.select('label, dt, th, .field-label')
        for label in field_labels:
            field_name = label.text.strip().rstrip(':')
            if field_name and field_name not in ['Email', 'Телефон', 'ФИО']:
                # Ищем связанное значение
                value = None

                # Проверяем атрибут 'for'
                if label.get('for'):
                    input_id = label.get('for')
                    input_elem = soup.select_one(f'#{input_id}')
                    if input_elem:
                        value = input_elem.get('value', '')

                # Проверяем следующий соседний элемент
                if not value and label.next_sibling:
                    sibling = label.next_sibling
                    if isinstance(sibling, Tag):
                        value = sibling.text.strip()

                # Добавляем в данные, если найдено
                if value:
                    block_data[field_name] = value

        return block_data if len(block_data) > 1 else None

    def _extract_field_pairs(self, block):
        """Извлекает все пары поле-значение из блока, используя различные шаблоны"""
        pairs = []

        # Шаблон 1: Стандартная структура column-flex-content
        flex_contents = block.select("div.column--flex--content")
        for content in flex_contents:
            title_elem = content.select_one("div.column--flex--title")
            value_elem = content.select_one("div.column--flex--result")

            if title_elem and value_elem:
                field_name = title_elem.text.strip().rstrip(':')
                field_value = value_elem.text.strip()
                pairs.append((field_name, field_value))

        # Шаблон 2: Общие пары метка-значение
        if not pairs:
            # Пробуем найти пары поле-значение в разных форматах
            labels = block.select("label, .field-label, .form-label, dt, th")
            for label in labels:
                field_name = label.text.strip().rstrip(':')
                # Ищем значение в соседнем или связанном элементе
                if label.next_sibling:
                    value_elem = label.next_sibling
                    if isinstance(value_elem, Tag):
                        field_value = value_elem.text.strip()
                        pairs.append((field_name, field_value))
                # Пробуем шаблон поля формы
                elif label.get('for'):
                    field_id = label.get('for')
                    value_elem = block.select_one(f"#{field_id}")
                    if value_elem:
                        field_value = value_elem.get('value', '').strip()
                        if field_value:
                            pairs.append((field_name, field_value))

        # Шаблон 3: Списки определений
        definitions = block.select("dl")
        for dl in definitions:
            terms = dl.select("dt")
            values = dl.select("dd")
            for i in range(min(len(terms), len(values))):
                field_name = terms[i].text.strip().rstrip(':')
                field_value = values[i].text.strip()
                pairs.append((field_name, field_value))

        # Шаблон 4: Строки таблицы
        rows = block.select("tr")
        for row in rows:
            cells = row.select("td, th")
            if len(cells) >= 2:
                field_name = cells[0].text.strip().rstrip(':')
                field_value = cells[1].text.strip()
                pairs.append((field_name, field_value))

        # Шаблон 5: Структура item--list из примера
        item_lists = block.select(".item--list")
        for item in item_lists:
            title_elem = item.select_one(".title--item")
            about_elem = item.select_one(".about--item")

            if title_elem and about_elem:
                field_name = title_elem.text.strip()
                field_value = about_elem.text.strip()
                pairs.append((field_name, field_value))

        # Шаблон 6: Структура about/title--item из нового примера
        about_divs = block.select(".about")
        for about in about_divs:
            title_elem = about.select_one(".title--item")
            about_elem = about.select_one(".about--item")

            if title_elem and about_elem:
                field_name = title_elem.text.strip()
                # Особое внимание к элементам additionalsearch, которые часто содержат email
                email_elem = about_elem.select_one("a.additionalsearch span")
                if email_elem and '@' in email_elem.text:
                    field_value = email_elem.text.strip()
                else:
                    field_value = about_elem.text.strip()
                pairs.append((field_name, field_value))

        return pairs

    def _extract_phones(self, soup):
        """Извлекает телефонные номера со страницы"""
        phones = set()

        # Шаблон 1: Телефон в специфическом элементе
        phone_elements = soup.select('[class*="phone"], [id*="phone"], [class*="tel"], [id*="tel"]')
        for element in phone_elements:
            text = element.text.strip()
            phone = self._clean_phone(text)
            if phone:
                phones.add(phone)

        # Шаблон 2: Метки телефонов
        phone_labels = soup.select('label:contains("Телефон"), span:contains("Телефон"), div:contains("Телефон")')
        for label in phone_labels:
            # Проверяем соседние и дочерние элементы
            for element in list(label.next_siblings) + list(label.find_all()):
                if isinstance(element, Tag):
                    text = element.text.strip()
                    phone = self._clean_phone(text)
                    if phone:
                        phones.add(phone)

        # Шаблон 3: Шаблон телефона в тексте
        phone_pattern = r'(?:\+7|8)[- (]?\d{3}[- )]?\d{3}[- ]?\d{2}[- ]?\d{2}'
        for element in soup.find_all(text=True):
            if not isinstance(element, str):
                continue
            text = element.strip()
            match = re.search(phone_pattern, text)
            if match:
                phone = self._clean_phone(match.group(0))
                if phone:
                    phones.add(phone)

        return list(phones)

    def _extract_names(self, soup):
        """Извлекает имена людей со страницы"""
        names = set()

        # Шаблон 1: Имя в специфическом элементе
        name_elements = soup.select('[class*="name"], [id*="name"], [class*="fio"], [id*="fio"]')
        for element in name_elements:
            text = element.text.strip()
            if len(text.split()) >= 2 and len(text) > 5:
                names.add(text)

        # Шаблон 2: Метки имён
        name_labels = soup.select('label:contains("ФИО"), span:contains("ФИО"), div:contains("ФИО")')
        for label in name_labels:
            # Проверяем соседние и дочерние элементы
            for element in list(label.next_siblings) + list(label.find_all()):
                if isinstance(element, Tag):
                    text = element.text.strip()
                    if len(text.split()) >= 2 and len(text) > 5:
                        names.add(text)

        return list(names)

    def _clean_phone(self, phone_text):
        """Очищает и форматирует телефонный номер"""
        if not phone_text:
            return None

        # Извлекаем только цифры
        digits = ''.join(c for c in phone_text if c.isdigit())

        # Проверяем, похоже ли это на телефонный номер
        if len(digits) >= 10:
            # Форматируем в стандартную форму
            if len(digits) == 10:
                return f"+7{digits}"
            elif len(digits) == 11 and digits[0] in ('7', '8'):
                return f"+7{digits[1:]}"
            else:
                return f"+{digits}"

        return None

    def _is_valid_email(self, email):
        """
        Улучшенная валидация структуры email-адреса с фильтрацией версий библиотек и других false-positive
        """
        if not email or '@' not in email:
            return False

        # Проверка минимальной длины и наличие точки в домене
        if len(email) < 5 or '.' not in email.split('@')[1]:
            return False

        # Разбиваем email на части до и после @
        username, domain = email.split('@', 1)

        # Проверка на корректность имени пользователя
        if not username or len(username) < 2:
            return False

        # Проверка на корректность домена
        if not domain or len(domain) < 4 or '.' not in domain:
            return False

        # Проверка на распространенные TLD (top-level domains)
        valid_tlds = [
            '.ru', '.com', '.net', '.org', '.info', '.biz', '.edu', '.gov',
            '.cc', '.io', '.co', '.uk', '.de', '.fr', '.it', '.es', '.ca',
            '.eu', '.me', '.ua', '.by', '.kz', '.su', '.рф', '.москва'
        ]

        has_valid_tld = False
        for tld in valid_tlds:
            if domain.lower().endswith(tld):
                has_valid_tld = True
                break

        # Если нет валидного TLD, это, скорее всего, не email
        if not has_valid_tld:
            # Проверяем специфические шаблоны распространенных российских доменов
            if not (domain.endswith('.ru') or domain.endswith('.рф') or
                    domain.endswith('.su') or domain.endswith('.москва') or
                    domain.endswith('.mail.ru') or domain.endswith('.yandex.ru') or
                    domain.endswith('.gmail.com') or domain.endswith('.ya.ru')):
                return False

        # Проверка на версии библиотек и пакетов
        version_patterns = [
            r'@\d+\.\d+\.\d+',       # @1.2.3
            r'@v\d+',                # @v1
            r'@latest',              # @latest
            r'@dev',                 # @dev
            r'@alpha',               # @alpha
            r'@beta',                # @beta
            r'@\d+\.\d+',            # @1.0
        ]

        for pattern in version_patterns:
            if re.search(pattern, email):
                return False

        # Проверка на другие невалидные паттерны
        invalid_patterns = [
            r'example\.com$',        # example.com
            r'test\.com$',           # test.com
            r'sample\.com$',         # sample.com
            r'@\.',                  # @.
            r'\.\@',                 # .@
            r'@$',                   # @
            r'^@',                   # @
            r'cdn',                  # cdn в адресе
            r'\.js',                 # .js
            r'\.min',                # .min
            r'\.css',                # .css
            r'script',               # script
            r'library',              # library
            r'sweetalert',           # sweetalert
            r'toast',                # toast
            r'jquery',               # jquery
            r'bootstrap',            # bootstrap
            r'protected',            # protected
        ]

        for pattern in invalid_patterns:
            if re.search(pattern, email, re.IGNORECASE):
                return False

        # Проверка на наличие нецензурных слов или spam-indicators
        bad_words = ['spam', 'sex', 'fuck', 'admin']
        if any(word in email.lower() for word in bad_words):
            return False

        # Проверка на слишком короткий домен верхнего уровня
        tld = domain.split('.')[-1]
        if len(tld) < 2:
            return False

        return True

    def _find_all_emails_on_page(self, soup, html_content):
        """Find all email addresses on the page from various sources"""
        emails = set()

        # Method 1: Regular expression on the whole HTML but exclude script and link tags
        # Сначала удалим содержимое тегов script и link из анализируемого HTML
        html_no_scripts = re.sub(r'<script[^>]*>.*?</script>', '', html_content, flags=re.DOTALL)
        html_no_scripts = re.sub(r'<link[^>]*>.*?</link>', '', html_no_scripts, flags=re.DOTALL)

        email_pattern = r'([a-zA-Z0-9_.+-]+)@([a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+)'
        email_matches = re.finditer(email_pattern, html_no_scripts)
        for match in email_matches:
            email = match.group(0).lower()
            if self._is_valid_email(email):
                emails.add(email)

        # Method 2: Form input fields with email type or name
        email_inputs = soup.select('input[type="email"], input[name*="email"], input[placeholder*="mail"]')
        for input_field in email_inputs:
            value = input_field.get('value', '')
            if '@' in value and self._is_valid_email(value):
                emails.add(value.lower())

        # Method 3: Elements with "email" in class or id
        email_elements = soup.select('[class*="email"], [id*="email"], [class*="mail"], [id*="mail"]')
        for element in email_elements:
            text = element.text.strip()
            if '@' in text:
                # Extract email with regex to handle surrounding text
                email_match = re.search(email_pattern, text)
                if email_match and self._is_valid_email(email_match.group(0)):
                    emails.add(email_match.group(0).lower())

        # Method 4: Elements near email-related labels
        email_labels = soup.select('label:contains("Email"), span:contains("Email"), div:contains("Email")')
        for label in email_labels:
            # Check siblings and children
            for element in list(label.next_siblings) + list(label.find_all()):
                if isinstance(element, Tag):
                    text = element.text.strip()
                    if '@' in text:
                        email_match = re.search(email_pattern, text)
                        if email_match and self._is_valid_email(email_match.group(0)):
                            emails.add(email_match.group(0).lower())

        return list(emails)

    def _extract_email_from_tags(self, block):
        """Extract email from the list-tags section of a block"""
        try:
            # Pattern 1: Standard list-tags
            tags_section = block.select_one("div.list-tags")
            if tags_section:
                email_spans = tags_section.select("span:contains('Email:') a")
                for span in email_spans:
                    if '@' in span.text:
                        return span.text.strip()

            # Pattern 2: Email in any tag with explicit label
            email_tags = block.select('[class*="tag"]:contains("Email"), [class*="label"]:contains("Email")')
            for tag in email_tags:
                text = tag.text.strip()
                if '@' in text:
                    email_pattern = r'([a-zA-Z0-9_.+-]+)@([a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+)'
                    email_match = re.search(email_pattern, text)
                    if email_match:
                        return email_match.group(0)

            return None
        except Exception as e:
            logging.error(f"Error extracting email from tags: {e}")
            return None

    def get_stats(self):
        """
        Get comprehensive session statistics for diagnostics.

        Returns:
            dict: Session statistics
        """
        return {
            "session_id": self.session_id,
            "login": self.login,
            "is_authenticated": self.is_authenticated,
            "search_count": self.search_count,
            "successful_searches": self.successful_searches,
            "failed_searches": self.failed_searches,
            "error_count": self.error_count,
            "auth_attempts": self.auth_attempts,
            "auth_failures": self.consecutive_auth_failures,
            "avg_response_time": round(self.avg_response_time, 3) if self.avg_response_time else None,
            "last_activity": self.last_activity.isoformat() if self.last_activity else None,
            "session_valid_until": self.session_valid_until.isoformat() if self.session_valid_until else None,
            "is_busy": self.is_busy
        }

    @classmethod
    async def cleanup(cls):
        """
        Clean up connection pool - simplified to avoid trying to close connections that may already be closed
        """
        # Just log that cleanup was called - individual sessions will clean up their own resources
        logging.info("Session cleanup called")

        # No need to close the connection pool, as we're not using a shared connector anymore
        cls._conn_pool = None