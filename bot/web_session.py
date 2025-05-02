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

import asyncio
from typing import Tuple, Optional, Dict, List, Any

import aiohttp
from bs4 import BeautifulSoup, Tag
from pycparser.ply.yacc import debug_file

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

    # Add a new method to SauronWebSession:
    def _extract_real_emails_from_rendered_html(self, rendered_html):
        """
        Extract real email addresses from fully rendered HTML content.
        This will process the HTML after all JavaScript execution.

        Args:
            rendered_html: HTML content after browser rendering

        Returns:
            list: List of extracted email addresses
        """
        # Clean HTML by removing script tags
        cleaned_html = re.sub(r'<script.*?</script>', '', rendered_html, flags=re.DOTALL)

        # Look for email patterns
        email_pattern = r'[\w\.-]+@[\w\.-]+\.\w+'
        emails = re.findall(email_pattern, cleaned_html)

        # Filter valid emails
        valid_emails = []
        for email in emails:
            if self._is_valid_email(email):
                valid_emails.append(email)

        # Look for specific email containers in the DOM structure
        try:
            soup = BeautifulSoup(rendered_html, 'html.parser')

            # Find all elements containing email-related classes or text
            email_elements = soup.select('[class*="email"], [class*="mail"], [href*="mailto"]')
            for element in email_elements:
                # Extract text content
                text = element.get_text().strip()
                # Check for mailto: href
                href = element.get('href', '')

                # Extract from mailto: links
                if href.startswith('mailto:'):
                    email = href[7:].split('?')[0]  # Remove mailto: prefix and any parameters
                    if self._is_valid_email(email):
                        valid_emails.append(email)

                # Extract from text content
                email_matches = re.findall(email_pattern, text)
                for email in email_matches:
                    if self._is_valid_email(email):
                        valid_emails.append(email)
        except Exception as e:
            logging.error(f"Error extracting emails from DOM: {e}")

        # Remove duplicates and sort
        unique_emails = sorted(list(set(valid_emails)))
        return unique_emails

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
        Enhanced search method with fallback to Playwright for handling obfuscated emails.
        First tries the standard HTTP request, and if emails are obfuscated, falls back to browser automation.
        """
        if not self.is_authenticated:
            logging.warning(f"Session {self.session_id}: Attempting search without authentication")
            auth_success = await self.authenticate()
            if not auth_success:
                return False, "Authentication failed before search"

        # Check cache for identical query
        query_hash = hashlib.md5(query.encode()).hexdigest()

        async with self._cache_lock:
            if query_hash in self._result_cache:
                result, timestamp = self._result_cache[query_hash]
                age = (datetime.now() - timestamp).total_seconds()

                # Return cached result if fresh enough
                if age < self._result_cache_ttl:
                    logging.info(f"Session {self.session_id}: Using cached result for query '{query[:20]}...'")
                    return True, result

        self.is_busy = True
        try:
            # Increment search counter
            self.search_count += 1
            logging.info(f"Session {self.session_id}: Performing search #{self.search_count} for query: {query}")

            # First try with standard HTTP request
            try:
                # Prepare search headers with appropriate content type
                search_headers = {
                    "Content-Type": "application/x-www-form-urlencoded",
                    "X-Requested-With": "XMLHttpRequest",
                    "Origin": self.BASE_URL,
                    "Referer": f"{self.BASE_URL}/dashboard"
                }

                # Send search request with optimized timeout
                search_data = {"query": query}
                status, content = await self._make_request(
                    "POST",
                    "/api/v1/search/full",
                    data=search_data,
                    headers=search_headers,
                    timeout=35
                )

                if status != 200:
                    self.failed_searches += 1
                    logging.error(f"Session {self.session_id}: Search failed with status {status}")
                    return False, f"Search error, status {status}"

                try:
                    # Parse JSON response
                    response_data = json.loads(content)
                    logging.info(f"Search response status: ok={response_data.get('ok')}")

                    if response_data.get('ok') == True:
                        # Check for redirection
                        redirection = None
                        if response_data.get('result') and response_data['result'].get('redirection'):
                            redirection = response_data['result']['redirection']

                        if redirection:
                            logging.info(f"Following redirection to: {redirection}")

                            # Save HTML before parsing
                            status, result_page = await self._make_request(
                                "GET",
                                redirection,
                                timeout=40,
                                headers={"Referer": f"{self.BASE_URL}/api/v1/search/full"}
                            )

                            if status != 200:
                                self.failed_searches += 1
                                logging.error(
                                    f"Session {self.session_id}: Failed to load results page, status {status}")
                                return False, "Failed to load results page"

                            # Check if emails are obfuscated
                            if "[email protected]" in result_page:
                                logging.warning("Detected obfuscated emails in response. Trying Playwright fallback.")
                                # Fall back to browser automation
                                success, rendered_content = await self.get_rendered_page(query)

                                if success:
                                    logging.info("Successfully retrieved rendered content with Playwright")
                                    # Use the fully rendered content instead
                                    result_page = rendered_content

                            # Increment successful search counter
                            self.successful_searches += 1

                            # Save result to cache
                            async with self._cache_lock:
                                self._result_cache[query_hash] = (result_page, datetime.now())

                                # Clean up cache if it gets too large
                                if len(self._result_cache) > 100:
                                    sorted_items = sorted(self._result_cache.items(),
                                                          key=lambda x: x[1][1])
                                    for key, _ in sorted_items[:20]:
                                        del self._result_cache[key]

                            return True, result_page
                        else:
                            self.failed_searches += 1
                            logging.error(f"Session {self.session_id}: No redirection in search response")
                            return False, "No redirection in search response"
                    else:
                        # Search error
                        self.failed_searches += 1
                        error_message = response_data.get('description', 'Unknown error')
                        logging.error(f"Session {self.session_id}: Search error: {error_message}")

                        # Check if related to authentication
                        if any(term in error_message.lower() for term in ["авториз", "session", "login", "токен"]):
                            self.is_authenticated = False
                            self.session_valid_until = None

                        return False, error_message
                except Exception as e:
                    self.failed_searches += 1
                    logging.error(f"Session {self.session_id}: Error parsing search response: {str(e)}")
                    return False, f"Error processing search response: {str(e)}"
            except Exception as e:
                logging.error(f"Error in standard search method: {e}")
                # Try with Playwright as fallback for any error
                logging.info("Attempting fallback to browser automation with Playwright")
                success, rendered_content = await self.get_rendered_page(query)

                if success:
                    # Save to cache
                    async with self._cache_lock:
                        self._result_cache[query_hash] = (rendered_content, datetime.now())

                    return True, rendered_content
                else:
                    return False, rendered_content
        finally:
            self.is_busy = False

    async def parse_results(self, html_content):
        """
        Улучшенный парсер для извлечения структурированных данных из HTML с улучшенной
        обработкой email, специфичной для sauron.info.

        Args:
            html_content: HTML строка со страницей результатов

        Returns:
            list: Список словарей с извлеченными данными
        """
        try:
            # Check if content is from Playwright (fully rendered)
            is_rendered = "[email protected]" not in html_content

            # Extract emails based on content type
            if is_rendered:
                emails_from_html = self._extract_real_emails_from_rendered_html(html_content)
                logging.info(f"Extracted {len(emails_from_html)} emails from rendered HTML")
            else:
                emails_from_html = self._extract_emails_from_raw_html(html_content)
                logging.info(f"Extracted {len(emails_from_html)} emails from raw HTML")
            try:
                with open(debug_file, "w", encoding="utf-8") as f:
                    for email in emails_from_raw_html:
                        f.write(f"{email}\n")
            except Exception as e:
                logging.error(f"Ошибка сохранения email для отладки: {e}")

            # Создаем BeautifulSoup для парсинга структуры
            soup = BeautifulSoup(html_content, 'html.parser')

            # Проверяем валидность страницы
            if ("Запрос:" not in html_content and
                    "Подробный отчет" not in html_content and
                    "Общая сводка" not in html_content):
                logging.warning("Нестандартный формат страницы - попытаемся извлечь данные в любом случае")

            # Извлекаем результаты поиска
            result_data = []

            # Извлекаем все блоки данных
            blocks = soup.select("div.simple--block.simple--result--ltd")
            if not blocks:
                # Пробуем альтернативные шаблоны блоков
                blocks = soup.select("div.simple--block")

            logging.info(f"Найдено {len(blocks)} блоков данных")

            # Если блоки не найдены, пытаемся извлечь данные из основной структуры
            if not blocks:
                main_block = self._extract_data_from_whole_page(soup, emails_from_raw_html)
                if main_block:
                    result_data.append(main_block)
            else:
                # Обрабатываем каждый блок с email-адресами из сырого HTML
                for i, block in enumerate(blocks):
                    block_data = self._process_block_with_emails(block, emails_from_raw_html, i)
                    if block_data and len(block_data) > 1:  # Больше, чем просто "database"
                        result_data.append(block_data)

            # Если данные не найдены, но у нас есть email-адреса, создаем базовые записи
            if not result_data and emails_from_raw_html:
                logging.warning("Структурированные данные не найдены - создаем базовые записи из email-адресов")

                for i, email in enumerate(emails_from_raw_html):
                    basic_data = {
                        "database": f"Извлеченные данные {i + 1}",
                        "Email": email
                    }

                    # Пытаемся найти телефонные номера
                    phones = self._extract_phones(soup)
                    if phones and i < len(phones):
                        basic_data["Телефон"] = phones[i]
                    elif phones:
                        basic_data["Телефон"] = phones[0]

                    # Пытаемся найти имена
                    names = self._extract_names(soup)
                    if names and i < len(names):
                        basic_data["ФИО"] = names[i]
                    elif names:
                        basic_data["ФИО"] = names[0]

                    result_data.append(basic_data)

            # Стандартизируем названия полей
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

                    std_key = field_mapping.get(key, key)
                    std_item[std_key] = value

                standardized_data.append(std_item)

            logging.info(f"Обработано {len(standardized_data)} элементов результата")
            return standardized_data

        except Exception as e:
            logging.error(f"Ошибка парсинга результатов: {str(e)}")
            logging.error(traceback.format_exc())
            return []

    def _extract_emails_from_raw_html(self, html_content):
        """
        Извлекает email-адреса напрямую из сырого HTML с улучшенными шаблонами,
        специально нацеленными на структуру HTML сайта sauron.info.

        Args:
            html_content: Сырой HTML-контент

        Returns:
            list: Список найденных email-адресов
        """
        valid_emails = []

        # Сначала очищаем HTML от тегов script, чтобы избежать JavaScript-строк
        cleaned_html = re.sub(r'<script.*?</script>', '', html_content, flags=re.DOTALL | re.IGNORECASE)

        # === СПЕЦИФИЧЕСКИЕ ШАБЛОНЫ ДЛЯ SAURON.INFO ===

        # Шаблон 1: <a class="additionalsearch"><span>EMAIL</span></a>
        # Это самый распространенный шаблон на sauron.info
        additionalsearch_pattern = r'<a\s+class=["\']additionalsearch["\'][^>]*>\s*<span>([^<]+)</span>\s*</a>'
        for match in re.finditer(additionalsearch_pattern, cleaned_html):
            email_candidate = match.group(1).strip()
            if '@' in email_candidate:
                valid_emails.append(email_candidate)
                logging.info(f"Найден email через шаблон additionalsearch: {email_candidate}")

        # Шаблон 2: Email в структуре column--flex
        # <div class="column--flex--title">Email:</div><div class="column--flex--result">EMAIL</div>
        flex_pattern = r'<div\s+class=["\']column--flex--title["\'][^>]*>\s*(?:Email|Почта|E-mail|Электронная почта)[^<]*</div>\s*<div\s+class=["\']column--flex--result["\'][^>]*>\s*([^<]+)\s*</div>'
        for match in re.finditer(flex_pattern, cleaned_html, re.IGNORECASE):
            email_candidate = match.group(1).strip()
            if '@' in email_candidate:
                valid_emails.append(email_candidate)
                logging.info(f"Найден email через шаблон column-flex: {email_candidate}")

        # Шаблон 3: Секция list-tags с email
        # <div class="list-tags">...<span>Email: <a class="additionalsearch">EMAIL</a></span>...</div>
        tags_pattern = r'<div\s+class=["\']list-tags["\'][^>]*>.*?<span>[^<]*(?:Email|Почта|E-mail):\s*<a[^>]*>([^<]+)</a>\s*</span>.*?</div>'
        for match in re.finditer(tags_pattern, cleaned_html, re.IGNORECASE | re.DOTALL):
            email_candidate = match.group(1).strip()
            if '@' in email_candidate:
                valid_emails.append(email_candidate)
                logging.info(f"Найден email через шаблон list-tags: {email_candidate}")

        # Шаблон 4: Email в любом div с классом "about--item" (часто в секциях профиля)
        about_item_pattern = r'<div\s+class=["\'][^"\']*about--item[^"\']*["\'][^>]*>\s*<a[^>]*>\s*<span>([^<]+)</span>\s*</a>'
        for match in re.finditer(about_item_pattern, cleaned_html):
            email_candidate = match.group(1).strip()
            if '@' in email_candidate:
                valid_emails.append(email_candidate)
                logging.info(f"Найден email через шаблон about-item: {email_candidate}")

        # Шаблон 5: Прямой email-адрес в любом теге <a> с href="mailto:"
        mailto_pattern = r'href=["\']mailto:([^"\'>]+)["\']'
        mailto_matches = re.findall(mailto_pattern, cleaned_html)
        for email in mailto_matches:
            if '@' in email and self._is_valid_email(email):
                valid_emails.append(email)
                logging.info(f"Найден email через шаблон mailto: {email}")

        # Шаблон 6: Поиск email-подобных структур рядом с текстом "Email:"
        email_label_pattern = r'(?:Email|Почта|E-mail|Электронная почта)[:]?\s*(?:<[^>]*>)*\s*([a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+)'
        email_label_matches = re.findall(email_label_pattern, cleaned_html, re.IGNORECASE)
        for email in email_label_matches:
            if self._is_valid_email(email):
                valid_emails.append(email)
                logging.info(f"Найден email рядом с меткой: {email}")

        # === ЗАПАСНЫЕ ШАБЛОНЫ ===

        # Если мы все еще не нашли email, пробуем более общие шаблоны
        if not valid_emails:
            # Общий шаблон регулярного выражения для email
            email_pattern = r'([a-zA-Z0-9_.+-]+)@([a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+)'
            for match in re.finditer(email_pattern, cleaned_html):
                email = f"{match.group(1)}@{match.group(2)}"
                if self._is_valid_email(email):
                    valid_emails.append(email)
                    logging.info(f"Найден email через общий шаблон: {email}")

        # Удаляем дубликаты и сортируем
        unique_emails = sorted(list(set(valid_emails)))

        if unique_emails:
            logging.info(f"Успешно извлечено {len(unique_emails)} уникальных email-адресов")
        else:
            logging.warning("В HTML-контенте не найдено email-адресов")

        return unique_emails

    def _process_block_with_emails(self, block, emails_from_raw_html, block_index):
        """
        Улучшенная обработка блоков с лучшей обработкой email для sauron.info

        Args:
            block: BeautifulSoup-элемент блока данных
            emails_from_raw_html: Список email-адресов из сырого HTML
            block_index: Индекс блока (для сопоставления email)

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

        # Преобразуем блок в строку для прямого поиска email
        block_html = str(block)

        # Ищем email-адреса в HTML блока напрямую
        block_emails = []
        for email in emails_from_raw_html:
            # Поиск без учета регистра для максимального совпадения
            if email.lower() in block_html.lower():
                block_emails.append(email)

        # Если мы нашли email в этом блоке, добавляем первый из них
        if block_emails:
            block_data["Email"] = block_emails[0]
            logging.info(f"Привязан email к блоку '{block_data['database']}': {block_emails[0]}")

        # Обрабатываем все пары поле-значение в блоке
        field_pairs = self._extract_field_pairs(block)
        for field_name, field_value in field_pairs:
            # Специальная обработка для полей email
            if field_name.lower() in ["email", "почта", "e-mail", "электронная почта"]:
                # Обнаружение обфусцированных email типа [email protected]
                if (field_value == "[email protected]" or
                        "[email protected]" in field_value or
                        '@' not in field_value or
                        not self._is_valid_email(field_value)):

                    # Заменяем реальным email в следующем порядке приоритета:
                    # 1. Email, специфичные для блока
                    # 2. Email из глобального списка, соответствующий позиции
                    # 3. Любой доступный email
                    if block_emails:
                        field_value = block_emails[0]
                        logging.info(f"Заменен [email protected] на email из блока: {field_value}")
                    elif emails_from_raw_html:
                        # Пытаемся получить email по позиции или используем первый
                        if block_index < len(emails_from_raw_html):
                            field_value = emails_from_raw_html[block_index]
                        else:
                            field_value = emails_from_raw_html[0]
                        logging.info(f"Заменен [email protected] на извлеченный email: {field_value}")

            # Добавляем поле в наши данные, если существуют и имя, и значение
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

    # Inside the SauronWebSession class, add:
    @classmethod
    async def setup_playwright(cls):
        """Initialize Playwright for browser automation"""
        try:
            from playwright.async_api import async_playwright
            # Install browsers if not already installed
            import subprocess
            import sys
            try:
                subprocess.run([sys.executable, "-m", "playwright", "install", "chromium"], check=True)
                logging.info("Playwright browsers installed successfully")
            except subprocess.CalledProcessError as e:
                logging.error(f"Failed to install Playwright browsers: {e}")

            cls._playwright_initialized = True
            logging.info("Playwright setup completed")
        except ImportError:
            logging.error("Playwright not installed. Install with: pip install playwright")
            cls._playwright_initialized = False

    async def get_rendered_page(self, query: str) -> Tuple[bool, str]:
        """
        Retrieve fully rendered page content using Playwright browser automation.
        This bypasses email obfuscation by executing all JavaScript.

        Args:
            query: Search query text

        Returns:
            Tuple[bool, str]: Success status and page content/error message
        """
        try:
            from playwright.async_api import async_playwright

            if not hasattr(self.__class__, '_playwright_initialized'):
                await self.__class__.setup_playwright()

            if not getattr(self.__class__, '_playwright_initialized', False):
                return False, "Playwright not initialized"

            logging.info(f"Session {self.session_id}: Retrieving rendered page for query '{query}'")

            async with async_playwright() as p:
                # Use incognito context for clean session
                browser = await p.chromium.launch(headless=True)
                context = await browser.new_context(
                    user_agent=self.user_agent,
                    viewport={"width": 1280, "height": 800}
                )

                page = await context.new_page()

                # Go to login page
                try:
                    logging.info(f"Session {self.session_id}: Opening login page")
                    await page.goto(f"{self.BASE_URL}/login", wait_until="networkidle")
                except Exception as e:
                    logging.error(f"Failed to load login page: {e}")
                    await browser.close()
                    return False, f"Login page error: {str(e)}"

                # Perform login
                try:
                    logging.info(f"Session {self.session_id}: Logging in as {self.login}")
                    await page.fill('input[name="login"]', self.login)
                    await page.fill('input[name="password"]', self.password)
                    await page.click('button[type="submit"]')

                    # Wait for login to complete
                    await page.wait_for_navigation(wait_until="networkidle", timeout=30000)
                except Exception as e:
                    logging.error(f"Login failed: {e}")
                    await browser.close()
                    return False, f"Authentication error: {str(e)}"

                # Verify login success
                if "/login" in page.url:
                    logging.error(f"Session {self.session_id}: Login failed - still on login page")
                    await browser.close()
                    return False, "Authentication failed"

                # Perform search
                try:
                    # Navigate to search page
                    search_url = f"{self.BASE_URL}/dashboard"
                    await page.goto(search_url, wait_until="networkidle")

                    # Enter search query and submit
                    await page.fill('input[name="query"]', query)
                    await page.press('input[name="query"]', 'Enter')

                    # Wait for search results
                    await page.wait_for_selector('.simple--block, .simple--result--ltd', timeout=60000)

                    # Wait additional time for any async rendering
                    await asyncio.sleep(2)

                    # Get the fully rendered content
                    content = await page.content()

                    # Save the content for debugging
                    timestamp = int(time.time())
                    debug_file = f"debug_playwright_{timestamp}.html"
                    try:
                        with open(debug_file, "w", encoding="utf-8") as f:
                            f.write(content)
                        logging.info(f"Saved rendered HTML to {debug_file}")
                    except Exception as e:
                        logging.error(f"Error saving debug HTML: {e}")

                    await browser.close()
                    return True, content

                except Exception as e:
                    logging.error(f"Search failed: {e}")
                    await browser.close()
                    return False, f"Search error: {str(e)}"

        except ImportError:
            logging.error("Playwright not installed. Search falling back to regular HTTP requests.")
            return False, "Playwright not available"
        except Exception as e:
            logging.error(f"Unexpected error in get_rendered_page: {e}")
            return False, f"Error: {str(e)}"

    @classmethod
    async def cleanup(cls):
        """
        Clean up connection pool - simplified to avoid trying to close connections that may already be closed
        """
        # Just log that cleanup was called - individual sessions will clean up their own resources
        logging.info("Session cleanup called")

        # No need to close the connection pool, as we're not using a shared connector anymore
        cls._conn_pool = None