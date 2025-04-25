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
import logging
import random
import re
import time
import traceback
from datetime import datetime, timedelta
import json
import hashlib

import aiohttp
from bs4 import BeautifulSoup

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
        Perform a search with the specified query using optimized handling.

        Args:
            query: Search query string

        Returns:
            tuple: (success, result_or_error)
        """
        if not self.is_authenticated:
            logging.warning(f"Session {self.session_id}: Attempting search without authentication")
            auth_success = await self.authenticate()
            if not auth_success:
                return False, "Authentication failed before search"

        # Check result cache for identical query
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
            # Increment search count
            self.search_count += 1
            logging.info(f"Session {self.session_id}: Performing search #{self.search_count} for query: {query}")

            # Prepare search headers with appropriate content type
            search_headers = {
                "Content-Type": "application/x-www-form-urlencoded",
                "X-Requested-With": "XMLHttpRequest",  # Indicate AJAX request
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
                timeout=35  # Slightly increased timeout for search
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
                        # Follow redirection to get results with extended timeout
                        status, result_page = await self._make_request(
                            "GET",
                            redirection,
                            timeout=40,  # Extended timeout for results page
                            headers={"Referer": f"{self.BASE_URL}/api/v1/search/full"}
                        )

                        if status != 200:
                            self.failed_searches += 1
                            logging.error(
                                f"Session {self.session_id}: Failed to load results page, status {status}")
                            return False, "Failed to load results page"

                        # Increment successful searches counter
                        self.successful_searches += 1

                        # Verify results page content
                        if "Подробный отчет" in result_page:
                            logging.info("Found search results page with 'Подробный отчет' section")
                        else:
                            logging.warning("Search results page doesn't contain 'Подробный отчет' section")

                        # Cache the result
                        async with self._cache_lock:
                            self._result_cache[query_hash] = (result_page, datetime.now())

                            # Clean up cache if it gets too large (over 100 items)
                            if len(self._result_cache) > 100:
                                # Remove oldest 20 items
                                sorted_items = sorted(self._result_cache.items(),
                                                      key=lambda x: x[1][1])  # Sort by timestamp
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

                    # Check if error is related to authentication
                    if any(term in error_message.lower() for term in ["авториз", "session", "login", "токен"]):
                        self.is_authenticated = False
                        # Force re-authentication on next search
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
        Parse HTML results page to extract structured data.

        Args:
            html_content: HTML string from search results page

        Returns:
            list: List of dictionaries with extracted data
        """
        try:
            soup = BeautifulSoup(html_content, 'html.parser')

            # Verify this is a valid results page
            if "Запрос:" not in html_content or "Подробный отчет" not in html_content:
                logging.error("Invalid results page received")
                # Log the start of the page for debugging
                logging.debug(f"Page start: {html_content[:500]}")
                return []

            # Extract search results
            result_data = []

            # Get query title
            title_elem = soup.select_one("div.title--main")
            query = ""
            if title_elem:
                query_text = title_elem.text
                if "Запрос:" in query_text:
                    query = query_text.replace("Запрос:", "").strip()
                    logging.info(f"Parsed query: {query}")

            # Get result blocks
            blocks = soup.select("div.simple--block.simple--result--ltd")
            logging.info(f"Found {len(blocks)} result blocks")

            for block in blocks:
                block_data = {"database": ""}

                # Get database name
                header = block.select_one("div.simple--block--header div.title")
                if header:
                    block_data["database"] = header.text.strip()
                    logging.debug(f"Found database: {block_data['database']}")

                # Get data fields
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

                # Add block to results if it contains data
                if len(block_data) > 1:  # More than just "database"
                    result_data.append(block_data)

            # Return empty list if no data found
            if not result_data:
                logging.warning("No data found in search results")
                return result_data

            # Normalize field names
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

            # Standardize field names across results
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
            logging.error(traceback.format_exc())
            return []

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