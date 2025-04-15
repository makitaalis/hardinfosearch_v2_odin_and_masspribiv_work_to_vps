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
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:90.0) Gecko/20100101 Firefox/90.0",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/94.0.4606.81 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/95.0.4638.69 Safari/537.36 Edg/95.0.1020.44",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/94.0.4606.81 Safari/537.36",
    "Mozilla/5.0 (iPhone; CPU iPhone OS 15_0 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/15.0 Mobile/15E148 Safari/604.1"
]


class SauronWebSession:
    """Enhanced class for web session management with improved authentication and connection pooling"""

    BASE_URL = "https://sauron.info"

    # Class-level rate limiting variables
    # Shared across all instances to prevent hammering the server with the same credentials
    _last_auth_time = {}  # Dictionary to track last auth time per credential
    _auth_lock = asyncio.Lock()  # Lock to synchronize authentication attempts
    _conn_pool = None  # Shared connection pool
    _auth_rate_limit = 1.5  # seconds between auth attempts with same credentials
    _auth_backoff_base = 2.0  # Base for exponential backoff
    _auth_backoff_max = 30.0  # Maximum backoff time in seconds
    _session_pool_stats = {"total_connections": 0, "active_connections": 0}

    @classmethod
    async def get_connector(cls):
        """Get or create shared connection pool"""
        if cls._conn_pool is None:
            # Create an optimized TCPConnector with higher connection limits
            cls._conn_pool = aiohttp.TCPConnector(
                limit=300,  # Increased from 100 to handle more simultaneous connections
                limit_per_host=100,  # Add host-specific limit
                ttl_dns_cache=300,  # DNS cache time in seconds
                use_dns_cache=True,
                force_close=False,  # Keep connections alive
                enable_cleanup_closed=True,  # Automatically clean up closed connections
                keepalive_timeout=15.0,  # Keep-alive timeout
            )
            logging.info("Created shared connection pool for web sessions")
        return cls._conn_pool

    def __init__(self, login, password, session_id):
        self.login = login
        self.password = password
        self.session_id = session_id
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

        # Tracking for credential uniqueness
        self.credential_key = f"{login}:{password}"

        # Statistics
        self.search_count = 0
        self.successful_searches = 0

    async def _make_request(self, method, url, data=None, headers=None, json=None, allow_redirects=True, timeout=30):
        """Optimized HTTP request handling with enhanced connection pooling"""
        if not url.startswith("http"):
            url = f"{self.BASE_URL}/{url.lstrip('/')}"

        if headers is None:
            headers = {}

        # Enhanced headers for better browser simulation
        if "User-Agent" not in headers:
            headers["User-Agent"] = self.user_agent

        if "Referer" not in headers and "login" not in url:
            headers["Referer"] = f"{self.BASE_URL}/dashboard"

        # Add enhanced headers for better connection handling
        headers["Connection"] = "keep-alive"
        headers["Accept"] = "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8"
        headers["Accept-Language"] = "en-US,en;q=0.5"
        headers["Accept-Encoding"] = "gzip, deflate, br"
        headers["Upgrade-Insecure-Requests"] = "1"

        # Add cache control for non-authentication requests
        if "login" not in url and "authenticateUser" not in url:
            headers["Cache-Control"] = "max-age=0"

        # Update last activity timestamp
        self.last_activity = datetime.now()

        # Get shared connector instead of creating new ones for each request
        connector = await self.get_connector()

        # Create optimized timeout settings - more granular control
        timeout_obj = aiohttp.ClientTimeout(
            total=timeout,
            connect=min(10, timeout / 2),  # Connection timeout
            sock_connect=min(10, timeout / 2),  # Socket connection timeout
            sock_read=min(timeout - 5, 25),  # Socket read timeout
        )

        # Retry mechanism for transient errors
        max_retries = 3 if "login" not in url and "authenticateUser" not in url else 1
        retry_delay = 1.0  # Base delay in seconds

        last_exception = None

        for attempt in range(max_retries):
            try:
                # Use ClientSession with shared connector for connection pooling
                async with aiohttp.ClientSession(
                        cookies=self.cookies,
                        timeout=timeout_obj,
                        connector=connector,
                        auto_decompress=True,  # Handle compression
                        trust_env=True,  # Use environment proxy settings
                        raise_for_status=False  # Don't raise exceptions for HTTP errors
                ) as session:
                    # Track connections for debugging
                    SauronWebSession._session_pool_stats["total_connections"] += 1
                    SauronWebSession._session_pool_stats["active_connections"] += 1

                    try:
                        async with session.request(
                                method=method,
                                url=url,
                                data=data,
                                headers=headers,
                                json=json,
                                allow_redirects=allow_redirects,
                                ssl=False,  # Skip SSL verification for performance
                        ) as response:
                            # Save all cookies
                            if response.cookies:
                                for key, cookie in response.cookies.items():
                                    self.cookies[key] = cookie.value

                            # Get response content
                            content = await response.text()

                            # Check for specific error patterns that indicate need for re-authentication
                            if response.status == 403 or response.status == 401 or "авторизаци" in content.lower():
                                self.is_authenticated = False

                            # Return status and content
                            return response.status, content
                    finally:
                        # Update connection tracking
                        SauronWebSession._session_pool_stats["active_connections"] = max(
                            0, SauronWebSession._session_pool_stats["active_connections"] - 1
                        )

            except asyncio.TimeoutError as e:
                last_exception = e
                logging.warning(
                    f"Session {self.session_id} timeout on attempt {attempt + 1}/{max_retries} for URL: {url}")
                if attempt < max_retries - 1:
                    await asyncio.sleep(retry_delay * (attempt + 1))  # Incremental backoff

            except aiohttp.ClientError as e:
                last_exception = e
                logging.warning(
                    f"Session {self.session_id} client error on attempt {attempt + 1}/{max_retries}: {str(e)}")
                if attempt < max_retries - 1:
                    await asyncio.sleep(retry_delay * (attempt + 1))

            except Exception as e:
                last_exception = e
                logging.error(
                    f"Session {self.session_id} unexpected error on attempt {attempt + 1}/{max_retries}: {str(e)}")
                self.error_count += 1
                if attempt < max_retries - 1:
                    await asyncio.sleep(retry_delay * (attempt + 1))

        # If we exhaust all retries, raise the last exception
        logging.error(f"Session {self.session_id} request failed after {max_retries} retries for URL: {url}")
        self.error_count += 1
        raise last_exception or RuntimeError(f"Request failed after {max_retries} attempts")

    async def authenticate(self):
        """Enhanced authentication with rate limiting, backoff, and improved session reuse"""
        # Apply rate limiting for authentication to avoid hammering the server
        # Especially important when using the same credentials across multiple sessions
        async with self._auth_lock:
            # Get current time for comparisons
            current_time = time.time()

            # Check if we need to respect rate limiting for this credential
            if self.credential_key in self._last_auth_time:
                time_since_last_auth = current_time - self._last_auth_time[self.credential_key]
                if time_since_last_auth < self._auth_rate_limit:
                    # Apply rate limiting with jitter to prevent thundering herd
                    wait_time = self._auth_rate_limit - time_since_last_auth
                    wait_time += random.uniform(0, 0.5)  # Add jitter
                    logging.info(f"Session {self.session_id}: Rate limiting auth, waiting {wait_time:.2f}s")
                    await asyncio.sleep(wait_time)

            # Update last authentication time for this credential
            self._last_auth_time[self.credential_key] = time.time()

        self.is_busy = True
        try:
            # Increment authentication attempts counter
            self.auth_attempts += 1
            logging.info(f"Session {self.session_id}: Authentication attempt {self.auth_attempts}")

            # Check if session is still valid before attempting re-authentication
            if self.is_authenticated and self.cookies:
                if self.session_valid_until and datetime.now() < self.session_valid_until:
                    # Session is still within validity period, test it with a lightweight request
                    try:
                        status, content = await self._make_request("GET", "/dashboard", timeout=15)
                        if status == 200 and "Авторизация" not in content:
                            logging.info(f"Session {self.session_id}: Reusing existing authenticated session")
                            self.consecutive_auth_failures = 0  # Reset failure counter on success
                            return True
                    except Exception as e:
                        logging.warning(f"Session {self.session_id}: Session validity check failed: {str(e)}")
                        # Continue with full authentication

            # Apply exponential backoff for repeated authentication failures
            if self.consecutive_auth_failures > 0:
                backoff_time = min(
                    self._auth_backoff_max,
                    self._auth_backoff_base ** self.consecutive_auth_failures
                )
                # Add jitter to prevent all sessions retrying simultaneously
                backoff_time *= random.uniform(0.8, 1.2)
                logging.warning(
                    f"Session {self.session_id}: Applying auth backoff of {backoff_time:.2f}s "
                    f"after {self.consecutive_auth_failures} failures"
                )
                await asyncio.sleep(backoff_time)

            # Load login page to get fresh cookies and CSRF tokens if needed
            try:
                status, content = await self._make_request("GET", "/login", timeout=10)
                if status != 200:
                    self.consecutive_auth_failures += 1
                    logging.error(f"Session {self.session_id}: Failed to load login page, status {status}")
                    return False

                # Parse page for any CSRF or other required form fields
                soup = BeautifulSoup(content, 'html.parser')
                csrf_token = None

                # Look for common CSRF token fields
                for field_name in ['_csrf_token', 'csrf_token', '_token', 'csrftoken']:
                    token_field = soup.select_one(f'input[name="{field_name}"]')
                    if token_field and 'value' in token_field.attrs:
                        csrf_token = token_field['value']
                        break

            except Exception as e:
                self.consecutive_auth_failures += 1
                logging.error(f"Session {self.session_id}: Error loading login page: {str(e)}")
                return False

            # Prepare authentication data with CSRF token if found
            auth_data = {
                "login": self.login,
                "password": self.password
            }

            if csrf_token:
                auth_data['_csrf_token'] = csrf_token

            # Additional headers for auth request
            auth_headers = {
                "Content-Type": "application/x-www-form-urlencoded",
                "Origin": self.BASE_URL,
                "Referer": f"{self.BASE_URL}/login"
            }

            # Send authentication request
            try:
                status, content = await self._make_request(
                    "POST",
                    "/api/v1/user/authenticateUser",
                    data=auth_data,
                    headers=auth_headers,
                    timeout=15
                )
            except Exception as e:
                self.consecutive_auth_failures += 1
                logging.error(f"Session {self.session_id}: Error during authentication request: {str(e)}")
                return False

            # Check result
            if status != 200:
                self.consecutive_auth_failures += 1
                logging.error(f"Session {self.session_id}: Authentication failed with status {status}")
                return False

            try:
                # Parse JSON response
                response_data = json.loads(content)

                if response_data.get('ok') == True:
                    # Check for 2FA
                    if response_data.get('error_code') == 1004:
                        self.consecutive_auth_failures += 1
                        logging.error(f"Session {self.session_id}: 2FA required, not supported!")
                        return False

                    # Check for redirection
                    redirection = None
                    if response_data.get('result') and response_data['result'].get('redirection'):
                        redirection = response_data['result']['redirection']

                    if redirection:
                        # Follow redirection
                        status, content = await self._make_request("GET", redirection, timeout=15)
                        if status != 200:
                            self.consecutive_auth_failures += 1
                            logging.error(f"Session {self.session_id}: Failed to follow redirection, status {status}")
                            return False

                    # Verify authentication by loading dashboard
                    status, dashboard = await self._make_request("GET", "/dashboard", timeout=15)
                    if status != 200 or "Авторизация" in dashboard:
                        self.consecutive_auth_failures += 1
                        logging.error(f"Session {self.session_id}: Failed to load dashboard after authentication")
                        return False

                    # Authentication successful - update session state
                    self.is_authenticated = True
                    self.error_count = 0
                    self.consecutive_auth_failures = 0  # Reset failure counter
                    self.last_auth_success = datetime.now()

                    # Set session validity period - estimate 6 hours from successful login
                    # (adjust based on actual server session timeout)
                    self.session_valid_until = datetime.now() + timedelta(hours=6)

                    logging.info(
                        f"Session {self.session_id}: Successfully authenticated, valid until {self.session_valid_until}")
                    return True
                else:
                    # Authentication error
                    self.consecutive_auth_failures += 1
                    error_message = response_data.get('description', 'Unknown error')
                    logging.error(f"Session {self.session_id}: Authentication error: {error_message}")
                    return False
            except Exception as e:
                self.consecutive_auth_failures += 1
                logging.error(f"Session {self.session_id}: Error parsing authentication response: {str(e)}")
                return False
        finally:
            self.is_busy = False

    async def search(self, query):
        """Perform a search with the specified query using optimized handling"""
        if not self.is_authenticated:
            logging.warning(f"Session {self.session_id}: Attempting search without authentication")
            auth_success = await self.authenticate()
            if not auth_success:
                return False, "Authentication failed before search"

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
                            logging.error(f"Session {self.session_id}: Failed to load results page, status {status}")
                            return False, "Failed to load results page"

                        # Increment successful searches counter
                        self.successful_searches += 1

                        # Verify results page content
                        if "Подробный отчет" in result_page:
                            logging.info("Found search results page with 'Подробный отчет' section")
                        else:
                            logging.warning("Search results page doesn't contain 'Подробный отчет' section")

                        return True, result_page
                    else:
                        logging.error(f"Session {self.session_id}: No redirection in search response")
                        return False, "No redirection in search response"
                else:
                    # Search error
                    error_message = response_data.get('description', 'Unknown error')
                    logging.error(f"Session {self.session_id}: Search error: {error_message}")

                    # Check if error is related to authentication
                    if any(term in error_message.lower() for term in ["авториз", "session", "login", "токен"]):
                        self.is_authenticated = False
                        # Force re-authentication on next search
                        self.session_valid_until = None

                    return False, error_message
            except Exception as e:
                logging.error(f"Session {self.session_id}: Error parsing search response: {str(e)}")
                return False, f"Error processing search response: {str(e)}"
        finally:
            self.is_busy = False

    async def parse_results(self, html_content):
        """Parse HTML results page to extract structured data with improved handling"""
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
            import traceback
            logging.error(traceback.format_exc())
            return []

    def get_stats(self):
        """Return enhanced session statistics"""
        return {
            "session_id": self.session_id,
            "login": self.login,
            "is_authenticated": self.is_authenticated,
            "search_count": self.search_count,
            "successful_searches": self.successful_searches,
            "error_count": self.error_count,
            "auth_attempts": self.auth_attempts,
            "auth_failures": self.consecutive_auth_failures,
            "last_activity": self.last_activity.isoformat() if self.last_activity else None,
            "session_valid_until": self.session_valid_until.isoformat() if self.session_valid_until else None,
            "is_busy": self.is_busy
        }

    @classmethod
    async def cleanup(cls):
        """Clean up connection pool - call this when shutting down the application"""
        if cls._conn_pool is not None:
            await cls._conn_pool.close()
            cls._conn_pool = None
            logging.info("Closed shared connection pool")