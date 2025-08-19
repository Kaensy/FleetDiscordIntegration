import asyncio
import aiohttp
import time
import json
import logging
from pathlib import Path
from typing import Optional, Dict, Any

logger = logging.getLogger(__name__)


class BoltOAuthClient:
    """
    Production-ready OAuth2 client for Bolt Fleet API with automatic token refresh.
    Handles 10-minute token expiration with 2-minute refresh buffer.
    """

    def __init__(self,
                 client_id: str,
                 client_secret: str,
                 token_url: str = "https://oidc.bolt.eu/token",
                 scope: str = "fleet-integration:api",
                 token_file: str = "oauth_token.json",
                 refresh_buffer: int = 120):  # 2 minutes buffer
        self.client_id = client_id
        self.client_secret = client_secret
        self.token_url = token_url
        self.scope = scope
        self.token_file = Path(token_file)
        self.refresh_buffer = refresh_buffer
        self.token = None
        self._token_lock = asyncio.Lock()
        self._refresh_task = None

        # Load existing token if available
        self._load_token()

    def _load_token(self):
        """Load token from persistent storage"""
        if self.token_file.exists():
            try:
                with open(self.token_file, 'r') as f:
                    self.token = json.load(f)
                logger.info("Token loaded from storage")
            except Exception as e:
                logger.warning(f"Failed to load token: {e}")

    def _save_token(self):
        """Save token to persistent storage"""
        if self.token:
            try:
                with open(self.token_file, 'w') as f:
                    json.dump(self.token, f, indent=2)
                logger.info("Token saved to storage")
            except Exception as e:
                logger.error(f"Failed to save token: {e}")

    def _is_token_expired(self) -> bool:
        """Check if token is expired or will expire soon"""
        if not self.token or 'expires_at' not in self.token:
            return True
        return time.time() + self.refresh_buffer >= self.token['expires_at']

    async def get_valid_token(self) -> str:
        """Get a valid access token, refreshing if necessary"""
        async with self._token_lock:
            if self._is_token_expired():
                await self._refresh_token()
            return self.token['access_token']

    async def _refresh_token(self):
        """Refresh the OAuth2 token using aiohttp with CURL-like headers"""
        try:
            logger.info("Refreshing OAuth2 token...")

            # Prepare the token request data as form data
            data = aiohttp.FormData()
            data.add_field('client_id', self.client_id)
            data.add_field('client_secret', self.client_secret)
            data.add_field('grant_type', 'client_credentials')
            data.add_field('scope', self.scope)

            # Headers that mimic CURL to bypass Cloudflare
            headers = {
                'User-Agent': 'curl/7.68.0',
                'Accept': '*/*',
                'Content-Type': 'application/x-www-form-urlencoded'
            }

            # Create a connector with SSL verification
            connector = aiohttp.TCPConnector(ssl=True)

            async with aiohttp.ClientSession(connector=connector) as session:
                async with session.post(
                        self.token_url,
                        data=data,
                        headers=headers,
                        allow_redirects=False
                ) as response:
                    response.raise_for_status()
                    self.token = await response.json()

            # Add expires_at timestamp
            if 'expires_in' in self.token:
                self.token['expires_at'] = time.time() + self.token['expires_in']

            self._save_token()

            # Schedule next refresh
            if self._refresh_task:
                self._refresh_task.cancel()

            refresh_in = self.token['expires_in'] - self.refresh_buffer
            self._refresh_task = asyncio.create_task(
                self._schedule_refresh(refresh_in)
            )

            logger.info(f"Token refreshed successfully, next refresh in {refresh_in} seconds")

        except Exception as e:
            logger.error(f"Token refresh failed: {e}")
            raise

    async def _schedule_refresh(self, delay: int):
        """Schedule the next token refresh"""
        try:
            await asyncio.sleep(delay)
            async with self._token_lock:
                await self._refresh_token()
        except asyncio.CancelledError:
            logger.info("Scheduled refresh cancelled")
        except Exception as e:
            logger.error(f"Scheduled refresh failed: {e}")

    async def make_request(self,
                           session: aiohttp.ClientSession,
                           method: str,
                           url: str,
                           **kwargs) -> Dict[Any, Any]:
        """Make an authenticated API request with retry logic"""
        max_retries = 3
        backoff_factor = 2

        for attempt in range(max_retries):
            try:
                token = await self.get_valid_token()
                headers = kwargs.get('headers', {})
                headers['Authorization'] = f'Bearer {token}'
                headers['User-Agent'] = 'curl/7.68.0'  # Add CURL user agent
                headers['Accept'] = 'application/json'
                kwargs['headers'] = headers

                async with session.request(method, url, **kwargs) as response:
                    if response.status == 401:
                        # Token might be invalid, force refresh
                        logger.warning("Received 401, forcing token refresh")
                        async with self._token_lock:
                            await self._refresh_token()
                        continue
                    elif response.status == 429:
                        # Rate limiting
                        retry_after = int(response.headers.get('Retry-After', 60))
                        logger.warning(f"Rate limited, waiting {retry_after} seconds")
                        await asyncio.sleep(retry_after)
                        continue
                    elif response.status >= 500:
                        # Server error, retry with backoff
                        delay = backoff_factor ** attempt
                        logger.warning(f"Server error {response.status}, retrying in {delay}s")
                        await asyncio.sleep(delay)
                        continue

                    response.raise_for_status()
                    return await response.json()

            except aiohttp.ClientError as e:
                if attempt == max_retries - 1:
                    raise
                delay = backoff_factor ** attempt
                logger.warning(f"Network error: {e}, retrying in {delay}s")
                await asyncio.sleep(delay)

        raise Exception(f"Max retries ({max_retries}) exceeded")

    async def close(self):
        """Clean up resources"""
        if self._refresh_task:
            self._refresh_task.cancel()
            try:
                await self._refresh_task
            except asyncio.CancelledError:
                pass