import asyncio
import requests
import time
import json
import logging
from pathlib import Path
from typing import Optional, Dict, Any
import aiohttp
from concurrent.futures import ThreadPoolExecutor

logger = logging.getLogger(__name__)


class BoltOAuthClient:
    """
    OAuth2 client using requests library to bypass Cloudflare.
    """

    def __init__(self,
                 client_id: str,
                 client_secret: str,
                 token_url: str = "https://oidc.bolt.eu/token",
                 scope: str = "fleet-integration:api",
                 token_file: str = "oauth_token.json",
                 refresh_buffer: int = 120):
        self.client_id = client_id
        self.client_secret = client_secret
        self.token_url = token_url
        self.scope = scope
        self.token_file = Path(token_file)
        self.refresh_buffer = refresh_buffer
        self.token = None
        self._token_lock = asyncio.Lock()
        self._refresh_task = None
        self.executor = ThreadPoolExecutor(max_workers=2)

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

    def _refresh_token_sync(self):
        """Synchronous token refresh using requests"""
        data = {
            'client_id': self.client_id,
            'client_secret': self.client_secret,
            'grant_type': 'client_credentials',
            'scope': self.scope
        }

        headers = {
            'User-Agent': 'curl/7.68.0',
            'Content-Type': 'application/x-www-form-urlencoded'
        }

        response = requests.post(self.token_url, data=data, headers=headers)
        response.raise_for_status()
        return response.json()

    async def get_valid_token(self) -> str:
        """Get a valid access token, refreshing if necessary"""
        async with self._token_lock:
            if self._is_token_expired():
                await self._refresh_token()
            return self.token['access_token']

    async def _refresh_token(self):
        """Refresh the OAuth2 token"""
        try:
            logger.info("Refreshing OAuth2 token...")

            # Run the synchronous requests call in a thread pool
            loop = asyncio.get_event_loop()
            self.token = await loop.run_in_executor(
                self.executor,
                self._refresh_token_sync
            )

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
        """Make an authenticated API request"""
        token = await self.get_valid_token()
        headers = kwargs.get('headers', {})
        headers['Authorization'] = f'Bearer {token}'
        headers['User-Agent'] = 'curl/7.68.0'
        kwargs['headers'] = headers

        async with session.request(method, url, **kwargs) as response:
            response.raise_for_status()
            return await response.json()

    async def close(self):
        """Clean up resources"""
        if self._refresh_task:
            self._refresh_task.cancel()
            try:
                await self._refresh_task
            except asyncio.CancelledError:
                pass
        self.executor.shutdown(wait=False)