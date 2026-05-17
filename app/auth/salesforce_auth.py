import time
import jwt
import httpx
import logging
from app.core.config import settings

logger = logging.getLogger(__name__)


class SalesforceTokenManager:
    """
    Manages OAuth 2.0 JWT Bearer tokens with automatic background refresh.
    Ensures the service stays authenticated without human intervention.
    """

    def __init__(self):
        self._access_token = None
        self._instance_url = None
        self._expires_at = 0

    async def get_token(self) -> tuple[str, str]:
        """Returns (access_token, instance_url), refreshing if within 5 minutes of expiry."""
        # Guard Clause: Check if token is expired or expiring in the next 300 seconds (5 mins)
        if time.time() > (self._expires_at - 300):
            await self._refresh_token()

        return self._access_token, self._instance_url

    async def _refresh_token(self):
        """Generates a new JWT and exchanges it for a fresh access token."""
        now = int(time.time())

        # 1. Build the JWT Claim
        claim = {
            "iss": settings.SF_CONSUMER_KEY,
            "sub": settings.SF_USERNAME,
            "aud": settings.SF_LOGIN_URL,
            "exp": now + 180,  # JWT assertion expires in 3 mins
        }

        # 2. Sign it with our Vault-secured RSA Private Key
        private_key = settings.SF_PRIVATE_KEY_PEM.get_secret_value()
        try:
            signed_jwt = jwt.encode(claim, private_key, algorithm="RS256")
        except Exception as e:
            logger.error(
                "Failed to sign JWT. Ensure SF_PRIVATE_KEY_PEM is a valid RSA private key."
            )
            raise e

        # 3. Exchange the JWT for an Access Token asynchronously
        async with httpx.AsyncClient() as client:
            resp = await client.post(
                f"{settings.SF_LOGIN_URL}/services/oauth2/token",
                data={
                    "grant_type": "urn:ietf:params:oauth:grant-type:jwt-bearer",
                    "assertion": signed_jwt,
                },
                timeout=30.0,
            )

            # Fail fast if authentication is rejected
            resp.raise_for_status()
            data = resp.json()

            self._access_token = data["access_token"]
            self._instance_url = data["instance_url"]
            self._expires_at = now + 7200  # Salesforce tokens typically last 2 hours


# Create a singleton instance to be used across the app
token_manager = SalesforceTokenManager()
