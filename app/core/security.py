import hmac
import hashlib
import time
from fastapi import HTTPException, Header
from app.core.config import settings


def verify_hmac(
    x_timestamp: int = Header(..., description="Unix timestamp of the request"),
    x_signature: str = Header(..., description="HMAC SHA256 Signature"),
    x_key_type: str = Header("core", description="Must be 'core' or 'engineer'"),
) -> str:
    """
    FastAPI Dependency to verify HMAC signatures for internal service requests.
    Enforces strict signature validation and TTL expiration to prevent replay attacks.
    """
    # Guard Clause 1: Is HMAC globally disabled for local dev testing?
    if not settings.HMAC_ENABLED:
        return "bypassed_for_dev"

    # Guard Clause 2: Check timestamp expiration (Replay Attack Prevention)
    current_time = int(time.time())
    if abs(current_time - x_timestamp) > settings.HMAC_SIGNATURE_MAX_AGE:
        # Standardized error response format
        raise HTTPException(
            status_code=401,
            detail={
                "error": {
                    "code": "TOKEN_EXPIRED",
                    "message": "Request signature is too old or from the future.",
                }
            },
        )

    # Select the correct secret based on the key type
    if x_key_type == "core":
        secret = settings.HMAC_SECRET_KEY_CORE.get_secret_value()
    elif x_key_type == "engineer":
        secret = settings.HMAC_SECRET_KEY_ENGINEER.get_secret_value()
    else:
        raise HTTPException(
            status_code=400,
            detail={
                "error": {
                    "code": "INVALID_KEY_TYPE",
                    "message": "x_key_type must be 'core' or 'engineer'",
                }
            },
        )

    # Reconstruct the expected signature
    # In a full POST request, you would hash the raw body. For GETs/basic verification,
    # we hash the timestamp + key_type as the base message.
    message = f"{x_timestamp}:{x_key_type}".encode("utf-8")

    expected_signature = hmac.new(
        key=secret.encode("utf-8"), msg=message, digestmod=hashlib.sha256
    ).hexdigest()

    # Guard Clause 3: Constant-time comparison to prevent timing attacks
    if not hmac.compare_digest(expected_signature, x_signature):
        raise HTTPException(
            status_code=401,
            detail={
                "error": {"code": "UNAUTHORIZED", "message": "Invalid HMAC signature."}
            },
        )

    return x_key_type
