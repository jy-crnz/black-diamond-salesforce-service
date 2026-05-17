import hmac
import hashlib
import time
import os
from dotenv import load_dotenv

# Load the local .env file directly, bypassing Pydantic's app-wide model validation
load_dotenv()


def generate_test_credentials():
    # 1. Get the current Unix timestamp
    timestamp = str(int(time.time()))
    key_type = "core"

    # 2. Extract the raw HMAC secret directly from the environment configuration
    raw_secret = os.getenv("HMAC_SECRET_KEY_CORE", "local_core_secret")
    secret_key = raw_secret.encode("utf-8")

    # 3. Construct the exact message string expected by the security layer
    message = f"{timestamp}:{key_type}".encode("utf-8")

    # 4. Compute the SHA256 HMAC signature
    signature = hmac.new(secret_key, message, hashlib.sha256).hexdigest()

    print("\n" + "=" * 50)
    print("🔑 STANDALONE HMAC HEADERS (Valid for 5 Minutes)")
    print("=" * 50)
    print(f"x-timestamp : {timestamp}")
    print(f"x-signature : {signature}")
    print(f"x-key-type  : {key_type}")
    print("=" * 50 + "\n")


if __name__ == "__main__":
    generate_test_credentials()
