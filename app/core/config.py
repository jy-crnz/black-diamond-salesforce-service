from typing import Literal, Optional
import os
from pydantic import Field, SecretStr, field_validator, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """
    Hardened configuration schema for Black Diamond Salesforce Service.
    Enforces strict typing and presence bounds at ignition phase to guarantee
    fail-fast properties during standalone Docker runs or Nomad task scheduling.
    """

    # ==========================================================================
    # 🌍 Core Web Application Configuration
    # ==========================================================================
    ENVIRONMENT: Literal["dev", "stage", "prod"] = "dev"
    PORT: int = Field(default=5710, ge=1024, le=65535)
    HOST: str = "0.0.0.0"
    FLASK_ENV: str = "development"
    FLASK_DEBUG: bool = False  # Maintained for parsing parity with legacy HCL models
    SECRET_KEY: SecretStr = Field(
        default=SecretStr("local_development_fallback_secret_key_32_chars_minimum")
    )
    LOG_LEVEL: Literal["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"] = "DEBUG"
    LOG_FORMAT: Literal["text", "json"] = "text"
    LOKI_ENABLED: bool = False
    ALLOWED_ORIGINS: str = "*"
    BD_CORE_URL: str = "http://localhost:5700"

    # ==========================================================================
    # ☁️ Salesforce Core Configuration
    # ==========================================================================
    SF_CONSUMER_KEY: str = Field(
        ..., description="Salesforce Connected App Consumer Key"
    )
    SF_USERNAME: str = Field(
        ..., description="Salesforce Integration System Account Username"
    )
    SF_LOGIN_URL: str = "https://test.salesforce.com"
    SF_API_VERSION: str = "v59.0"
    SF_PRIVATE_KEY_PATH: str = "certs/server.key"
    SF_PRIVATE_KEY_PEM: Optional[SecretStr] = Field(
        default=None, description="RSA Private Key string extracted from system env"
    )

    # Operational Sync Knobs
    SF_BULK_PAGE_SIZE: int = Field(default=50000, ge=1, le=250000)
    SF_MAX_JOB_TIMEOUT_HOURS: int = Field(default=2, ge=1)
    MAX_CONCURRENT_SCANS: int = Field(default=2, ge=1)
    SCAN_TIMEOUT_HOURS: int = Field(default=2, ge=1)
    CLEANUP_DAYS: int = Field(default=7, ge=1)

    # ==========================================================================
    # 🗄️ Relational Database Settings (PostgreSQL/Supabase metadata store)
    # ==========================================================================
    DB_HOST: str = "localhost"
    DB_PORT: int = 5432
    DB_NAME: str = "salesforce_dev"
    DB_USER: str = "dev_user"
    DB_PASSWORD: SecretStr = Field(..., description="Database administrative password")
    DB_SCHEMA: str = "public"
    DATABASE_URL: str = Field(
        ..., description="Asynchronous SQL Alchemy wrapper connection target string"
    )

    # ==========================================================================
    # 📦 MinIO Object Storage Setup
    # ==========================================================================
    MINIO_ENABLED: bool = False
    MINIO_ENDPOINT: str = "localhost:9000"
    MINIO_ACCESS_KEY: Optional[str] = "minioadmin"
    MINIO_SECRET_KEY: Optional[SecretStr] = SecretStr("minioadmin")
    MINIO_SECURE: bool = False
    MINIO_BUCKET: str = "salesforce-dev"

    # ==========================================================================
    # 📊 ClickHouse Analytics Store (Target analytical storage sink)
    # ==========================================================================
    CLICKHOUSE_ENABLED: bool = False
    CLICKHOUSE_HOST: str = "localhost"
    CLICKHOUSE_PORT: int = 9000
    CLICKHOUSE_USER: str = "default"
    CLICKHOUSE_PASSWORD: Optional[SecretStr] = SecretStr("")
    CLICKHOUSE_DATABASE: str = "salesforce"

    # ==========================================================================
    # 🔐 Internal Security Gateway
    # ==========================================================================
    HMAC_ENABLED: bool = True
    HMAC_SIGNATURE_MAX_AGE: int = 300
    HMAC_SECRET_KEY_CORE: SecretStr = Field(
        ..., description="Shared secret for core platform calls"
    )
    HMAC_SECRET_KEY_ENGINEER: SecretStr = Field(
        ..., description="Shared secret for debug overrides"
    )

    # ==========================================================================
    # 🧼 Crypto-Key Sanitization Middleware
    # ==========================================================================
    @field_validator("SF_PRIVATE_KEY_PEM", mode="before")
    @classmethod
    def sanitize_pem_key(cls, v: any) -> any:
        """
        Intercepts raw environment strings coming from Docker, peeling away
        literal string artifacts and restoring true carriage structural breaks.
        """
        if isinstance(v, str):
            normalized = v.strip('"').strip("'").replace("\\n", "\n").replace("\r", "")
            core_body = (
                normalized.replace("-----BEGIN RSA PRIVATE KEY-----", "")
                .replace("-----END RSA PRIVATE KEY-----", "")
                .replace("\n", "")
                .replace(" ", "")
            )
            clean_base64 = "".join(
                c
                for c in core_body
                if c
                in "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/="
            )
            v = f"-----BEGIN RSA PRIVATE KEY-----\n{clean_base64}\n-----END RSA PRIVATE KEY-----\n"
        return v

    # ==========================================================================
    # 📂 File-System Validation Layer (Fallback Mechanism)
    # ==========================================================================
    @model_validator(mode="after")
    def load_and_verify_private_key(self) -> "Settings":
        pem_val = (
            self.SF_PRIVATE_KEY_PEM.get_secret_value()
            if self.SF_PRIVATE_KEY_PEM
            else ""
        )

        if not pem_val or "YOUR_KEY_BODY_HERE" in pem_val:
            if os.path.exists(self.SF_PRIVATE_KEY_PATH):
                with open(self.SF_PRIVATE_KEY_PATH, "r", encoding="utf-8") as f:
                    sanitized_text = self.sanitize_pem_key(f.read())
                    self.SF_PRIVATE_KEY_PEM = SecretStr(sanitized_text)
                    pem_val = self.SF_PRIVATE_KEY_PEM.get_secret_value()

        if not pem_val or pem_val.strip() == "":
            raise ValueError(
                "Critical Configuration Failure: SF_PRIVATE_KEY_PEM string is unassigned "
                f"and no structural fallback certificate file found at: '{self.SF_PRIVATE_KEY_PATH}'"
            )

        return self

    # Model Configuration
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=True,
        extra="ignore",  # Seamlessly handle downstream keys injected by Nomad templates
    )


# Instantiation triggers schema boundary checking immediately
settings = Settings()
