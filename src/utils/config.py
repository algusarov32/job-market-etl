"""
Application configuration loaded from environment variables.

All settings are read once at import time via python-dotenv.
Use Config.<ATTR> anywhere in the codebase — never read os.environ directly.
"""

import os
from dataclasses import dataclass, field
from pathlib import Path

from dotenv import load_dotenv

# Load .env from project root (two levels up from this file)
_ENV_PATH = Path(__file__).resolve().parents[3] / ".env"
load_dotenv(_ENV_PATH)


def _require(key: str) -> str:
    """Return env-var value or raise early with a clear message."""
    value = os.getenv(key)
    if not value:
        raise EnvironmentError(
            f"Required environment variable '{key}' is not set. "
            f"Check your .env file at {_ENV_PATH}"
        )
    return value


def _get(key: str, default: str = "") -> str:
    return os.getenv(key, default)


# ---------------------------------------------------------------------------
# Typed config sections (dataclasses keep things readable & testable)
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class MinIOConfig:
    endpoint_url: str = field(default_factory=lambda: _get("MINIO_ENDPOINT_URL", "http://localhost:9000"))
    root_user: str = field(default_factory=lambda: _get("MINIO_ROOT_USER", "minioadmin"))
    root_password: str = field(default_factory=lambda: _get("MINIO_ROOT_PASSWORD", "minioadmin"))
    bucket_raw: str = field(default_factory=lambda: _get("MINIO_BUCKET_RAW", "raw-data"))
    region: str = field(default_factory=lambda: _get("MINIO_REGION", "us-east-1"))
    api_port: int = field(default_factory=lambda: int(_get("MINIO_API_PORT", "9000")))
    console_port: int = field(default_factory=lambda: int(_get("MINIO_CONSOLE_PORT", "9001")))


@dataclass(frozen=True)
class GreenplumConfig:
    host: str = field(default_factory=lambda: _get("GP_HOST", "localhost"))
    port: int = field(default_factory=lambda: int(_get("GP_PORT", "5432")))
    database: str = field(default_factory=lambda: _get("GP_DATABASE", "jobs_db"))
    user: str = field(default_factory=lambda: _get("GP_USER", "gpadmin"))
    password: str = field(default_factory=lambda: _get("GP_PASSWORD", ""))


@dataclass(frozen=True)
class ClickHouseConfig:
    host: str = field(default_factory=lambda: _get("CLICKHOUSE_HOST", "localhost"))
    http_port: int = field(default_factory=lambda: int(_get("CLICKHOUSE_HTTP_PORT", "8123")))
    native_port: int = field(default_factory=lambda: int(_get("CLICKHOUSE_NATIVE_PORT", "9002")))
    database: str = field(default_factory=lambda: _get("CLICKHOUSE_DATABASE", "analytics"))
    user: str = field(default_factory=lambda: _get("CLICKHOUSE_USER", "default"))
    password: str = field(default_factory=lambda: _get("CLICKHOUSE_PASSWORD", ""))


@dataclass(frozen=True)
class AirflowConfig:
    postgres_user: str = field(default_factory=lambda: _get("AIRFLOW_POSTGRES_USER", "airflow"))
    postgres_password: str = field(default_factory=lambda: _get("AIRFLOW_POSTGRES_PASSWORD", "airflow"))
    postgres_db: str = field(default_factory=lambda: _get("AIRFLOW_POSTGRES_DB", "airflow"))
    fernet_key: str = field(default_factory=lambda: _get("AIRFLOW_FERNET_KEY", ""))
    admin_user: str = field(default_factory=lambda: _get("AIRFLOW_ADMIN_USER", "admin"))
    admin_password: str = field(default_factory=lambda: _get("AIRFLOW_ADMIN_PASSWORD", "admin"))
    webserver_port: int = field(default_factory=lambda: int(_get("AIRFLOW_WEBSERVER_PORT", "8080")))


@dataclass(frozen=True)
class GrafanaConfig:
    admin_user: str = field(default_factory=lambda: _get("GRAFANA_ADMIN_USER", "admin"))
    admin_password: str = field(default_factory=lambda: _get("GRAFANA_ADMIN_PASSWORD", "admin"))
    port: int = field(default_factory=lambda: int(_get("GRAFANA_PORT", "3000")))


@dataclass(frozen=True)
class HHApiConfig:
    base_url: str = field(default_factory=lambda: _get("HH_API_BASE_URL", "https://api.hh.ru"))
    user_agent: str = field(default_factory=lambda: _get("HH_API_USER_AGENT", "JobMarketETL/1.0"))
    rate_limit_delay: float = field(
        default_factory=lambda: float(_get("HH_API_RATE_LIMIT_DELAY", "0.25"))
    )
 
@dataclass(frozen=True)
class TheirStackApiConfig:
    api_key: str = field(default_factory=lambda: _get("THEIRSTACK_API_KEY", ""))
    base_url: str = field(default_factory=lambda: _get("THEIRSTACK_API_BASE_URL", "https://api.theirstack.com/v1"))
    rate_limit_delay: float = field(
        default_factory=lambda: float(_get("THEIRSTACK_API_RATE_LIMIT_DELAY", "1.0"))
    )
    max_per_page: int = field(
        default_factory=lambda: int(_get("THEIRSTACK_API_MAX_PER_PAGE", "500"))
    )

# ---------------------------------------------------------------------------
# Public singletons — import these everywhere
# ---------------------------------------------------------------------------

minio = MinIOConfig()
greenplum = GreenplumConfig()
clickhouse = ClickHouseConfig()
airflow = AirflowConfig()
grafana = GrafanaConfig()
hh_api = HHApiConfig()
theirstack_api = TheirStackApiConfig()
