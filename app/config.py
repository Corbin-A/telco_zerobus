import os
from dataclasses import dataclass

from dotenv import load_dotenv


load_dotenv()


def _get_bool(name: str, default: bool = False) -> bool:
    raw_value = os.getenv(name)
    if raw_value is None:
        return default
    return raw_value.strip().lower() in {"1", "true", "yes", "on"}


@dataclass
class Settings:
    databricks_host: str
    databricks_pat: str
    zerobus_endpoint_path: str = "/api/2.0/lakeflow/connect/zerobus/events:ingest"
    use_databricks_sdk: bool = True
    default_topic: str = "optus_zerobus_demo"
    target_table: str = "main.default.zerobus_demo"
    dry_run: bool = False
    default_interval_seconds: float = 1.0
    request_timeout_seconds: float = 10.0

    @property
    def endpoint_url(self) -> str:
        host = (self.databricks_host or "").rstrip("/")
        path = self.zerobus_endpoint_path
        if not path.startswith("/"):
            path = f"/{path}"
        return f"{host}{path}" if host else path


def get_settings() -> Settings:
    return Settings(
        databricks_host=os.getenv("DATABRICKS_HOST", ""),
        databricks_pat=os.getenv("DATABRICKS_PAT", ""),
        zerobus_endpoint_path=os.getenv(
            "ZEROBUS_ENDPOINT_PATH",
            "/api/2.0/lakeflow/connect/zerobus/events:ingest",
        ),
        default_topic=os.getenv("ZEROBUS_TOPIC", "optus_zerobus_demo"),
        target_table=os.getenv("ZEROBUS_TARGET_TABLE", "main.default.zerobus_demo"),
        dry_run=_get_bool("ZEROBUS_DRY_RUN", False),
        use_databricks_sdk=_get_bool("ZEROBUS_USE_SDK", True),
        default_interval_seconds=float(os.getenv("DEFAULT_INTERVAL_SECONDS", 1.0)),
        request_timeout_seconds=float(os.getenv("REQUEST_TIMEOUT_SECONDS", 10.0)),
    )

