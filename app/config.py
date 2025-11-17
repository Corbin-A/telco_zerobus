import os
from dataclasses import dataclass

from dotenv import load_dotenv


load_dotenv()


def _get_bool(name: str, default: bool = False) -> bool:
    raw_value = os.getenv(name)
    if raw_value is None:
        return default
    return raw_value.strip().lower() in {"1", "true", "yes", "on"}


DEFAULT_ZEROBUS_PATH = "/api/2.0/lakeflow/connect/zerobus/events:ingest"


@dataclass
class Settings:
    databricks_host: str
    databricks_pat: str
    zerobus_endpoint_path: str = ""
    zerobus_destination_id: str = ""
    use_databricks_sdk: bool = True
    default_topic: str = "optus_zerobus_demo"
    target_table: str = "main.default.zerobus_demo"
    dry_run: bool = False
    default_interval_seconds: float = 1.0
    request_timeout_seconds: float = 10.0

    @property
    def resolved_endpoint_path(self) -> str:
        path = self.zerobus_endpoint_path.strip()
        if path:
            return path if path.startswith("/") else f"/{path}"

        if self.zerobus_destination_id:
            return (
                f"/api/2.0/lakeflow/connect/destinations/{self.zerobus_destination_id}"
                "/events:ingest"
            )

        return DEFAULT_ZEROBUS_PATH

    @property
    def endpoint_url(self) -> str:
        host = (self.databricks_host or "").rstrip("/")
        path = self.resolved_endpoint_path
        return f"{host}{path}" if host else path


def get_settings() -> Settings:
    return Settings(
        databricks_host=os.getenv("DATABRICKS_HOST", ""),
        databricks_pat=os.getenv("DATABRICKS_PAT", ""),
        zerobus_endpoint_path=os.getenv("ZEROBUS_ENDPOINT_PATH", ""),
        zerobus_destination_id=os.getenv("ZEROBUS_DESTINATION_ID", ""),
        default_topic=os.getenv("ZEROBUS_TOPIC", "optus_zerobus_demo"),
        target_table=os.getenv("ZEROBUS_TARGET_TABLE", "main.default.zerobus_demo"),
        dry_run=_get_bool("ZEROBUS_DRY_RUN", False),
        use_databricks_sdk=_get_bool("ZEROBUS_USE_SDK", True),
        default_interval_seconds=float(os.getenv("DEFAULT_INTERVAL_SECONDS", 1.0)),
        request_timeout_seconds=float(os.getenv("REQUEST_TIMEOUT_SECONDS", 10.0)),
    )

