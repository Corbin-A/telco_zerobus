import os
from dataclasses import dataclass

from dotenv import load_dotenv


load_dotenv()


def _get_bool(name: str, default: bool = False) -> bool:
    raw_value = os.getenv(name)
    if raw_value is None:
        return default
    return raw_value.strip().lower() in {"1", "true", "yes", "on"}


DESTINATION_PATH_TEMPLATE = (
    "/api/2.0/lakeflow/connect/destinations/{destination_id}/events:ingest"
)


@dataclass
class Settings:
    databricks_host: str
    databricks_pat: str
    zerobus_endpoint_path: str = ""
    zerobus_destination_id: str = ""
    zerobus_server_endpoint: str = ""
    zerobus_client_id: str = ""
    zerobus_client_secret: str = ""
    zerobus_proto_module: str = ""
    zerobus_proto_message: str = ""
    use_ingest_sdk: bool = True
    allow_http_fallback: bool = False
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

        destination_id = self.zerobus_destination_id.strip()
        if destination_id:
            return DESTINATION_PATH_TEMPLATE.format(destination_id=destination_id)

        return ""

    @property
    def endpoint_url(self) -> str:
        host = (self.databricks_host or "").rstrip("/")
        path = self.resolved_endpoint_path
        if not host or not path:
            return ""
        return f"{host}{path}"


def get_settings() -> Settings:
    return Settings(
        databricks_host=os.getenv("DATABRICKS_HOST", ""),
        databricks_pat=os.getenv("DATABRICKS_PAT", ""),
        zerobus_endpoint_path=os.getenv("ZEROBUS_ENDPOINT_PATH", ""),
        zerobus_destination_id=os.getenv("ZEROBUS_DESTINATION_ID", ""),
        zerobus_server_endpoint=os.getenv("ZEROBUS_SERVER_ENDPOINT", ""),
        zerobus_client_id=os.getenv("ZEROBUS_CLIENT_ID", ""),
        zerobus_client_secret=os.getenv("ZEROBUS_CLIENT_SECRET", ""),
        zerobus_proto_module=os.getenv("ZEROBUS_PROTO_MODULE", ""),
        zerobus_proto_message=os.getenv("ZEROBUS_PROTO_MESSAGE", ""),
        default_topic=os.getenv("ZEROBUS_TOPIC", "optus_zerobus_demo"),
        target_table=os.getenv("ZEROBUS_TARGET_TABLE", "main.default.zerobus_demo"),
        dry_run=_get_bool("ZEROBUS_DRY_RUN", False),
        use_ingest_sdk=_get_bool(
            "ZEROBUS_USE_INGEST_SDK",
            _get_bool("ZEROBUS_USE_SDK", True),
        ),
        allow_http_fallback=_get_bool("ZEROBUS_ALLOW_HTTP_FALLBACK", False),
        default_interval_seconds=float(os.getenv("DEFAULT_INTERVAL_SECONDS", 1.0)),
        request_timeout_seconds=float(os.getenv("REQUEST_TIMEOUT_SECONDS", 10.0)),
    )

