import asyncio
import json
import logging
from importlib import import_module
from typing import Any, Dict, List, Optional, Type

import httpx

try:  # pragma: no cover - optional dependency
    from databricks.sdk import WorkspaceClient  # type: ignore
except Exception:  # pragma: no cover - optional dependency
    WorkspaceClient = None  # type: ignore[misc,assignment]

try:  # pragma: no cover - optional dependency
    from zerobus.sdk.sync import ZerobusSdk  # type: ignore
    from zerobus.sdk.shared import (  # type: ignore
        NonRetriableException,
        TableProperties,
        ZerobusException,
    )
except Exception:  # pragma: no cover - optional dependency
    ZerobusSdk = None  # type: ignore[misc,assignment]
    TableProperties = None  # type: ignore[misc,assignment]
    ZerobusException = NonRetriableException = Exception  # type: ignore[assignment]

from .config import Settings


logger = logging.getLogger(__name__)


class ZeroBusClient:
    """Lightweight HTTP client for the Lakeflow Connect Zerobus ingestion API.

    The payload shape is intentionally explicit and easy to tweak: adjust
    :meth:`build_payload` to match any API changes without touching the rest of the
    demo code.
    """

    def __init__(self, settings: Settings):
        self.settings = settings
        self._client = httpx.AsyncClient(timeout=settings.request_timeout_seconds)
        self._sdk_client: WorkspaceClient | None = None
        self._zerobus_sdk: ZerobusSdk | None = None  # type: ignore[assignment]
        self._zerobus_stream = None
        self._proto_message_cls: Type[Any] | None = None
        self._init_sdk_client()
        self._init_ingest_sdk()

    async def aclose(self) -> None:
        if self._zerobus_stream is not None:
            await asyncio.to_thread(self._zerobus_stream.close)
            self._zerobus_stream = None
        await self._client.aclose()

    def build_payload(self, events: List[Dict[str, Any]], topic: Optional[str]) -> Dict[str, Any]:
        """Construct the request body for Zerobus.

        The default schema mirrors what the public docs describe today: a topic/stream
        plus a list of event payloads. If the Zerobus API evolves, edit this method
        (and only this method) to match the expected contract.
        """

        return {
            "topic": topic or self.settings.default_topic,
            "target_table": self.settings.target_table,
            "events": events,
        }

    def _headers(self) -> Dict[str, str]:
        return {
            "Authorization": f"Bearer {self.settings.databricks_pat}",
            "Content-Type": "application/json",
        }

    async def send_events(
        self, events: List[Dict[str, Any]], topic: Optional[str] = None
    ) -> Dict[str, Any]:
        if self.settings.dry_run:
            logger.info("[dry-run] would send %s events to %s", len(events), topic)
            logger.debug("[dry-run] payload=%s", json.dumps(self.build_payload(events, topic)))
            return {"dry_run": True, "sent": len(events), "topic": topic or self.settings.default_topic}

        if not self.settings.databricks_host:
            raise RuntimeError(
                "DATABRICKS_HOST must be set (or enable ZEROBUS_DRY_RUN) to describe the workspace"
            )

        payload = self.build_payload(events, topic)

        if self._zerobus_stream is not None:
            return await self._send_via_ingest_sdk(events)

        if self.settings.allow_http_fallback:
            if not self.settings.databricks_pat:
                raise RuntimeError(
                    "DATABRICKS_PAT must be set to use the Zerobus HTTP fallback path"
                )
            endpoint = self.settings.endpoint_url
            if not endpoint:
                raise RuntimeError(
                    "HTTP fallback enabled but no Zerobus endpoint configured. Set "
                    "ZEROBUS_ENDPOINT_PATH or ZEROBUS_DESTINATION_ID."
                )
            if self._sdk_client is not None and payload:
                return await self._send_via_sdk(payload)
            return await self._send_via_http(payload)

        raise RuntimeError(
            "Zerobus ingest SDK is not configured. Please follow the Databricks "
            "documentation to provide the Zerobus endpoint, client id/secret, and "
            "generated protobuf module."
        )

    def _init_sdk_client(self) -> None:
        if not self.settings.allow_http_fallback:
            return

        if WorkspaceClient is None:
            logger.warning(
                "databricks-sdk not installed; falling back to direct HTTP requests"
            )
            return

        if not self.settings.databricks_host or not self.settings.databricks_pat:
            logger.warning(
                "Missing Databricks host or token; falling back to direct HTTP requests"
            )
            return

        try:
            self._sdk_client = WorkspaceClient(
                host=self.settings.databricks_host,
                token=self.settings.databricks_pat,
            )
            logger.info("Using databricks-sdk WorkspaceClient for Zerobus ingestion")
        except Exception as exc:  # pragma: no cover - defensive logging
            logger.warning("Unable to initialize databricks-sdk client: %s", exc)
            self._sdk_client = None

    def _init_ingest_sdk(self) -> None:
        if not self.settings.use_ingest_sdk:
            return

        if ZerobusSdk is None or TableProperties is None:
            logger.warning(
                "databricks-zerobus-ingest-sdk not installed; set ZEROBUS_USE_INGEST_SDK=false "
                "or install the package to enable direct ingestion."
            )
            return

        required = {
            "ZEROBUS_SERVER_ENDPOINT": self.settings.zerobus_server_endpoint,
            "DATABRICKS_HOST": self.settings.databricks_host,
            "ZEROBUS_CLIENT_ID": self.settings.zerobus_client_id,
            "ZEROBUS_CLIENT_SECRET": self.settings.zerobus_client_secret,
            "ZEROBUS_PROTO_MODULE": self.settings.zerobus_proto_module,
            "ZEROBUS_PROTO_MESSAGE": self.settings.zerobus_proto_message,
        }
        missing = [name for name, value in required.items() if not value]
        if missing:
            logger.warning(
                "Cannot initialize Zerobus ingest SDK stream; missing %s.",
                ", ".join(missing),
            )
            return

        try:
            proto_cls = self._load_proto_message(
                self.settings.zerobus_proto_module,
                self.settings.zerobus_proto_message,
            )
        except Exception as exc:
            logger.error(
                "Unable to load protobuf %s.%s: %s",
                self.settings.zerobus_proto_module,
                self.settings.zerobus_proto_message,
                exc,
            )
            logger.error(
                "Ensure the compiled *_pb2.py file is on PYTHONPATH and the class name is correct."
            )
            return

        try:
            sdk = ZerobusSdk(
                self.settings.zerobus_server_endpoint,
                self.settings.databricks_host,
            )
        except Exception as exc:  # pragma: no cover - network heavy
            logger.error("Failed to initialize ZerobusSdk: %s", exc)
            return

        try:
            table_properties = TableProperties(
                self.settings.target_table,
                proto_cls.DESCRIPTOR,
            )
        except Exception as exc:
            logger.error("Failed to build TableProperties for %s: %s", self.settings.target_table, exc)
            return

        try:
            stream = sdk.create_stream(
                self.settings.zerobus_client_id,
                self.settings.zerobus_client_secret,
                table_properties,
            )
        except Exception as exc:  # pragma: no cover - network heavy
            logger.error("Unable to create Zerobus ingest stream: %s", exc)
            return

        self._zerobus_sdk = sdk
        self._zerobus_stream = stream
        self._proto_message_cls = proto_cls
        logger.info(
            "Using Zerobus ingest SDK stream for %s via %s",
            self.settings.target_table,
            self.settings.zerobus_server_endpoint,
        )

    @staticmethod
    def _load_proto_message(module_name: str, message_name: str) -> Type[Any]:
        module = import_module(module_name)
        try:
            return getattr(module, message_name)
        except AttributeError as exc:  # pragma: no cover - configuration issue
            raise RuntimeError(
                f"Module '{module_name}' does not expose message '{message_name}'."
            ) from exc

    @staticmethod
    def _normalize_path(path: str) -> str:
        if not path.startswith("/"):
            return f"/{path}"
        return path

    async def _send_via_http(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        url = self.settings.endpoint_url
        response = await self._client.post(url, headers=self._headers(), json=payload)
        response.raise_for_status()
        try:
            return response.json()
        except ValueError:
            return {"status": response.status_code, "text": response.text}

    async def _send_via_ingest_sdk(self, events: List[Dict[str, Any]]) -> Dict[str, Any]:
        assert self._zerobus_stream is not None and self._proto_message_cls is not None

        def _ingest() -> Dict[str, Any]:
            sent = 0
            for event in events:
                payload = event.get("payload")
                if payload is None:
                    raise NonRetriableException("Event missing payload for Zerobus ingestion")
                record = self._proto_message_cls(**payload)
                ack = self._zerobus_stream.ingest_record(record)
                ack.wait_for_ack()
                sent += 1
            return {
                "sent": sent,
                "transport": "zerobus_ingest_sdk",
                "table": self.settings.target_table,
            }

        return await asyncio.to_thread(_ingest)

    async def _send_via_sdk(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        assert self._sdk_client is not None  # nosec B101 - guarded by caller
        path = self._normalize_path(self.settings.resolved_endpoint_path)

        def _post() -> Dict[str, Any]:
            return self._sdk_client.api_client.do("POST", path, body=payload)

        return await asyncio.to_thread(_post)

