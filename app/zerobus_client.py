import asyncio
import json
import logging
from typing import Any, Dict, List, Optional

import httpx

try:  # pragma: no cover - optional dependency
    from databricks.sdk import WorkspaceClient  # type: ignore
except Exception:  # pragma: no cover - optional dependency
    WorkspaceClient = None  # type: ignore[misc,assignment]

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
        self._init_sdk_client()

    async def aclose(self) -> None:
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

        if not self.settings.databricks_host or not self.settings.databricks_pat:
            raise RuntimeError(
                "DATABRICKS_HOST and DATABRICKS_PAT must be set (or enable ZEROBUS_DRY_RUN)"
            )

        payload = self.build_payload(events, topic)

        if self._sdk_client is not None:
            return await self._send_via_sdk(payload)

        return await self._send_via_http(payload)

    def _init_sdk_client(self) -> None:
        if not self.settings.use_databricks_sdk:
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

    async def _send_via_sdk(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        assert self._sdk_client is not None  # nosec B101 - guarded by caller
        path = self._normalize_path(self.settings.resolved_endpoint_path)

        def _post() -> Dict[str, Any]:
            return self._sdk_client.api_client.do("POST", path, body=payload)

        return await asyncio.to_thread(_post)

