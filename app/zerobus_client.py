import json
import logging
from typing import Any, Dict, List, Optional

import httpx

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
        url = self.settings.endpoint_url

        response = await self._client.post(url, headers=self._headers(), json=payload)
        response.raise_for_status()
        try:
            return response.json()
        except ValueError:
            return {"status": response.status_code, "text": response.text}

