import asyncio
import logging
import random
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Dict, List

from .config import Settings
from .schemas import ManualAlertRequest, ProducerStatus, StartProducerRequest
from .zerobus_client import ZeroBusClient


logger = logging.getLogger(__name__)


@dataclass
class ProducerState:
    producer_id: str
    topic: str
    interval_seconds: float
    jitter_seconds: float
    payload_template: Dict
    started_at: datetime
    last_sent_at: datetime | None = None
    messages_sent: int = 0
    recent_events: list[dict] = field(default_factory=list)
    task: asyncio.Task | None = None
    stop_event: asyncio.Event = field(default_factory=asyncio.Event)


class ProducerManager:
    def __init__(self, zerobus_client: ZeroBusClient, settings: Settings):
        self.client = zerobus_client
        self.settings = settings
        self._producers: Dict[str, ProducerState] = {}
        self._lock = asyncio.Lock()

    async def start_producer(self, request: StartProducerRequest) -> ProducerStatus:
        async with self._lock:
            if request.producer_id in self._producers:
                raise ValueError(f"Producer '{request.producer_id}' already running")

            state = ProducerState(
                producer_id=request.producer_id,
                topic=request.topic or self.settings.default_topic,
                interval_seconds=request.interval_seconds,
                jitter_seconds=request.jitter_seconds,
                payload_template=request.payload_template,
                started_at=datetime.now(timezone.utc),
            )

            state.task = asyncio.create_task(self._run_producer(state))
            self._producers[state.producer_id] = state

        logger.info(
            "Started producer %s (interval=%ss, jitter=%ss, topic=%s)",
            state.producer_id,
            state.interval_seconds,
            state.jitter_seconds,
            state.topic,
        )
        return self._to_status(state)

    async def _run_producer(self, state: ProducerState) -> None:
        while not state.stop_event.is_set():
            await self._send_event(state)
            sleep_for = state.interval_seconds + random.random() * state.jitter_seconds
            try:
                await asyncio.wait_for(state.stop_event.wait(), timeout=sleep_for)
            except asyncio.TimeoutError:
                continue

    async def _send_event(self, state: ProducerState) -> None:
        now = datetime.now(timezone.utc)
        event = {
            "event_id": str(uuid.uuid4()),
            "producer_id": state.producer_id,
            "event_type": "demo_tick",
            "event_time": now.isoformat(),
            "payload": {
                "sequence": state.messages_sent + 1,
                "producer_id": state.producer_id,
                "observed_at": now.isoformat(),
                **(state.payload_template or {}),
            },
        }

        try:
            await self.client.send_events([event], topic=state.topic)
            state.messages_sent += 1
            state.last_sent_at = now
            state.recent_events.append(event)
            if len(state.recent_events) > 10:
                state.recent_events.pop(0)
        except Exception as exc:  # pragma: no cover - defensive logging only
            logger.exception("Failed to send event from %s: %s", state.producer_id, exc)

    async def stop_producer(self, producer_id: str) -> bool:
        async with self._lock:
            state = self._producers.pop(producer_id, None)

        if not state:
            return False

        state.stop_event.set()
        if state.task:
            await state.task

        logger.info("Stopped producer %s", producer_id)
        return True

    async def stop_all(self) -> None:
        async with self._lock:
            producer_ids = list(self._producers.keys())

        await asyncio.gather(*(self.stop_producer(pid) for pid in producer_ids))

    async def send_manual_alert(self, request: ManualAlertRequest) -> Dict:
        now = datetime.now(timezone.utc)
        event = {
            "event_id": str(uuid.uuid4()),
            "producer_id": request.producer_id or "manual_alert",
            "event_type": "alert",
            "event_time": now.isoformat(),
            "payload": {
                "producer_id": request.producer_id or "manual_alert",
                "sequence": -1,
                "observed_at": now.isoformat(),
                "severity": request.severity,
                "details": request.details,
            },
        }
        return await self.client.send_events([event], topic=request.topic)

    def list_status(self) -> List[ProducerStatus]:
        return [self._to_status(state) for state in self._producers.values()]

    def _to_status(self, state: ProducerState) -> ProducerStatus:
        return ProducerStatus(
            producer_id=state.producer_id,
            running=not state.stop_event.is_set(),
            interval_seconds=state.interval_seconds,
            jitter_seconds=state.jitter_seconds,
            started_at=state.started_at,
            last_sent_at=state.last_sent_at,
            messages_sent=state.messages_sent,
            topic=state.topic,
            recent_events=list(state.recent_events),
        )

