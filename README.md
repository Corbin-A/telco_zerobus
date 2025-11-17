# Optus Zerobus local producer demo

This project spins up a lightweight FastAPI web UI so you can launch multiple local
"data producer" loops that publish events to the Lakeflow Connect **Zerobus**
ingestion endpoint. It also exposes a one-click manual alert trigger to prove out
end-to-end routing into Databricks.

The UI replaces running multiple terminal shells: you can start/stop producers, send
an alert, and watch counters update in real time while monitoring the Delta table in
Databricks.

## Prerequisites

- Python 3.10+ on your laptop
- Databricks workspace URL and PAT with access to your Lakeflow Connect destination
- Zerobus ingestion path (defaults to `/api/2.0/lakeflow/connect/zerobus/events:ingest`)

## Quick start

1. From `zerobus/`, create a virtual environment and install dependencies:

   ```bash
   python -m venv .venv
   source .venv/bin/activate
   pip install -r requirements.txt
   ```

2. Copy `.env` (or create a new one) with your workspace info:

   ```bash
   cp .env.example .env  # edit values inside
   ```

   Required variables:
   - `DATABRICKS_HOST` – e.g. `https://<workspace-url>`
   - `DATABRICKS_PAT` – PAT with permission to ingest

   Optional overrides:
   - `ZEROBUS_ENDPOINT_PATH` – if the ingestion path differs
   - `ZEROBUS_TOPIC` – default topic/stream name
   - `ZEROBUS_TARGET_TABLE` – fully-qualified table to land data
   - `ZEROBUS_USE_SDK=true` – leverage the Databricks Python SDK (recommended)
   - `ZEROBUS_DRY_RUN=true` – log payloads instead of sending

3. Launch the web UI:

   ```bash
   uvicorn app.main:app --reload
   ```

   Open http://localhost:8000 to control producers.

## How it works

- `app/producer_manager.py` houses an async manager that tracks producers, spawns a
  background task per producer, and sends events through `ZeroBusClient`.
- `app/zerobus_client.py` is the only place that knows the Zerobus contract. It now
  prefers the Databricks Python SDK and falls back to direct HTTP when the SDK is not
  available.
- `app/static/index.html` is a minimal UI that calls the FastAPI endpoints and shows
  live status.

## Endpoints

- `GET /api/producers` – list running producers
- `POST /api/producers` – start a producer (JSON body: `producer_id`, optional
  `topic`, `interval_seconds`, `jitter_seconds`, `payload_template`)
- `DELETE /api/producers/{producer_id}` – stop a producer
- `POST /api/alert` – send a manual alert (`message`, optional `severity`, `topic`,
  `producer_id`)

## Tailoring to your Zerobus endpoint

The default payload sent to Zerobus looks like:

```json
{
  "topic": "optus_zerobus_demo",
  "target_table": "main.default.zerobus_demo",
  "events": [
    {
      "event_id": "...",
      "producer_id": "kiosk-1",
      "event_type": "demo_tick",
      "event_time": "2024-04-22T02:42:11.388Z",
      "payload": { "sequence": 1, "region": "NSW", "channel": "retail" }
    }
  ]
}
```

If the Zerobus ingestion API expects different keys, update
`ZeroBusClient.build_payload` in `app/zerobus_client.py` to reshape the JSON while
leaving the rest of the app untouched.

## Notes for the demo

- Use the "Launch 3 demo producers" button to instantly show multi-producer
  concurrency writing into the same table.
- Set `ZEROBUS_DRY_RUN=true` if you want to test locally without hitting Databricks;
  payloads will be logged to stdout.
- Alerts use the same Zerobus client, making it easy to route into a downstream
  Delta table, alerting workflow, or dashboard.

