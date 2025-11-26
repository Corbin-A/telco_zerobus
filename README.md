# Optus Zerobus local producer demo

This project spins up a FastAPI web UI so you can launch multiple "tower" producer
loops on your laptop and publish events directly into the Lakeflow Connect
**Zerobus** ingestion endpoint. It mirrors the workflow documented in the
[Databricks Zerobus ingest guide](https://docs.databricks.com/aws/en/ingestion/lakeflow-connect/zerobus-ingest):
you supply a Unity Catalog table, generate the protobuf schema, and the app takes
care of using the official Zerobus Ingest SDK to stream data.

## Prerequisites

From Databricks:

1. **Workspace URL + ID** – log into the workspace you will target and grab the
   browser URL. The `https://<workspace-host>` portion becomes `DATABRICKS_HOST`.
   The numeric/hex value after `?o=` (or `/o=`) becomes your `<workspace-id>` and
   is required when Databricks provisions your Zerobus server endpoint.
2. **Unity Catalog table** – identify (or create) the table that should receive
   these demo events. The Zerobus SDK **does not support VARIANT columns**,
   so keep the schema flat and typed. Example aligned to the demo payload:

   ```sql
   CREATE TABLE main.default.zerobus_demo (
     producer_id STRING,
     sequence BIGINT,
     observed_at STRING,
     region STRING,
     channel STRING,
     campaign STRING,
     site_status STRING,
     severity STRING,
     message STRING
   );
   ```

   If you previously used a `VARIANT`/`MAP` column for the payload, replace it
   with explicit columns before generating the protobuf.

3. **Service principal** – create one in the workspace, capture its client id and
   secret, and grant it `USE CATALOG`, `USE SCHEMA`, plus `MODIFY`/`SELECT` on the
   target table. These become `ZEROBUS_CLIENT_ID` / `ZEROBUS_CLIENT_SECRET`.
4. **Zerobus server endpoint** – Databricks support (or your account team) will
   provide a hostname shaped like
   `<workspace-id>.zerobus.<region>.cloud.databricks.com`. This is
   `ZEROBUS_SERVER_ENDPOINT`.

On your laptop:

- Python 3.10+
- `pip install grpcio-tools` (only needed once to compile the protobuf definition)
- Clone this repo
- Ability to install Python dependencies listed in `requirements.txt`

## Quick start

1. From `zerobus/`, create a virtual environment and install dependencies:

   ```bash
   python -m venv .venv
   source .venv/bin/activate
   pip install -r requirements.txt
   ```

2. Copy `.env.example` to `.env` and populate the required fields before doing
   anything else. (They are referenced by the CLI tooling as well as the app.)

   ```bash
   python -m zerobus.tools.generate_proto \
     --uc-endpoint "$DATABRICKS_HOST" \
     --client-id "$ZEROBUS_CLIENT_ID" \
     --client-secret "$ZEROBUS_CLIENT_SECRET" \
     --table "$ZEROBUS_TARGET_TABLE" \
     --output record.proto
   ```

   Key variables:
   - `DATABRICKS_HOST` – **must** include the `https://` scheme and omit the `/o=`
     suffix (for example `https://e2-demo-field-eng.cloud.databricks.com`).
   - `ZEROBUS_SERVER_ENDPOINT` – Zerobus host from Databricks
   - `ZEROBUS_CLIENT_ID` / `ZEROBUS_CLIENT_SECRET` – service principal values
- `ZEROBUS_PROTO_MODULE` / `ZEROBUS_PROTO_MESSAGE` – e.g. `record_pb2` /
  `ZerobusDemo` (matching the message name generated from your table)
   - `ZEROBUS_TARGET_TABLE` – fully-qualified Unity Catalog table

3. Export the values so the Databricks CLI tooling can read them (or manually
   inline the values instead of relying on environment variables):

   ```bash
   set -a
   source .env
   set +a
   ```

   Double-check that `echo "$DATABRICKS_HOST"` prints a URL that starts with
   `https://`. If this variable is blank or missing the scheme, tools such as
   `zerobus.tools.generate_proto` will emit errors like `Invalid URL '/oidc/v1/token':
   No scheme supplied`.

4. Generate the protobuf class that mirrors your target table. Run the tool from
   the Databricks Zerobus SDK once per schema change:

   ```bash
   python -m zerobus.tools.generate_proto \
     --uc-endpoint "$DATABRICKS_HOST" \
     --client-id "$ZEROBUS_CLIENT_ID" \
     --client-secret "$ZEROBUS_CLIENT_SECRET" \
     --table "$ZEROBUS_TARGET_TABLE" \
     --output record.proto
   ```

   If you see `Unknown column type: variant`, rebuild the Unity Catalog table
   with explicit columns (no `VARIANT`/`MAP`), then re-run the generator.

   **Seeing `OAuth request failed ... invalid_client`?** That means the
   Databricks workspace rejected the service principal credentials. Double-check
   that:

   - The `ZEROBUS_CLIENT_ID`/`ZEROBUS_CLIENT_SECRET` pair came from
     **Settings → Identity & access → Service principals** within the same
     workspace referenced by `DATABRICKS_HOST` (account-level service principals
     must also be assigned to the workspace).
   - The secret was generated using **Generate secret** (copy the value when it
     appears—Databricks will not show it again).
   - The service principal still exists and has not been rotated or deleted.

   To verify the credentials outside of the Zerobus tooling, try requesting a
   token directly:

   ```bash
   curl -u "$ZEROBUS_CLIENT_ID:$ZEROBUS_CLIENT_SECRET" \
     -d 'grant_type=client_credentials&scope=all-apis' \
     "$DATABRICKS_HOST/oidc/v1/token"
   ```

   A JSON token response confirms the credentials are valid; an `invalid_client`
   response means you must fix the ID/secret (or grant the service principal to
   the workspace) before the protobuf generator will work.

5. Compile the protobuf to a Python module. The example below emits
   `record_pb2.py` in the repo root so it is importable without touching
   `PYTHONPATH`:

   ```bash
   python -m grpc_tools.protoc --python_out=. --proto_path=. record.proto
   ```

   Optional toggles in `.env`:
   - `ZEROBUS_TOPIC` – default topic for new towers
   - `ZEROBUS_DRY_RUN=true` – log payloads without calling Databricks
   - `ZEROBUS_USE_INGEST_SDK=false` – fall back to the legacy HTTP flow (not recommended)
   - `ZEROBUS_ALLOW_HTTP_FALLBACK=true` – opt into direct HTTP calls if you have a
     bespoke gateway. Requires `DATABRICKS_PAT` **and** either
     `ZEROBUS_DESTINATION_ID` or a custom `ZEROBUS_ENDPOINT_PATH`.

5. Launch the web UI:

   ```bash
   uvicorn app.main:app --reload
   ```

   Open http://localhost:8000 to control producers.

## How it works

- `app/producer_manager.py` houses an async manager that tracks producers, spawns a
  background task per producer, and sends events through `ZeroBusClient`.
- `app/zerobus_client.py` is the only place that knows the Zerobus contract. It
  instantiates the Zerobus Ingest SDK stream (using your protobuf class) and only
  falls back to direct HTTP when explicitly allowed.
- `app/static/index.html` is a minimal UI that calls the FastAPI endpoints and shows
  live status.

## Endpoints

- `GET /api/producers` – list running producers
- `POST /api/producers` – start a producer (JSON body: `producer_id`, optional
  `topic`, `interval_seconds`, `jitter_seconds`, `payload_template`)
- `DELETE /api/producers/{producer_id}` – stop a producer
- `POST /api/alert` – send a manual alert (`message`, optional `severity`, `topic`,
  `producer_id`)

## Tailoring to your Zerobus ingest stream

The "Launch tower" modal lets you tweak payload fields and automatically maps the
result into your protobuf class. Make sure the template fields align with your
Unity Catalog columns; otherwise the SDK will raise an error when it tries to
instantiate the protobuf message.

Need to test without touching Databricks? Set `ZEROBUS_DRY_RUN=true` and the app
will log every payload instead of calling the SDK. To demonstrate HTTP ingestion
against a bespoke gateway, set `ZEROBUS_ALLOW_HTTP_FALLBACK=true`, provide a
`DATABRICKS_PAT`, and either `ZEROBUS_DESTINATION_ID` or the fully-qualified
`ZEROBUS_ENDPOINT_PATH` documented in the Lakeflow Connect guide.

## Notes for the demo

- Use the "Launch 3 demo producers" button to instantly show multi-producer
  concurrency writing into the same table.
- Set `ZEROBUS_DRY_RUN=true` if you want to test locally without hitting Databricks;
  payloads will be logged to stdout.
- Alerts use the same Zerobus client, making it easy to route into a downstream
  Delta table, alerting workflow, or dashboard.

