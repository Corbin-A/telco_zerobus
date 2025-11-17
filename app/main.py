import logging
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles

from .config import get_settings
from .producer_manager import ProducerManager
from .schemas import ManualAlertRequest, ProducerStatus, StartProducerRequest
from .zerobus_client import ZeroBusClient


logging.basicConfig(level=logging.INFO, format="[%(asctime)s] %(levelname)s %(name)s: %(message)s")
logger = logging.getLogger(__name__)


settings = get_settings()
zerobus_client = ZeroBusClient(settings)
producer_manager = ProducerManager(zerobus_client, settings)
BASE_DIR = Path(__file__).resolve().parent


@asynccontextmanager
async def lifespan(app: FastAPI):  # pragma: no cover - wiring only
    logger.info("ZeroBus demo starting (dry_run=%s)", settings.dry_run)
    try:
        yield
    finally:
        await producer_manager.stop_all()
        await zerobus_client.aclose()
        logger.info("ZeroBus demo stopped")


app = FastAPI(title="Optus Zerobus Demo", lifespan=lifespan)
app.mount("/static", StaticFiles(directory=BASE_DIR / "static"), name="static")


@app.get("/")
async def index() -> FileResponse:
    return FileResponse(BASE_DIR / "static" / "index.html")


@app.get("/api/health")
async def health() -> dict:
    return {"status": "ok", "dry_run": settings.dry_run}


@app.get("/api/producers", response_model=list[ProducerStatus])
async def list_producers():
    return producer_manager.list_status()


@app.post("/api/producers", response_model=ProducerStatus)
async def start_producer(request: StartProducerRequest):
    try:
        return await producer_manager.start_producer(request)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@app.delete("/api/producers/{producer_id}")
async def stop_producer(producer_id: str):
    stopped = await producer_manager.stop_producer(producer_id)
    if not stopped:
        raise HTTPException(status_code=404, detail=f"Producer '{producer_id}' not running")
    return {"stopped": True}


@app.post("/api/alert")
async def manual_alert(request: ManualAlertRequest):
    response = await producer_manager.send_manual_alert(request)
    return {"result": response}

