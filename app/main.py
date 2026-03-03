from contextlib import asynccontextmanager

from fastapi import FastAPI

from app.scheduler import start_scheduler, stop_scheduler


@asynccontextmanager
async def lifespan(app: FastAPI):
    start_scheduler()
    yield
    stop_scheduler()


app = FastAPI(title="WorkCafe-Agent", lifespan=lifespan)


@app.get("/health")
async def health():
    return {"status": "ok"}
