"""
SocialSphere Event Ingestion API.

Endpoints:
    GET  /health          - liveness check
    GET  /metrics          - ingestion counters (ingested/flushed/buffered)
    POST /events           - ingest a single validated event
    POST /events/batch     - ingest a batch of validated events

Run locally:
    uvicorn main:app --reload --port 8000

Run via Docker Compose:
    see docker-compose.yml (service: ingestion_service)
"""

from fastapi import FastAPI, HTTPException
from pydantic import ValidationError

from models import ProductEvent, EventBatch, IngestResponse
from storage import event_store
from config import API_TITLE, API_VERSION

app = FastAPI(title=API_TITLE, version=API_VERSION)


@app.get("/health")
def health_check():
    return {"status": "healthy", "service": API_TITLE}


@app.get("/metrics")
def metrics():
    return event_store.stats()


@app.post("/events", response_model=IngestResponse)
def ingest_event(event: ProductEvent):
    """
    Ingest a single event. FastAPI + Pydantic validate the payload against
    the ProductEvent schema before this function body even runs; invalid
    events are rejected with a 422 automatically.
    """
    event_store.add_event(event.model_dump(mode="json"))
    return IngestResponse(message="event received", event_id=event.event_id, accepted=1, rejected=0)


@app.post("/events/batch", response_model=IngestResponse)
def ingest_event_batch(batch: EventBatch):
    """
    Ingest a batch of events in one call. Each event is validated against
    the same ProductEvent schema. This is the endpoint the event generator
    or any high-throughput producer should use for efficiency.
    """
    events_as_dicts = [e.model_dump(mode="json") for e in batch.events]
    event_store.add_events(events_as_dicts)
    return IngestResponse(
        message="batch received",
        accepted=len(events_as_dicts),
        rejected=0,
    )


@app.exception_handler(ValidationError)
def validation_exception_handler(request, exc):
    raise HTTPException(status_code=422, detail=str(exc))
