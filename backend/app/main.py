from contextlib import asynccontextmanager
import asyncio
from decimal import Decimal

import orjson
from aiokafka import AIOKafkaProducer
from fastapi import FastAPI, Header, HTTPException, status
from fastapi.responses import ORJSONResponse

from app.config import settings
from app.schemas import IngestAccepted, OrderEvent

producer: AIOKafkaProducer | None = None
MAX_PAYLOAD_BYTES = 262144
MAX_CONCURRENT_INGESTIONS = 200


class BodySizeLimitMiddleware:
    """Bound webhook request buffering, including chunked requests without Content-Length."""

    def __init__(self, app, limit: int, max_concurrency: int):
        self.app = app
        self.limit = limit
        self.semaphore = asyncio.Semaphore(max_concurrency)

    async def __call__(self, scope, receive, send):
        if scope["type"] != "http" or scope["path"] != "/webhooks/orders" or scope["method"] != "POST":
            await self.app(scope, receive, send)
            return
        try:
            await asyncio.wait_for(self.semaphore.acquire(), timeout=0.01)
        except TimeoutError:
            body = b'{"detail":"ingestion is at capacity; retry later"}'
            await send({"type": "http.response.start", "status": 503, "headers": [(b"content-type", b"application/json"), (b"retry-after", b"1"), (b"content-length", str(len(body)).encode())]})
            await send({"type": "http.response.body", "body": body})
            return
        try:
            await self._handle_request(scope, receive, send)
        except Exception:
            body = b'{"detail":"internal server error"}'
            await send({"type": "http.response.start", "status": 500, "headers": [(b"content-type", b"application/json"), (b"content-length", str(len(body)).encode())]})
            await send({"type": "http.response.body", "body": body})
        finally:
            self.semaphore.release()

    async def _handle_request(self, scope, receive, send):
        content_length = next((value for key, value in scope["headers"] if key.lower() == b"content-length"), None)
        if content_length and int(content_length) > self.limit:
            await self._reject(send)
            return

        chunks: list[dict] = []
        received_bytes = 0
        while True:
            message = await receive()
            if message["type"] == "http.disconnect":
                return
            chunk = message.get("body", b"")
            received_bytes += len(chunk)
            if received_bytes > self.limit:
                await self._reject(send)
                return
            chunks.append(message)
            if not message.get("more_body", False):
                break

        position = 0

        async def replay():
            nonlocal position
            if position < len(chunks):
                item = chunks[position]
                position += 1
                return item
            return {"type": "http.disconnect"}

        await self.app(scope, replay, send)

    @staticmethod
    async def _reject(send):
        body = b'{"detail":"event payload too large"}'
        await send({"type": "http.response.start", "status": 413, "headers": [(b"content-type", b"application/json"), (b"content-length", str(len(body)).encode())]})
        await send({"type": "http.response.body", "body": body})


@asynccontextmanager
async def lifespan(_: FastAPI):
    global producer
    producer = AIOKafkaProducer(
        bootstrap_servers=settings.kafka_bootstrap_servers,
        acks="all",
        enable_idempotence=True,
        max_request_size=MAX_PAYLOAD_BYTES,
        max_batch_size=65536,
        linger_ms=10,
    )
    await producer.start()
    try:
        yield
    finally:
        await producer.stop()


app = FastAPI(title="E-commerce Events API", version="1.0.0", default_response_class=ORJSONResponse, lifespan=lifespan)
app.add_middleware(BodySizeLimitMiddleware, limit=MAX_PAYLOAD_BYTES, max_concurrency=MAX_CONCURRENT_INGESTIONS)


@app.get("/health")
async def health() -> dict[str, str]:
    if producer is None:
        raise HTTPException(status_code=503, detail="Kafka producer is not ready")
    return {"status": "ok"}


@app.post("/webhooks/orders", response_model=IngestAccepted, status_code=status.HTTP_202_ACCEPTED)
async def ingest_order(event: OrderEvent, idempotency_key: str | None = Header(default=None)):
    if idempotency_key and idempotency_key != event.event_id:
        raise HTTPException(status_code=400, detail="Idempotency-Key must match event_id")
    if event.status not in {"PENDING", "APPROVED", "REJECTED", "CANCELLED"}:
        raise HTTPException(status_code=422, detail="unsupported order status")
    expected_amount = sum((item.quantity * item.unit_price for item in event.items), Decimal("0.00"))
    if event.payment.amount is not None and event.payment.amount != expected_amount:
        raise HTTPException(status_code=422, detail="payment amount must match the item total")
    if producer is None:
        raise HTTPException(status_code=503, detail="ingestion queue unavailable")

    encoded = orjson.dumps(event.model_dump(mode="json"), option=orjson.OPT_SORT_KEYS)
    if len(encoded) > MAX_PAYLOAD_BYTES:
        raise HTTPException(status_code=413, detail="event payload too large")
    try:
        await producer.send_and_wait(settings.kafka_topic, encoded, key=str(event.order_id).encode())
    except Exception as exc:
        raise HTTPException(status_code=503, detail="ingestion queue unavailable") from exc
    return IngestAccepted(event_id=event.event_id, status="queued")
