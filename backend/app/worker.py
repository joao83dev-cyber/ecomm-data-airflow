import asyncio
from datetime import UTC, datetime, timedelta
from uuid import UUID

import httpx
import orjson
from aiokafka import AIOKafkaConsumer
from aiokafka.errors import CommitFailedError
from sqlalchemy import select, text, update

from app.config import settings
from app.database import SessionLocal
from app.models import OutboxEvent
from app.schemas import ConnectionBatch, OrderEvent
from app.services import persist_connection_batch, persist_event


async def consume_connection_batches() -> None:
    consumer = AIOKafkaConsumer(
        settings.connections_topic,
        bootstrap_servers=settings.kafka_bootstrap_servers,
        group_id=settings.connections_group_id,
        enable_auto_commit=False,
        auto_offset_reset="earliest",
        max_poll_records=10,
        fetch_max_bytes=4 * 1024 * 1024,
    )
    await consumer.start()
    try:
        async for message in consumer:
            try:
                batch = ConnectionBatch.model_validate_json(message.value)
            except (ValueError, TypeError) as exc:
                await asyncio.to_thread(persist_dead_letter, message.value, str(exc)[:2000])
            else:
                try:
                    await asyncio.to_thread(persist_connection_batch, batch)
                except Exception:
                    continue
            try:
                await consumer.commit()
            except CommitFailedError:
                continue
    finally:
        await consumer.stop()


async def consume_events() -> None:
    consumer = AIOKafkaConsumer(
        settings.kafka_topic,
        bootstrap_servers=settings.kafka_bootstrap_servers,
        group_id=settings.kafka_group_id,
        enable_auto_commit=False,
        auto_offset_reset="earliest",
        max_poll_records=50,
        fetch_max_bytes=4 * 1024 * 1024,
    )
    await consumer.start()
    try:
        async for message in consumer:
            try:
                event = OrderEvent.model_validate_json(message.value)
                # Commit offsets only after the database transaction commits. On a crash,
                # Kafka redelivery is made harmless by processed_events(event_id).
                await asyncio.to_thread(persist_event, event)
            except (ValueError, TypeError) as exc:
                await asyncio.to_thread(persist_dead_letter, message.value, str(exc)[:2000])
                await consumer.commit()
            except Exception:
                # Leave the offset uncommitted so a transient database failure is retried.
                continue
            try:
                await consumer.commit()
            except CommitFailedError:
                # The consumer group may be rebalancing; processing is idempotent.
                continue
    finally:
        await consumer.stop()


def persist_dead_letter(raw_payload: bytes, reason: str) -> None:
    """Persist invalid messages for inspection before advancing Kafka offset."""
    with SessionLocal.begin() as session:
        session.execute(
            text("INSERT INTO ecommerce.dead_letter_events (payload, reason) VALUES (:payload, :reason)"),
            {"payload": raw_payload.decode("utf-8", errors="replace"), "reason": reason},
        )


async def deliver_outbox() -> None:
    timeout = httpx.Timeout(10.0, connect=3.0)
    async with httpx.AsyncClient(timeout=timeout) as client:
        while True:
            row_id, row_order_id, payload, attempts = await asyncio.to_thread(claim_outbox_event)
            if row_id is None:
                await asyncio.sleep(settings.outbox_poll_seconds)
                continue

            try:
                response = await client.post(
                    settings.external_erp_url,
                    json=payload,
                    headers={"Idempotency-Key": str(row_order_id)},
                )
                response.raise_for_status()
                await asyncio.to_thread(mark_outbox_delivered, row_id, attempts)
            except Exception as exc:
                delay_seconds = min(3600, 2 ** min(attempts, 12))
                await asyncio.to_thread(mark_outbox_failed, row_id, attempts, delay_seconds, str(exc)[:2000])


def claim_outbox_event():
    """Claim one due outbox row across competing worker replicas."""
    with SessionLocal.begin() as session:
        row = session.execute(
            select(OutboxEvent)
            .where(OutboxEvent.delivered.is_(False), OutboxEvent.next_attempt_at <= datetime.now(UTC))
            .order_by(OutboxEvent.created_at)
            .limit(1)
            .with_for_update(skip_locked=True)
        ).scalars().first()
        if row is None:
            return None, None, None, 0
        # Temporarily lease the row for 30 seconds. It stays retryable after crashes.
        row.next_attempt_at = datetime.now(UTC) + timedelta(seconds=30)
        return row.id, row.order_id, row.payload, row.attempts


def mark_outbox_delivered(row_id: UUID, attempts: int) -> None:
    with SessionLocal.begin() as session:
        session.execute(
            update(OutboxEvent).where(OutboxEvent.id == row_id, OutboxEvent.delivered.is_(False)).values(
                delivered=True, delivered_at=datetime.now(UTC), attempts=attempts + 1, last_error=None
            )
        )


def mark_outbox_failed(row_id: UUID, attempts: int, delay_seconds: int, error: str) -> None:
    with SessionLocal.begin() as session:
        session.execute(
            update(OutboxEvent).where(OutboxEvent.id == row_id, OutboxEvent.delivered.is_(False)).values(
                attempts=attempts + 1,
                next_attempt_at=datetime.now(UTC) + timedelta(seconds=delay_seconds),
                last_error=error,
            )
        )


async def main() -> None:
    await asyncio.gather(consume_events(), consume_connection_batches(), deliver_outbox())


if __name__ == "__main__":
    asyncio.run(main())
