import asyncio
from datetime import UTC, datetime, timedelta

import httpx
import orjson
from aiokafka import AIOKafkaConsumer
from sqlalchemy import select, update

from app.config import settings
from app.database import SessionLocal
from app.models import OutboxEvent
from app.schemas import OrderEvent
from app.services import persist_event


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
            event = OrderEvent.model_validate_json(message.value)
            # Commit offsets only after the database transaction commits. On a crash,
            # Kafka redelivery is made harmless by processed_events(event_id).
            await asyncio.to_thread(persist_event, event)
            await consumer.commit()
    finally:
        await consumer.stop()


async def deliver_outbox() -> None:
    timeout = httpx.Timeout(10.0, connect=3.0)
    async with httpx.AsyncClient(timeout=timeout) as client:
        while True:
            with SessionLocal() as session:
                row = session.execute(
                    select(OutboxEvent)
                    .where(OutboxEvent.delivered.is_(False), OutboxEvent.next_attempt_at <= datetime.now(UTC))
                    .order_by(OutboxEvent.created_at)
                    .limit(1)
                ).scalar_one_or_none()
                row_id = row.id if row else None
                row_order_id = row.order_id if row else None
                payload = row.payload if row else None
                attempts = row.attempts if row else 0
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
                with SessionLocal.begin() as session:
                    session.execute(
                        update(OutboxEvent).where(OutboxEvent.id == row_id, OutboxEvent.delivered.is_(False)).values(
                            delivered=True, delivered_at=datetime.now(UTC), attempts=attempts + 1, last_error=None
                        )
                    )
            except Exception as exc:
                delay_seconds = min(3600, 2 ** min(attempts, 12))
                with SessionLocal.begin() as session:
                    session.execute(
                        update(OutboxEvent).where(OutboxEvent.id == row_id, OutboxEvent.delivered.is_(False)).values(
                            attempts=attempts + 1,
                            next_attempt_at=datetime.now(UTC) + timedelta(seconds=delay_seconds),
                            last_error=str(exc)[:2000],
                        )
                    )


async def main() -> None:
    await asyncio.gather(consume_events(), deliver_outbox())


if __name__ == "__main__":
    asyncio.run(main())
