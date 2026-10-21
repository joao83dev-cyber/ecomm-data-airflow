from datetime import UTC, datetime
from decimal import Decimal

import orjson
from sqlalchemy import select, text, update
from sqlalchemy.dialects.postgresql import insert

from app.database import SessionLocal
from app.models import Address, Client, Connection, Invoice, Order, OrderItem, OrderEventHistory, OutboxEvent, Payment, ProcessedEvent
from app.schemas import ConnectionBatch, OrderEvent


def persist_connection_batch(batch: ConnectionBatch) -> int:
    """Insert a connection batch atomically; duplicate IDs/logins are ignored on replay."""
    rows = [record.model_dump() for record in batch.records]
    with SessionLocal.begin() as session:
        result = session.execute(
            insert(Connection).values(rows).on_conflict_do_nothing()
        )
        return result.rowcount or 0


def persist_event(event: OrderEvent) -> bool:
    """Persist one webhook atomically; return False for a duplicate event."""
    payload = orjson.loads(orjson.dumps(event.model_dump(mode="json"), option=orjson.OPT_SORT_KEYS))
    order_total = sum((item.quantity * item.unit_price for item in event.items), Decimal("0.00"))
    payment_amount = event.payment.amount if event.payment.amount is not None else order_total
    if payment_amount != order_total:
        raise ValueError("payment amount must match the item total")

    with SessionLocal.begin() as session:
        inserted = session.execute(
            insert(ProcessedEvent).values(event_id=event.event_id).on_conflict_do_nothing().returning(ProcessedEvent.event_id)
        ).scalar_one_or_none()
        if inserted is None:
            return False

        session.execute(
            insert(Client)
            .values(external_id=event.client.external_id, name=event.client.name, email=event.client.email)
            .on_conflict_do_nothing(index_elements=[Client.external_id])
        )
        client_id = session.execute(
            select(Client.id).where(Client.external_id == event.client.external_id).with_for_update()
        ).scalar_one()

        session.execute(
            text("SELECT pg_advisory_xact_lock(hashtextextended(:address_key, 0))"),
            {"address_key": f"{client_id}:{event.address.external_id}"},
        )
        address_values = event.address.model_dump()
        latest = session.execute(
            select(Address).where(Address.client_id == client_id, Address.external_id == event.address.external_id)
            .order_by(Address.version.desc()).limit(1).with_for_update()
        ).scalar_one_or_none()
        address_fields = {key: value for key, value in address_values.items() if key != "external_id"}
        if latest is None or any(getattr(latest, key) != value for key, value in address_fields.items()):
            address = Address(client_id=client_id, version=(latest.version + 1 if latest else 1), **address_values)
            session.add(address)
            session.flush()
            address_id = address.id
        else:
            address_id = latest.id

        session.execute(
            text("SELECT pg_advisory_xact_lock(hashtextextended(:order_key, 0))"),
            {"order_key": str(event.order_id)},
        )
        inserted_order = session.execute(
            insert(Order)
            .values(
                id=event.order_id,
                external_id=str(event.order_id),
                client_id=client_id,
                address_id=address_id,
                status=event.status,
                currency=event.currency.upper(),
                total_amount=order_total,
                customer_snapshot=event.client.model_dump(mode="json"),
                address_snapshot=event.address.model_dump(mode="json"),
                items_snapshot=[item.model_dump(mode="json") for item in event.items],
                created_at=event.occurred_at,
                closed_at=event.occurred_at if event.status in {"APPROVED", "REJECTED", "CANCELLED"} else None,
            )
            .on_conflict_do_nothing(index_elements=[Order.external_id])
            .returning(Order.id)
        ).scalar_one_or_none()
        order = session.execute(select(Order).where(Order.id == event.order_id).with_for_update()).scalar_one_or_none()
        if order is None:
            raise ValueError("order already exists under a different identifier")
        if inserted_order is None:
            if order.client_id != client_id or order.customer_snapshot != event.client.model_dump(mode="json") or order.address_snapshot != event.address.model_dump(mode="json"):
                raise ValueError("customer or address snapshot cannot be changed by a later order event")
            expected_items = [item.model_dump(mode="json") for item in event.items]
            if order.items_snapshot != expected_items or order.total_amount != order_total or order.currency != event.currency.upper():
                raise ValueError("an existing order cannot be changed by a later event")
            if order.closed_at is not None and order.status != event.status:
                raise ValueError("a closed order cannot transition to a different status")
            if order.status == "PENDING" and event.status != "PENDING":
                session.execute(
                    update(Order).where(Order.id == event.order_id, Order.closed_at.is_(None)).values(
                        status=event.status, closed_at=event.occurred_at
                    )
                )
        else:
            session.execute(
                update(Client)
                .where(Client.id == client_id)
                .values(name=event.client.name, email=event.client.email, updated_at=datetime.now(UTC))
            )
            session.add_all([
                OrderItem(order_id=event.order_id, line_number=index, **item.model_dump())
                for index, item in enumerate(event.items, start=1)
            ])
        session.add(OrderEventHistory(event_id=event.event_id, order_id=event.order_id, status=event.status, payload=payload, occurred_at=event.occurred_at))

        previous_payment = session.execute(select(Payment).where(Payment.order_id == event.order_id).with_for_update()).scalar_one_or_none()
        if previous_payment is None:
            session.add(Payment(
                order_id=event.order_id,
                provider_reference=event.payment.provider_reference,
                status=event.payment.status,
                provider_payload=event.payment.model_dump(mode="json"),
                amount=payment_amount,
            ))
        elif previous_payment.status == "PENDING" and event.payment.status in {"APPROVED", "DECLINED"}:
            previous_payment.status = event.payment.status
            previous_payment.provider_reference = event.payment.provider_reference or previous_payment.provider_reference
            previous_payment.provider_payload = event.payment.model_dump(mode="json")
            previous_payment.amount = payment_amount
        elif previous_payment.status != event.payment.status:
            raise ValueError("payment status cannot be changed after it is finalized")
        elif previous_payment.amount != payment_amount:
            raise ValueError("payment amount cannot be changed after it is finalized")
        if event.status == "APPROVED" and event.payment.status == "APPROVED":
            invoice_number = f"INV-{event.order_id}"
            session.execute(
                insert(Invoice)
                .values(order_id=event.order_id, invoice_number=invoice_number, amount=order_total,
                        payload={"invoice_number": invoice_number, "order_id": str(event.order_id), "amount": str(order_total), "currency": event.currency.upper()})
                .on_conflict_do_nothing(index_elements=[Invoice.order_id])
            )
            session.execute(
                insert(OutboxEvent)
                .values(order_id=event.order_id, topic="orders.approved", payload=payload)
                .on_conflict_do_nothing(index_elements=[OutboxEvent.order_id, OutboxEvent.topic])
            )
    return True
