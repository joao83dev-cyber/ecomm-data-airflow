from datetime import UTC, datetime
from uuid import uuid4

import pytest
from pydantic import ValidationError

from app.schemas import OrderEvent


def valid_event() -> dict:
    return {
        "event_id": "evt-001",
        "occurred_at": datetime.now(UTC).isoformat(),
        "order_id": str(uuid4()),
        "client": {"external_id": "client-1", "name": "Ada Example", "email": "ada@example.test"},
        "address": {
            "external_id": "home", "line1": "10 Example St", "city": "London",
            "region": "London", "postal_code": "N1 1AA", "country": "GB",
        },
        "items": [{"sku": "SKU-1", "description": "Sample", "quantity": 2, "unit_price": "12.50"}],
        "currency": "GBP",
        "status": "APPROVED",
        "payment": {"status": "APPROVED", "amount": "25.00", "provider_reference": "pay-1"},
    }


def test_accepts_complete_approved_order_event() -> None:
    event = OrderEvent.model_validate(valid_event())
    assert event.status == "APPROVED"
    assert event.items[0].quantity == 2


def test_rejects_empty_items() -> None:
    payload = valid_event()
    payload["items"] = []
    with pytest.raises(ValidationError):
        OrderEvent.model_validate(payload)


def test_rejects_unexpected_fields() -> None:
    payload = valid_event()
    payload["untrusted_extra"] = "unexpected"
    with pytest.raises(ValidationError):
        OrderEvent.model_validate(payload)
