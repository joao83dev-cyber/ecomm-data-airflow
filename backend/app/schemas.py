from datetime import datetime
from decimal import Decimal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, model_validator


class CustomerInput(BaseModel):
    external_id: str = Field(min_length=1, max_length=128)
    name: str = Field(min_length=1, max_length=200)
    email: str = Field(min_length=3, max_length=320)


class AddressInput(BaseModel):
    external_id: str = Field(min_length=1, max_length=128)
    line1: str = Field(min_length=1, max_length=250)
    line2: str | None = Field(default=None, max_length=250)
    city: str = Field(min_length=1, max_length=120)
    region: str = Field(min_length=1, max_length=120)
    postal_code: str = Field(min_length=1, max_length=32)
    country: str = Field(min_length=2, max_length=2)


class ItemInput(BaseModel):
    sku: str = Field(min_length=1, max_length=128)
    description: str = Field(min_length=1, max_length=500)
    quantity: int = Field(gt=0)
    unit_price: Decimal = Field(ge=0, max_digits=14, decimal_places=2)


class PaymentInput(BaseModel):
    status: str = Field(pattern="^(PENDING|APPROVED|DECLINED)$")
    amount: Decimal | None = Field(default=None, ge=0, max_digits=14, decimal_places=2)
    provider_reference: str | None = Field(default=None, max_length=200)


class OrderEvent(BaseModel):
    model_config = ConfigDict(extra="forbid")

    event_id: str = Field(min_length=1, max_length=128)
    event_type: str = "order.created"
    occurred_at: datetime
    order_id: UUID
    client: CustomerInput
    address: AddressInput
    items: list[ItemInput] = Field(min_length=1, max_length=100)
    currency: str = Field(min_length=3, max_length=3)
    status: str = "PENDING"
    payment: PaymentInput

    @model_validator(mode="after")
    def validate_status_consistency(self):
        if self.status == "APPROVED" and self.payment.status != "APPROVED":
            raise ValueError("approved orders require an approved payment")
        if self.status == "PENDING" and self.payment.status != "PENDING":
            raise ValueError("pending orders require a pending payment")
        return self


class IngestAccepted(BaseModel):
    event_id: str
    status: str
    duplicate: bool = False
