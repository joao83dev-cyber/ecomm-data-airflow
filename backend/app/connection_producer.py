import base64
import hashlib
import os
from datetime import UTC, datetime
from uuid import uuid4

from faker import Faker

from app.schemas import ConnectionBatch, ConnectionRecord

PBKDF2_ITERATIONS = 120_000


def hash_mock_password(password: str) -> str:
    salt = os.urandom(16)
    digest = hashlib.pbkdf2_hmac("sha256", password.encode("utf-8"), salt, PBKDF2_ITERATIONS)
    return "pbkdf2_sha256${}${}${}".format(
        PBKDF2_ITERATIONS,
        base64.urlsafe_b64encode(salt).decode("ascii"),
        base64.urlsafe_b64encode(digest).decode("ascii"),
    )


def generate_connection_batch(count: int = 200) -> ConnectionBatch:
    if not 1 <= count <= 200:
        raise ValueError("count must be between 1 and 200")

    fake = Faker()
    records = []
    for _ in range(count):
        userid = uuid4()
        login = f"{fake.user_name()[:110]}_{userid.hex[:12]}"
        password = fake.password(length=24)
        records.append(
            ConnectionRecord(
                userid=userid,
                login=login,
                hashpass=hash_mock_password(password),
                date_access=datetime.now(UTC),
            )
        )
    return ConnectionBatch(batch_id=uuid4(), records=records)
