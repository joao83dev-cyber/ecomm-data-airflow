from fastapi import FastAPI, Header, HTTPException
from fastapi.responses import ORJSONResponse

app = FastAPI(title="Mock ERP", default_response_class=ORJSONResponse)
_delivered: set[str] = set()


@app.get("/health")
def health():
    return {"status": "ok"}


@app.post("/orders")
def accept_order(payload: dict, idempotency_key: str = Header(...)):
    # Demo sink: a real external ERP must persist this key durably for deduplication.
    duplicate = idempotency_key in _delivered
    if payload.get("force_failure"):
        raise HTTPException(status_code=503, detail="simulated temporary ERP failure")
    _delivered.add(idempotency_key)
    return {"accepted": True, "duplicate": duplicate, "order_id": idempotency_key}
