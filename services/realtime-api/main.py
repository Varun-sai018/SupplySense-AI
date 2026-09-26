import json
import logging
import os
import sys
from fastapi import FastAPI, Request, HTTPException, Header, status
from fastapi.responses import JSONResponse

# Ensure repository root is in sys.path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../../')))

from config import settings
import importlib

square_adapter_module = importlib.import_module('services.realtime-api.square_adapter')
verify_square_signature = square_adapter_module.verify_square_signature
normalize_square_event = square_adapter_module.normalize_square_event
persist_and_publish_event = square_adapter_module.persist_and_publish_event


logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')
logger = logging.getLogger(__name__)

app = FastAPI(
    title="SupplySense AI - Realtime Webhook Service",
    description="Real-time Square Sandbox Webhook Ingestion Service",
    version="1.0.0"
)


@app.get("/health")
def health_check():
    """Health check endpoint for the realtime API service."""
    return {
        "status": "ok",
        "service": "SupplySense AI Realtime API",
        "environment": settings.SQUARE_ENVIRONMENT or "sandbox",
        "base_url": settings.SQUARE_BASE_URL or "https://connect.squareupsandbox.com"
    }


@app.post("/webhooks/square")
async def square_webhook(request: Request):
    """
    Square Webhook Endpoint.
    Receives real-time operational events from Square, verifies signature,
    normalizes payloads, and persists events to MySQL and Kafka.
    """
    raw_body = await request.body()
    signature_header = request.headers.get("x-square-hmacsha256-signature") or request.headers.get("X-Square-HmacSHA256-Signature")

    signature_key = settings.SQUARE_WEBHOOK_SIGNATURE_KEY

    # Signature Verification
    if signature_key and signature_key.strip():
        proto = request.headers.get("x-forwarded-proto") or request.url.scheme
        host = request.headers.get("x-forwarded-host") or request.headers.get("host") or request.url.netloc
        path = request.url.path
        notification_url = f"{proto}://{host}{path}"

        is_valid = verify_square_signature(
            raw_body=raw_body,
            signature_header=signature_header or "",
            signature_key=signature_key,
            notification_url=notification_url
        )
        if not is_valid and notification_url != str(request.url):
            is_valid = verify_square_signature(
                raw_body=raw_body,
                signature_header=signature_header or "",
                signature_key=signature_key,
                notification_url=str(request.url)
            )

        if not is_valid:
            logger.warning("Square Webhook signature verification failed.")
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="Invalid Square Webhook Signature"
            )


    try:
        payload = json.loads(raw_body)
    except Exception as e:
        logger.error(f"Malformed JSON received in webhook: {e}")
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Malformed JSON payload"
        )

    # Normalize event
    normalized = normalize_square_event(payload)

    if not normalized.get("supported"):
        logger.info(f"Ignored unsupported Square event: {normalized.get('reason')}")
        return JSONResponse(
            status_code=status.HTTP_200_OK,
            content={"status": "IGNORED", "reason": normalized.get("reason")}
        )

    # Persist and publish
    try:
        result = persist_and_publish_event(normalized)
        return JSONResponse(
            status_code=status.HTTP_200_OK,
            content={"status": "SUCCESS", "details": result}
        )
    except Exception as e:
        logger.error(f"Error processing webhook: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Error processing event"
        )


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="127.0.0.1", port=8000)

