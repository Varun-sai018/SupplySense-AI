# Realtime API - Square Sandbox Integration (Phase 1)

This service provides the initial Square Sandbox integration for the SupplySense AI system.

## Phase 1 Scope

In Phase 1, we focus solely on:
- Square Sandbox credential configuration via `.env`
- Secure credential loading using `python-dotenv` and centralized configuration in `config/settings.py`
- Sandbox connectivity testing against the Square Sandbox Locations API (`/v2/locations`)

> **Note**: Later phases will handle real-time operational webhooks, event generation, Kafka integration, and dependency engine signaling. Do not implement webhooks, inventory events, or data pipeline modifications in this phase.

---

## Configuration

Before running the connection test, verify that your `.env` file at the root of the project contains your Square Sandbox credentials.

Ensure that the underscore placeholders in `.env` are replaced with your real Sandbox credentials:

```ini
# Square Sandbox Configuration
SQUARE_ENVIRONMENT=sandbox
SQUARE_BASE_URL=https://connect.squareupsandbox.com
SQUARE_APPLICATION_ID=sandbox-sq0idb-Gu2xuSGJMMA4gFWcCR8WNw
SQUARE_ACCESS_TOKEN=EAAAlzH-E01WnI_an4oiAjztmeZkQ4OMO0L_YROw6NhnDNRVVQFoFU046eLi7Jun
SQUARE_LOCATION_ID=LM2QZQNXN9P7F
```

> **Security Note**:
> - Never commit your `.env` file to version control (`.gitignore` protects `.env`).
> - Never hardcode access tokens in source code.
> - Diagnostic outputs mask tokens and only display `CONFIGURED` / `NOT CONFIGURED` status.

---

## Running the Connection Test

Run the test script from the project root:

```bash
python services/realtime-api/test_square_connection.py
```

### Expected Output

When configured properly with valid Sandbox credentials:

```text
========================================
SupplySense AI - Square Sandbox Test
========================================
Environment : sandbox
Base URL    : https://connect.squareupsandbox.com
Application : CONFIGURED
Token       : CONFIGURED
Location ID : CONFIGURED

Calling Square Sandbox Locations API...

Square Sandbox connection: SUCCESS
HTTP Status: 200
Number of locations: 1
```

If credentials are missing or invalid, the script provides safe diagnostic failure output without leaking sensitive tokens or headers:

```text
Square Sandbox connection: FAILED
HTTP Status: 401
Error: UNAUTHORIZED
```

---

## Phase 2 — Square Inventory API Test

### Purpose
Verifies that SupplySense AI can successfully connect and retrieve inventory counts from the Square Sandbox Inventory API (`GET /v2/inventory/counts`) scoped to the configured `SQUARE_LOCATION_ID`.

### Sandbox Environment
- Base URL: `https://connect.squareupsandbox.com`
- Environment: `sandbox`
- Endpoint: `/v2/inventory/counts`

### Required Environment Variables
Ensure the following variables are defined in your `.env` file (loaded via `python-dotenv`):
- `SQUARE_ENVIRONMENT`: Set to `sandbox`
- `SQUARE_BASE_URL`: `https://connect.squareupsandbox.com`
- `SQUARE_APPLICATION_ID`: Your Square Sandbox Application ID
- `SQUARE_ACCESS_TOKEN`: Your Square Sandbox Access Token
- `SQUARE_LOCATION_ID`: Your target Square Location ID

> **Security Warning**:
> Never commit real access tokens or credentials to version control. Always keep `.env` excluded via `.gitignore`. The test scripts never output full tokens or authorization headers to the terminal.

### Test Command
Run the test from the repository root:

```bash
python services/realtime-api/test_square_inventory.py
```

### Expected Output

```text
========================================
SupplySense AI - Square Inventory Test
========================================

Environment : sandbox
Base URL    : https://connect.squareupsandbox.com
Application : CONFIGURED
Token       : CONFIGURED
Location ID : CONFIGURED

Calling Square Sandbox Inventory API...

Square Inventory API: SUCCESS
HTTP Status: 200
Inventory records received: 0

The Square Sandbox API connection works, but no inventory records currently exist.
```

### Meaning of Zero Inventory Records
In a newly initialized Square Sandbox account, no inventory counts exist by default until catalog items and stock adjustments are created. A response of `HTTP 200` with `Inventory records received: 0` is considered a completely **successful API integration**, confirming that credentials and permissions are valid.

---

## Phase 3 — Local Square Webhook Service

> **Important Note**:
> **Square Developer Dashboard webhook subscription is intentionally not configured during this phase.**
> This phase prepares and verifies the complete local FastAPI webhook pipeline before public tunneling (e.g. ngrok or Cloudflare tunnel) and Square Dashboard subscription are configured.

### Purpose
Implements the local FastAPI webhook ingestion service for Square operational events, handling signature verification, event normalization, MySQL persistence (`dataset_events` & `dataset_metadata`), and Kafka publishing (`dataset-events` topic).

### Local Service Architecture

```text
Square Webhook Payload
         ↓
POST /webhooks/square (FastAPI @ http://127.0.0.1:8000)
         ↓
HMAC-SHA256 Signature Verification (SQUARE_WEBHOOK_SIGNATURE_KEY)
         ↓
Square Event Adapter (inventory.count.updated -> Inventory / DATA_UPDATED)
         ↓
MySQL Persistence (dataset_events table & dataset_metadata version increment)
         ↓
Kafka Producer (Publish normalized event to dataset-events topic)
         ↓
Dependency Engine Evaluation (ALL / ANY / QUORUM rules)
```

### Endpoints

- **`GET /health`**: Health check returning service status and environment metadata.
- **`POST /webhooks/square`**: Ingests Square webhook events, verifies HMAC-SHA256 signatures, normalizes `inventory.count.updated` events, persists them to MySQL, and publishes to Kafka.

### Environment Variables
Configure in `.env` (sample placeholders in `.env.example`):
- `SQUARE_ENVIRONMENT`: `sandbox`
- `SQUARE_BASE_URL`: `https://connect.squareupsandbox.com`
- `SQUARE_APPLICATION_ID`: Square Sandbox Application ID
- `SQUARE_ACCESS_TOKEN`: Square Sandbox Access Token
- `SQUARE_LOCATION_ID`: Square Location ID
- `SQUARE_WEBHOOK_SIGNATURE_KEY`: Square Webhook Signature Key (from Square Developer Dashboard when webhook subscription is created)

> **Security Rules**:
> - Never commit `.env` or raw signature keys to Git (`.gitignore` protects `.env`).
> - Verification calculates HMAC-SHA256 over `notification_url` + `raw_request_body`.
> - If `SQUARE_WEBHOOK_SIGNATURE_KEY` is set, invalid/missing signatures return `403 Forbidden`.

### Running the Local Service

Start the FastAPI application locally on port 8000:

```bash
python services/realtime-api/main.py
```

### Verification Commands

1. **Local Health Check**:
   ```bash
   python -c "import requests; print(requests.get('http://127.0.0.1:8000/health').json())"
   ```
   **Response**: `{'status': 'ok', 'service': 'SupplySense AI Realtime API', 'environment': 'sandbox', 'base_url': 'https://connect.squareupsandbox.com'}`

2. **Run Webhook Unit & Integration Tests**:
   ```bash
   python -m unittest tests/unit/test_square_webhook.py
   python -m unittest tests/integration/test_square_persistence.py
   ```

3. **Run Entire Project Test Suite**:
   ```bash
   python -m unittest discover -s tests
   ```


