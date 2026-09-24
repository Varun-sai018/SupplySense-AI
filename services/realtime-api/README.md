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
