import os
import sys
import requests

# Ensure repository root is in sys.path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../../')))

from config import settings


def test_square_connection() -> bool:
    """
    Tests connectivity to the Square Sandbox Locations API.
    Validates required configuration, sends a GET request to /v2/locations,
    and reports safe diagnostic information without exposing sensitive credentials.
    """
    env = settings.SQUARE_ENVIRONMENT or ""
    base_url = settings.SQUARE_BASE_URL or ""
    app_id = settings.SQUARE_APPLICATION_ID or ""
    access_token = settings.SQUARE_ACCESS_TOKEN or ""
    location_id = settings.SQUARE_LOCATION_ID or ""

    print("========================================")
    print("SupplySense AI - Square Sandbox Test")
    print("========================================")
    print(f"Environment : {env if env.strip() else 'NOT CONFIGURED'}")
    print(f"Base URL    : {base_url if base_url.strip() else 'NOT CONFIGURED'}")
    print(f"Application : {'CONFIGURED' if app_id.strip() else 'NOT CONFIGURED'}")
    print(f"Token       : {'CONFIGURED' if access_token.strip() else 'NOT CONFIGURED'}")
    print(f"Location ID : {'CONFIGURED' if location_id.strip() else 'NOT CONFIGURED'}")
    print()

    # Validate that none of the required values are empty
    missing_fields = []
    if not env.strip():
        missing_fields.append("SQUARE_ENVIRONMENT")
    if not base_url.strip():
        missing_fields.append("SQUARE_BASE_URL")
    if not app_id.strip():
        missing_fields.append("SQUARE_APPLICATION_ID")
    if not access_token.strip():
        missing_fields.append("SQUARE_ACCESS_TOKEN")
    if not location_id.strip():
        missing_fields.append("SQUARE_LOCATION_ID")

    if missing_fields:
        print("Square Sandbox connection: FAILED")
        print(f"Error: Missing required configuration values: {', '.join(missing_fields)}")
        return False

    print("Calling Square Sandbox Locations API...")
    print()

    endpoint = f"{base_url.rstrip('/')}/v2/locations"
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json"
    }

    try:
        response = requests.get(endpoint, headers=headers, timeout=15)

        if response.status_code == 200:
            try:
                data = response.json()
                locations = data.get("locations", [])
                num_locations = len(locations)
            except Exception:
                num_locations = "Unknown"

            print("Square Sandbox connection: SUCCESS")
            print(f"HTTP Status: {response.status_code}")
            print(f"Number of locations: {num_locations}")
            return True
        else:
            print("Square Sandbox connection: FAILED")
            print(f"HTTP Status: {response.status_code}")
            safe_error = "Unknown error"
            try:
                err_data = response.json()
                if "errors" in err_data and isinstance(err_data["errors"], list):
                    messages = [
                        err.get("detail") or err.get("code") or err.get("category")
                        for err in err_data["errors"]
                        if isinstance(err, dict)
                    ]
                    safe_error = "; ".join(filter(None, messages)) or safe_error
                elif "message" in err_data:
                    safe_error = err_data["message"]
            except Exception:
                safe_error = response.reason or "Non-200 response"
            print(f"Error: {safe_error}")
            return False

    except requests.exceptions.Timeout:
        print("Square Sandbox connection: FAILED")
        print("Error: Request timed out while connecting to Square Sandbox API.")
        return False
    except requests.exceptions.RequestException as e:
        print("Square Sandbox connection: FAILED")
        print(f"Error: Network or connection error: {type(e).__name__}")
        return False


if __name__ == "__main__":
    success = test_square_connection()
    sys.exit(0 if success else 1)
