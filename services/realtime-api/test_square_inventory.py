import json
import os
import sys
import requests

# Ensure repository root is in sys.path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../../')))

from config import settings


def test_square_inventory() -> bool:
    """
    Tests retrieval of inventory counts from the Square Sandbox Inventory API.
    Validates required configuration, sends a GET request to /v2/inventory/counts,
    and reports safe diagnostic information without exposing sensitive credentials.
    """
    env = settings.SQUARE_ENVIRONMENT or ""
    base_url = settings.SQUARE_BASE_URL or ""
    app_id = settings.SQUARE_APPLICATION_ID or ""
    access_token = settings.SQUARE_ACCESS_TOKEN or ""
    location_id = settings.SQUARE_LOCATION_ID or ""

    print("========================================")
    print("SupplySense AI - Square Inventory Test")
    print("========================================")
    print()
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
        print("Square Inventory API: FAILED")
        print(f"Error: Missing required configuration values: {', '.join(missing_fields)}")
        return False

    print("Calling Square Sandbox Inventory API...")
    print()

    endpoint = f"{base_url.rstrip('/')}/v2/inventory/counts"
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json"
    }
    params = {
        "location_ids": location_id
    }

    try:
        response = requests.get(endpoint, headers=headers, params=params, timeout=15)

        if response.status_code == 200:
            try:
                data = response.json()
            except Exception:
                print("Square Inventory API: FAILED")
                print("HTTP Status: 200")
                print("Error: Invalid JSON response received from API.")
                return False

            counts = data.get("counts", []) if isinstance(data, dict) else []
            num_records = len(counts)

            print("Square Inventory API: SUCCESS")
            print(f"HTTP Status: {response.status_code}")
            print(f"Inventory records received: {num_records}")
            print()

            if num_records > 0:
                print("Sample inventory record:")
                sample = {
                    "catalog_object_id": counts[0].get("catalog_object_id"),
                    "catalog_object_type": counts[0].get("catalog_object_type"),
                    "state": counts[0].get("state"),
                    "location_id": counts[0].get("location_id"),
                    "quantity": counts[0].get("quantity"),
                    "calculated_at": counts[0].get("calculated_at")
                }
                print(json.dumps(sample, indent=2))
            else:
                print("The Square Sandbox API connection works, but no inventory records currently exist.")

            return True

        else:
            print("Square Inventory API: FAILED")
            print(f"HTTP Status: {response.status_code}")

            status_desc = {
                401: "Unauthorized: Invalid or expired access token.",
                403: "Forbidden: Access token does not have permissions for the Inventory API.",
                404: "Not Found: The requested Inventory endpoint was not found.",
                429: "Rate Limit Exceeded: Too many requests to Square Sandbox API."
            }

            safe_error = status_desc.get(response.status_code)
            if response.status_code >= 500:
                safe_error = f"Server Error: Square Sandbox API returned internal server error ({response.status_code})."

            if not safe_error:
                try:
                    err_data = response.json()
                    if "errors" in err_data and isinstance(err_data["errors"], list):
                        messages = [
                            err.get("detail") or err.get("code") or err.get("category")
                            for err in err_data["errors"]
                            if isinstance(err, dict)
                        ]
                        safe_error = "; ".join(filter(None, messages))
                    elif "message" in err_data:
                        safe_error = str(err_data["message"])
                except Exception:
                    pass

            safe_error = safe_error or response.reason or "Non-200 HTTP response."
            print(f"Error: {safe_error}")
            return False

    except requests.exceptions.Timeout:
        print("Square Inventory API: FAILED")
        print("Error: Request timed out while connecting to Square Sandbox Inventory API.")
        return False
    except requests.exceptions.ConnectionError:
        print("Square Inventory API: FAILED")
        print("Error: Network or connection error: Failed to establish connection to Square Sandbox API.")
        return False
    except requests.exceptions.RequestException as e:
        print("Square Inventory API: FAILED")
        print(f"Error: Request exception occurred: {type(e).__name__}")
        return False


if __name__ == "__main__":
    success = test_square_inventory()
    sys.exit(0 if success else 1)
