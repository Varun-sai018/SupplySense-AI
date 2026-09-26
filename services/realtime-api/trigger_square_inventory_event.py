import os
import sys
import uuid
from datetime import datetime, timezone
import requests

# Ensure repository root is in sys.path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../../')))

from config import settings


def trigger_sandbox_inventory_change():
    """
    Creates/updates a Sandbox catalog item and posts a physical count inventory adjustment.
    This triggers a real 'inventory.count.updated' webhook event from Square Sandbox
    to the configured ngrok notification URL.
    """
    base_url = (settings.SQUARE_BASE_URL or "https://connect.squareupsandbox.com").rstrip("/")
    access_token = settings.SQUARE_ACCESS_TOKEN
    location_id = settings.SQUARE_LOCATION_ID

    if not access_token or not location_id:
        print("Error: SQUARE_ACCESS_TOKEN and SQUARE_LOCATION_ID must be configured in .env")
        return False

    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json"
    }

    # Step 1: Create or find a catalog item variation in Sandbox
    print("Creating/updating Sandbox catalog item for test...")
    catalog_url = f"{base_url}/v2/catalog/object"
    item_idempotency = str(uuid.uuid4())
    catalog_payload = {
        "idempotency_key": item_idempotency,
        "object": {
            "type": "ITEM",
            "id": "#supplysense_test_item",
            "item_data": {
                "name": "SupplySense Realtime Test Item",
                "variations": [
                    {
                        "type": "ITEM_VARIATION",
                        "id": "#supplysense_test_var",
                        "item_variation_data": {
                            "name": "Standard Variation",
                            "pricing_type": "FIXED_PRICING",
                            "price_money": {
                                "amount": 2500,
                                "currency": "USD"
                            }
                        }
                    }
                ]
            }
        }
    }

    r_catalog = requests.post(catalog_url, headers=headers, json=catalog_payload, timeout=15)
    if r_catalog.status_code not in (200, 201):
        print(f"Failed to create catalog item: HTTP {r_catalog.status_code}")
        try:
            print("Error details:", r_catalog.json().get("errors"))
        except Exception:
            pass
        return False

    catalog_data = r_catalog.json()
    objects = catalog_data.get("catalog_object", {}).get("item_data", {}).get("variations", [])
    if not objects:
        print("Error: Could not retrieve variation ID from response.")
        return False

    variation_id = objects[0]["id"]
    print(f"Catalog Item Variation created: SUCCESS")

    # Step 2: Perform physical count inventory change to trigger inventory.count.updated webhook
    print("Submitting inventory physical count adjustment to trigger Square Sandbox webhook...")
    inventory_url = f"{base_url}/v2/inventory/changes/batch-create"
    inventory_idempotency = str(uuid.uuid4())
    occurred_at = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    inventory_payload = {
        "idempotency_key": inventory_idempotency,
        "changes": [
            {
                "type": "PHYSICAL_COUNT",
                "physical_count": {
                    "catalog_object_id": variation_id,
                    "state": "IN_STOCK",
                    "location_id": location_id,
                    "quantity": "75",
                    "occurred_at": occurred_at
                }
            }
        ]
    }

    r_inventory = requests.post(inventory_url, headers=headers, json=inventory_payload, timeout=15)
    if r_inventory.status_code in (200, 201):
        print("Inventory adjustment submitted successfully!")
        print("Square Sandbox will dispatch 'inventory.count.updated' webhook to ngrok URL.")
        return True
    else:
        print(f"Failed to submit inventory adjustment: HTTP {r_inventory.status_code}")
        try:
            print("Error details:", r_inventory.json().get("errors"))
        except Exception:
            pass
        return False


if __name__ == "__main__":
    success = trigger_sandbox_inventory_change()
    sys.exit(0 if success else 1)
