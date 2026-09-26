import os
import sys
import unittest
from unittest.mock import MagicMock

# Ensure repository root is in sys.path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../../')))

from common.database import get_connection
import importlib
square_adapter = importlib.import_module('services.realtime-api.square_adapter')


class TestSquarePersistenceIntegration(unittest.TestCase):
    def setUp(self):
        self.conn = get_connection()
        self.cursor = self.conn.cursor()
        self.test_event_ids = []
        
        # Ensure Inventory dataset exists in dataset_metadata
        self.cursor.execute(
            """
            INSERT INTO dataset_metadata (dataset_name, source_type, table_name, current_version, row_count, status)
            VALUES ('Inventory', 'SQUARE', 'square_inventory', 0, 0, 'WAITING')
            ON DUPLICATE KEY UPDATE source_type = 'SQUARE'
            """
        )
        self.conn.commit()

    def tearDown(self):
        if self.conn:
            # Clean up only test events generated during test execution
            if self.test_event_ids:
                format_strings = ','.join(['%s'] * len(self.test_event_ids))
                self.cursor.execute(
                    f"DELETE FROM dataset_events WHERE event_id IN ({format_strings})",
                    tuple(self.test_event_ids)
                )
            self.cursor.execute("DELETE FROM pipeline_executions WHERE triggering_event_id BETWEEN 9980 AND 9989")
            self.conn.commit()
            self.conn.close()


    def test_persist_inventory_event(self):
        """Test persisting a normalized Inventory event to MySQL dataset_events and dataset_metadata."""
        sample_payload = {
            "merchant_id": "ML1234567",
            "type": "inventory.count.updated",
            "event_id": "evt_integration_1001",
            "created_at": "2026-09-24T12:00:00Z",
            "data": {
                "type": "inventory_count",
                "id": "cnt_1001",
                "object": {
                    "inventory_counts": [
                        {
                            "catalog_object_id": "ITEM_TEST_1",
                            "catalog_object_type": "ITEM_VARIATION",
                            "state": "IN_STOCK",
                            "location_id": "LM2QZQNXN9P7F",
                            "quantity": "25",
                            "calculated_at": "2026-09-24T12:00:00Z"
                        }
                    ]
                }
            }
        }

        self.cursor.execute("SELECT current_version FROM dataset_metadata WHERE dataset_name = 'Inventory'")
        meta_init = self.cursor.fetchone()
        initial_version = meta_init["current_version"] if meta_init else 0

        normalized = square_adapter.normalize_square_event(sample_payload)
        self.assertTrue(normalized["supported"])

        result = square_adapter.persist_and_publish_event(normalized, connection=self.conn)
        self.test_event_ids.append(result["event_id"])
        self.assertEqual(result["status"], "PROCESSED")
        self.assertEqual(result["dataset_name"], "Inventory")
        self.assertGreater(result["event_id"], 0)

        # Verify dataset_metadata update
        self.cursor.execute("SELECT current_version, row_count, status FROM dataset_metadata WHERE dataset_name = 'Inventory'")
        meta = self.cursor.fetchone()
        self.assertEqual(meta["current_version"], initial_version + 1)
        self.assertEqual(meta["status"], "READY")

        # Verify dataset_events record
        self.cursor.execute("SELECT event_type, dataset_version, rows_changed FROM dataset_events WHERE event_id = %s", (result["event_id"],))
        evt = self.cursor.fetchone()
        self.assertEqual(evt["event_type"], "DATA_UPDATED")
        self.assertEqual(evt["dataset_version"], initial_version + 1)
        self.assertEqual(evt["rows_changed"], 1)


    def test_kafka_publication_success(self):
        """Test publishing normalized Inventory event to Kafka."""
        mock_producer = MagicMock()
        mock_future = MagicMock()
        mock_future.get.return_value = MagicMock(partition=0, offset=1)
        mock_producer.send.return_value = mock_future

        sample_payload = {
            "type": "inventory.count.updated",
            "event_id": "evt_kafka_1",
            "data": {"object": {"inventory_counts": [{"id": "k1"}]}}
        }
        normalized = square_adapter.normalize_square_event(sample_payload)

        res = square_adapter.persist_and_publish_event(
            normalized,
            connection=self.conn,
            kafka_producer=mock_producer
        )
        self.test_event_ids.append(res["event_id"])


        self.assertEqual(res["status"], "PROCESSED")
        self.assertTrue(res["published_to_kafka"])
        mock_producer.send.assert_called_once()
        args, kwargs = mock_producer.send.call_args
        self.assertEqual(args[0], "dataset-events")
        self.assertEqual(kwargs["value"]["dataset_name"], "Inventory")

    def test_dependency_engine_idempotency_for_inventory_event(self):
        """Test that duplicate event handling in Dependency Engine is idempotent."""
        import importlib
        dep_engine = importlib.import_module('services.dependency-engine.main')

        event_id = 9988
        pipeline_name = 'Demand Forecast Pipeline'

        # Insert a decision record first, then execution record to simulate prior trigger
        self.cursor.execute(
            """
            INSERT INTO pipeline_decisions (pipeline_name, condition_type, decision, reason)
            VALUES (%s, 'ALL', 'TRIGGER', 'Test trigger')
            """,
            (pipeline_name,)
        )
        decision_id = self.cursor.lastrowid

        self.cursor.execute(
            """
            INSERT INTO pipeline_executions (pipeline_name, decision_id, triggering_event_id, status)
            VALUES (%s, %s, %s, 'COMPLETED')
            """,
            (pipeline_name, decision_id, event_id)
        )
        self.conn.commit()


        event_data = {
            "event_id": event_id,
            "dataset_id": 5,
            "dataset_name": "Inventory",
            "event_type": "DATA_UPDATED",
            "dataset_version": 1,
            "rows_changed": 1
        }

        # Processing an event that was already executed must return SKIPPED
        res_decision, res_reason = dep_engine.process_event(event_data, self.conn)
        self.assertEqual(res_decision, "SKIPPED")
        self.assertEqual(res_reason, "Idempotency: already triggered")


if __name__ == "__main__":
    unittest.main()


