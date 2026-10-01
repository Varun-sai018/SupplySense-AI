"""
Unit tests for Task 8.2: CDC Deduplication Schema and Constraints on dataset_events.
"""

import os
import sys
import unittest
import pymysql

# Ensure repository root is on sys.path
REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../'))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

from common.database import get_connection


class TestCDCDedupSchema(unittest.TestCase):

    def setUp(self):
        self.conn = get_connection()
        self.cursor = self.conn.cursor()
        # Clean up any test records created by this test
        self.cursor.execute("DELETE FROM dataset_events WHERE source_change_id LIKE 'test_binlog_%'")
        self.conn.commit()

    def tearDown(self):
        if self.conn:
            self.cursor.execute("DELETE FROM dataset_events WHERE source_change_id LIKE 'test_binlog_%'")
            self.conn.commit()
            self.conn.close()

    def test_schema_has_cdc_and_batch_columns(self):
        """1. Verify that dataset_events contains the new CDC metadata and batch columns."""
        self.cursor.execute("DESCRIBE dataset_events")
        columns = {row["Field"]: row for row in self.cursor.fetchall()}

        self.assertIn("source_file", columns)
        self.assertIn("source_pos", columns)
        self.assertIn("source_change_id", columns)
        self.assertIn("batch_id", columns)

        self.assertEqual(columns["source_file"]["Null"], "YES")
        self.assertEqual(columns["source_pos"]["Null"], "YES")
        self.assertEqual(columns["source_change_id"]["Null"], "YES")
        self.assertEqual(columns["batch_id"]["Null"], "YES")

    def test_insert_different_cdc_source_events(self):
        """2. Verify multiple distinct CDC source events can be inserted without issue."""
        self.cursor.execute("SELECT dataset_id FROM dataset_metadata LIMIT 1")
        dataset_id = self.cursor.fetchone()["dataset_id"]

        self.cursor.execute("""
            INSERT INTO dataset_events (
                dataset_id, dataset_name, event_type, dataset_version,
                source_file, source_pos, source_change_id, batch_id
            ) VALUES (%s, 'Orders', 'DATASET_UPDATED', 1001, 'mysql-bin.000001', 100, 'test_binlog_1', 'batch_1')
        """, (dataset_id,))

        self.cursor.execute("""
            INSERT INTO dataset_events (
                dataset_id, dataset_name, event_type, dataset_version,
                source_file, source_pos, source_change_id, batch_id
            ) VALUES (%s, 'Orders', 'DATASET_UPDATED', 1002, 'mysql-bin.000001', 200, 'test_binlog_2', 'batch_1')
        """, (dataset_id,))
        self.conn.commit()

        self.cursor.execute("SELECT COUNT(*) AS c FROM dataset_events WHERE source_change_id IN ('test_binlog_1', 'test_binlog_2')")
        self.assertEqual(self.cursor.fetchone()["c"], 2)

    def test_duplicate_cdc_source_event_raises_integrity_error(self):
        """3. Verify inserting the exact same source_change_id triggers duplicate key violation."""
        self.cursor.execute("SELECT dataset_id FROM dataset_metadata LIMIT 1")
        dataset_id = self.cursor.fetchone()["dataset_id"]

        self.cursor.execute("""
            INSERT INTO dataset_events (
                dataset_id, dataset_name, event_type, dataset_version,
                source_file, source_pos, source_change_id
            ) VALUES (%s, 'Orders', 'DATASET_UPDATED', 1001, 'mysql-bin.000001', 500, 'test_binlog_dup_1')
        """, (dataset_id,))
        self.conn.commit()

        # Attempt to insert identical source_change_id
        with self.assertRaises(pymysql.err.IntegrityError):
            self.cursor.execute("""
                INSERT INTO dataset_events (
                    dataset_id, dataset_name, event_type, dataset_version,
                    source_file, source_pos, source_change_id
                ) VALUES (%s, 'Orders', 'DATASET_UPDATED', 1002, 'mysql-bin.000001', 500, 'test_binlog_dup_1')
            """, (dataset_id,))
            self.conn.commit()

        self.conn.rollback()

    def test_multiple_null_source_change_id_allowed(self):
        """4. Verify legacy/non-CDC events with NULL source metadata do not collide."""
        self.cursor.execute("SELECT dataset_id FROM dataset_metadata LIMIT 1")
        dataset_id = self.cursor.fetchone()["dataset_id"]

        # Insert 2 non-CDC rows with NULL CDC coordinates
        self.cursor.execute("""
            INSERT INTO dataset_events (
                dataset_id, dataset_name, event_type, dataset_version,
                source_file, source_pos, source_change_id, batch_id
            ) VALUES (%s, 'Orders', 'DATASET_UPDATED', 2001, NULL, NULL, NULL, NULL)
        """, (dataset_id,))
        id1 = self.cursor.lastrowid

        self.cursor.execute("""
            INSERT INTO dataset_events (
                dataset_id, dataset_name, event_type, dataset_version,
                source_file, source_pos, source_change_id, batch_id
            ) VALUES (%s, 'Orders', 'DATASET_UPDATED', 2002, NULL, NULL, NULL, NULL)
        """, (dataset_id,))
        id2 = self.cursor.lastrowid
        self.conn.commit()

        self.assertNotEqual(id1, id2)

        # Cleanup
        self.cursor.execute("DELETE FROM dataset_events WHERE event_id IN (%s, %s)", (id1, id2))
        self.conn.commit()


if __name__ == '__main__':
    unittest.main()
