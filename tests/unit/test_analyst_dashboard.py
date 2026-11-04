"""
Unit tests for the Analyst Dashboard service (Module 2 — Phase F5 Step 3).

Tests cover:
- Financial unit economics correctness and UNAVAILABLE field strictness
- Operational churn inventory and backlog metrics
- Customer experience conversion funnel and payment health
- Data provenance catalog completeness
- Full consolidated payload structure from /api/observability/analyst/metrics
"""

import unittest
from unittest.mock import patch, MagicMock
import os
import sys

REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../'))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)


# ---------------------------------------------------------------------------
# Helpers: build mock cursor row lists
# ---------------------------------------------------------------------------

def _row(**kwargs):
    """Returns a dict that behaves like a MySQLdb DictCursor row."""
    return kwargs


def _make_cursor(fetchone_rows=None, fetchall_rows=None):
    """Creates a MagicMock cursor returning specified responses sequentially."""
    cursor = MagicMock()
    fetchone_seq = list(fetchone_rows or [])
    fetchall_seq = list(fetchall_rows or [])
    cursor.fetchone.side_effect = fetchone_seq
    cursor.fetchall.side_effect = fetchall_seq
    return cursor


def _make_conn(cursor):
    conn = MagicMock()
    conn.cursor.return_value = cursor
    return conn


# ---------------------------------------------------------------------------
# Tests for get_financial_unit_economics
# ---------------------------------------------------------------------------

class TestFinancialUnitEconomics(unittest.TestCase):

    def _call(self, cursor_fetchones, cursor_fetchalls):
        from services.observability.analyst_service import get_financial_unit_economics
        cursor = MagicMock()
        cursor.fetchone.side_effect = list(cursor_fetchones)
        cursor.fetchall.side_effect = list(cursor_fetchalls)
        conn = MagicMock()
        conn.cursor.return_value = cursor
        return get_financial_unit_economics(connection=conn)

    def test_gmv_aov_calculated_correctly(self):
        """GMV and AOV must match the payment and order counts."""
        result = self._call(
            cursor_fetchones=[
                _row(gmv=16008872.12),     # payment sum
                _row(total_orders=99441),  # order count
                _row(delivered_orders=96478, canceled_orders=625),  # status counts
            ],
            cursor_fetchalls=[
                [  # monthly trend
                    _row(month='2016-09', order_count=4),
                    _row(month='2018-08', order_count=813),
                ]
            ]
        )
        self.assertAlmostEqual(result['gmv'], 16008872.12, places=1)
        self.assertAlmostEqual(result['aov'], round(16008872.12 / 99441, 2), places=1)
        self.assertEqual(result['total_orders'], 99441)
        self.assertEqual(result['delivered_orders'], 96478)
        self.assertEqual(result['canceled_orders'], 625)

    def test_net_profit_is_strictly_none(self):
        """Net profit must be None with a non-empty reason — never reported as zero."""
        result = self._call(
            cursor_fetchones=[
                _row(gmv=1000.0),
                _row(total_orders=10),
                _row(delivered_orders=9, canceled_orders=1),
            ],
            cursor_fetchalls=[[]]
        )
        self.assertIsNone(result['net_profit'])
        self.assertEqual(result['net_profit_status'], 'UNAVAILABLE')
        self.assertTrue(len(result.get('net_profit_reason', '')) > 0)

    def test_profit_margin_is_strictly_none(self):
        """Profit margin must be None — not zero."""
        result = self._call(
            cursor_fetchones=[
                _row(gmv=5000.0),
                _row(total_orders=50),
                _row(delivered_orders=48, canceled_orders=2),
            ],
            cursor_fetchalls=[[]]
        )
        self.assertIsNone(result['profit_margin'])
        self.assertEqual(result['profit_margin_status'], 'UNAVAILABLE')

    def test_refund_and_chargeback_none(self):
        """Refund and chargeback rates must be None — not zero."""
        result = self._call(
            cursor_fetchones=[
                _row(gmv=2000.0),
                _row(total_orders=20),
                _row(delivered_orders=19, canceled_orders=1),
            ],
            cursor_fetchalls=[[]]
        )
        self.assertIsNone(result['refund_rate'])
        self.assertEqual(result['refund_status'], 'UNAVAILABLE')
        self.assertIsNone(result['chargeback_rate'])
        self.assertEqual(result['chargeback_status'], 'UNAVAILABLE')

    def test_cogs_is_strictly_none(self):
        """COGS must be None — not zero."""
        result = self._call(
            cursor_fetchones=[
                _row(gmv=500.0),
                _row(total_orders=5),
                _row(delivered_orders=5, canceled_orders=0),
            ],
            cursor_fetchalls=[[]]
        )
        self.assertIsNone(result['cogs'])
        self.assertEqual(result['cogs_status'], 'UNAVAILABLE')

    def test_monthly_trend_capped_at_24(self):
        """Monthly trend must contain at most 24 entries."""
        trend_data = [_row(month=f'20{y:02d}-{m:02d}', order_count=100)
                      for y in range(16, 22) for m in range(1, 13)]
        result = self._call(
            cursor_fetchones=[
                _row(gmv=1000.0),
                _row(total_orders=10),
                _row(delivered_orders=10, canceled_orders=0),
            ],
            cursor_fetchalls=[trend_data]
        )
        self.assertLessEqual(len(result['monthly_order_trend']), 24)

    def test_provenance_classification_is_historical(self):
        """Financial metrics must be classified HISTORICAL."""
        result = self._call(
            cursor_fetchones=[
                _row(gmv=0.0),
                _row(total_orders=0),
                _row(delivered_orders=0, canceled_orders=0),
            ],
            cursor_fetchalls=[[]]
        )
        self.assertEqual(result['provenance']['classification'], 'HISTORICAL')

    def test_aov_zero_division_safe(self):
        """AOV must be 0.0 (not error) when total_orders is 0."""
        result = self._call(
            cursor_fetchones=[
                _row(gmv=0.0),
                _row(total_orders=0),
                _row(delivered_orders=0, canceled_orders=0),
            ],
            cursor_fetchalls=[[]]
        )
        self.assertEqual(result['aov'], 0.0)


# ---------------------------------------------------------------------------
# Tests for get_operational_churn
# ---------------------------------------------------------------------------

class TestOperationalChurn(unittest.TestCase):

    def _call(self, backlog_row, reorders, inv_row=None, supp_row=None):
        from services.observability.analyst_service import get_operational_churn
        if inv_row is None:
            inv_row = _row(total_skus=106, total_on_hand=4235, total_reserved=0, total_available=4235, total_safe_threshold=640, low_stock_skus=3)
        if supp_row is None:
            supp_row = _row(total_suppliers=7, online_count=5, degraded_count=1, offline_count=1)

        cursor = MagicMock()
        cursor.fetchone.side_effect = [backlog_row, inv_row, supp_row]
        cursor.fetchall.return_value = reorders
        conn = MagicMock()
        conn.cursor.return_value = cursor
        return get_operational_churn(connection=conn)

    def test_fulfillment_backlog_from_db(self):
        """Backlog count must match what the database returns."""
        result = self._call(
            backlog_row=_row(backlog_count=622, processing_count=301, invoiced_count=314, approved_count=2, created_count=5),
            reorders=[]
        )
        self.assertEqual(result['fulfillment_queue_backlog'], 622)
        self.assertEqual(result['backlog_breakdown']['processing'], 301)
        self.assertEqual(result['backlog_breakdown']['invoiced'], 314)

    def test_inventory_above_threshold_is_optimal(self):
        """When inventory > safe threshold, status must be OPTIMAL."""
        result = self._call(
            backlog_row=_row(backlog_count=0, processing_count=0, invoiced_count=0, approved_count=0, created_count=0),
            reorders=[]
        )
        self.assertEqual(result['inventory_status'], 'OPTIMAL')
        self.assertEqual(result['stockout_risk'], 'UNAVAILABLE')

    def test_depletion_rate_and_stockout_unavailable_without_history(self):
        """Depletion rate and stockout hours must be null / unavailable without time-series history."""
        result = self._call(
            backlog_row=_row(backlog_count=0, processing_count=0, invoiced_count=0, approved_count=0, created_count=0),
            reorders=[]
        )
        self.assertIsNone(result['depletion_rate_per_hour'])
        self.assertIsNone(result['estimated_hours_to_stockout'])
        self.assertIn("Unavailable", result['depletion_rate_label'])

    def test_safe_threshold_from_db(self):
        result = self._call(
            backlog_row=_row(backlog_count=0, processing_count=0, invoiced_count=0, approved_count=0, created_count=0),
            reorders=[],
            inv_row=_row(total_skus=106, total_on_hand=4235, total_reserved=0, total_available=4235, total_safe_threshold=640, total_target_capacity=10340, low_stock_skus=3)
        )
        self.assertEqual(result['safe_threshold'], 640)
        self.assertEqual(result['current_inventory_level'], 4235)
        self.assertEqual(result['total_target_capacity'], 10340)
        self.assertEqual(result['reorder_point'], 2337)
        self.assertEqual(result['low_stock_skus'], 3)

        # Check exact expected percentage calculations
        inv_pct = round((result['current_inventory_level'] / result['total_target_capacity']) * 100, 2)
        safe_pct = round((result['safe_threshold'] / result['total_target_capacity']) * 100, 2)
        reorder_pct = round((result['reorder_point'] / result['total_target_capacity']) * 100, 2)
        self.assertEqual(inv_pct, 40.96)
        self.assertEqual(safe_pct, 6.19)
        self.assertEqual(reorder_pct, 22.60)

    def test_zero_capacity_returns_zero(self):
        result = self._call(
            backlog_row=_row(backlog_count=0, processing_count=0, invoiced_count=0, approved_count=0, created_count=0),
            reorders=[],
            inv_row=_row(total_skus=0, total_on_hand=0, total_reserved=0, total_available=0, total_safe_threshold=0, total_target_capacity=0, low_stock_skus=0)
        )
        self.assertEqual(result['total_target_capacity'], 0)

    def test_reorders_list_populated(self):
        from datetime import datetime
        reorders = [_row(
            action_id=1,
            rule_id='rule_inventory_reorder',
            idempotency_key='reorder-1',
            status='CONFIRMED',
            retry_count=0,
            created_at=datetime(2024, 1, 1, 10, 0, 0),
            resolved_at=datetime(2024, 1, 1, 10, 5, 0)
        )]
        result = self._call(
            backlog_row=_row(backlog_count=0, processing_count=0, invoiced_count=0, approved_count=0, created_count=0),
            reorders=reorders
        )
        self.assertEqual(len(result['recent_reorders']), 1)
        self.assertEqual(result['recent_reorders'][0]['status'], 'CONFIRMED')


# ---------------------------------------------------------------------------
# Tests for get_customer_experience_leakage
# ---------------------------------------------------------------------------

class TestCustomerExperienceLeakage(unittest.TestCase):

    def _call(self, alert_row=None):
        from services.observability.analyst_service import get_customer_experience_leakage
        cursor = MagicMock()
        cursor.fetchone.return_value = alert_row
        conn = MagicMock()
        conn.cursor.return_value = cursor
        return get_customer_experience_leakage(connection=conn)

    def test_funnel_has_five_stages(self):
        result = self._call()
        self.assertEqual(len(result['funnel']), 5)

    def test_funnel_first_stage_is_sessions(self):
        result = self._call()
        self.assertEqual(result['funnel'][0]['stage'], 'Sessions')

    def test_overall_conversion_rate_present(self):
        result = self._call()
        self.assertGreater(result['overall_conversion_rate'], 0)

    def test_payment_success_rate_above_threshold_is_healthy(self):
        result = self._call()
        self.assertEqual(result['payment_status'], 'HEALTHY')

    def test_active_payment_alert_none_when_no_db_row(self):
        result = self._call(alert_row=None)
        self.assertIsNone(result['active_payment_alert'])

    def test_active_payment_alert_populated_when_row_exists(self):
        from datetime import datetime
        alert = _row(action_id=99, payload='{}', status='OPEN', created_at=datetime(2024, 1, 1))
        result = self._call(alert_row=alert)
        self.assertIsNotNone(result['active_payment_alert'])
        self.assertEqual(result['active_payment_alert']['action_id'], 99)

    def test_hourly_profile_has_twelve_entries(self):
        result = self._call()
        self.assertEqual(len(result['hourly_abandonment_profile']), 12)

    def test_cart_dropoff_threshold_is_25(self):
        result = self._call()
        self.assertEqual(result['cart_dropoff_threshold'], 25.0)

    def test_cart_dropoff_rate_below_threshold_is_healthy(self):
        result = self._call()
        if result['rolling_cart_dropoff_rate'] < result['cart_dropoff_threshold']:
            self.assertEqual(result['cart_dropoff_status'], 'HEALTHY')


# ---------------------------------------------------------------------------
# Tests for get_data_provenance_catalog
# ---------------------------------------------------------------------------

class TestDataProvenanceCatalog(unittest.TestCase):

    def setUp(self):
        from services.observability.analyst_service import get_data_provenance_catalog
        self.catalog = get_data_provenance_catalog()

    def test_catalog_is_non_empty(self):
        self.assertGreater(len(self.catalog), 0)

    def test_every_entry_has_required_fields(self):
        required = {'category', 'metric', 'classification', 'source', 'description'}
        for entry in self.catalog:
            self.assertEqual(required, required & set(entry.keys()), f"Missing fields in: {entry}")

    def test_unavailable_metrics_are_present(self):
        unavailable = [e for e in self.catalog if e['classification'] == 'UNAVAILABLE']
        self.assertGreater(len(unavailable), 0, "At least one UNAVAILABLE metric must appear in catalog")

    def test_historical_metrics_are_present(self):
        historical = [e for e in self.catalog if e['classification'] == 'HISTORICAL']
        self.assertGreater(len(historical), 0)

    def test_net_profit_margin_is_unavailable(self):
        profit_entry = next((e for e in self.catalog if 'Profit' in e['metric'] and 'Margin' in e['metric']), None)
        self.assertIsNotNone(profit_entry)
        self.assertEqual(profit_entry['classification'], 'UNAVAILABLE')

    def test_gmv_is_historical(self):
        gmv_entry = next((e for e in self.catalog if 'GMV' in e['metric'] or 'Gross Merchandise' in e['metric']), None)
        self.assertIsNotNone(gmv_entry)
        self.assertEqual(gmv_entry['classification'], 'HISTORICAL')

    def test_live_inventory_provenance_updated(self):
        inv_entry = next((e for e in self.catalog if 'Inventory' in e['metric']), None)
        self.assertIsNotNone(inv_entry)
        self.assertIn("operational_inventory", inv_entry['source'])
        self.assertIn("offline", inv_entry['source'].lower())


# ---------------------------------------------------------------------------
# Integration-style test for get_full_analyst_dashboard_metrics
# ---------------------------------------------------------------------------

class TestFullAnalystDashboardMetrics(unittest.TestCase):

    def test_consolidated_payload_structure(self):
        """Full payload must have status, timestamp, financial, operational, customer_experience, provenance_catalog."""
        from services.observability.analyst_service import (
            get_financial_unit_economics,
            get_operational_churn,
            get_customer_experience_leakage,
            get_data_provenance_catalog,
        )

        financial_mock = {
            'gmv': 16008872.12, 'aov': 154.10,
            'net_profit': None, 'net_profit_status': 'UNAVAILABLE',
            'provenance': {'classification': 'HISTORICAL', 'source': 'olist_order_payments'}
        }
        operational_mock = {
            'current_inventory_level': 153, 'fulfillment_queue_backlog': 622,
            'provenance': {}
        }
        customer_mock = {
            'overall_conversion_rate': 14.2, 'funnel': [],
            'provenance': {}
        }
        catalog_mock = [{'category': 'Financial', 'metric': 'GMV', 'classification': 'HISTORICAL', 'source': 'olist', 'description': 'test'}]

        with patch('services.observability.analyst_service.get_financial_unit_economics', return_value=financial_mock), \
             patch('services.observability.analyst_service.get_operational_churn', return_value=operational_mock), \
             patch('services.observability.analyst_service.get_customer_experience_leakage', return_value=customer_mock), \
             patch('services.observability.analyst_service.get_data_provenance_catalog', return_value=catalog_mock):

            # Call with a fake connection that doesn't get closed
            conn = MagicMock()
            from services.observability.analyst_service import get_full_analyst_dashboard_metrics
            result = get_full_analyst_dashboard_metrics(connection=conn)

        self.assertEqual(result['status'], 'success')
        self.assertIn('timestamp', result)
        self.assertIn('financial', result)
        self.assertIn('operational', result)
        self.assertIn('customer_experience', result)
        self.assertIn('provenance_catalog', result)
        self.assertIsNone(result['financial']['net_profit'])
        self.assertEqual(result['financial']['net_profit_status'], 'UNAVAILABLE')

    def test_financial_gmv_value(self):
        """Financial GMV must be present in payload."""
        with patch('services.observability.analyst_service.get_financial_unit_economics',
                   return_value={'gmv': 16008872.12, 'net_profit': None, 'net_profit_status': 'UNAVAILABLE', 'provenance': {}}), \
             patch('services.observability.analyst_service.get_operational_churn', return_value={'provenance': {}}), \
             patch('services.observability.analyst_service.get_customer_experience_leakage', return_value={'provenance': {}}), \
             patch('services.observability.analyst_service.get_data_provenance_catalog', return_value=[]):
            conn = MagicMock()
            from services.observability.analyst_service import get_full_analyst_dashboard_metrics
            result = get_full_analyst_dashboard_metrics(connection=conn)
        self.assertAlmostEqual(result['financial']['gmv'], 16008872.12, places=1)


# ---------------------------------------------------------------------------
# FastAPI endpoint smoke test
# ---------------------------------------------------------------------------

class TestAnalystEndpoint(unittest.TestCase):

    def _get_client(self):
        from fastapi.testclient import TestClient
        from services.observability.main import app
        return TestClient(app)

    def test_analyst_metrics_returns_200(self):
        """GET /api/observability/analyst/metrics must return 200 with correct structure."""
        mock_payload = {
            'status': 'success',
            'timestamp': '2024-01-01T00:00:00Z',
            'financial': {
                'gmv': 16008872.12,
                'aov': 154.10,
                'net_profit': None,
                'net_profit_status': 'UNAVAILABLE',
                'provenance': {}
            },
            'operational': {'provenance': {}},
            'customer_experience': {'provenance': {}},
            'provenance_catalog': []
        }
        with patch('services.observability.analyst_service.get_full_analyst_dashboard_metrics', return_value=mock_payload):
            client = self._get_client()
            response = client.get('/api/observability/analyst/metrics')
        self.assertEqual(response.status_code, 200)
        data = response.json()
        self.assertIn('financial', data)
        self.assertIn('operational', data)
        self.assertIn('customer_experience', data)
        self.assertIn('provenance_catalog', data)

    def test_analyst_metrics_net_profit_none_in_response(self):
        """Endpoint must return null for net_profit — never 0."""
        mock_payload = {
            'status': 'success',
            'timestamp': '2024-01-01T00:00:00Z',
            'financial': {
                'gmv': 0.0,
                'net_profit': None,
                'net_profit_status': 'UNAVAILABLE',
                'net_profit_reason': 'COGS unavailable',
                'provenance': {}
            },
            'operational': {'provenance': {}},
            'customer_experience': {'provenance': {}},
            'provenance_catalog': []
        }
        with patch('services.observability.analyst_service.get_full_analyst_dashboard_metrics', return_value=mock_payload):
            client = self._get_client()
            response = client.get('/api/observability/analyst/metrics')
        data = response.json()
        self.assertIsNone(data['financial']['net_profit'])


if __name__ == '__main__':
    unittest.main()
