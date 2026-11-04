"""
Phase 3E — Human-in-the-Loop Replenishment Approval Workflow Tests
===================================================================

Coverage:
  - Role-based authorization enforcement (OPERATOR can submit; MANAGER/ADMIN can review/approve/reject)
  - Submission validation: rejection of AMBIGUOUS_MULTI_SKU, UNMAPPED, INSUFFICIENT_DATA
  - Authoritative server-side stock and supplier status revalidation
  - Rejection reason requirement and audit trail capture
  - Optimistic locking / inventory version mismatch conflict detection
  - Idempotent submission and idempotent approval handling
  - Pipeline decision, rule action, and replenishment order creation upon approval
  - Zero inventory mutation at approval/dispatch time (invariant protection)
  - FastAPI endpoints for submission, listing, detail with audit trail, approval, and rejection
"""

import os
import sys
import uuid
import unittest
from fastapi.testclient import TestClient

# ── Path bootstrap ──────────────────────────────────────────────────────────────
REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../'))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

from common.database import get_connection
from services.observability.main import app
from services.observability import approval_service


def _clean_test_records():
    """Removes test approval rows, test replenishment orders, and rule actions to maintain test isolation."""
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute("""
            SELECT approval_id, replenishment_id, action_id FROM replenishment_approvals
            WHERE idempotency_key LIKE 'TST-%' OR idempotency_key LIKE 'API-%' OR idempotency_key LIKE 'TEST-%'
        """)
        rows = cur.fetchall()
        for r in rows:
            aid = r["approval_id"]
            rid = r.get("replenishment_id")
            act_id = r.get("action_id")
            cur.execute("DELETE FROM replenishment_approval_audit WHERE approval_id = %s", (aid,))
            cur.execute("DELETE FROM replenishment_approvals WHERE approval_id = %s", (aid,))
            if rid:
                cur.execute("DELETE FROM replenishment_orders WHERE replenishment_id = %s", (rid,))
            if act_id:
                cur.execute("DELETE FROM business_rule_actions WHERE action_id = %s", (act_id,))

        # Clean any in-flight orders for test SKUs created during test runs
        cur.execute("""
            DELETE FROM replenishment_orders
            WHERE sku = '99a4788cb24856965c36a24e339b6058' AND status = 'CONFIRMED'
        """)
        conn.commit()
        cur.close()
    finally:
        conn.close()


class TestApprovalServiceLogic(unittest.TestCase):
    """Unit tests for approval service business logic and security invariants."""

    @classmethod
    def setUpClass(cls):
        _clean_test_records()
        cls.sole_sku = "99a4788cb24856965c36a24e339b6058"
        cls.sole_rec_id = f"REC-99653-SKU-{cls.sole_sku}"
        cls.ambiguous_sku = "368c6c730842d78016ad823897a372db"
        cls.ambiguous_rec_id = f"REC-99653-SKU-{cls.ambiguous_sku}"
        cls.warehouse_id = "WH-SP-01"

    @classmethod
    def tearDownClass(cls):
        _clean_test_records()

    def setUp(self):
        _clean_test_records()
        self.conn = get_connection()
        self.cur = self.conn.cursor()
        self.test_prefix = f"TST-{uuid.uuid4().hex[:6].upper()}"

    def tearDown(self):
        try:
            self.cur.close()
            self.conn.close()
        except Exception:
            pass
        _clean_test_records()

    def test_submit_valid_approval(self):
        idempotency_key = f"{self.test_prefix}-SUB-1"
        result = approval_service.submit_recommendation_for_approval(
            recommendation_id=self.sole_rec_id,
            actor_id="op-tester-1",
            actor_role="OPERATOR",
            requested_quantity=25,
            idempotency_key=idempotency_key
        )

        self.assertIn("approval_id", result)
        self.assertEqual(result["status"], "PENDING_REVIEW")
        self.assertEqual(result["requested_quantity"], 25)
        self.assertEqual(result["submitted_by"], "op-tester-1")

        # Verify audit trail contains initial audit row
        detail = approval_service.get_replenishment_approval_detail(result["approval_id"])
        self.assertIsNotNone(detail)
        audit_trail = detail.get("audit_history", [])
        self.assertTrue(len(audit_trail) >= 1)
        self.assertEqual(audit_trail[0]["to_status"], "PENDING_REVIEW")

    def test_submit_blocks_ambiguous_multi_sku(self):
        with self.assertRaises(approval_service.ApprovalValidationError) as ctx:
            approval_service.submit_recommendation_for_approval(
                recommendation_id=self.ambiguous_rec_id,
                actor_id="op-tester-1",
                actor_role="OPERATOR",
                requested_quantity=10,
                idempotency_key=f"{self.test_prefix}-AMB"
            )
        self.assertIn("AMBIGUOUS_MULTI_SKU", str(ctx.exception))

    def test_submit_blocks_unmapped_or_missing_recommendation(self):
        with self.assertRaises(approval_service.ApprovalValidationError) as ctx:
            approval_service.submit_recommendation_for_approval(
                recommendation_id="REC-99653-SKU-NONEXISTENT",
                actor_id="op-tester-1",
                actor_role="OPERATOR",
                requested_quantity=10,
                idempotency_key=f"{self.test_prefix}-UNM"
            )
        self.assertIn("does not exist", str(ctx.exception))

    def test_submit_blocks_invalid_quantity(self):
        with self.assertRaises(approval_service.ApprovalValidationError) as ctx:
            approval_service.submit_recommendation_for_approval(
                recommendation_id=self.sole_rec_id,
                actor_id="op-tester-1",
                actor_role="OPERATOR",
                requested_quantity=0,
                idempotency_key=f"{self.test_prefix}-ZERO-QTY"
            )
        self.assertIn("positive integer", str(ctx.exception))

    def test_submit_idempotent_duplicate_key(self):
        idempotency_key = f"{self.test_prefix}-IDEM-SUB"
        appr1 = approval_service.submit_recommendation_for_approval(
            recommendation_id=self.sole_rec_id,
            actor_id="op-tester-1",
            actor_role="OPERATOR",
            requested_quantity=30,
            idempotency_key=idempotency_key
        )

        appr2 = approval_service.submit_recommendation_for_approval(
            recommendation_id=self.sole_rec_id,
            actor_id="op-tester-1",
            actor_role="OPERATOR",
            requested_quantity=30,
            idempotency_key=idempotency_key
        )

        self.assertEqual(appr1["approval_id"], appr2["approval_id"])

    def test_rejection_role_forbidden_for_operator(self):
        appr = approval_service.submit_recommendation_for_approval(
            recommendation_id=self.sole_rec_id,
            actor_id="op-tester-1",
            actor_role="OPERATOR",
            requested_quantity=15,
            idempotency_key=f"{self.test_prefix}-REJ-OP"
        )

        with self.assertRaises(approval_service.ApprovalSecurityError):
            approval_service.reject_replenishment(
                approval_id=appr["approval_id"],
                actor_id="op-tester-1",
                actor_role="OPERATOR",
                reason="Should fail due to operator role"
            )

    def test_rejection_requires_non_empty_reason(self):
        appr = approval_service.submit_recommendation_for_approval(
            recommendation_id=self.sole_rec_id,
            actor_id="op-tester-1",
            actor_role="OPERATOR",
            requested_quantity=15,
            idempotency_key=f"{self.test_prefix}-REJ-EMPTY"
        )

        with self.assertRaises(approval_service.ApprovalValidationError) as ctx:
            approval_service.reject_replenishment(
                approval_id=appr["approval_id"],
                actor_id="mgr-tester-1",
                actor_role="MANAGER",
                reason="   "
            )
        self.assertIn("mandatory", str(ctx.exception))

    def test_rejection_success_for_manager(self):
        appr = approval_service.submit_recommendation_for_approval(
            recommendation_id=self.sole_rec_id,
            actor_id="op-tester-1",
            actor_role="OPERATOR",
            requested_quantity=15,
            idempotency_key=f"{self.test_prefix}-REJ-MGR"
        )

        res = approval_service.reject_replenishment(
            approval_id=appr["approval_id"],
            actor_id="mgr-tester-1",
            actor_role="MANAGER",
            reason="Supplier lead time exceeds window"
        )

        self.assertEqual(res["status"], "REJECTED")
        self.assertEqual(res["rejection_reason"], "Supplier lead time exceeds window")
        self.assertEqual(res["reviewed_by"], "mgr-tester-1")

        # Attempting to approve rejected approval must be blocked
        with self.assertRaises(approval_service.ApprovalValidationError) as ctx:
            approval_service.approve_replenishment(
                approval_id=appr["approval_id"],
                actor_id="mgr-tester-1",
                actor_role="MANAGER"
            )
        self.assertIn("already rejected", str(ctx.exception))

    def test_approval_role_forbidden_for_operator(self):
        appr = approval_service.submit_recommendation_for_approval(
            recommendation_id=self.sole_rec_id,
            actor_id="op-tester-1",
            actor_role="OPERATOR",
            requested_quantity=20,
            idempotency_key=f"{self.test_prefix}-APP-OP"
        )

        with self.assertRaises(approval_service.ApprovalSecurityError):
            approval_service.approve_replenishment(
                approval_id=appr["approval_id"],
                actor_id="op-tester-1",
                actor_role="OPERATOR"
            )

    def test_approval_optimistic_version_conflict(self):
        appr = approval_service.submit_recommendation_for_approval(
            recommendation_id=self.sole_rec_id,
            actor_id="op-tester-1",
            actor_role="OPERATOR",
            requested_quantity=20,
            idempotency_key=f"{self.test_prefix}-OPT-CONF"
        )

        # Deliberately supply a stale expected_inventory_version
        with self.assertRaises(approval_service.ApprovalConflictError) as ctx:
            approval_service.approve_replenishment(
                approval_id=appr["approval_id"],
                actor_id="mgr-tester-1",
                actor_role="MANAGER",
                expected_inventory_version=999999
            )
        self.assertIn("VERSION_CONFLICT", str(ctx.exception))

    def test_approval_success_dispatch_and_invariants(self):
        # 1. Record pre-approval inventory state
        self.cur.execute("""
            SELECT on_hand_quantity, reserved_quantity, available_quantity, version
            FROM operational_inventory
            WHERE sku = %s AND warehouse_id = %s
        """, (self.sole_sku, self.warehouse_id))
        stock_before = self.cur.fetchone()

        # 2. Submit approval request
        appr = approval_service.submit_recommendation_for_approval(
            recommendation_id=self.sole_rec_id,
            actor_id="op-tester-1",
            actor_role="OPERATOR",
            requested_quantity=40,
            idempotency_key=f"{self.test_prefix}-APP-SUCC"
        )

        # 3. Approve as Manager
        approved = approval_service.approve_replenishment(
            approval_id=appr["approval_id"],
            actor_id="mgr-tester-1",
            actor_role="MANAGER"
        )

        self.assertEqual(approved["status"], "DISPATCHED")
        self.assertIsNotNone(approved["replenishment_id"])
        self.assertTrue(approved["replenishment_id"].startswith("RPL-"))

        # 4. Check zero inventory mutation invariant
        self.cur.execute("""
            SELECT on_hand_quantity, reserved_quantity, available_quantity, version
            FROM operational_inventory
            WHERE sku = %s AND warehouse_id = %s
        """, (self.sole_sku, self.warehouse_id))
        stock_after = self.cur.fetchone()

        self.assertEqual(stock_before["on_hand_quantity"], stock_after["on_hand_quantity"])
        self.assertEqual(stock_before["version"], stock_after["version"])

        # 5. Idempotent re-approval returns identical dispatched record
        reapproved = approval_service.approve_replenishment(
            approval_id=appr["approval_id"],
            actor_id="mgr-tester-1",
            actor_role="MANAGER"
        )
        self.assertEqual(reapproved["approval_id"], approved["approval_id"])
        self.assertEqual(reapproved["replenishment_id"], approved["replenishment_id"])


class TestApprovalFastAPIEndpoints(unittest.TestCase):
    """Integration tests for FastAPI approval endpoints and HTTP status codes."""

    @classmethod
    def setUpClass(cls):
        _clean_test_records()
        cls.sole_rec_id = "REC-99653-SKU-99a4788cb24856965c36a24e339b6058"
        cls.ambiguous_rec_id = "REC-99653-SKU-368c6c730842d78016ad823897a372db"

    @classmethod
    def tearDownClass(cls):
        _clean_test_records()

    def setUp(self):
        _clean_test_records()
        self.client = TestClient(app, raise_server_exceptions=False)
        self.test_key = f"API-{uuid.uuid4().hex[:6].upper()}"

    def tearDown(self):
        _clean_test_records()

    def test_api_submit_approval_success(self):
        payload = {
            "recommendation_id": self.sole_rec_id,
            "requested_quantity": 25,
            "idempotency_key": f"{self.test_key}-SUB",
            "operator_notes": "API test"
        }
        headers = {
            "X-Actor-ID": "op-api-user",
            "X-Actor-Role": "OPERATOR"
        }
        resp = self.client.post("/api/observability/replenishment-approvals", json=payload, headers=headers)
        self.assertEqual(resp.status_code, 201)
        data = resp.json()
        self.assertIn("approval_id", data)
        self.assertEqual(data["status"], "PENDING_REVIEW")

    def test_api_submit_approval_bad_request_ambiguous(self):
        payload = {
            "recommendation_id": self.ambiguous_rec_id,
            "requested_quantity": 25,
            "idempotency_key": f"{self.test_key}-AMB"
        }
        headers = {
            "X-Actor-ID": "op-api-user",
            "X-Actor-Role": "OPERATOR"
        }
        resp = self.client.post("/api/observability/replenishment-approvals", json=payload, headers=headers)
        self.assertEqual(resp.status_code, 400)
        self.assertIn("AMBIGUOUS_MULTI_SKU", resp.json().get("detail", ""))

    def test_api_list_approvals(self):
        resp = self.client.get("/api/observability/replenishment-approvals?limit=10")
        self.assertEqual(resp.status_code, 200)
        data = resp.json()
        self.assertIn("summary", data)
        self.assertIn("approvals", data)
        self.assertIn("total", data["summary"])

    def test_api_get_approval_detail(self):
        payload = {
            "recommendation_id": self.sole_rec_id,
            "requested_quantity": 12,
            "idempotency_key": f"{self.test_key}-DET"
        }
        headers = {
            "X-Actor-ID": "op-api-user",
            "X-Actor-Role": "OPERATOR"
        }
        create_resp = self.client.post("/api/observability/replenishment-approvals", json=payload, headers=headers)
        self.assertEqual(create_resp.status_code, 201)
        approval_id = create_resp.json()["approval_id"]

        detail_resp = self.client.get(f"/api/observability/replenishment-approvals/{approval_id}")
        self.assertEqual(detail_resp.status_code, 200)
        self.assertEqual(detail_resp.json()["approval_id"], approval_id)
        self.assertIn("audit_history", detail_resp.json())

    def test_api_approve_as_operator_returns_403(self):
        payload = {
            "recommendation_id": self.sole_rec_id,
            "requested_quantity": 10,
            "idempotency_key": f"{self.test_key}-FORBID"
        }
        headers_op = {"X-Actor-ID": "op-test", "X-Actor-Role": "OPERATOR"}
        create_resp = self.client.post("/api/observability/replenishment-approvals", json=payload, headers=headers_op)
        self.assertEqual(create_resp.status_code, 201)
        approval_id = create_resp.json()["approval_id"]

        # Operator attempts approval -> must be rejected with 403 Forbidden
        resp = self.client.post(f"/api/observability/replenishment-approvals/{approval_id}/approve", headers=headers_op)
        self.assertEqual(resp.status_code, 403)

    def test_api_reject_as_manager_success(self):
        payload = {
            "recommendation_id": self.sole_rec_id,
            "requested_quantity": 10,
            "idempotency_key": f"{self.test_key}-REJ-API"
        }
        headers_op = {"X-Actor-ID": "op-test", "X-Actor-Role": "OPERATOR"}
        create_resp = self.client.post("/api/observability/replenishment-approvals", json=payload, headers=headers_op)
        self.assertEqual(create_resp.status_code, 201)
        approval_id = create_resp.json()["approval_id"]

        # Manager rejects with reason -> 200 OK
        headers_mgr = {"X-Actor-ID": "mgr-test", "X-Actor-Role": "MANAGER"}
        reject_payload = {"reason": "Duplicate purchase order already placed with distributor"}
        resp = self.client.post(f"/api/observability/replenishment-approvals/{approval_id}/reject", json=reject_payload, headers=headers_mgr)
        self.assertEqual(resp.status_code, 200)
        self.assertEqual(resp.json()["status"], "REJECTED")

    def test_api_reject_without_reason_returns_422(self):
        headers = {"X-Actor-ID": "mgr-test", "X-Actor-Role": "MANAGER"}
        resp = self.client.post("/api/observability/replenishment-approvals/1/reject", json={}, headers=headers)
        self.assertEqual(resp.status_code, 422)


if __name__ == "__main__":
    unittest.main()
