# Review-2 Demonstration Scenario

This deterministic script outlines exactly how to demonstrate Objective-1 to reviewers using the integration test workflow.

## Pre-requisites
The `Demand Forecast Pipeline` expects an `ALL` condition for 3 datasets:
1. Orders
2. Products
3. Sellers

## STEP 1: Partial Readiness (BLOCK)
**Action:**
Set `Orders` and `Products` to `READY` status. Submit an event for `Products` into the Dependency Engine.

**Expected Result:**
The Dependency Engine outputs `BLOCK`. 
**Reason:** Only 2 of the 3 required datasets are READY. `Sellers` is missing.

**Verification:**
The `pipeline_decisions` table displays `BLOCK`. No entry is made in `pipeline_executions`.

## STEP 2: Satisfying Dependencies (TRIGGER)
**Action:**
Generate an update event for the missing `Sellers` dataset. The status updates to `READY`. Submit this new event.

**Expected Result:**
The Dependency Engine outputs `TRIGGER`.
**Reason:** 3 out of 3 required datasets are now READY. The `ALL` condition is satisfied.

**Verification:**
The `pipeline_decisions` table displays `TRIGGER`.

## STEP 3: Execution and Idempotency
**Action (A):**
Inspect the downstream `pipeline_executions` table.
**Expected:** A new execution record exists with status `RUNNING`.

**Action (B) [Idempotency]:**
Resubmit the exact same event for `Sellers`.
**Expected:** The system catches the duplicate via `triggering_event_id` checking. It logs: `IDEMPOTENCY: Pipeline was already triggered`. No secondary execution is spawned.

*Note: The actual downstream Spark/ML jobs are omitted from Review-2; this strictly proves the orchestration gate behaves correctly.*
