# SupplySense AI - Pipeline Runner Service

## Overview
The **Pipeline Runner Service** is responsible for executing downstream Python workloads when the Dependency Engine emits a `TRIGGER` decision. It manages the full execution lifecycle (`RUNNING` -> `COMPLETED` / `FAILED`), invokes registered pipelines, captures metrics and output locations, and guarantees robust failure tracking and idempotency.

## Architecture & Responsibilities
1. **Registry Pattern**: Maintains an extensible mapping of pipeline names (e.g. `Demand Forecast Pipeline`) to pipeline handlers.
2. **Validation**: Ensures required prepared datasets exist prior to running downstream jobs.
3. **Execution Tracking**: Updates `pipeline_executions` status, timestamps (`started_at`, `completed_at`), `output_location`, and `error_message`.
4. **Idempotency & Concurrency**: Integrates with database-level unique constraints and atomic state transitions to prevent duplicate concurrent runs.

## Supported Pipelines (Phase 4)
- **Demand Forecast Pipeline**: Executes the weekly category-level naive demand baseline (`ml.baseline.naive_baseline`) and verifies output artifacts.
