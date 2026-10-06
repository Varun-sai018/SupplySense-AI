"""
SupplySense AI - Standalone CLI for Processing Pipeline Execution Retries

Scans the database for eligible FAILED pipeline executions whose next_retry_at
timestamp has passed and attempts to re-execute them with bounded exponential backoff.
"""

import os
import sys
import argparse
import logging
import importlib

# Ensure repository root is on sys.path
REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '../'))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

retry_module = importlib.import_module('services.pipeline-runner.retry_service')
process_retry_candidates = retry_module.process_retry_candidates


def main():
    parser = argparse.ArgumentParser(
        description="SupplySense AI - Pipeline Execution Retry Tool"
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=10,
        help="Maximum number of retry candidates to process in one sweep (default: 10)"
    )
    parser.add_argument(
        "--verbose",
        "-v",
        action="store_true",
        help="Enable detailed debug logging"
    )
    args = parser.parse_args()

    log_level = logging.DEBUG if args.verbose else logging.INFO
    logging.basicConfig(
        level=log_level,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
    )

    print("=" * 80)
    print("      SUPPLYSENSE AI - PIPELINE EXECUTION RETRY RECOVERY")
    print("=" * 80)
    print(f"  Batch Limit : {args.batch_size}")
    print("  Scanning for eligible FAILED executions ready for retry...\n")

    try:
        results = process_retry_candidates(max_batch=args.batch_size)
        print("=" * 80)
        print("RETRY RECOVERY SUMMARY")
        print("=" * 80)
        print(f"  Total Candidates Processed : {len(results)}")
        succeeded = sum(1 for r in results if r.get("status") == "COMPLETED")
        failed = sum(1 for r in results if r.get("status") == "FAILED")
        skipped = sum(1 for r in results if r.get("status") in ("SKIPPED", "EXHAUSTED", "NON_RETRYABLE"))
        print(f"  Succeeded (COMPLETED)     : {succeeded}")
        print(f"  Failed (Scheduled/Exhaust): {failed}")
        print(f"  Skipped / Non-Retryable   : {skipped}")
        print("=" * 80)
    except Exception as exc:
        print(f"\n[ERROR] Retry sweep failed: {exc}", file=sys.stderr)
        sys.exit(1)


if __name__ == '__main__':
    main()
