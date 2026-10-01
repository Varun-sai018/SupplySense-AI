"""
SupplySense AI - Standalone CLI for Pipeline Execution Reconciliation / Reaper

Scans the database for stale pipeline executions in 'RUNNING' status that have
exceeded the configured stale execution timeout and transitions them to 'FAILED'.
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

reaper_module = importlib.import_module('services.pipeline-runner.reaper')
reap_stale_executions = reaper_module.reap_stale_executions
from config.settings import PIPELINE_EXECUTION_STALE_TIMEOUT_SECONDS


def main():
    parser = argparse.ArgumentParser(
        description="SupplySense AI - Pipeline Execution Reaper / Reconciliation Tool"
    )
    parser.add_argument(
        "--timeout",
        type=int,
        default=None,
        help=f"Stale timeout threshold in seconds (default: {PIPELINE_EXECUTION_STALE_TIMEOUT_SECONDS}s)"
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

    effective_timeout = args.timeout or PIPELINE_EXECUTION_STALE_TIMEOUT_SECONDS

    print("=" * 80)
    print("      SUPPLYSENSE AI - PIPELINE EXECUTION REAPER & RECONCILIATION")
    print("=" * 80)
    print(f"  Configured Timeout Threshold : {effective_timeout} seconds ({effective_timeout / 60:.1f} minutes)")
    print("  Running sweep for orphaned RUNNING executions...\n")

    try:
        stats = reap_stale_executions(timeout_seconds=args.timeout)
        print("=" * 80)
        print("REAPER RECONCILIATION SUMMARY")
        print("=" * 80)
        print(f"  Total RUNNING Executions Scanned : {stats['scanned']}")
        print(f"  Stale Executions Identified     : {stats['stale_found']}")
        print(f"  Executions Reaped (Marked FAILED): {stats['reconciled']}")
        print(f"  Executions Skipped (Race Safe)   : {stats['skipped']}")
        print("=" * 80)
    except Exception as exc:
        print(f"\n[ERROR] Reaper execution failed: {exc}", file=sys.stderr)
        sys.exit(1)


if __name__ == '__main__':
    main()
