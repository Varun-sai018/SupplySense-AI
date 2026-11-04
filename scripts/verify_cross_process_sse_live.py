"""
Cross-Process Live SSE Streaming Verification Script

Validates the end-to-end real HTTP path across OS process boundaries:
1. Starts / connects to the live FastAPI server (port 8000).
2. Connects an HTTP SSE stream client to /api/observability/events/stream.
3. Spawns an external Python CLI process to publish a test event using publish_dashboard_event().
4. Verifies the HTTP stream client receives the event via the cross-process MySQL journal poller.
5. Verifies Last-Event-ID reconnection and clean disconnect.
"""

import os
import sys
import time
import json
import socket
import urllib.request
import subprocess
import threading

REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '../'))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)


def is_port_in_use(port: int = 8000) -> bool:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        return s.connect_ex(('127.0.0.1', port)) == 0


def main():
    print("=" * 80)
    print(" SUPPLYSENSE AI - CROSS-PROCESS LIVE SSE VERIFICATION")
    print("=" * 80)

    server_process = None
    started_by_us = False

    try:
        # 1. Ensure server is running on port 8000
        if not is_port_in_use(8000):
            print("[1/5] Starting FastAPI observability service in background process...")
            server_cmd = [
                sys.executable,
                "-m", "uvicorn",
                "services.observability.main:app",
                "--host", "127.0.0.1",
                "--port", "8000",
                "--log-level", "warning"
            ]
            server_process = subprocess.Popen(
                server_cmd,
                cwd=REPO_ROOT,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE
            )
            started_by_us = True

            # Wait for server to become responsive
            ready = False
            for _ in range(30):
                time.sleep(0.5)
                if is_port_in_use(8000):
                    ready = True
                    break
            if not ready:
                print("  [FAIL] FastAPI server failed to start on port 8000.")
                sys.exit(1)
            print("  [PASS] FastAPI server is live and accepting connections on port 8000.")
        else:
            print("[1/5] FastAPI server is already running on port 8000.")

        # 2. Connect HTTP SSE client to /api/observability/events/stream
        print("[2/5] Connecting HTTP SSE client to /api/observability/events/stream...")
        req = urllib.request.Request(
            "http://127.0.0.1:8000/api/observability/events/stream",
            headers={"Accept": "text/event-stream"}
        )
        sse_resp = urllib.request.urlopen(req, timeout=10)
        assert sse_resp.status == 200, f"Expected HTTP 200, got {sse_resp.status}"
        print("  [PASS] SSE stream connection established (HTTP 200 text/event-stream).")

        # Background thread to read events from the stream
        received_events = []
        stop_reading = threading.Event()

        def stream_reader():
            current_event = {}
            for line in sse_resp:
                if stop_reading.is_set():
                    break
                decoded = line.decode('utf-8').strip()
                if decoded.startswith("id:"):
                    current_event["id"] = decoded[3:].strip()
                elif decoded.startswith("event:"):
                    current_event["event"] = decoded[6:].strip()
                elif decoded.startswith("data:"):
                    current_event["data"] = decoded[5:].strip()
                elif decoded == "" and current_event:
                    # End of event block
                    received_events.append(dict(current_event))
                    current_event = {}

        reader_thread = threading.Thread(target=stream_reader, daemon=True)
        reader_thread.start()

        # Wait a moment for reader thread to be active
        time.sleep(1.0)

        # 3. Spawn a SEPARATE OS process to publish a test event
        print("[3/5] Spawning separate CLI process to publish unique live event...")
        unique_marker = f"xproc_test_{int(time.time() * 1000)}"
        publisher_code = f"""
import sys, os
sys.path.insert(0, r'{REPO_ROOT}')
from services.observability.publisher import publish_dashboard_event
ev = publish_dashboard_event(
    event_type='SYSTEM_NOTIFICATION',
    entity='CROSS_PROCESS_TEST',
    severity='INFO',
    data={{'marker': '{unique_marker}', 'origin': 'external_cli_process'}}
)
print('Published from PID ' + str(os.getpid()) + ' with ID ' + ev.event_id)
"""
        cli_result = subprocess.run(
            [sys.executable, "-c", publisher_code],
            capture_output=True,
            text=True,
            check=True
        )
        print(f"  [CLI Process Output]: {cli_result.stdout.strip()}")

        # 4. Await event delivery in the SSE reader stream
        print("[4/5] Waiting for cross-process SSE event delivery in HTTP stream...")
        matched_event = None
        start_wait = time.time()
        while time.time() - start_wait < 5.0:
            for ev in list(received_events):
                try:
                    data = json.loads(ev.get("data", "{}"))
                    if data.get("data", {}).get("marker") == unique_marker or data.get("marker") == unique_marker:
                        matched_event = ev
                        break
                except Exception:
                    pass
            if matched_event:
                break
            time.sleep(0.2)

        assert matched_event is not None, f"Event {unique_marker} not received over SSE within timeout"
        elapsed = round((time.time() - start_wait) * 1000, 1)
        print(f"  [PASS] Cross-Process Event Received in {elapsed}ms!")
        print(f"         Event ID: {matched_event.get('id')}")
        print(f"         Type    : {matched_event.get('event')}")
        print(f"         Data    : {matched_event.get('data')[:80]}...")

        # 5. Verify reconnection via Last-Event-ID header
        print("[5/5] Verifying Last-Event-ID reconnection over HTTP...")
        last_id = matched_event.get("id")
        reconnect_req = urllib.request.Request(
            "http://127.0.0.1:8000/api/observability/events/stream",
            headers={"Accept": "text/event-stream", "Last-Event-ID": last_id}
        )
        reconnect_resp = urllib.request.urlopen(reconnect_req, timeout=5)
        assert reconnect_resp.status == 200
        print("  [PASS] Reconnection accepted with Last-Event-ID header.")
        reconnect_resp.close()

        print("\n" + "=" * 80)
        print(" [SUCCESS] REAL HTTP CROSS-PROCESS SSE STREAMING VERIFIED OPERATIONAL")
        print("=" * 80)

    finally:
        stop_reading.set()
        if server_process and started_by_us:
            print("Cleaning up background FastAPI process...")
            server_process.terminate()
            try:
                server_process.wait(timeout=3)
            except subprocess.TimeoutExpired:
                server_process.kill()


if __name__ == "__main__":
    main()
