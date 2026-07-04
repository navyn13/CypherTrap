#!/usr/bin/env python3
"""CypherTrap TLS stress test — 5-minute duration run.

Connects over TLS and sends newline-delimited ALLOW lines:
  ALLOW <ip> <company> <keyName> <apiKey> <policy>

Each connection sends a batch every second. Second 0 = 100 requests, second 1 = 200,
second 2 = 400, doubling each second (100 × 2^s). Every request uses a fresh UUID.
All connections follow the same schedule in parallel.

Usage:
  python3 stress.py                          # interactive prompts
  python3 stress.py --yes -c admin -k login -p free --connections 5
  python3 stress.py --yes ... --calls 100 --connections 5
"""

from __future__ import annotations

import argparse
import getpass
import os
import random
import socket
import ssl
import sys
import threading
import time
import uuid
from collections import Counter, defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed

DEFAULT_DURATION_SEC = 5 * 60
DEFAULT_REQUESTS_PER_SECOND = 100
SECOND_GROWTH = 2
MAX_REQUESTS_PER_SECOND = 6_400  # cap: 100 × 2^6; doubles until this ceiling


def prompt(label: str, default: str | None = None) -> str:
    if default is not None:
        value = input(f"{label} [{default}]: ").strip()
        return value or default
    while True:
        value = input(f"{label}: ").strip()
        if value:
            return value


def prompt_secret(label: str, preset: str | None = None) -> str:
    if preset:
        return preset
    while True:
        value = getpass.getpass(f"{label}: ")
        if value:
            return value


def prompt_positive_int(label: str, default: int | None = None) -> int:
    while True:
        raw = prompt(label, str(default) if default is not None else None).strip()
        if raw.isdigit() and int(raw) > 0:
            return int(raw)
        print("Enter a positive integer.")


def new_client_ip(rng: random.Random | None = None) -> str:
    """Fresh random UUID for each ALLOW — unique client IP every request."""
    return str(uuid.uuid4())


def requests_for_second(second: int, base: int) -> int:
    """Requests each connection sends during wall-clock second s (100, 200, 400, …)."""
    batch = base * (SECOND_GROWTH**second)
    return min(batch, MAX_REQUESTS_PER_SECOND)


def per_second_batches(duration_sec: int, base: int) -> list[int]:
    return [requests_for_second(s, base) for s in range(duration_sec)]


def total_planned_calls(connections: int, duration_sec: int, base: int) -> int:
    per_conn = sum(requests_for_second(s, base) for s in range(duration_sec))
    return connections * per_conn


def describe_schedule(duration_sec: int, base: int, preview: int = 8) -> list[dict]:
    rows: list[dict] = []
    capped_from: int | None = None
    for second in range(duration_sec):
        batch = requests_for_second(second, base)
        uncapped = base * (SECOND_GROWTH**second)
        if capped_from is None and batch < uncapped:
            capped_from = second
        rows.append({"second": second, "requests_per_connection": batch})
    return rows


def print_schedule_preview(duration_sec: int, base: int, preview: int = 8) -> None:
    print("  Per-second batch (each connection, unique UUID per request):")
    show = min(preview, duration_sec)
    for second in range(show):
        batch = requests_for_second(second, base)
        print(f"    second {second:3d}: {batch:6d} requests/conn")
    if duration_sec > show:
        last_batch = requests_for_second(duration_sec - 1, base)
        capped = requests_for_second(show, base) >= MAX_REQUESTS_PER_SECOND
        if capped:
            print(f"    … seconds {show}–{duration_sec - 1}: {last_batch}/conn (doubling capped at {MAX_REQUESTS_PER_SECOND})")
        else:
            print(f"    … through second {duration_sec - 1} (last batch {last_batch}/conn)")
    print()


class RequestTracker:
    """Thread-safe total request counter."""

    def __init__(self) -> None:
        self._sent = 0
        self._lock = threading.Lock()

    def record_send(self) -> None:
        with self._lock:
            self._sent += 1

    @property
    def total_sent(self) -> int:
        with self._lock:
            return self._sent


def classify_response(line: str) -> str:
    mapping = {
        "ALLOWED": "allowed",
        "BLOCKED": "blocked",
        "UNAUTHORIZED": "unauthorized",
        "INVALID MESSAGE": "invalid",
        "INTERNAL SERVER ERROR": "server_error",
        "UNKNOWN COMMAND": "unknown",
    }
    return mapping.get(line.strip(), "other")


def allow_line(
    client_id: str, company: str, key_name: str, api_key: str, policy: str
) -> str:
    return f"ALLOW {client_id} {company} {key_name} {api_key} {policy}\n"


def format_duration(seconds: float) -> str:
    minutes, secs = divmod(int(seconds), 60)
    return f"{minutes}:{secs:02d}"


class MetricsRecorder:
    """Thread-safe per-second response counters for console logging."""

    def __init__(self, start: float, bucket_sec: float = 1.0) -> None:
        self._start = start
        self._bucket_sec = bucket_sec
        self._lock = threading.Lock()
        self._totals: Counter[str] = Counter()
        self._buckets: dict[int, Counter[str]] = defaultdict(Counter)

    def record(self, kind: str, when: float | None = None) -> None:
        elapsed = max(0.0, (when or time.perf_counter()) - self._start)
        bucket = int(elapsed // self._bucket_sec)
        with self._lock:
            self._totals[kind] += 1
            self._buckets[bucket][kind] += 1

    def bucket_counts(self, second: int) -> Counter[str]:
        with self._lock:
            return Counter(self._buckets.get(second, Counter()))

    def cumulative_total_through(self, second: int) -> int:
        with self._lock:
            total = 0
            for idx in range(second + 1):
                total += sum(self._buckets.get(idx, Counter()).values())
            return total

    @property
    def totals(self) -> Counter[str]:
        with self._lock:
            return Counter(self._totals)


def format_second_log_line(second: int, counts: Counter[str], cumulative: int) -> str:
    this_sec = sum(counts.values())
    parts = [f"[{second:4d}s] +{this_sec:3d} this second | total {cumulative:5d}"]
    for kind in ("allowed", "blocked", "unauthorized", "server_error", "invalid", "other"):
        if counts[kind]:
            parts.append(f"{kind}={counts[kind]}")
    return " | ".join(parts)


def second_log_loop(
    start: float,
    duration_sec: int,
    metrics: MetricsRecorder,
) -> None:
    print("=== Per-second log ===")
    print("  second | calls this sec | running total | breakdown")
    print("  -------+---------------+---------------+------------------")

    for second in range(duration_sec):
        wake_at = start + second + 1
        remaining = wake_at - time.perf_counter()
        if remaining > 0:
            time.sleep(remaining)

        counts = metrics.bucket_counts(second)
        cumulative = metrics.cumulative_total_through(second)
        line = format_second_log_line(second, counts, cumulative)
        print(f"  {line}")

    print()


def open_tls(server_addr: str) -> tuple[socket.socket, ssl.SSLSocket]:
    host, port_str = server_addr.rsplit(":", 1)
    port = int(port_str)
    ctx = ssl.create_default_context()
    ctx.check_hostname = False
    ctx.verify_mode = ssl.CERT_NONE
    sock = socket.create_connection((host, port), timeout=30)
    tls = ctx.wrap_socket(sock, server_hostname=host)
    return sock, tls


def send_allow(
    tls: ssl.SSLSocket, user: str, company: str, key_name: str, api_key: str, policy: str
) -> str:
    tls.sendall(allow_line(user, company, key_name, api_key, policy).encode())
    buf = b""
    while b"\n" not in buf:
        chunk = tls.recv(256)
        if not chunk:
            raise ConnectionError("connection closed while reading response")
        buf += chunk
    return buf.decode().strip()


def run_worker(
    worker_id: int,
    test_start: float,
    duration_sec: int,
    base_requests: int,
    tracker: RequestTracker,
    metrics: MetricsRecorder,
    server_addr: str,
    company: str,
    key_name: str,
    api_key: str,
    policy: str,
) -> Counter[str]:
    counts: Counter[str] = Counter()
    rng = random.Random(worker_id)
    sock: socket.socket | None = None
    tls: ssl.SSLSocket | None = None
    deadline = test_start + duration_sec

    def ensure_connected() -> ssl.SSLSocket:
        nonlocal sock, tls
        if tls is not None:
            return tls
        sock, tls = open_tls(server_addr)
        return tls

    try:
        for second in range(duration_sec):
            second_start = test_start + second
            now = time.perf_counter()
            if now < second_start:
                time.sleep(second_start - now)

            if time.perf_counter() >= deadline:
                break

            batch_size = requests_for_second(second, base_requests)

            for _ in range(batch_size):
                if time.perf_counter() >= deadline:
                    return counts

                client_ip = new_client_ip(rng)

                try:
                    conn = ensure_connected()
                    line = send_allow(conn, client_ip, company, key_name, api_key, policy)
                    if line:
                        kind = classify_response(line)
                        counts[kind] += 1
                        metrics.record(kind, time.perf_counter())
                    tracker.record_send()
                except (ConnectionError, OSError, ssl.SSLError):
                    if tls is not None:
                        try:
                            tls.close()
                        except OSError:
                            pass
                    if sock is not None:
                        try:
                            sock.close()
                        except OSError:
                            pass
                    sock, tls = None, None
                    time.sleep(0.05 * (1 + rng.random()))
    finally:
        if tls is not None:
            try:
                tls.close()
            except OSError:
                pass
        if sock is not None:
            try:
                sock.close()
            except OSError:
                pass

    return counts


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="CypherTrap 5-minute TLS stress test")
    parser.add_argument("--server", default=os.environ.get("CYPHER_SERVER", "localhost:7878"))
    parser.add_argument("-c", "--company", default=os.environ.get("CYPHER_COMPANY"))
    parser.add_argument("-k", "--key-name", default=os.environ.get("CYPHER_KEY_NAME"))
    parser.add_argument("-a", "--api-key", default=os.environ.get("CYPHER_API_KEY"))
    parser.add_argument("-p", "--policy", default=os.environ.get("CYPHER_POLICY"))
    parser.add_argument(
        "--calls",
        type=int,
        default=None,
        help=f"Requests in second 0 per connection (default: {DEFAULT_REQUESTS_PER_SECOND}, doubles each second)",
    )
    parser.add_argument(
        "--connections",
        type=int,
        default=None,
        help="Parallel TLS connections (each sends unique UUID per request)",
    )
    parser.add_argument(
        "--duration",
        type=int,
        default=int(os.environ.get("CYPHER_STRESS_DURATION", DEFAULT_DURATION_SEC)),
        help="Test duration in seconds (default: 300)",
    )
    parser.add_argument("--yes", "-y", action="store_true", help="Skip confirmation prompt")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    duration_sec = max(1, args.duration)

    print("=== CypherTrap stress test (timed run) ===")
    print("Connects over TLS and sends ALLOW lines.")
    print()

    server_addr = args.server or prompt("Server address", "localhost:7878")
    company = args.company or prompt("Company")
    key_name = args.key_name or prompt("Key name")
    api_key = prompt_secret("API key", args.api_key)
    policy = args.policy or prompt("Policy")
    connections = (
        args.connections
        if args.connections
        else prompt_positive_int("Parallel connections", 1)
    )
    if args.calls is not None:
        base_requests = args.calls
    else:
        base_requests = prompt_positive_int(
            "Starting requests per second (per connection)", DEFAULT_REQUESTS_PER_SECOND
        )

    per_conn_total = sum(requests_for_second(s, base_requests) for s in range(duration_sec))
    total_target = total_planned_calls(connections, duration_sec, base_requests)
    unique_ips = total_target

    print()
    print("Configuration:")
    print(f"  Duration:        {format_duration(duration_sec)} (fixed)")
    print(f"  Server:          {server_addr}")
    print(f"  Company:         {company}")
    print(f"  Key name:        {key_name}")
    print(f"  Policy:          {policy}")
    print(f"  Connections:     {connections} (identical schedule each)")
    print(f"  Second 0 batch:  {base_requests} req/conn (default {DEFAULT_REQUESTS_PER_SECOND})")
    print(f"  Growth:          ×{SECOND_GROWTH} each second (100 → 200 → 400 → …)")
    print(f"  Per-conn total:  {per_conn_total} requests, {per_conn_total} unique UUIDs")
    print(f"  Target calls:    {total_target} ({connections} connections)")
    print(f"  Cap per second:  {MAX_REQUESTS_PER_SECOND} req/conn (after doubling ceiling)")
    print()
    print_schedule_preview(duration_sec, base_requests)

    if not args.yes:
        confirm = input("Start stress test? [y/N]: ").strip().lower()
        if confirm not in ("y", "yes"):
            print("Aborted.")
            return 0

    print(f"Running for {format_duration(duration_sec)}...")
    print()

    tracker = RequestTracker()
    start = time.perf_counter()
    metrics = MetricsRecorder(start, bucket_sec=1.0)

    log_thread = threading.Thread(
        target=second_log_loop,
        args=(start, duration_sec, metrics),
        daemon=True,
    )
    log_thread.start()

    with ThreadPoolExecutor(max_workers=connections) as pool:
        futures = [
            pool.submit(
                run_worker,
                worker_id,
                start,
                duration_sec,
                base_requests,
                tracker,
                metrics,
                server_addr,
                company,
                key_name,
                api_key,
                policy,
            )
            for worker_id in range(connections)
        ]
        for future in as_completed(futures):
            try:
                future.result()
            except Exception as exc:
                print(f"Worker failed: {exc}", file=sys.stderr)

    remaining = (start + duration_sec) - time.perf_counter()
    if remaining > 0:
        time.sleep(remaining)

    log_thread.join(timeout=5)

    elapsed = time.perf_counter() - start
    totals = metrics.totals
    responses = sum(totals.values())
    rps = responses / elapsed if elapsed > 0 else 0.0

    print("=== Results ===")
    print(f"  Duration:      {format_duration(elapsed)} / {format_duration(duration_sec)}")
    print(f"  Responses:     {responses} / {total_target} target")
    print(f"  Unique IPs:    {tracker.total_sent} sent ({unique_ips} planned, 1 UUID per request)")
    print(f"  Throughput:    {rps:.1f} req/s")
    print(f"  ALLOWED:       {totals['allowed']}")
    print(f"  BLOCKED:       {totals['blocked']}")
    print(f"  UNAUTHORIZED:  {totals['unauthorized']}")
    print(f"  INVALID MSG:   {totals['invalid']}")
    print(f"  SERVER ERROR:  {totals['server_error']}")
    print(f"  UNKNOWN CMD:   {totals['unknown']}")
    print(f"  OTHER:         {totals['other']}")

    if responses < total_target:
        print()
        print(
            f"Warning: only {responses}/{total_target} responses in the test window.",
            file=sys.stderr,
        )

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
