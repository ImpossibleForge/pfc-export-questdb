#!/usr/bin/env python3
"""
pfc-export-questdb — Comprehensive Test Suite
==============================================
Happy-Path + Error/Resilience Tests
Run on server: python3 test_pfc_export_questdb.py

Author: ForgeBuddy + Dante | 2026-04-23
"""

import json
import os
import shutil
import subprocess
import sys
import tempfile
import time
from pathlib import Path
from datetime import datetime, timezone, timedelta

# ── Config ──────────────────────────────────────────────────────────────────
SCRIPT     = Path(__file__).parent / "pfc_export_questdb.py"
PFC_BIN    = "/usr/local/bin/pfc_jsonl"
QUEST_HOST = "localhost"
QUEST_PORT = 8812
QUEST_USER = "admin"
QUEST_PASS = "quest"
QUEST_DB   = "qdb"
TEST_TABLE = "pfc_export_test"
OUTDIR     = Path("/root/pfc_export_questdb_test_output")
OUTDIR.mkdir(exist_ok=True)

results = []

def run(name, cmd, expect_exit=0, expect_in_stdout=None):
    t0  = time.time()
    res = subprocess.run(cmd, capture_output=True, text=True)
    dt  = time.time() - t0

    ok = True
    reasons = []

    if res.returncode != expect_exit:
        ok = False
        reasons.append(f"exit={res.returncode} expected={expect_exit}")

    for s in (expect_in_stdout or []):
        if s not in (res.stdout + res.stderr):
            ok = False
            reasons.append(f"missing: {s!r}")

    status = "✅ PASS" if ok else "❌ FAIL"
    print(f"  {status}  [{dt:.1f}s]  {name}")
    if not ok:
        for r in reasons:
            print(f"           → {r}")
        if res.stdout.strip():
            print(f"           stdout: {res.stdout.strip()[:200]}")
        if res.stderr.strip():
            print(f"           stderr: {res.stderr.strip()[:200]}")

    results.append((name, ok, dt))
    return ok, res


def py(args, name=None, **kwargs):
    label = name or " ".join(str(a) for a in args[:3])
    return run(label, ["python3", str(SCRIPT)] + [str(a) for a in args], **kwargs)


def db_connect():
    import psycopg2
    return psycopg2.connect(
        host=QUEST_HOST, port=QUEST_PORT,
        user=QUEST_USER, password=QUEST_PASS,
        dbname=QUEST_DB, connect_timeout=10,
    )


def setup_test_data(rows=50_000):
    """Create QuestDB test table and insert rows via psycopg2."""
    import psycopg2

    conn = db_connect()
    conn.autocommit = True
    cur  = conn.cursor()

    cur.execute(f'DROP TABLE IF EXISTS {TEST_TABLE}')
    cur.execute(f"""
        CREATE TABLE {TEST_TABLE} (
            ts         TIMESTAMP,
            level      SYMBOL,
            service    SYMBOL,
            message    STRING,
            request_id STRING,
            latency_ms INT,
            user_id    INT
        ) timestamp(ts) PARTITION BY DAY
    """)

    levels   = ["INFO", "WARN", "ERROR", "DEBUG"]
    services = ["api-gateway", "auth", "payment", "notification"]
    base_ts  = datetime(2026, 1, 1, tzinfo=timezone.utc)

    batch_size = 5000
    for batch_start in range(0, rows, batch_size):
        batch = []
        for i in range(batch_start, min(batch_start + batch_size, rows)):
            ts = (base_ts + timedelta(seconds=i * 2)).strftime('%Y-%m-%dT%H:%M:%S')
            batch.append((
                ts,
                levels[i % len(levels)],
                services[i % len(services)],
                f"Request processed for user {i % 1000}",
                f"req-{i:08d}",
                10 + (i % 490),
                i % 10000,
            ))
        cur.executemany(
            f"INSERT INTO {TEST_TABLE} VALUES (%s, %s, %s, %s, %s, %s, %s)",
            batch
        )

    time.sleep(1)
    cur.execute(f'SELECT count() FROM {TEST_TABLE}')
    count = cur.fetchone()[0]
    conn.close()
    return count


def count_rows_in_pfc(pfc_path):
    res = subprocess.run(
        [PFC_BIN, "decompress", str(pfc_path), "-"],
        capture_output=True
    )
    if res.returncode != 0:
        return -1
    return sum(1 for line in res.stdout.split(b"\n") if line.strip().startswith(b"{"))


# ══════════════════════════════════════════════════════════════════════════════
print("=" * 65)
print("pfc-export-questdb v0.1.0 — Test Suite")
print("=" * 65)

print("\n[SETUP] Creating QuestDB test table with 50,000 rows ...")
try:
    inserted = setup_test_data(50_000)
    print(f"        ✅ {inserted:,} rows inserted")
except Exception as e:
    print(f"        ❌ Setup failed: {e}")
    sys.exit(1)

# ══════════════════════════════════════════════════════════════════════════════
print("\n[1] HAPPY PATH — Full export")

out01 = OUTDIR / "T01_full.pfc"
ok, _ = py(["--host", QUEST_HOST, "--port", QUEST_PORT,
            "--table", TEST_TABLE, "--output", out01, "--verbose"],
           name="T01: Full table export (50k rows)",
           expect_in_stdout=["rows"])

if ok and out01.exists():
    rows = count_rows_in_pfc(out01)
    status = "✅" if rows == 50_000 else f"⚠️  got {rows}"
    print(f"           {status} Roundtrip row count: {rows:,}")
    results.append(("T01b: Roundtrip row count", rows == 50_000, 0))

out02 = OUTDIR / "T02_range.pfc"
py(["--host", QUEST_HOST, "--port", QUEST_PORT,
    "--table", TEST_TABLE,
    "--ts-column", "ts",
    "--from-ts", "2026-01-01T00:00:00",
    "--to-ts",   "2026-01-08T00:00:00",
    "--output", out02, "--verbose"],
   name="T02: Time-range export (Jan 1–7)",
   expect_in_stdout=["rows"])

orig_dir = Path.cwd()
os.chdir(OUTDIR)
py(["--host", QUEST_HOST, "--port", QUEST_PORT, "--table", TEST_TABLE,
    "--ts-column", "ts",
    "--from-ts", "2026-01-01T06:00:00",
    "--to-ts",   "2026-01-01T12:00:00"],
   name="T03: Auto output filename",
   expect_in_stdout=["rows"])
os.chdir(orig_dir)

# ── Stress test ───────────────────────────────────────────────────────────────
print("\n[2] STRESS TEST")
out06 = OUTDIR / "T06_stress.pfc"
t0 = time.time()
ok, _ = py(["--host", QUEST_HOST, "--port", QUEST_PORT,
            "--table", TEST_TABLE, "--output", out06],
           name="T06: Stress export 50k rows (timing)")
if ok and out06.exists():
    mb    = out06.stat().st_size / 1_048_576
    speed = 50_000 / (time.time() - t0)
    print(f"           → Output: {mb:.1f} MiB  |  Speed: {speed:.0f} rows/s")

# ── Error handling ────────────────────────────────────────────────────────────
print("\n[3] ERROR HANDLING")

py(["--host", "nonexistent.host.invalid", "--port", QUEST_PORT,
    "--table", TEST_TABLE, "--output", OUTDIR / "T07.pfc"],
   name="T07: Wrong host → clear error",
   expect_exit=1, expect_in_stdout=["ERROR"])

py(["--host", QUEST_HOST, "--port", "9998",
    "--table", TEST_TABLE, "--output", OUTDIR / "T08.pfc"],
   name="T08: Wrong port → clear error",
   expect_exit=1, expect_in_stdout=["ERROR"])

py(["--host", QUEST_HOST, "--port", QUEST_PORT,
    "--user", "wronguser", "--password", "wrongpass",
    "--table", TEST_TABLE, "--output", OUTDIR / "T09.pfc"],
   name="T09: Wrong credentials → clear error",
   expect_exit=1, expect_in_stdout=["ERROR"])

py(["--host", QUEST_HOST, "--port", QUEST_PORT,
    "--table", "nonexistent_table_xyz", "--output", OUTDIR / "T10.pfc"],
   name="T10: Table not found → clear error",
   expect_exit=1, expect_in_stdout=["ERROR"])

py(["--host", QUEST_HOST, "--port", QUEST_PORT, "--table", TEST_TABLE,
    "--pfc-binary", "/nonexistent/pfc_jsonl", "--output", OUTDIR / "T11.pfc"],
   name="T11: pfc_jsonl binary missing → clear error",
   expect_exit=1, expect_in_stdout=["ERROR"])

py(["--host", QUEST_HOST, "--port", QUEST_PORT, "--table", TEST_TABLE,
    "--ts-column", "ts",
    "--from-ts", "2099-01-01T00:00:00",
    "--to-ts",   "2099-01-02T00:00:00",
    "--output", OUTDIR / "T12.pfc"],
   name="T12: Empty result set (future date) → 0 rows gracefully",
   expect_in_stdout=["0 rows"],
   expect_exit=0)

# ── Resilience: QuestDB down ──────────────────────────────────────────────────
print("\n[4] RESILIENCE — QuestDB down")
subprocess.run(["docker", "stop", "quest-test"], capture_output=True)
time.sleep(2)

py(["--host", QUEST_HOST, "--port", QUEST_PORT,
    "--table", TEST_TABLE, "--output", OUTDIR / "T13.pfc"],
   name="T13: QuestDB stopped → clean error, no crash",
   expect_exit=1, expect_in_stdout=["ERROR"])

subprocess.run(["docker", "start", "quest-test"], capture_output=True)
time.sleep(5)
print("           (QuestDB restarted)")

run("T14: --version flag",
    ["python3", str(SCRIPT), "--version"],
    expect_in_stdout=["0.1.0"])

run("T15: --help flag",
    ["python3", str(SCRIPT), "--help"],
    expect_in_stdout=["--host", "--table"])

# ── Results ───────────────────────────────────────────────────────────────────
print("\n[5] OUTPUT FILES")
for p in sorted(OUTDIR.glob("*.pfc")):
    if p.stat().st_size > 0:
        print(f"  📄 {p.name:<35} {p.stat().st_size/1_048_576:.2f} MiB")

passed = sum(1 for _, ok, _ in results if ok)
failed = sum(1 for _, ok, _ in results if not ok)
total  = len(results)
total_time = sum(dt for _, _, dt in results)

print(f"\n{'='*65}")
print(f"RESULTS: {passed}/{total} PASS  |  {failed} FAIL  |  {total_time:.1f}s total")
print(f"{'='*65}")

if failed > 0:
    print("\nFailed tests:")
    for name, ok, _ in results:
        if not ok:
            print(f"  ❌ {name}")

sys.exit(0 if failed == 0 else 1)
