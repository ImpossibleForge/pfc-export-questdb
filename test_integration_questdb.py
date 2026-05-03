#!/usr/bin/env python3
"""
Integration tests for pfc-export-questdb — requires live QuestDB.

Run on the server:
  python3 test_integration_questdb.py

QuestDB: localhost:8812 (Docker container quest-test)
"""

import json
import os
import subprocess
import sys
import tempfile
import unittest
from datetime import datetime, timezone, timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

QUEST_HOST = "localhost"
QUEST_PORT = 8812
QUEST_USER = "admin"
QUEST_PASS = "quest"
QUEST_DB   = "qdb"
PFC_BINARY = "/usr/local/bin/pfc_jsonl"

try:
    import psycopg2
    HAS_PSYCOPG2 = True
except ImportError:
    HAS_PSYCOPG2 = False


def get_conn():
    return psycopg2.connect(
        host=QUEST_HOST, port=QUEST_PORT,
        user=QUEST_USER, password=QUEST_PASS,
        dbname=QUEST_DB, connect_timeout=10,
    )


def pfc_binary_available():
    return os.path.isfile(PFC_BINARY) and os.access(PFC_BINARY, os.X_OK)


@unittest.skipUnless(HAS_PSYCOPG2, "psycopg2 not installed")
class TestQuestDBIntegration(unittest.TestCase):

    TABLE = "pfc_export_integration_test"

    @classmethod
    def setUpClass(cls):
        cls.conn = get_conn()
        cls.conn.autocommit = True
        cur = cls.conn.cursor()
        # QuestDB: DROP + CREATE if exists
        try:
            cur.execute(f"DROP TABLE IF EXISTS {cls.TABLE}")
        except Exception:
            cls.conn.rollback()
        cur.execute(f"""
            CREATE TABLE {cls.TABLE} (
                id      INT,
                ts      TIMESTAMP,
                level   SYMBOL,
                message STRING,
                value   DOUBLE
            ) TIMESTAMP(ts) PARTITION BY DAY
        """)
        # Insert 200 rows spanning 10 days
        base = datetime(2024, 1, 1, tzinfo=timezone.utc)
        for i in range(200):
            ts = base + timedelta(hours=i)
            level = ["INFO", "WARN", "ERROR"][i % 3]
            ts_us = int(ts.timestamp() * 1_000_000)
            cur.execute(
                f"INSERT INTO {cls.TABLE} VALUES (%s, %s, %s, %s, %s)",
                (i, ts_us, level, f"log message {i}", float(i) * 1.5),
            )
        cur.close()

    @classmethod
    def tearDownClass(cls):
        cur = cls.conn.cursor()
        try:
            cur.execute(f"DROP TABLE IF EXISTS {cls.TABLE}")
        except Exception:
            pass
        cur.close()
        cls.conn.close()

    # ------------------------------------------------------------------
    # 1. Basic export — all rows
    # ------------------------------------------------------------------
    def test_basic_export_row_count(self):
        from pfc_export_questdb import export_to_pfc
        with tempfile.TemporaryDirectory() as tmp:
            out = Path(tmp) / "test_basic.pfc"
            result = export_to_pfc(
                host=QUEST_HOST, port=QUEST_PORT,
                user=QUEST_USER, password=QUEST_PASS,
                dbname=QUEST_DB,
                table=self.TABLE, output_path=out,
                pfc_binary=PFC_BINARY,
            )
            self.assertEqual(result["rows"], 200, f"Expected 200 rows, got {result['rows']}")
            self.assertTrue(out.exists(), ".pfc file not created")

    # ------------------------------------------------------------------
    # 2. .bidx file is created
    # ------------------------------------------------------------------
    @unittest.skipUnless(pfc_binary_available(), "pfc_jsonl binary not found")
    def test_bidx_created(self):
        from pfc_export_questdb import export_to_pfc
        with tempfile.TemporaryDirectory() as tmp:
            out = Path(tmp) / "test_bidx.pfc"
            export_to_pfc(
                host=QUEST_HOST, port=QUEST_PORT,
                user=QUEST_USER, password=QUEST_PASS,
                dbname=QUEST_DB,
                table=self.TABLE, output_path=out,
                pfc_binary=PFC_BINARY, ts_column="ts",
            )
            bidx = Path(str(out) + ".bidx")
            self.assertTrue(bidx.exists(), ".pfc.bidx not created")

    # ------------------------------------------------------------------
    # 3. Time-range filter
    # ------------------------------------------------------------------
    def test_time_range_filter(self):
        from pfc_export_questdb import export_to_pfc
        with tempfile.TemporaryDirectory() as tmp:
            out = Path(tmp) / "test_range.pfc"
            result = export_to_pfc(
                host=QUEST_HOST, port=QUEST_PORT,
                user=QUEST_USER, password=QUEST_PASS,
                dbname=QUEST_DB,
                table=self.TABLE, output_path=out,
                pfc_binary=PFC_BINARY, ts_column="ts",
                from_ts="2024-01-01T00:00:00",
                to_ts="2024-01-03T00:00:00",
            )
            self.assertEqual(result["rows"], 48, f"Expected 48 rows, got {result['rows']}")

    # ------------------------------------------------------------------
    # 4. Field integrity
    # ------------------------------------------------------------------
    @unittest.skipUnless(pfc_binary_available(), "pfc_jsonl binary not found")
    def test_field_integrity(self):
        from pfc_export_questdb import export_to_pfc
        with tempfile.TemporaryDirectory() as tmp:
            out = Path(tmp) / "test_fields.pfc"
            export_to_pfc(
                host=QUEST_HOST, port=QUEST_PORT,
                user=QUEST_USER, password=QUEST_PASS,
                dbname=QUEST_DB,
                table=self.TABLE, output_path=out,
                pfc_binary=PFC_BINARY, ts_column="ts",
                from_ts="2024-01-01T00:00:00",
                to_ts="2024-01-01T01:00:00",
            )
            result = subprocess.run(
                [PFC_BINARY, "decompress", str(out), "-"],
                capture_output=True, text=True,
            )
            lines = [l for l in result.stdout.splitlines() if l.startswith("{")]
            self.assertGreater(len(lines), 0, "No JSON lines in decompress output")
            record = json.loads(lines[0])
            self.assertIn("id", record)
            self.assertIn("level", record)
            self.assertIn("message", record)
            self.assertIn("value", record)

    # ------------------------------------------------------------------
    # 5. .pfc file is non-empty and decompresses without error
    # ------------------------------------------------------------------
    @unittest.skipUnless(pfc_binary_available(), "pfc_jsonl binary not found")
    def test_pfc_decompresses_cleanly(self):
        from pfc_export_questdb import export_to_pfc
        with tempfile.TemporaryDirectory() as tmp:
            out = Path(tmp) / "test_decomp.pfc"
            result = export_to_pfc(
                host=QUEST_HOST, port=QUEST_PORT,
                user=QUEST_USER, password=QUEST_PASS,
                dbname=QUEST_DB,
                table=self.TABLE, output_path=out,
                pfc_binary=PFC_BINARY,
            )
            self.assertGreater(os.path.getsize(out), 0, ".pfc file is empty")
            proc = subprocess.run(
                [PFC_BINARY, "decompress", str(out), "-"],
                capture_output=True,
            )
            self.assertEqual(proc.returncode, 0, f"decompress failed: {proc.stderr.decode()[:200]}")

    # ------------------------------------------------------------------
    # 6. Empty range — no crash
    # ------------------------------------------------------------------
    def test_empty_range_no_crash(self):
        from pfc_export_questdb import export_to_pfc
        with tempfile.TemporaryDirectory() as tmp:
            out = Path(tmp) / "test_empty.pfc"
            result = export_to_pfc(
                host=QUEST_HOST, port=QUEST_PORT,
                user=QUEST_USER, password=QUEST_PASS,
                dbname=QUEST_DB,
                table=self.TABLE, output_path=out,
                pfc_binary=PFC_BINARY, ts_column="ts",
                from_ts="2030-01-01T00:00:00",
                to_ts="2030-01-02T00:00:00",
            )
            self.assertEqual(result["rows"], 0)

    # ------------------------------------------------------------------
    # 7. QuestDB-specific: SYMBOL type survives roundtrip as string
    # ------------------------------------------------------------------
    @unittest.skipUnless(pfc_binary_available(), "pfc_jsonl binary not found")
    def test_symbol_type_as_string(self):
        from pfc_export_questdb import export_to_pfc
        with tempfile.TemporaryDirectory() as tmp:
            out = Path(tmp) / "test_symbol.pfc"
            export_to_pfc(
                host=QUEST_HOST, port=QUEST_PORT,
                user=QUEST_USER, password=QUEST_PASS,
                dbname=QUEST_DB,
                table=self.TABLE, output_path=out,
                pfc_binary=PFC_BINARY, ts_column="ts",
                from_ts="2024-01-01T00:00:00",
                to_ts="2024-01-01T04:00:00",
            )
            result = subprocess.run(
                [PFC_BINARY, "decompress", str(out), "-"],
                capture_output=True, text=True,
            )
            lines = [l for l in result.stdout.splitlines() if l.startswith("{")]
            for line in lines:
                record = json.loads(line)
                self.assertIsInstance(record["level"], str,
                    f"SYMBOL 'level' should be string, got {type(record['level'])}")
                self.assertIn(record["level"], ["INFO", "WARN", "ERROR"])


if __name__ == "__main__":
    print(f"QuestDB Integration Tests — {QUEST_HOST}:{QUEST_PORT}")
    print(f"pfc_jsonl binary: {'found' if pfc_binary_available() else 'NOT FOUND — binary tests will skip'}")
    print("-" * 60)
    unittest.main(verbosity=2)
