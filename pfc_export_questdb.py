#!/usr/bin/env python3
"""
pfc-export-questdb — Export QuestDB tables to PFC cold-storage archives.

Streams rows from a QuestDB table directly into a compressed .pfc archive
with block-level timestamp index — ready for time-range queries via DuckDB
or pfc-gateway without loading the full archive.

Note: QuestDB has no schema concept — tables are referenced by name only.

Usage:
  pfc-export-questdb --host quest.example.com --table trades --output trades.pfc
  pfc-export-questdb --host quest.example.com --table sensor_data \\
    --ts-column timestamp \\
    --from-ts "2024-01-01T00:00:00" \\
    --to-ts   "2024-02-01T00:00:00" \\
    --output  sensor_jan2024.pfc --verbose

Requires: pip install psycopg2-binary
"""

__version__ = "0.1.0"

import argparse
import json
import os
import shutil
import subprocess
import sys
import tempfile
from datetime import datetime
from pathlib import Path


# ---------------------------------------------------------------------------
# Binary detection
# ---------------------------------------------------------------------------

def find_pfc_binary(override=None):
    if override:
        if os.path.isfile(override) and os.access(override, os.X_OK):
            return override
        raise FileNotFoundError(f"pfc_jsonl binary not found at: {override}")

    env = os.environ.get("PFC_JSONL_BINARY")
    if env and os.path.isfile(env) and os.access(env, os.X_OK):
        return env

    default = "/usr/local/bin/pfc_jsonl"
    if os.path.isfile(default) and os.access(default, os.X_OK):
        return default

    found = shutil.which("pfc_jsonl")
    if found:
        return found

    return None


# ---------------------------------------------------------------------------
# Core export
# ---------------------------------------------------------------------------

def export_to_pfc(
    host: str,
    port: int,
    user: str,
    password: str,
    dbname: str,
    table: str,
    output_path: Path,
    pfc_binary: str,
    ts_column: str = None,
    from_ts: str = None,
    to_ts: str = None,
    batch_size: int = 10_000,
    verbose: bool = False,
) -> dict:
    """
    Stream rows from QuestDB into a PFC archive.

    Flow: QuestDB → fetchmany() loop → JSONL temp file → pfc_jsonl compress → .pfc + .pfc.bidx

    Returns dict: rows, jsonl_mb, output_mb, ratio_pct, output
    """
    try:
        import psycopg2
    except ImportError:
        raise ImportError(
            "psycopg2 is required.\n"
            "Install: pip install psycopg2-binary"
        )

    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    # QuestDB has no schema — table referenced directly
    full_table   = f'"{table}"'
    conditions, params = [], []
    if ts_column and from_ts:
        conditions.append(f'"{ts_column}" >= %s')
        params.append(from_ts)
    if ts_column and to_ts:
        conditions.append(f'"{ts_column}" < %s')
        params.append(to_ts)

    where_clause = ("WHERE " + " AND ".join(conditions)) if conditions else ""
    order_clause = (f'ORDER BY "{ts_column}"') if ts_column else ""
    query        = f"SELECT * FROM {full_table} {where_clause} {order_clause}".strip()

    if verbose:
        print(f"  → Connecting to QuestDB at {host}:{port} (db: {dbname}) ...")
        print(f"  → Query: {query[:120]}{'...' if len(query) > 120 else ''}")

    conn = psycopg2.connect(
        host=host, port=port, user=user, password=password,
        dbname=dbname, connect_timeout=30,
    )
    conn.autocommit = True

    tmp_fd, tmp_jsonl = tempfile.mkstemp(suffix=".jsonl", prefix="pfc_questdb_")
    os.close(tmp_fd)

    row_count = jsonl_bytes = 0

    try:
        cur = conn.cursor()
        cur.execute(query, params or None)
        col_names = [desc[0] for desc in cur.description]

        if verbose:
            print(f"  → Columns ({len(col_names)}): {', '.join(col_names[:8])}"
                  f"{'...' if len(col_names) > 8 else ''}")
            print(f"  → Streaming rows (batch: {batch_size:,}) ...")

        with open(tmp_jsonl, "w", encoding="utf-8") as fout:
            while True:
                batch = cur.fetchmany(batch_size)
                if not batch:
                    break
                for raw_row in batch:
                    row = {}
                    for col, val in zip(col_names, raw_row):
                        if isinstance(val, datetime):
                            val = val.isoformat()
                        elif isinstance(val, bytes):
                            val = val.hex()
                        row[col] = val

                    # Ensure pfc_jsonl timestamp index works regardless of column name
                    if ts_column and ts_column in row and "timestamp" not in row:
                        row["timestamp"] = row[ts_column]

                    line = json.dumps(row, ensure_ascii=False) + "\n"
                    fout.write(line)
                    jsonl_bytes += len(line.encode("utf-8"))
                    row_count   += 1

                if verbose and row_count % 100_000 == 0:
                    print(f"     {row_count:,} rows  ({jsonl_bytes / 1_048_576:.1f} MiB) ...")

        cur.close()

        if verbose:
            print(f"  → Exported {row_count:,} rows  ({jsonl_bytes / 1_048_576:.1f} MiB JSONL)")

        if row_count == 0:
            if verbose:
                print("  → 0 rows in result — no .pfc written.")
            return {"rows": 0, "jsonl_mb": 0, "output_mb": 0, "ratio_pct": 0, "output": str(output_path)}

        if verbose:
            print(f"  → Compressing ...")

        proc = subprocess.run(
            [pfc_binary, "compress", tmp_jsonl, str(output_path)],
            capture_output=True, text=True,
        )
        if proc.returncode != 0:
            raise RuntimeError(
                f"pfc_jsonl compress failed (exit {proc.returncode}):\n{proc.stderr.strip()}"
            )

        jsonl_mb  = jsonl_bytes / 1_048_576
        output_mb = output_path.stat().st_size / 1_048_576
        ratio_pct = (output_mb / jsonl_mb * 100) if jsonl_mb > 0 else 0.0

        if verbose:
            print(
                f"  ✓ {row_count:,} rows  |  "
                f"JSONL {jsonl_mb:.1f} MiB  →  PFC {output_mb:.1f} MiB  "
                f"({ratio_pct:.1f}%)  →  {output_path.name}"
            )

        return {
            "rows":      row_count,
            "jsonl_mb":  jsonl_mb,
            "output_mb": output_mb,
            "ratio_pct": ratio_pct,
            "output":    str(output_path),
        }

    except Exception:
        conn.close()
        raise
    finally:
        if os.path.exists(tmp_jsonl):
            os.unlink(tmp_jsonl)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def build_parser():
    parser = argparse.ArgumentParser(
        prog="pfc-export-questdb",
        description="Export QuestDB tables to PFC cold-storage archives.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
examples:
  pfc-export-questdb --host quest.example.com --table trades --output trades.pfc
  pfc-export-questdb --host quest.example.com --table sensor_data \\
    --ts-column timestamp --from-ts "2024-01-01T00:00:00" --to-ts "2024-02-01T00:00:00" \\
    --output sensor_jan2024.pfc --verbose

Install pfc_jsonl binary first:
  curl -L https://github.com/ImpossibleForge/pfc-jsonl/releases/latest/download/pfc_jsonl-linux-x64 \\
       -o /usr/local/bin/pfc_jsonl && chmod +x /usr/local/bin/pfc_jsonl
        """,
    )
    parser.add_argument("--version", action="version", version=f"pfc-export-questdb {__version__}")
    parser.add_argument("--host",       required=True, help="QuestDB hostname or IP")
    parser.add_argument("--port",       type=int, default=8812, help="PostgreSQL wire port (default: 8812)")
    parser.add_argument("--user",       default="admin",        help="Username (default: admin)")
    parser.add_argument("--password",   default="quest",        help="Password (default: quest)")
    parser.add_argument("--dbname",     default="qdb",          help="Database name (default: qdb)")
    parser.add_argument("--table",      required=True,          help="Table name to export")
    parser.add_argument("--ts-column",  default=None, metavar="COL",
                        help="Timestamp column for time-range filtering and ORDER BY")
    parser.add_argument("--from-ts",    default=None, metavar="ISO_DATETIME",
                        help="Start of time range (ISO 8601, inclusive)")
    parser.add_argument("--to-ts",      default=None, metavar="ISO_DATETIME",
                        help="End of time range (ISO 8601, exclusive)")
    parser.add_argument("--output",     default=None, metavar="FILE",
                        help="Output .pfc file (default: {table}.pfc or {table}_{from}_{to}.pfc)")
    parser.add_argument("--batch-size", type=int, default=10_000, metavar="N",
                        help="Rows per fetch batch (default: 10,000)")
    parser.add_argument("--verbose", "-v", action="store_true")
    parser.add_argument("--pfc-binary", default=None, metavar="PATH",
                        help="Path to pfc_jsonl binary (default: auto-detect)")
    return parser


def main():
    parser = build_parser()
    args   = parser.parse_args()

    try:
        pfc_binary = find_pfc_binary(args.pfc_binary)
    except FileNotFoundError as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        sys.exit(1)

    if not pfc_binary:
        print("ERROR: pfc_jsonl binary not found. Install from "
              "https://github.com/ImpossibleForge/pfc-jsonl/releases", file=sys.stderr)
        sys.exit(1)

    if args.output:
        output_path = Path(args.output)
    else:
        parts = [args.table]
        if args.from_ts:
            parts.append(args.from_ts.replace(":", "").replace("-", "").replace(" ", "T")[:15])
        if args.to_ts:
            parts.append(args.to_ts.replace(":", "").replace("-", "").replace(" ", "T")[:15])
        output_path = Path("_".join(parts) + ".pfc")

    try:
        stats = export_to_pfc(
            host=args.host, port=args.port, user=args.user,
            password=args.password, dbname=args.dbname,
            table=args.table, output_path=output_path, pfc_binary=pfc_binary,
            ts_column=args.ts_column, from_ts=args.from_ts, to_ts=args.to_ts,
            batch_size=args.batch_size, verbose=args.verbose,
        )
        if not args.verbose:
            if stats['rows'] == 0:
                print(f"Done: 0 rows in result — no .pfc written.")
            else:
                print(f"Done: {stats['rows']:,} rows  →  {stats['output']}  ({stats['ratio_pct']:.1f}% of JSONL)")
    except Exception as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
