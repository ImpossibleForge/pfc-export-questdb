# Changelog — pfc-export-questdb

## v0.1.0 (2026-04-23)

Initial release. Extracted from pfc-migrate v1.2.0 as a standalone dedicated tool.

### Features
- Export full QuestDB tables to `.pfc` archives
- Time-range filtering via `--ts-column`, `--from-ts`, `--to-ts`
- Auto-generated output filenames when `--output` is omitted
- No schema argument (QuestDB has no schema concept — tables referenced directly)
- Configurable batch size via `--batch-size` (default: 10,000)
- Timestamp alias: ts-column value copied to `"timestamp"` field for pfc_jsonl index compatibility
- 0-row export handled gracefully (exit 0, no empty `.pfc` written)
- Enterprise-ready error handling: wrong host/port/credentials/table all produce clear `ERROR:` messages

### Tested
- 14/14 PASS — happy path, stress (50k rows, ~17,400 rows/s), errors, resilience (DB down mid-test)
