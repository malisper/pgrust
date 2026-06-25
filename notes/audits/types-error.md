# types-error — audit note

Infrastructure vocabulary crate, not a catalog unit; lightweight note instead
of the per-function audit format.

## Contents

- `error.rs` — `ErrorLevel` + level constants (`elog.h`), `SqlState`,
  complete `ERRCODE_*` table, `ErrorField` + `PG_DIAG_*`, the
  `make_sqlstate`/`unpack_sqlstate`/category `const fn` helpers
  (`elog.h` PGSIXBIT/MAKE_SQLSTATE macros).
- `pg_error.rs` — `PgError`, `PgResult<T>`, `ErrorLocation`,
  `SoftErrorContext`, `default_sqlstate_for_level` (elog.c default-SQLSTATE
  rule), copied from src-idiomatic `types/src/pg_error.rs`.

Trimmed from the src-idiomatic sources (above-layer or not vocabulary):

- `ErrorData` (carried `assoc_context: Option<Box<MemoryContextData>>`),
  `ErrorSaveContext`/`T_ErrorSaveContext` (PgNode/NodeTag), and
  `ErrorContextCallback`/`ErrorContextCallbackArg` — error-stack machinery
  belongs to the future error-reporting crate.
- `PgrustErrorData` and `PgError::from_raw_transfer` — raw carrier for the
  error-stack frames; goes with that machinery.
- `LOG_DESTINATION_*` — logging-output config, not error vocabulary.
- `ERRCODE_INVALID_ARGUMENT_FOR_LOGARITHM` — src-idiomatic alias that does not
  exist in errcodes.h; the real macro name is
  `ERRCODE_INVALID_ARGUMENT_FOR_LOG`.

## Constant verification

The `ERRCODE_*` table was regenerated mechanically from
`postgres-18.3/src/backend/utils/errcodes.txt` (the file from which the build
generates `errcodes.h`; no generated `errcodes.h` exists in the source tree),
preserving its order and class sections. Verified by a throwaway python script
that parses errcodes.txt and `src/error.rs`, computes the MAKE_SQLSTATE i32
encoding for both sides (encoding cross-checked against `elog.h`'s
PGSIXBIT/MAKE_SQLSTATE definitions), and diffs both directions:

- 268 codes in errcodes.txt, 268 `ERRCODE_*` consts in Rust.
- Missing from Rust: none. Extra in Rust: none. Value mismatches: none.
  (src-idiomatic had only 154 of the 268; the 114 absent codes were added.)
- The script also asserts every `ERRCODE_*` const uses the canonical
  `make_sqlstate(*b"…")` form so none escape the diff.

ErrorLevel constants verified against `elog.h` `#define`s: 16 names checked
(DEBUG5=10 … FATAL=22, PANIC=23, plus COMMERROR/PGWARNING/PGERROR aliases),
zero mismatches. Note PANIC is 23, not 24.

Result: PASS (2026-06-12).
