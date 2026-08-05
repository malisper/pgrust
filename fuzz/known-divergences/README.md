# Known open divergences (p1-lanel, datetime family)

Inputs that panic their differential target TODAY — kept OUT of corpus/
(which must replay clean) until fixed. Each entry: replay with
`cargo +nightly fuzz run <target> known-divergences/<file>`.

## interval-decode-sqlstd-dterr-1-vs-2  (target: interval_engine_diff)
RESOLVED 2026-07-31 — ORACLE SHIM DEFECT, pgrust was RIGHT.
DecodeInterval, IntervalStyle=sql_standard, range=YEAR|MONTH (0x2800000;
the original note misread it as HOUR|MINUTE): C shim returned
DTERR_BAD_FORMAT (-1), Rust DTERR_FIELD_OVERFLOW (-2). Root cause: the
oracle wrapper pg_diff_decode_interval sized its ParseDateTime workbuf
`MAXDATELEN + 1` (129, date.c's frame) while real 18.3 interval_in uses
`char workbuf[256]` (timestamp.c:908); the Rust driver side used 153
(timestamp_in's frame) — NEITHER matched. The repro's fields+NUL bytes
total 130, so only the shim's C side hit ParseDateTime's buffer-full
DTERR_BAD_FORMAT arm; real PostgreSQL 18.3 (docker, Debian) parses on and
rejects with "interval field value out of range" (22015), agreeing with
pgrust. Fix: both sides now model interval_in's 256-byte frame (fix sha in
lane log). The input is banked as
corpus/interval_engine_diff/seed-resolved-sqlstd-workbuf-256 and this
artifact file is kept for the record; it replays clean.
