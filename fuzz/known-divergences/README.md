# Known open divergences (p1-lanel, datetime family)

Inputs that panic their differential target TODAY — kept OUT of corpus/
(which must replay clean) until fixed. Each entry: replay with
`cargo +nightly fuzz run <target> known-divergences/<file>`.

## interval-decode-sqlstd-dterr-1-vs-2  (target: interval_engine_diff)
DecodeInterval, IntervalStyle=sql_standard, range=HOUR|MINUTE (0x2800000):
C returns DTERR_BAD_FORMAT (-1 -> 22007), Rust returns
DTERR_FIELD_OVERFLOW (-2 -> 22015). Found 2026-07-31, fuzzer exec ~145k.
Error-code plane only (both sides reject). NOT yet root-caused; NOT yet
ground-truthed against postgres:18.3 at the SQL level (engine-level C
oracle is verbatim 18.3, so the C side is presumptively PG's behavior).
Minimal known repro needs the long multi-field tail; short forms agree.
Owner: adt/adt_datetime routes row DecodeInterval (status blocked on this).
