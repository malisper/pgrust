# Banked seeds for the future `formatting_diff` target (adt/formatting)

There is **no** `fuzz/fuzz_targets/formatting_diff.rs` yet. These files are
banked seeds, not a live corpus: a lane that scaffolds the adt/formatting
differential target (`to_date` / `to_timestamp` / `to_char` against the
PostgreSQL **18.3** oracle — never `:latest`, never 18.4) should adopt this
directory as its starting corpus.

Encoding: each file is `input_text` `\0` `format_picture` — the two arguments
of `to_date(text, text)` / `to_timestamp(text, text)`. Adjust when the target's
real input grammar is fixed; the point is the *values*, not the framing.

## Provenance

Banked by lane `fix/y-yyy-range` (2026-07-30) while fixing `DCH_Y_YYY`
millennia range-checking. See `notes/y-yyy-range-lane.md`.

Why this class is worth seeding: `DCH_Y_YYY` is the only numeric DCH field C
parses with a raw `sscanf(s, "%d,%03d%n", ...)` (formatting.c:3589) instead of
`from_char_parse_int_len`. The unbounded `%d` destroys the magnitude of an
out-of-`int` millennia field before the `pg_mul_s32_overflow` /
`pg_add_s32_overflow` guard on formatting.c:3597 can see it. `%d` overflow is
formally UB in C, so C's observed acceptance is a glibc accident (`strtol`
saturates at `LONG_MAX`, the assignment to `int` truncates mod 2^32).

pgrust **deliberately diverges** here and rejects; the `y_yyy_wrap_*` and
`y_yyy_saturate_longmax` seeds are therefore **expected-divergence** cells, not
bugs. A differential target must carve them explicitly (they are recorded on
ledger oids 1778 / 1780 in `proofs/USER_FACING_FUNCTIONS.tsv`) or it will
report them as mismatches forever. Everything else in this directory is
expected to match C byte-for-byte including SQLSTATE.

## Coverage note

All 11 adt/formatting ledger rows (oids 1768, 1770, 1772-1778, 1780, 2049) are
`excluded(wall: format-picture engine ...)`, and there is no fuzz target — so
**neither** campaign instrument covers this code today. That is why the
`fix/y-yyy-range` change was gated by a hand-built docker-18.3 differential
plus unit pins instead.
