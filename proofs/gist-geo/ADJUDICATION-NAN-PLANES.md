# Adjudication package: gist-geo NaN planes (2 divergence candidates)

Decode lane, 2026-07-29.

RESOLUTION: both candidates were adjudicated as pgrust bugs — regressions
of upstream commit 1acf757255 ("Fix GiST index build for NaN values in
geometric types", bug #14238, back-patched to 9.2) — and are FIXED:
`adjust_box` uses the NaN-aware float8_lt/float8_gt comparisons
(crates/backend/utils/adt/geo/src/lib.rs, with a unit test
adjust_box_adopts_nan_like_c) and fc_gist_box_same compares via float8_eq
(crates/backend/access/gist/gistproc/src/lib.rs). The analysis below is
the original decode record.

## Candidate 1 (NEW, CONFIRMED): adjustBox NaN adoption — gist_box_union

Fleet eq_gist_box_union_n2/n3 bit-exact FAILs are REAL, not harness debt.

Mechanism: C gistproc.c adjustBox grows the union box with the NaN-aware
float.h comparators — `float8_lt(b->high.x, addon->high.x)` is TRUE when
b is finite and addon is NaN, so C ADOPTS NaN coordinates into the union.
Shipped Rust `adt_geo::adjust_box` (crates/backend/utils/adt/geo/src/
lib.rs:166) uses raw `<`/`>` — `finite < NaN` is false, so Rust KEEPS the
finite bound.

Evidence:
  - Decoded counterexample (local kani playback, eq_gist_box_union_n2):
    inputs include coordinate bits 0xFFFFFFFFFFFFFFFF (NaN) against
    0xBFFFFFFFFFFFFFFB (finite); first failing check =
    rbox.high.x bit-compare.
  - Native replay: C float8_lt(finite, NaN) = true vs Rust
    `finite < NaN` = false — divergence concrete.

Reach: adjust_box feeds gist union, picksplit, and penalty bounding boxes
(gistproc/src/lib.rs:214,268,274,408,503,513) — index shape/keys diverge
for NaN-containing geometry. Same NaN-aware-comparator family as
candidate 2; a single fix (use float8_lt/gt-equivalent NaN-aware
comparisons in adjust_box) would close both union rows.

## Candidate 2 (witness now decodable): gist_box_same NaN plane

probe_gist_box_same_nan_plane originally "expected-fail-ok" on the fleet
for the WRONG reason (memcmp unwind artifact), then timed out at the 75s
kissat cap (wall, not witness). After the unwind(12->34) repair it FAILS
on the INTENDED check (r == cres, src/lib.rs:556) in 6.8s with the
default solver — C float8_eq(NaN,NaN) = true vs shipped raw `==` false.
Witness stands; adjudication owed together with candidate 1 (same
comparator family).
