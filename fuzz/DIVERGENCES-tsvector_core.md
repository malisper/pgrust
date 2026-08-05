# DIVERGENCES — tsvector_core_diff (lane p1-laneae)

Status: found during harness bring-up smoke tests, 2026-07-31. NOT yet
ground-truthed against `postgres:18.3` Docker (coordinator to adjudicate;
vendored-oracle evidence only, per the ground-truth law).

## KNOWN-DIVERGENCE-1: tsvectorrecv needSort path — datum IMAGE layout skew

Input (wire bytes, arm 1): two lexemes out of order —
`00000002 "bb\0" 0000 "aa\0" 0000`
(hex: `00 00 00 02 62 62 00 00 00 61 61 00 00 00`)

- C (tsvector.c tsvectorrecv, upstream 447-553): when entries arrive
  unsorted (`needSort`), sorts ONLY the WordEntry array in place with
  `qsort_arg(ARRPTR(vec), ..., compareentry, STRPTR(vec))`; the lexeme
  string storage keeps WIRE order. Result payload:
  `[2][entry(aa)->pos=2][entry(bb)->pos=0]["bbaa"]`.
- Rust (io.rs tsvector_recv_core, "Rare wire case: rebuild via sort on a
  decoded view"): rebuilds the whole image with storage in sorted order:
  `[2][entry(aa)->pos=0][entry(bb)->pos=2]["aabb"]`.

Decoded CONTENT is identical (same sorted entries, lexemes, positions);
the stored DATUM BYTES differ. Anything hashing or memcmp-ing the datum
(binary COPY round-trip byte identity, datum-image equality paths) sees
different bytes than C would produce.

Also latent in the same path: C's qsort_arg is UNSTABLE while Rust's
`sort_by` is STABLE — a wire message with DUPLICATE lexemes may order the
duplicate entries differently, which would be a SEMANTIC divergence (the
harness's semantic plane panics on it; none observed yet).

Harness handling: strict image compare first; on mismatch a decoded-content
comparison (`tsvec_semantic_eq`) must pass, so only *semantic* skew aborts
the fuzzer. Triage class: pgrust-bug candidate (representation parity), low
severity; fix would be to mirror C (sort entries in place, keep storage
order) in tsvector_recv_core.

## DIVERGENCE-2: tsvectorin position overflow — C atoi WRAPS, Rust SATURATES

Input (arm 0 text): `a b:89,00020069458489`
- C (tsvector_parser.c INPOSINFO): `WEP_SETPOS(pos, LIMITPOS(atoi(str)))` —
  atoi is (int)strtol: 20069458489 truncates to int 2889589305→(int)-1405377991
  ... net effect after `& 0x3fff`: position **8761** (bytes 39 22).
- Rust (parser.rs InPosInfo): `saturating_mul/add` then `limitpos` —
  position **16383** (0x3fff).

Reachable from plain SQL: `SELECT 'b:20069458489'::tsvector`. Real PG's
behavior is the integer-truncating cast (glibc and macOS agree: 64-bit
long strtol exact, cast to int truncates), so pgrust's saturation is a
functional divergence on positions with numeric value >= 2^31. NEEDS
Docker `postgres:18.3` ground-truth + adjudication (match-the-UB vs
document-as-improvement); the same `atoi`-shaped parse exists in the
tsquery parser (p1-laneaf's crate — flag to that lane).

Harness handling until adjudicated: inputs whose digit-runs exceed the
int32 range are SKIPPED (documented executable carve in
`has_overflowing_number`); everything below 2^31 stays on the strict
image plane (both sides clamp via LIMITPOS above 16383 identically).

## DIVERGENCE-1b: recv needSort duplicate-lexeme TIE ORDER (stable vs unstable)

Confirmed live (fuzzer, arm 1): a wire message with many duplicate/empty
lexemes arriving unsorted. C's needSort qsort_arg is UNSTABLE — equal
lexemes' entries (with different position lists) land in an
implementation-defined order; Rust's stable sort keeps wire order. The
decoded ENTRY SEQUENCE differs, the (lexeme, positions) MULTISET does not.
Same fix locus as KNOWN-DIVERGENCE-1 (tsvector_recv_core needSort path).
Also note: tsvectorrecv on BOTH sides accepts duplicate lexemes from
binary input without dedup — the result violates the ts_type.h sortedness
/uniqueness contract in both engines alike (upstream-parity, no action).

Harness handling: the recv semantic plane is a SORTED-MULTISET gate over
(lexeme, positions) pairs (GL-PARMERGE-1 within-tie precedent); position
LISTS stay order-strict.

---

## ADJUDICATION (lane coordinator, 2026-07-31 — Docker postgres:18.3, Debian aarch64)

- **DIVERGENCE-2: CONFIRMED pgrust-bug, FIXED.** Real PG 18.3:
  `SELECT 'b:20069458489'::tsvector` → `'b':8761`;
  `'a b:89,00020069458489'` → `'a' 'b':89,8761`. pgrust saturated to 16383.
  Fix: parser.rs InPosInfo now reproduces `(int)strtol` exactly (i64
  saturating accumulate = strtol LONG_MAX saturation, truncating cast to
  i32 = the (int) wrap, signed LIMITPOS, 14-bit mask). Regression tests
  `tsvector_position_atoi_wrap` + corpus seeds `seed-regr-atoi-*`
  (incl. `b:4294967296` → wrong-position error, `b:99…9`(20 digits) →
  strtol-saturation band → 16383). Harness carve `has_overflowing_number`
  RETIRED — strict image plane restored. Same atoi shape flagged to
  p1-laneaf for the tsquery parser.
- **KNOWN-DIVERGENCE-1: CONFIRMED pgrust representation bug, FIXED.**
  Ground-truth: binary-COPY'd the unsorted wire message into postgres:18.3,
  read the tuple with pageinspect: datum = entries SORTED (aa→pos2, bb→pos0),
  storage in WIRE order (`"bbaa"`). pgrust rebuilt storage sorted. Fix:
  io.rs tsvector_recv_core needSort path now sorts ONLY the WordEntry words
  in place (C tsvector.c:550-552 parity). Regression test
  `tsvector_recv_needsort_storage_wire_order` + seed
  `seed-regr-needsort-wireorder`. Harness recv plane tightened to strict
  image compare.
- **DIVERGENCE-1b: RATIFIED NON-SURFACE (carve stands, narrowed).** Within-tie
  entry order for DUPLICATE lexemes on the needSort path: C qsort_arg vs
  Rust sort_unstable are different unstable algorithms; GL-PARMERGE-1
  within-tie precedent. Harness now requires byte-equality UNLESS the
  decoded content has duplicate lexemes AND the sorted-multiset gate passes.
  (Duplicate lexemes from binary input violate the sortedness/uniqueness
  contract identically in both engines — upstream-parity.)

---

## DIVERGENCE-3 (fleet re-floor, 2026-07-31): uniquePos kept-weight at the
## 16383/MAXNUMPOS break — third instance of the pg_qsort tie class. FIXED.

Found by fleet job pgrust-fuzz-campaign-1785518399-3483-18016 (crash-afa0fa20…,
banked as corpus seed-regr-uniquepos-tieweight): a lexeme with >7 positions
including several that clamp to 16383 with different weights. C's uniquePos
(tsvector.c) sorts positions with qsort keyed on POSITION ONLY, then the dedup
loop BREAKS immediately after advancing onto a 16383 position — so the kept
entry's weight is whichever equal-position element the sort put first. Rust's
stable sort kept a different one (0x3fff vs C 0xffff in the image).

Docker postgres:18.3 ground truth (tie order is real PG behavior):
  'w:1,2,3,4,5,6,7,16384,20000A'  -> 'w':1,2,3,4,5,6,7,16383
  'w:1,2,3,4,5,6,7,20000A,16384'  -> 'w':1,2,3,4,5,6,7,16383A

Fix: crates/backend/utils/adt/tsvector_core/src/qsort.rs (verbatim pg_qsort
port, per-crate-copy convention) now used at ALL THREE C qsort sites in this
crate: uniquePos (position-only key), uniqueentry (lexeme key; index-
permutation variant since EntryIn is not Copy), and tsvectorrecv needSort.
The recv duplicate-lexeme multiset gate is RETIRED — recv image plane is now
fully strict. array_to_tsvector's qsort ties are byte-identical elements
(no observable order) — no port needed, documented here.

Also from the same fleet round: the ungated oversize arm collapsed throughput
to ~190 exec/s (each exec parses ~1.2 MiB on four sides) and blew the 3600 s
job deadline once before the divergence surfaced. The arm is now gated on a
4-byte magic ("OVSZ") so only its committed seeds pay the cost; it remains a
deterministic boundary witness replayed by every coverage capture.

Crate regression tests: tsvector_uniquepos_tie_weight_pg_qsort_parity.
