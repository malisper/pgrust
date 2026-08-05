# Fleet semantic-findings decode — 2026-07-30 (resume item 7)

Lane: proofs/fleet-decode-2026-07-30. Inputs: replicated full-suite runs
@3ade6dd7 (1067 pass / 58 efo / 6 semantic FAILs) and batches A+B
@994f9977; artifacts archived from the 7-day-lifecycle S3 prefix to
`~/pgrust-fleet-archive/kani-suite-20260730/` (120/120 objects, 360 MiB,
verified) before the ~Aug 4 expiry.

Every verdict below was re-derived from the archived fleet logs and
re-verified locally at main cbaa2c7117 (one kani at a time, kissat for
greens / default for expected-fails).

## 1. "char" signedness 6-row cluster — SPLITS (artifact x4 + known x2)

Rows: char/{eq_i4tochar, eq_text_char, charin_equiv,
charout_equiv_and_roundtrip}, hash-rows/{eq_hashchar, eq_hashcharextended}.
All six failed identically on BOTH replicated Linux-aarch64 runs; all were
equality-assertion FAILs (real value mismatches, not tool noise).

- **4 char rows = HARNESS ARTIFACT.** The mismatch lives in the C shim's
  `(int)(char)x` return widening — a seam real Postgres never exposes
  (Datum round-trips through DatumGetChar 8-bit truncation). Ground truth
  (docker postgres:18 Linux-aarch64 vs macOS psql, 2026-07-29): charout /
  chartoi4 / comparisons byte-identical across platforms. Fix (8-bit
  datum-value parity claim) was already in the curated tree; all four
  re-proved 0.42–0.97s.
- **2 hash rows = KNOWN platform split in C POSTGRES ITSELF** (char
  signedness: hashchar('\200') = 1361043915 on signed-char hosts vs
  1807103465 on Linux-aarch64). NOT a distinct pgrust defect. NEW on the
  v0.2 lineage: shipped fc_hashchar is the ZERO-extending arm
  (`as u8 as u32`) — matches deployment-platform (Linux-aarch64) C PG;
  the old adjudication doc said sign-extending (stale; addendum added).
  The stale pinned models model_*_signed_full FAILED against v0.2 shipped
  code — re-pointed to model_*_unsigned_full, both PROVED (0.23s/2.5s).
  eq_* stay fenced to the v>=0 portable plane (green on any host).
  Full package: proofs/char/ADJUDICATION-CHAR-SIGNEDNESS.md.

## 2. float-agg stddev/corr grids x3 — ARTIFACT (dsqrt dual-mode, sealed)

Fleet fail set @994f9977 = exactly the three sqrt-bearing finals
(grid_stddev_pop, grid_stddev_samp, grid_corr; vendored C lines
657/677/829); every sqrt-free grid passed. Fleet tip 994f9977 predates
the deterministic-sqrt-model fix (verified: 0 occurrences of the stub /
pg_proof_sqrt at that sha). Mechanism witness re-confirmed:
probe_sqrt_self_determinism (x.sqrt() != x.sqrt() SATISFIABLE, 3.26s) —
Kani/CBMC f64::sqrt is nondeterministic per call (TRIAGE tool defect #2).
With the in-tree det-sqrt stub all three grids PROVE: 3.9s / 5.4s / 16.0s.
sqrt VALUE parity stays owned by the native differential (8.03M, 0 diffs).
Ledger rows 1832/2513/2817 upgraded to proved(grid; modulo deterministic
sqrt model).

## 3. brin dist_time/timetz overflow x2 — ARTIFACT + witnessed candidate

Fleet FAILs (@994f9977 eq_dist_timetz; retry @33b04ba9 both) decode to
Rust debug-overflow-check panics inside shipped fc_dist_time/timetz
(builtins.rs:124/137) on OUT-OF-CONTRACT inputs (retry cex:
ta_time=-6606037682387444839) — fleet tips lacked the time fence. Shipped
RELEASE code wraps two's-complement exactly as C -fwrapv. Adjudication:
overflow-CHECK artifact, not a value divergence. Evidence: fenced
harnesses PROVE (eq_dist_time 0.37s, eq_dist_timetz 3.6s); native
differential (cargo run --release --bin native_dist_wrap): dist_time
full-i64 wrap plane 2.0M checks 0 diffs; dist_timetz in-bound zones 2.0M
checks 0 diffs.

**Zone-wrap divergence candidate — WITNESS BUILT** (no ruling filed):
C computes `(tb->zone - ta->zone)` in int32 (wraps under -fwrapv) BEFORE
the int64 multiply; shipped Rust widens each zone to i64 first. Value-
visible only for zone diffs outside i32 — unreachable from
timetz_in/timetz_recv-validated zones (|tz disp| <= MAX_TZDISP_HOUR).
Witnesses: probe_dist_timetz_zone_wrap FAILS on the exact parity check
(0.63s, check name verified) and the native replay reproduces 4/4
divergent points (e.g. za=INT32_MIN zb=INT32_MAX: rust=4294967295000000.0
vs c=-1000000.0). Expected adjudication: ratified-unreachable.

## 4. network/oracle unwind debt + vacuous control — CLEARED, gate verified

All were harness defects; fixes were in-tree, this lane re-verified each:
- network eq_inetpl_v4 / eq_inetmi_int8_v4: fleet "unwinding assertion
  memcmp.0 iteration 9" (result-image memcmp runs over the full 16-byte
  buffer even for v4). unwind 9 -> 18; PROVED 0.66s / 0.79s.
- oracle-compat eq_repeat_err_plane: concrete_varlena::<4> fill loop
  truncated at unwind(4) -> unwind 6; PROVED (48s kissat under load —
  watch the per-commit tier budget).
- oracle-compat eq_anychar_typmodout_band / spot_anychar_typmodout_wide:
  pg_ultoa_n digit loop truncated (iteration 4/6) -> unwind 12/14;
  PROVED 28.8s (release-gate) / 1.4s.
- control_varchar_clip_skew VACUOUS-PASS (broken gate): the old b"abcdef"
  input made BOTH sides fail with the same error class, so the skewed
  typmod was invisible to an error-class-only compare. Rebuilt on the
  fits-exactly plane (len 4 vs varchar(3)+1 skew flips the verdict);
  now FAILS on exactly "control: rig failed to detect a skewed typmod"
  (default solver; failing check name verified per gate-integrity law).

## Ledger deltas

USER_FACING_FUNCTIONS.tsv: 16 rows updated (33, 78, 446, 454, 944, 1245,
1622, 1832, 2513, 2630, 2632, 2817, 2914, 2916, 4630, 4632).
SUITE.tsv: 6 rows corrected (char x4 qualified names + claims, hashchar
x2 portable-plane notes) + 2 new pinned-model rows.
Code: proofs/hash-rows/src/lib.rs pinned models re-pointed
signed -> unsigned (v0.2 shipped arm).
