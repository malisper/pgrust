# Solver-cost triage: the measured cost model and proof-engineering laws

Measured on Kani 0.67.0 / CBMC 6.x, aarch64, 2026-07. This file is the
distilled methodology of the proof campaign: what makes a C≡Rust
equivalence harness cheap or impossible, learned by measurement rather
than guesswork. Times below are local solves under a 6 GiB RSS watchdog
unless noted; "@40GB" marks the high-memory retry tier.

## The cost model (measured, not guessed)

SAT cost tracks the **arithmetic circuit structure**, not input-space size
(all proofs quantify over full 2^32–2^96 spaces; that part is free).

- **FAST (ms–seconds, exhaustive)**: comparisons, branches, table lookups,
  xor/rotate/shift/add, multiplies by small constants; loops bounded ≤ ~32.
  Measured: pg_utf8_islegal full contract sweep 0.25s solver time;
  j2day (one `%7`) sub-second.
- **WALL (minutes–never)**: chained division/modulo by large non-power-of-2
  constants, symbolic×symbolic multiplies (with exceptions below),
  unbounded/data-dependent loops.
  Measured: j2date (`/146097`, `/1461`, `%365`, `*2141/65536` chain) —
  11+ CPU-minutes with no verdict, killed.

Triage rule: read the body. No `/`/`%` by large constants ⇒ fast class.
Divider chain ⇒ wall class; use the escalation ladder or exclude.

Refinements, each measured:

- **Multiply refutation**: symbolic×symbolic multiply up to 64×64 with
  checked overflow is kissat-FAST (1–3s, `--no-assertion-reach-checks`) —
  an apparent "int8mul wall at 240s" was really Box\<PgError\> drop glue
  (`mem::forget` on the Err arm: 16.4s → 0.18s). Audit drop-glue before
  believing any fallible-op wall.
- **Division**: symbolic÷symbolic proves when the DIVIDEND is 16-bit
  (25–27s) regardless of divisor width; ≥32-bit dividends wall — spot-prove
  the danger set (x/0, INT_MIN/−1, /−1, %−1).
- **Sloped dividers**: small-constant dividers (/10, /100, /10000
  digit-emission style) are a SLOPED wall — cost ~2× per decimal digit of
  domain width; reliable 30s ceiling ≈ 1e7-wide magnitude bands. So
  /146097-class = don't negotiate; ≤5-digit constants = negotiable via
  magnitude case-split (+ mandatory coverage), plus concrete spot proofs
  for the wide regimes.
- **Band-immune dividers**: magnitude bands only work when a LOOP shrinks
  with magnitude (structural, via unwind). A loop-free single expression
  `x / 86_400_000_000` has a fixed circuit — assumes never shrink it.
  Signature: symex completes fast, cover batches ~1s, the assertion
  batch's SAT never returns. Treatment: spots + native differential.
- **Unwind slack is catastrophic**: dead unreachable copies of a divider
  loop still enter the formula — set unwind to the band's exact iteration
  count (unwind(9) solved in 10s where unwind(22) walled). Unwind slack
  also converts directly to RSS (an exact-fit unwind saved 3–6 GB per
  harness; unwind(24) RSS-killed where len+2 proved in 64–163s).
- **Symbolic length**: fixed-length harnesses scale linearly (~2.5s per
  12-byte hash-mix iteration; len 64+ reachable); symbolic length costs
  ~×2.4 per +16 bytes — ≤32 is the practical symbolic ceiling. Production
  shape: symbolic len ≤32 + fixed-len spot proofs at loop boundaries.
- **i128 multiply**: width is not the wall — the multiplicand's
  symbolic-contributor structure is. i128 constant-mul of ONE 32-bit
  symbolic value ≈ 5s; a multiplicand summing TWO symbolic contributors
  with one ≥32 bits walls even if the other is bounded 2^10. Case-split by
  LITERAL-zeroing one contributor — an *assumed* zero does NOT
  constant-fold (this generalizes: `kani::assume` never folds symbolic
  offsets or loop bounds; only literals do).
- **Float**: comparisons/selection/widening are FAST (0.12–0.25s incl
  NaN/Inf/±0, bit-exact via to_bits). Arithmetic cost tracks
  SIGNIFICAND-MULTIPLIER WIDTH: f32 add/sub/mul full-symbolic GREEN
  (6–13s); f64/mixed add/sub GREEN (12–20s); anything with a 53-bit
  multiply/divide — INCLUDING multiply-by-constant — walls. Power-of-two
  multiplies/divides (×0.5, /2.0) are full-domain provable. Float↔int
  conversions are NOT a wall (0.24–6.7s full-domain, rint ≡
  round_ties_even machine-checked).
- **Symbolic binary search** scales ~n^1.5 in TABLE LENGTH and is
  domain-independent — constraining the codepoint window does not help
  (3368-entry table = 107s regardless).
- **Table-search loops inside engines** set the real unwind floor, not
  input length: linear scans (~390 entries) are unwind-hostile walls;
  binary searches are fine at ~12. Grep the vendored C for unbounded
  for-loops before choosing unwind.
- **Multi-format sscanf cascades** are a heaviest-circuit class (~2.4× per
  symbolic byte, depth-bound; splits stop helping past per-length).

## Known blockers beyond arithmetic

- `libc::*` calls through FFI (e.g. pg_strcasecmp's tolower) — Kani has no
  libc model; prove the `pg_ascii_*` siblings instead.
- Allocation/globals/memory-context machinery in the core body — see the
  mcx-stubs recipe below, or factor a pure core out.
- SIMD/core::arch intrinsics are unsupported_construct (prove the scalar
  path or exclude).
- md5/sha: bounded but enormous (64–80 rounds × word width).
- Data-dependent C loops (exit depends on a precondition) hang symbolic
  execution silently without `#[kani::unwind(N)]` — looks like a solver
  wall, isn't one. C `while (x < 0) x += DAY` rotate loops are this in
  miniature (one spun to unwinding iteration 16040; with `unwind(5)`,
  3–6s — a bounded-dividend `%` is solvable, only full-width dividends
  are band-immune).

## The wall taxonomy (named wall classes, each with a remedy)

1. **Result-image walls**: any claim comparing a WRITTEN RESULT IMAGE at
   data-dependent offsets (string builders, escape output, textcat/substr
   check_bytes) walls on CNF width — every store at a symbolic offset is a
   byte-mux over the whole object. Scalar-verdict harnesses over the same
   code prove. Remedies: concrete image cells + a scalar projection
   theorem (a length-parity projection proved in 4s where image cells hit
   8–18 GiB); fixed-output-frame refactors; native differential.
2. **Derived-length copy walls**: a memcpy whose LENGTH IS COMPUTED BY THE
   CODE UNDER PROOF (pstrdup's strlen) is symbolic at formula-build time
   even when assumptions pin it — it havocs the whole static-heap array
   per SSA version (6.7–9.7 GiB at every cap/solver combo). Case-splitting
   the input does NOT help. Remedy: concrete spots, or per-length cells
   only where length is an explicit argument; explicit-extent memcpy shims
   where NUL-free by construction.
3. **Pointer-datum round-trip walls**: recovering a `&mut StringInfo` from
   a Datum keeps pointer-provenance checks live on every field access.
   The send direction (image READ through a datum) proves fine. Remedy:
   core-level harnesses that construct the StringInfo in-harness (recv
   proofs then pass with full symbolic payloads, 77–127s), or C-shim
   pointee reads.
4. **Allocator (Mcx) walls**: memory-context arena machinery in the
   formula is a symex wall (MemoryContext::new_bump ALONE >240s). Broken
   by the **mcx-stubs recipe**: stub allocate → static bump buffer,
   env::var → "0", OnceLock::get_or_init → recompute, `mem::forget` the
   vec/ctx at harness end. Gotchas, each independently rediscovered:
   grow + deallocate stubs are LOAD-BEARING whenever the core can reach a
   `try_reserve` grow branch; a `std::fmt::format` stub and an explicit
   `kani::unwind` are needed when StringInfo construction is reachable;
   `.unwrap()` on PgResult walls symex (Box\<PgError\> Debug + drop glue) —
   forget + static-panic instead. Theorem qualifier: "modulo static-buffer
   allocator model" (allocation strategy is not part of any equivalence
   claim). Tiny proof heaps (64 KiB → 2 KiB) are load-bearing for
   small-alloc families. Token-context variants beat real bump contexts
   (3.5–16s vs RSS-kill).
5. **Std-Vec walls**: per-byte Vec push/extend walls symex (~30s at
   concrete len=2). Single-set_len unsafe-write loops prove fine. Prefer
   slice cores; where Vec stays, reserve+write+set_len beats push loops.
6. **Dead symbolic bytes**: unused `kani::any()` slots (elements beyond
   nelems, oversized shared buffers) inflate CNF to RSS-kill — a shared
   104-byte image buffer killed harnesses a 48-byte per-type buffer solved
   in 75s. Zero-fill unused slots with literals; assume-constraining does
   not substitute.
7. **Memory walls are CAP-RELATIVE**: record the RSS cap with every wall
   verdict. A high-memory retry converted an entire "structurally walled"
   16-byte-element array family into 7/7 proved (721–1623s @40GB), and
   turned other memory walls into plain SAT timeouts. The retry tier is
   the standard escalation before any refactor. Likewise, "needs a
   refactor" verdicts should be re-tested against the concrete-cell scheme
   first — two ledger families cleared that way with zero code changes.

## Harness patterns that work

- **Wrapper-level proofs**: construct a LocalFcinfo frame in the harness
  and call the shipped `fc_*` builtin directly — datum unwrap/pack is then
  inside the theorem. Negligible cost for by-value datums (0.06–0.1s).
  The SQL OVERLAPS 13-arm null-logic proved full 4×i64 × null-cube in 1.4s
  through the real fcinfo null protocol — nullable-protocol wrappers are
  cheap, not a wall class.
- **Varlena comparators**: reduce to (ptr,len) pairs with a pre-detoasted
  fence (the C caller contract post PG_GETARG_BYTEA_PP). Symbolic lens
  both sides at cap 8 is cheap (0.2–2.4s); no per-length split needed.
- **Seam stubbing** (state the code reads but the theorem doesn't cover):
  stub BOTH sides to one symbolic value universally quantified over the
  seam's full output range (a tz-offset seam at ±86400s ⊇ any real tz) —
  the proof covers ALL possible seam outputs with only seam internals
  excluded. ALWAYS add a skew control proving the seam model is
  load-bearing (feed the two sides different seam values; the harness
  must fail). Works for tz lookups, catalog rows (pg_amop), membership
  oracles, locale fields, sequence state, decompose seams
  (timestamp2tm → literal tm + shared symbolic fsec; the recompose
  direction stays in-theorem and constant-folds at the literal tm).
  Seam cost itself is ~free; circuit cost rules unchanged. Locale caveat:
  seam FIELDS that feed loop bounds or image offsets must be literal
  cells — symbolic frac_digits/posn/sep are CNF-width poison.
- **One-symbolic-index grids**: spot grids must use ONE SYMBOLIC INDEX
  into a concrete table, not a loop through the wrapper (loop symex grows
  superlinearly: 300s → 6–7s for the same theorem).
- **Literal planes with vacuity traps**: literal struct fields DO
  constant-fold through by-ref image store/reload and prune guarded call
  trees. Pair a C-side out-of-plane trap flag with a Rust-side panicking
  stub so plane vacuity is a loud FAIL.
- **Fences**: checked_mul/checked_add fences cost ~25× over
  constant-bound fences (105s → 4s) — fence with constants. Fence
  unreachable-aggregate counters (e.g. `0 <= n < 2^61` where a 3-counter
  sum at 2^62 overflows).
- **Coverage discipline**: `kani::cover!` witnesses prove fast-path
  regimes are actually reachable inside a symbolic harness — cheap
  vacuity insurance. But hoist covers into ONE shared regime harness:
  each inline cover is an extra SAT call, and each reach-check on a heavy
  circuit costs a full solve (~30–40s) — prefer
  `--no-assertion-reach-checks` + a dedicated cover harness.
- **Solver choice is strictly per-harness**, even within one family.
  Measured inversions in BOTH directions (kissat 53–72s where default
  walls; default green where kissat false-fails unwinding assertions;
  CaDiCaL wedging on formulas kissat solves in 2–3s). Try the other
  solver before diagnosing any wall.
- **`--no-unwinding-checks`** is legal ONLY when loop bounds are
  input-independent (structural ceilings), and any FAILED under this
  regime must be replayed natively before it means anything.
- **C shim hygiene**: whole-family `--c-lib` linking drags every
  sibling's dispatch tables into each goto program (~45s fixed read cost
  that looks like a solver wall) — prune per-harness C files and hoist
  table-wiring asserts into one dedicated wiring theorem. C harness
  allocators must be individually-NAMED static structs — pooled slots
  kill field sensitivity (11k symex paths → 9.9s). Byte-punned
  cross-language reads need typed staging (the memcpy builtin silently
  truncates under tight unwinds). Growth-exact dest caps per engine
  direction (D = maxgrowth·L+1); an undersized D fails as OOB — classify
  OOB/unwinding failures as harness defects before suspecting divergence.

## Verdict discipline (how a divergence gets recorded)

- **GROUND-TRUTH LAW**: before recording any divergence, reproduce it
  against a REAL running C Postgres on the TARGET platform — and on more
  than one platform when libc-flavored. Real instances: tidin empty-field
  acceptance is glibc-vs-BSD strtoul variance; C `char` signedness splits
  Linux-aarch64 from macOS/x86-64 inside C Postgres itself; interval FMA
  contraction is a compiler-codegen platform split. A vendored-model
  counterexample alone is not evidence, and one platform is not enough.
- **Native differential as the cheap adjudicator**: a bin target linking
  the SAME vendored C (build.rs cc bundle; the bin needs a
  `use <libcrate> as _;` or extern symbols go unresolved) runs millions of
  checks in seconds (8–20M checks typical). Weaker than proof, still
  census-grade; recorded as `tested(differential)`, never `proved`.
- **FAILED-at-tight-unwind is never a divergence** without native replay —
  CBMC fabricates counterexamples from truncated loops ("Not unwinding
  loop" decodes as garbage). Several walls' FAILED verdicts were refuted
  by exhaustive native tables over the exact failing domains.
- **A FAILED with no decodable playback is a WALL, not a witness** —
  expected-fail witnesses can memory-wall before producing their
  counterexample; narrow the plane until playback emits.
- **Gate integrity**: a must-fail control can fire for the WRONG reason
  (one skew control "passed" via an unrelated memcmp unwind artifact).
  Verify the failing CHECK NAME of every expected-fail, not just the
  verdict. A control that verifies SUCCESSFUL is a broken gate
  (vacuous-pass — the worst outcome; run-suite.sh treats it as fatal).
- **Judge near-threshold verdicts on CBMC's reported Verification Time**,
  not wall clock — cargo-kani overhead is 5–20s and load inflates
  identical harnesses up to 2.6×. Under load, a too-tight wall-clock cap
  fabricates walls (7s solves became 32s kills).
- **Environment failures are not verdicts**: goto .out binaries can fill
  a disk (fake rc=101/timeouts that were ENOSPC); "CBMC failed with
  status 15" is a killed/self-aborted solver, not a verdict; RSS
  watchdogs must sum the FULL descendant process tree.
- **Method blind spot (recorded deliberately)**: equivalence proofs find
  PORTING errors, never INHERITED ones — pgrust matching a buggy C
  operator proves green. One shipped pgrust bug (NaN-unaware GiST box
  adjustment) was itself the pre-fix form of an upstream C fix; the proof
  campaign caught it only because the vendored C was current.

## Tool defects found (verification-tool bugs, not pgrust or PostgreSQL)

Each carries a standing witness harness in-tree so a fixed toolchain is
detected:

1. **CBMC non-canonical NAN constant**: CBMC models `<math.h>` NAN with a
   non-canonical payload, fabricating NaN-payload divergences real silicon
   does not have (native: both sides 0x7ff8000000000000). Fix: shim-level
   `#define NAN` pinned to the canonical quiet NaN; regression kept in
   proofs/geo-cmp/tests/replay.rs.
2. **Kani/CBMC f64::sqrt is NONDETERMINISTIC PER CALL**:
   `x.sqrt() != x.sqrt()` is satisfiable (witness:
   proofs/float-agg probe_sqrt_self_determinism, 3.2s). No shared-symbol
   canonicalization can help. Fix: a deterministic in-model sqrt stubbed
   on BOTH sides (C `#define sqrt` → pg_proof_sqrt; Rust
   `#[kani::stub(f64::sqrt, ...)]`; note goto-cc does NOT define
   `__CPROVER__` in `--c-lib` preprocessing, and the harness must
   reference the no_mangle symbol or Kani won't codegen it). Claims read
   "modulo deterministic sqrt model"; native differential with real libm
   re-checks (8.03M checks, 0 diffs).
3. **Kani 0.67 Box provenance defect**: reads through
   `Err(Box<PgError>)` of a `Result<f64, _>`-returning fn corrupt Box
   provenance (garbage fields + spurious dealloc failures) — anywhere
   inside the call chain, even when the harness-visible type is
   `Result<Datum, _>`. `Result<f32,_>`/`Result<u64,_>` transports are
   clean; route verdict-only Err + C-errcode covers. proofs/float-arith
   carries a permanent MUST-FAIL/MUST-PASS witness pair — when the
   witness flips, the excluded grids can return.
4. **CBMC hypot contraction gap**: shipped Rust fuses `1.0 + yx*yx` via
   mul_add (matching aarch64 C codegen) but CBMC's C model does not
   contract — the general hypot path must be fenced from every theorem or
   it fabricates divergences.
5. **CBMC pointer-arith binary search**: builtin strcmp against
   `base + ((last - base) >> 1)` pointers cannot be bounded — unwinding
   assertion fails unwind-INSENSITIVELY with a deref cascade. Remedy:
   index-based C shim projection or native differential.

## Scope notes

- Whole-family exclusion reasons used in the ledger: `excluded(state)`
  (catalog/shared-state readers), `excluded(engine)` (executor/planner/
  parser engines, window functions, triggers, AM handlers),
  `excluded(typcache)` (polymorphic dispatch — though per-concrete-type
  instantiation proofs work and are recorded where done),
  `excluded(wall: ...)` (a named wall class above). Comparator families
  reputed "unprovable" repeatedly turned out to be arithmetic-only walls:
  numeric comparisons proved with the packed on-disk header decode
  in-theorem (17–28s), float comparators are fast-class, geo epsilon
  comparators proved wrapper-level (31 rows) — the wall reputation
  belongs to arithmetic, not the type.
- The suite tiers in SUITE.tsv encode the budget: per-commit (<10s
  documented), release-gate (10–30s), calibration (>30s greens),
  defect-witness (must-fail controls, run with every tier), unmeasured
  (no recorded time yet).
