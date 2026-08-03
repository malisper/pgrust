# STUBS.md — the shared stub-pin facility for differential fuzz targets

Michael-ratified 2026-08-01. Four facilities that let a differential fuzz
target pin session state IDENTICALLY on the shipped-Rust side and the
C-oracle side, unlocking state-dependent carves:

| facility       | pins                                                        |
|----------------|-------------------------------------------------------------|
| `stub:guc`     | GUC scalars: extra_float_digits, DateStyle+DateOrder, IntervalStyle, standard_conforming_strings; md5_password_warnings + scram_iterations (`guc::pin_md5_password_warnings`/`guc::pin_scram_iterations`; Rust side = the crypt/auth_scram session cells via their installed GUC accessors, C side = `pg_stub_md5_password_warnings`/`pg_stub_scram_iterations`; first consumer + must-fail controls: crypt_be_diff `control_guc_md5_password_warnings_pin`/`control_guc_scram_iterations_pin`) |
| `stub:clock`   | GetCurrentTimestamp-shaped reads, to a fuzzed TimestampTz; MONOTONIC half: INSTR_TIME_SET_CURRENT / `pg_clock::mono_ns`-shaped reads, to a fuzz-derived ns sequence (`clock::pin_mono_ns`; Rust side = pg_clock's default-off `fuzz_mono_pin` feature this workspace enables, C side = `pg_stub_get_mono_ns`; first consumer + must-fail control: tsm_system_time_diff `control_clock_mono_pin`) |
| `stub:prng`    | the global-prng analog, seeded from the fuzz input; SCRAM-salt channel: the 16-byte pg_strong_random read inside pg_be_scram_build_secret (`prng::pin_scram_salt`; Rust side = the shipped `PGRUST_SCRAM_FIXED_SALT_B64` determinism hook, C side = `pg_stub_scram_salt` copied by the oracle's pg_strong_random shim; first consumer + must-fail control: crypt_be_diff `control_prng_scram_salt_pin`) |
| `stub:workmem` | work_mem / maintenance_work_mem ceilings                     |

Code: Rust half `core/src/stubs.rs`; C half
`core/csrc/stubshims/pg_stub_state.{c,h}` (registered in the main
`core/build.rs` cc::Build).

## The contract

1. **One derivation.** The target hands fuzz bytes to a `stubs::*::pin_*`
   function. The pin derives the canonical value ONCE, bounded to the
   setting's legal range (ranges taken from the shipped guc_tables / type
   domains: efd [-15,3], the 5x3 DateStyle/DateOrder pairs, the 4
   IntervalStyles, work_mem [64, MAX_KILOBYTES] kB, timestamps
   [MIN_TIMESTAMP, END_TIMESTAMP)). Because both sides receive the SAME
   derived value, out-of-range fuzz bytes clamp identically by
   construction.
2. **Both sides, always.** The pin writes the Rust-side session seam (the
   real thread-local cell the shipped code reads: adt_float's
   extra_float_digits cell, adt_datetime's style cells, scan_fgram's scs
   cell, or the facility-owned cells for clock/prng/workmem) AND the
   C-side `pg_stub_*` thread-local. A pinned value is part of the compared
   input — never let one side default.
3. **C consumption.** A NEW oracle TU includes
   `csrc/stubshims/pg_stub_state.h` and reads the `pg_stub_*` globals
   (e.g. `#define extra_float_digits pg_stub_extra_float_digits` ahead of
   a verbatim paste) instead of defining another per-TU copy. Vendored C
   is never edited — where a family TU already exposes a
   value-as-argument entry point, a `pg_stub_*_guc` wrapper in the shim TU
   routes the pinned global into it (that is how the controls reach the
   verbatim consumers today).
4. **GUC assign hooks.** The facility pins the parsed, post-assign-hook
   internal values (style/order/istyle enum ints), the family convention
   since datetime_io_diff — legal-range derivation stands in for the hook.
   No shipped assign hook currently in the fuzz core does more than store
   the value; a GUC whose C-side assign hook does real work that the shim
   cannot reproduce must NOT be force-pinned (list it in the target header
   instead).

## Declaring pins in a target

```rust
use crate::stubs;

pub fn my_diff(data: &[u8]) {
    let Some((&b0, rest)) = data.split_first() else { return };
    // 1. declare + set the pins from leading input bytes (both sides):
    let efd = stubs::guc::pin_extra_float_digits(b0);
    let now = stubs::clock::pin_now(i64::from_le_bytes(...));
    stubs::prng::pin_seed(u64::from_le_bytes(...));
    let (wm, _) = stubs::workmem::pin(..., ...);
    // 2. run BOTH sides' state-reading entry points and compare.
}
```

Rust-side reads for driver-passed values: `stubs::clock::now_usecs()`,
`stubs::prng::rust_u64()/rust_double()`, `stubs::workmem::work_mem()/
maintenance_work_mem()`. Fuzz binaries (one target per process) that need
shipped pure code's `timestamp_seams::get_current_timestamp` to resolve to
the pin call `stubs::clock::install_timestamp_seam()` at init — first-wins,
returns whether it installed; never call it from the shared `cargo test`
binary (legacy targets install their own constant with an unguarded
`set()`).

## Must-fail controls (harness-detection-power law)

Every facility ships a control in `core/src/stubs.rs` tests proving the pin
is ALIVE: (a) parity through a REAL verbatim vendored consumer under
matched pins, then (b) a deliberate one-sided mismatch that the comparator
MUST see. Verified by a dead-plane sweep (all C setters temporarily
no-op'd): every control fails, only the pure-arithmetic `clamp_edges`
survives.

| control test                     | vendored C consumer                              |
|----------------------------------|--------------------------------------------------|
| `control_guc_efd_pin`            | float8out_internal_efd (pg_float_io.c)           |
| `control_guc_datestyle_pin`      | EncodeDateTime via pg_tsdiff_timestamp_out       |
| `control_guc_intervalstyle_pin`  | EncodeInterval via pg_tsdiff_interval_out        |
| `control_guc_scs_pin`            | (transport-level only: no vendored C lexer in csrc yet; Rust plane is the real scan_fgram cell) |
| `control_clock_pin`              | pg_stub_get_current_timestamp (+ domain clamp)   |
| `control_prng_pin`               | verbatim xoroshiro128** (pg_pg_prng_io.c) vs shipped pg_prng |
| `control_workmem_pin`            | verbatim bloom_create sizing (pg_libfam_io.c) vs shipped bloomfilter |

## Demonstration wiring

`float_misc_diff` arm 15 (core/src/diff.rs): the extra_float_digits pin
goes through `stubs::guc::pin_extra_float_digits` and both sides run their
GUC-READING output paths — shipped `adt_float::float8out` (reads the
session cell) vs `pg_stub_float8out_guc` (verbatim body reading
`pg_stub_extra_float_digits`) — no efd argument anywhere in the exec.

## Not wired (deliberately)

- `enable_*` planner flags: no planner consumer is linked into the fuzz
  core today; wire through the same pattern (a bool cell + a pg_stub
  global) when a planner-math target lands.
- standard_conforming_strings has no vendored C consumer in csrc yet
  (transport-level control only; see table).

---

# Constructed-state stub facilities (`stub:*`)

> **LANDING ORDER (coordinator ruling 2026-08-01): HOLD.** This branch's
> ancestry merges the LIVE lanes `proofs/p1-tupaccess` and `proofs/p1-nodes`
> (their claims are still open on main). Order of record: those lanes land
> first and flip their claims to done; THEN `fuzz/stub-constructed` lands as
> the small remaining delta — verify with `git cherry` + `git range-diff`
> before pushing (containment-audit rule). The coordinator owns the trigger.

Shared builders that construct STATE-SHAPED inputs identically on the Rust
side and the C-oracle side from the same fuzz bytes (branch
`fuzz/stub-constructed`, charter 2026-08-01). The constructed structure is
part of the compared input: both sides build from the same bytes and neither
side defaults anything. Modules live in `fuzz/core/src/stub_*.rs`; C shims in
`fuzz/core/csrc/pg_stub_*.c` (plus the tupaccess SECTION-D decoder already in
`pg_tupaccess_io.c`).

Constructor-audit discipline: a constructor bug fabricates agreement — both
sides consume the same wrong structure and the differential is blind. Every
facility therefore ships must-fail controls
(`fuzz/core/src/stub_controls_tests.rs`) that plant a ONE-SIDE-ONLY
construction difference and assert the differential catches it, and the
builders were injection-swept (results below).

Run everything: `cargo test -p decoder_fuzz stub_ -- --test-threads=1`
(the C-oracle tests in this crate are serial; the parallel-thread SIGBUS is
the known nodesfam stack-guard/thread interplay, not a defect).

## stub:tupdesc (`stub_tupdesc.rs`)

TupleDesc + heap-tuple values from fuzz bytes. Factored verbatim out of the
p1-tupaccess harness; `tupaccess_diff.rs` is the migration demo (it now
imports the builder instead of owning a copy).

How a target uses it:

```rust
use crate::stub_tupdesc::*;
let mut cur = Cursor { b: data, i: 0 };
let spec  = decode_desc(&mut cur);           // normalized DescSpec
let vals  = decode_values(&mut cur, &spec, spec.natts());
let sw    = spec_wire(&spec);                // -> C oracle (SECTION D decoder)
let vw    = values_wire(&vals);              // -> C oracle
let desc  = build_rust_desc(mcx, &spec);     // Rust side
let (values, isnull) = stage_values(mcx, &spec, &vals);
// target computes over (desc, values) and compares against the C call fed
// (sw, vw); ser-plane helpers stay in the target (they are comparison
// planes, not construction).
```

Clamps (compared-input contract, applied to the SPEC before either side
builds): natts `% 41`; menu index `% 12` (pinned 12-entry type menu);
cstring len `% 121` NUL-stripped; varlena short total `1..=127` / 4B payload
`< 300` / TOAST pointer 16B; defvals ≤ 3 with strictly increasing adnum ≤
natts; checks ≤ 2, ASCII NUL-free, sorted+deduped by name; `hasmissing`
masked off on dropped/cstring columns and constr-less descriptors. Width-1
byval Datums compare under the low-8-bit mask (ratified platform
non-surface; `byval_word`).

Unlocks: printtup formatting, toast size math, reloptions parsing (any
target needing a descriptor + staged row).

## stub:nodes (`stub_nodes.rs`)

Bounded node trees from fuzz bytes. Factored verbatim out of the p1-nodes
harness; `nodesfam_diff.rs` is the migration demo.

The bridge is the TEXT plane: the Rust side constructs the tree directly
(`build_value_node(mcx, bytes) -> Option<Node>`); the C side constructs the
SAME tree by reading the Rust tree's `nodeToString` rendering through
verbatim 18.3 `nodeRead`, and the target's re-out/copy/equal planes compare
the C-side structure back byte-for-byte.

Clamps: tag selector `% 8` (String / Integer / Float / Boolean /
escaped-String / List / IntList / OidList); nesting depth `< 6`; list len
`% 5`; int/oid list len `% 6`; strings `% 25` (escaped arm `% 17`),
NUL-stripped; Float literals forced numeric-looking (finite `{f:?}`, else
`1e300`).

KNOWN BLIND CLASS (documented, honest): a builder defect that produces a
DIFFERENT-BUT-LEGAL tree (e.g. an Integer decoded from fewer bytes) is
consumed identically by both sides — the differential cannot see it
(injection N3 below, planted expecting exactly this). Such a defect shrinks
the explored tree surface but never falsifies a verdict; the tag-census and
label-census tests in the demo target bound how much surface can silently
disappear. Builder defects that violate the producible-token contract (NULs,
non-numeric Float literals) ARE caught (N1, N2).

Unlocks: rewrite/manip, optimizer pure helpers (bitmapset, pathkey
arithmetic), walker-style code.

## stub:snapshot (`stub_snapshot.rs` + `csrc/pg_stub_snapshot.c`)

SnapshotData as a plain value — xmin, xmax, xip[], subxip[], flags, curcid,
speculativeToken, snapXactCompletionCount — built identically both sides
with zero transaction machinery. The C shim vendors the 18.3
`utils/snapshot.h` struct verbatim (GlobalVisState stays opaque,
pairingheap_node vendored as its three-pointer shape; both zeroed and out of
the compared plane, as are the Rust-only marshal cells).

How a target uses it:

```rust
let spec = decode_snap(&mut cur);                       // SnapSpec
assert_snapshot_construction_agrees(mcx, &spec);        // construction plane
let snap = build_rust_snapshot(mcx, &spec);             // Rust SnapshotData
let wire = snap_wire(&spec);                            // -> C oracle builds
// its own SnapshotData via the pg_stub_snapshot.c decoder; target then
// compares e.g. XidInMVCCSnapshot verdicts over the two structures.
```

Clamps: snapshot_type `% 7`; xcnt/subxcnt `% 65` (MAX_XIP = 64); xids /
curcid / speculativeToken raw LE u32 — NOT normalized (xmin ≤ xmax and xip ∈
[xmin, xmax) are C invariants a consumer target may impose; the builder
never fabricates them); flags byte bits 0..2 = suboverflowed,
takenDuringRecovery, copied.

Unlocks: XidInMVCCSnapshot, the pure core of heapam_visibility.

## stub:encoding (`stub_encoding.rs` + `csrc/pg_stub_encoding.c`)

The pg_enc universe pinned identically both sides: id ↔ official name
(`pg_enc2name_tbl`, encnames.c verbatim), maxmblen (the scalar column of
wchar.c `pg_wchar_table`, mechanically extracted), and the server-encoding
boundary (`PG_ENCODING_BE_LAST`). The Rust rows come from the SHIPPED
`wchar`/`mbutils` crates — a transcription defect on either side is a caught
divergence.

How a target uses it: `enc_from_byte(b)` derives the same valid encoding id
both sides from one fuzz byte (clamp `% 42`, itself pinned by a test);
`assert_encoding_tables_pinned()` (committed test; callable once per process
by targets) guarantees the id means the same encoding everywhere.
pg_conversion-style tables extend this module; the id/name/maxmblen pin is
the substrate.

## stub:syscache-row (`stub_syscache.rs` + `csrc/pg_stub_syscache.c`)

The NINTH facility (Michael-ratified 2026-08-01, phase2-plan.md §8 Q7):
catalog rows supplied as fuzz input, constructed identically both sides.
PostgreSQL's `lsyscache` layer is hundreds of single-row catalog probes
(`get_atttype`, `get_typlenbyval`, `get_opclass_family`, ...) — OPEN
through syscache→catcache→relation→buffers, but pure over (arguments, that
row) once the row is input. Rust side: a thread-local row store answering
the existing `syscache_seams` probes the shipped `lsyscache` crate reads.
C side: the same store loaded from the same wire, a
SearchSysCacheN/GetSysCacheOidN interception layer (fake-tuple GETSTRUCT
over vendored-verbatim FormData fixed prefixes), and VERBATIM 18.3
lsyscache consumer bodies (`get_opfamily_proc`, `get_opfamily_member`,
`get_opcode`, `get_opclass_family`, `get_typlenbyval`, `get_atttype`,
`get_func_rettype`) compiled over it — C bodies verbatim, interception
pure preprocessor. All C exports prefixed `pg_stub_syscache_` (nm census:
16 exports, all prefixed). New oracle TUs intercept via
`csrc/stubshims/pg_stub_syscache.h`.

Covered caches (extensible BY TABLE, never per lane): pg_amop
(AMOPSTRATEGY + AMOPOPID), pg_amproc (AMPROCNUM), pg_operator (OPEROID),
pg_opclass (CLAOID), pg_type (TYPEOID), pg_attribute (ATTNUM), pg_proc
(PROCOID). Fields carried = the wire fields in stub_syscache.rs; anything
else is NOT covered (zero on the C side, absent from the Rust shapes).

How a target uses it:

```rust
use crate::stub_syscache::*;
install_seams();                      // shipped lsyscache -> the store
let rows = decode_rows(&mut cur);     // fuzz path (menu-anchored), or
let rows = my_harvested_rows();       //   programmatic (migrated lanes)
assert_syscache_construction_agrees(&rows);   // loads BOTH sides + plane
// Rust side: lsyscache::get_opfamily_proc(...) (seam-routed) or rows_*();
// C side: c_get_opfamily_proc(...) etc. (verbatim bodies over the store).
```

Clamps (compared-input contract): ≤16 rows per cache (`% 17`); FIRST
matching row wins on BOTH sides (duplicate keys legal; wire-order scan;
pinned by `syscache_duplicate_key_first_match_pins_order`); attname 64 raw
bytes with byte 63 forced 0; all other fields raw LE, NOT normalized.
Fuzz derivation is menu-anchored: each row = a HARVESTED real catalog row
(index `% menu len`) with at most one mutated field — the domain stays
anchored to catalog-reachable shapes while the differential still sees
drifted fields.

UNREACHABLE-STATE HAZARD (band-2, the reason this facility is
`harness-audit:required`): a supplied row can be INCONSISTENT with the
catalog it was not supplied alongside (an amproc row whose proc oid names
no pg_proc row) — real PostgreSQL only reaches catalog-consistent states,
so verdicts over invented rows can exercise unreachable states.
Mitigations, both committed: the derivation menu/seed rows are HARVESTED
from a live catalog (`stub_syscache_harvest.rs`: 437 real rows from the
dev server's 18.3-.dat-pinned catalog; regenerate with
`fuzz/core/harvest_syscache.py`; replayed row-by-row through both
constructors AND the verbatim consumers by
`syscache_harvest_rows_all_agree`), and the constructor injection sweep
below.

SHARED-BINARY COLLISION BEHAVIOR (absorbed from the two lanes that solved
it independently): `install_seams()` claims the facility's nine
`syscache_seams` slots first-install-wins and returns whether it owns ALL
of them (`authoritative()`). In the shared `cargo test` binary a foreign
oracle (arrayfuncs/rowtypes/tupaccess pins) may own some — consumers must
then downgrade exactly as the absorbed lanes did (brin_bloom: Lazy→Pinned
mode; brin_minmax_multi: cache pre-seeding). Store-direct `rows_*` probes
and the construction plane never depend on seam ownership, so the
facility's own suite is collision-immune. In a one-target fuzz binary the
facility always owns its seams.

ABSORBED (pg_qsort consolidation rule — two lanes hand-rolled pinned
syscache menus on 2026-08-01 before this was a facility, now migrated):
`brin_bloom_diff` (was: a private `amproc_menu` + OnceLock install) and
`brin_minmax_multi_diff` (was: private pg_amop/pg_amproc/pg_operator/
pg_type menus + `slow_path_ok()`). Do not hand-roll a third menu — extend
the facility's row shapes by table instead.

Unlocks: the lsyscache single-probe family (7 of the caches bucket's 10
phase-2 OPENs are exactly this shape) and callers open only through those
probes; immediate phase-1 value = the two migrated brin lanes.

## Must-fail controls (all committed, all green)

| control | plants | proves |
|---|---|---|
| `tupdesc_control_one_side_notnull_flip_is_caught` | attnotnull flipped in the C wire only | desc field plane sees one-side construction drift |
| `tupdesc_control_one_side_menu_swap_is_caught` | att menu (attlen/byval shape) swapped C-side only | shape-level drift caught |
| `nodes_control_one_side_tree_difference_is_caught` | C fed a text describing a different tree | re-out plane sees it |
| `nodes_clamp_strings_are_nul_free` | NUL-riddled builder input | the NUL-stripping clamp holds (added to close injection N1) |
| `snapshot_control_c_side_tamper_is_caught` | xmax low byte flipped in the wire only | field plane sees C-side drift |
| `snapshot_control_rust_side_tamper_is_caught` | xip[0] flipped on the Rust side only | field plane sees Rust-side drift |
| `encoding_control_shifted_index_is_caught` | every Rust row compared against the wrong C row | comparator live at every index |
| `encoding_clamp_is_pinned` | – | the `% 42` clamp is a pinned contract, not silent drift |
| `syscache_control_one_side_{amop,amproc,operator,opclass,type,attribute,proc}_tamper_is_caught` (7) | one row field flipped on the C side only, per cache | store plane sees one-side row drift in EVERY covered cache |
| `syscache_control_rust_side_tamper_is_caught` | amopstrategy flipped on the Rust side only | plane sees Rust-side drift |
| `syscache_control_consumer_divergence_is_caught` | one-side amproc row difference, probed through REAL consumers | shipped Rust lsyscache vs verbatim C get_opfamily_proc DIVERGE — the differential a migrated target computes catches it |
| `syscache_seam_route_or_downgrade` | – | seam-authoritative: shipped lsyscache answers from the store; foreign-owned: `authoritative()` reports false (downgrade contract) |
| `syscache_duplicate_key_first_match_pins_order` | duplicate-keyed rows, different payloads | FIRST-match-wins is pinned on BOTH sides |
| `syscache_decode_clamps_are_pinned` | – | the `% 17` count clamp + `% menu len` index clamp are pinned contracts |

Plus baseline-agreement tests: `snapshot_construction_agrees` (fixed spec +
500 seeded pseudo-random specs through both constructors) and
`encoding_tables_are_pinned` (all 42 rows). The tupdesc/nodes builders'
structural validators are the demo targets' committed suites (seed-corpus
replay with all diversity buckets asserted nonzero; tag/label census tests).

## Builder injection sweep (2026-08-01)

Scratch plants applied to the builders themselves, one at a time
(scratchpad `inject_stub.py`); verdict = does the committed test slice fail.

| plant | builder defect | verdict |
|---|---|---|
| T1 | `spec_wire` drops the has_constr bit (C never builds constr) | CAUGHT (seed replay + controls) |
| T2 | `build_rust_desc` loses attnotnull | CAUGHT |
| T3 | `stage_datum` stages width-2 byval with i32 sign width | CAUGHT |
| N1 | `take_str` keeps NULs (text-bridge contract violation) | MISSED on the first pass — the committed suites never pushed a NUL through the string arms; closed by adding `nodes_clamp_strings_are_nul_free` (clamp-pin control), re-planted and now CAUGHT |
| N2 | Float literal emitted empty (unreadable token stream) | CAUGHT |
| N3 | Integer decoded from 2 bytes instead of 4 (different-but-legal tree) | MISSED — planted EXPECTING silence: this is the documented blind class of the text-bridge builder (both sides consume the same tree; surface shrinks, verdicts never falsify) |
| S1 | `snap_wire` swaps xmin/xmax | CAUGHT |
| S2 | Rust side silently caps xcnt at 32 | CAUGHT (mini-fuzz) |
| S3 | plane serializer omits `copied` | CAUGHT |
| E1 | Rust count pin 43 | CAUGHT |
| E2 | C maxmblen table defect (UTF8 -> 3) | CAUGHT |
| E3 | `enc_from_byte` clamp drifts to % 41 | CAUGHT (clamp-pin control) |

Final: 11/12 caught after the N1 gap was closed; 1/12 (N3) is the
documented-silent class, kept in the table so the limitation stays visible.

Reporting per the harness-detection-power law: counts above are honest; the
one expected-silent plant is the documented nodes blind class, kept in the
table so the limitation stays visible.

## stub:syscache-row injection sweep (2026-08-01)

Scratch plants applied to the constructor (one at a time; committed test
slice = `cargo test -p decoder_fuzz stub_ -- --test-threads=1`):

| plant | constructor defect | verdict |
|---|---|---|
| Y1 | `rows_wire` drops amoppurpose (wire writer field drop) | CAUGHT (13 tests fail: short-read loader status + plane) |
| Y2 | C decoder reads amprocnum as ONE byte (width defect) | CAUGHT (14 tests fail: plane + consumer parity) |
| Y3 | `rows_amproc` scans reversed — LAST-match-wins on the Rust side only | CAUGHT (`syscache_duplicate_key_first_match_pins_order`) |
| Y4 | derivation menu-index clamp drifts to `% (len-1)` | CAUGHT (`syscache_decode_clamps_are_pinned`) — but note the DIFFERENTIAL is blind to this class (a different-but-legal row selection reaches both sides identically, same as nodes N3); the clamp-pin control is what closes it |

Final: 4/4 caught. The Y4 class (derivation drift) never falsifies a
verdict — it shrinks the explored row surface — and is bounded by the
clamp pins, the same containment the nodes facility documents for N3.
