# soft-plane wave 2 — BULK small-types lane evidence (proofs/sp2-bulk, 2026-07-31)

New compared SOFT-ERROR (escontext) plane for five small pgrust-fast types
across four fuzz targets: **cash (money), macaddr, macaddr8, float4/float8,
and the bool fc-wrapper**. This is the `pg_input_is_valid()` /
`COPY ... ON_ERROR ignore` / errsave surface — the plane where C's input
functions take an `escontext` Node and `errsave`/`ereturn` stash the error
and return a dummy instead of throwing. pgrust must soften exactly where C
softens; a path that throws where C softens (or the reverse) is a
user-visible defect with repro
`SELECT pg_input_is_valid('<input>','<type>');`.

Pattern followed: `fuzz/core/src/rangetypes_diff.rs` `arm_text_in_soft`
(laneac precedent) for the four comparisons, and `uuid_diff.rs` for the
fc-wrapper soft shape.

## The four comparisons (identical in all four targets)

| # | compared |
|---|---|
| (a) | soft-error **OCCURRED** flag — C's armed escontext capture vs the Rust `SoftErrorContext` / `ErrorSaveNode` `error_occurred()` |
| (b) | the captured **sqlstate CLASS** (same small-int table as the thrown plane) |
| (c) | the **success image** under soft mode — a valid literal must yield the identical value/bytes with an armed context |
| (d) | **soft/hard verdict agreement**, each side independently: a literal is valid in soft mode iff it is valid in hard mode |

Each target runs the soft plane on **every exec of the existing in-function
arm** — no selector, no new input layout, the existing corpus is reused.
All other planes are untouched.

Rust side always drives the SHIPPED code twice: the core entry with a
`types_error::SoftErrorContext::new(true)` (details wanted), and the
shipped `fc_*` fmgr wrapper with a real `types_fmgr::ErrorSaveNode` in
`fcinfo.context`.

## CRITICAL: both-sides-armed

A prior lane shipped a "soft plane" that armed only the Rust side. Every C
shim here was hard-only before this lane, and each was upgraded to an
escontext-aware `ereturn`/`errsave` shim that **records class + occurred on
the armed context and returns the upstream dummy** when `escontext != NULL`,
keeping the existing hard entry (escontext == NULL) contract byte-for-byte.

One unit test per target witnesses a real C-side soft capture (nonzero
class) on known-bad literals **and** a no-capture/value-matches result on a
valid literal — so a regression that silently stops arming the C side fails
a test rather than passing a vacuous plane:

| target | C-side-armed witness test |
|---|---|
| cash_diff | `cash_diff::tests::c_side_soft_capture_witness` |
| mac_diff | `mac_diff::tests::c_side_soft_capture_witness` |
| float_in_diff | `diff::tests::c_side_soft_capture_witness` |
| bool_diff | `diff_charbool::tests::c_side_soft_capture_witness` |

Plus one corpus-replay test per target that drives the whole literal corpus
through the new plane: `cash_in_soft_plane_corpus`,
`mac_in_soft_plane_corpus`, `float_in_soft_plane_corpus`,
`bool_in_soft_plane_corpus`.

## Per-target detail

### 1. cash_diff — `cash_in` (money)

- C: `csrc/pg_cash_io.c` — the five `ereturn` sites in the verbatim
  `cash_in` body now go through the escontext-aware `PG_CASH_ERETURN` shim;
  `pg_diff_cash_in_common` is the shared body, `pg_diff_cash_in` the hard
  entry, `pg_diff_cash_in_soft` the armed entry.
- Rust: `adt_cash::cash_in` + `builtins::fc_cash_in`.
- Classes exercised soft: 22P02 (invalid input syntax) and 22003 (value out
  of range, four distinct overflow sites).
- **Ratified-carve note**: the money `i64::MIN / -1` → 22003 fence (ledger
  rows 865/867/3345) lives on the *division* arms. `cash_in` performs no
  division, so no carve extends onto this plane — nothing was skipped, the
  comparator is at full strength on every literal.

### 2. mac_diff — `macaddr_in` AND `macaddr8_in`

- C: `csrc/pg_mac_io.c` — `pgc_macaddr_in_common` / `pgc_macaddr8_in_common`
  take an escontext and use `PG_MAC_ERETURN`; `_soft` entries drive the armed
  branch; the existing `pgc_macaddr_in` / `pgc_macaddr8_in` return-code
  contracts are unchanged.
- Rust: `adt_mac::macaddr_in` + `fc_macaddr_in`, `adt_mac8::macaddr8_in` +
  `fc_macaddr8_in`.
- Classes exercised soft: macaddr 22P02 (syntax) **and** 22003 (octet value
  out of range — both C soft sites witnessed); macaddr8 has a single `fail:`
  ereturn, 22P02.
- **Ratified carve extended with the SAME key**: the row-436 wide-hex carve
  (`carve_row436`) and the negation-overflow guard are applied by
  `mac_in_diff` *before* the soft plane runs, so one key gates both planes;
  the soft plane adds a class check on top of the same key.

### 3. float_in_diff — `float4in` AND `float8in`

- C: `csrc/pg_float_io.c` — the TU's `ereturn` shim was
  `do { (void)(stuff); return (ret); }` with `struct Node` an opaque
  forward declaration and escontext *always NULL*. `struct Node` is now the
  armed-context record and `ereturn` is escontext-aware (it saves `occurred`
  plus the class `errcode()` just recorded, then returns the upstream dummy).
  `pg_diff_float8in_soft` / `pg_diff_float4in_soft` are the armed entries.
- Rust: `adt_float::float8in` / `float4in` + `fc_float8in` / `fc_float4in`.
- Classes exercised soft: **both** float soft sites — 22P02 (invalid input
  syntax, incl. trailing junk / empty) and 22003 (value out of range;
  float4in has its own boundary site at the float4 range).
- The host-conditional macOS `nan(` strtod carve is applied by the caller
  before the soft plane, so the same key gates both planes.

### 4. bool_diff — the `fc_boolin` soft shape + `details_wanted = true`

Survey note said "bool partial: core soft yes, fc hard-only,
details_wanted=true never run". Both holes are closed:

- C: `csrc/pg_bool.c` — `boolin`'s single ereturn site now goes through the
  escontext-aware `PG_BOOL_ERETURN`; `pg_diff_boolin_common` is the shared
  body, `pg_diff_boolin_soft` the armed entry. The C escontext branch was
  previously **never executed at all**.
- Rust: the pre-existing `details_wanted = false` core leg is KEPT and
  strengthened (it now also pins that no details are saved in that shape);
  a `details_wanted = true` core leg runs the full error-details path; and
  `fc_boolin` is driven with a real `ErrorSaveNode` in `fcinfo.context`
  (details wanted), which it had never seen.
- Class exercised soft: 22P02 (the only boolin error site).

## Recursion guard

Checked per crate (all five parsers are FLAT — no recursion on the soft
path, so no stack-depth interaction with errsave):

| crate | soft-path shape | recursion |
|---|---|---|
| `adt/cash` | single forward scan + trailing-symbol loop | none (no self-call) |
| `adt/mac` | fixed 7-format `scan_six`/`scan_double_groups` cascade over a flat `Scanner` | none |
| `adt/mac8` | single `while` over hex pairs | none |
| `adt/float` | `float8in_internal` / `float4in_internal` — separate bodies, neither calls the other nor itself (`float8in_internal` occurs 3× in io.rs: definition, the `float8in` call, one comment) | none |
| `adt/bool` | `boolin` → `parse_bool_with_len` (flat `switch`) | none |

## Results (laptop smokes; the 10M floor is the fleet's, coordinator-owned)

| target | smoke execs | divergences | corpus after |
|---|---|---|---|
| cash_diff | 300,000 | 0 | 719 |
| mac_diff | 400,000 | 0 | 1509 |
| float_in_diff | 500,000 | 0 | 1561 |
| bool_diff | 1,000,000 | 0 | 1096 |

**ZERO divergences across 2.2M local execs** — pgrust's soft-error contract
is C-exact on this whole family (money, macaddr, macaddr8, float4, float8,
boolean), at both core grain and fc-wrapper grain, on the OCCURRED flag, the
captured class, the success image, and soft/hard verdict agreement.

Unit gates: `cargo test -p decoder_fuzz` — all four
`c_side_soft_capture_witness` tests and all four `*_soft_plane_corpus`
replays pass, plus every pre-existing test in the four modules
(cash 9/9, mac 7/7, float 8/8, bool 8/8).

Soft-failing corpus seeds committed per target (`fuzz/corpus/<target>/soft*`):
cash 5, mac 11, float_in 12, bool 8 — each hits a distinct soft site
(22P02 / 22003 / valid-literal control) on the relevant width.

### Divergence classification

None to classify. No docker ground-truthing was required (docker daemon was
unresponsive in this session; with zero divergences there was no verdict to
adjudicate). SQL reachability of the plane itself is the standing one:
`SELECT pg_input_is_valid('<lit>','money'|'macaddr'|'macaddr8'|'float4'|'float8'|'boolean')`
and `pg_input_error_info(...)`, plus `COPY ... WITH (ON_ERROR ignore)` on a
column of any of those types.

## LEDGER PREP — candidate retirements (ledger files NOT edited here)

Method: joined `proofs/coverage/phase1-exceptions.tsv` against the line
ranges of the five shipped `fc_*` in-functions and of every
`ereturn`/`escontext` line in the five cores.

**Core soft lines: nothing to retire.** No `ereturn`/`escontext` line in
`adt/{cash,mac,mac8,bool}/src/lib.rs` or `adt/float/src/io.rs` carries an
exception row — those lines were already measured by the pre-existing core
soft legs (bool/uuid-style) or by the hard plane.

**Candidate retirements — `excluded-state` rows now EXECUTED with an armed
`ErrorSaveNode` by `float_in_soft_plane_corpus` / the fuzz plane:**

- `crates/backend/utils/adt/float/src/builtins.rs:60` (fc_float4in entry)
- `crates/backend/utils/adt/float/src/builtins.rs:61`
- `crates/backend/utils/adt/float/src/builtins.rs:63` (`soft_error_context()`)
- `crates/backend/utils/adt/float/src/builtins.rs:64` (`float4in(&num, esc)?`)
- `crates/backend/utils/adt/float/src/builtins.rs:67` (fc_float8in entry)
- `crates/backend/utils/adt/float/src/builtins.rs:68`
- `crates/backend/utils/adt/float/src/builtins.rs:70` (`soft_error_context()`)
- `crates/backend/utils/adt/float/src/builtins.rs:71` (`float8in(&num, esc)?`)

Caveat for the ledger owner: these eight rows are class `excluded-state`
with the justification "fmgr-frame glue … soft-error-context … fmgr
machinery is OUT per the phase-1 filter (named carve, claim row)". That is a
**scope** carve, not an unmeasured-line carve, so retiring them is a scope
decision, not merely an evidence decision — but the evidence side is now
satisfied: the lines execute, with the soft branch taken, under a shipped
wrapper driven by a real `ErrorSaveNode`. The equivalent
`fc_cash_in`/`fc_macaddr_in`/`fc_macaddr8_in`/`fc_boolin` lines carry NO
exception rows, so there is nothing to retire for those four.

lcov join deferred to the coordinator/ledger owner (a local export over
these crates is not cheap relative to this lane's budget); the execution
witness is the passing `c_side_soft_capture_witness` +
`*_soft_plane_corpus` tests, which drive every one of the eight lines with
an armed context.

## Commits (branch `proofs/sp2-bulk`, base `proofs/softerror-plane-2` = 0b8be188d4)

1. `cash_diff: soft-error (escontext) plane — armed C errsave shim + Rust core/fc ErrorSaveNode`
2. `mac_diff: soft-error (escontext) plane for macaddr_in + macaddr8_in`
3. `float_in_diff: soft-error (escontext) plane for float4in + float8in`
4. `bool_diff: arm the boolin soft-error plane on BOTH sides (C escontext + details_wanted=true)`
