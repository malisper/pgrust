# Datum physical-representation keystone (F0) — DONE (enum + accessors + trait relocation + workspace match-site fan-out)

KEYSTONE: widen the canonical byte-lane value enum `types_tuple::Datum<'mcx>`
(`crates/types-tuple/src/backend_access_common_heaptuple.rs`) from the 2-arm
`ByVal(usize) | ByRef(PgVec<'mcx,u8>)` to the full physical-representation set,
ADDITIVELY.

## What landed (faithful, no stubs, no todo!/unimplemented!)

### Enum (4 new arms, additive)

- `ByVal(usize)` / `ByRef(PgVec<'mcx,u8>)` kept AS-IS (no rename; `ByRef` is the
  flat varlena / fixed-by-ref bytes arm = "Varlena", documented in the arm doc).
- `+ Cstring(String)` — `typlen == -2`, owned text, no varlena header (mirrors
  `RefPayload::Cstring`).
- `+ Composite(FormedTuple<'mcx>)` — record/row-as-value, reusing the existing
  `FormedTuple` already in this module.
- `+ Expanded(Box<dyn ExpandedObject>)` — `VARATT_IS_EXPANDED` live object,
  reusing the SAME `ExpandedObject` trait that `RefPayload::Expanded` uses.
- `+ Internal(Box<dyn core::any::Any>)` — `internal` pseudo-type opaque object.

### Trait relocation (layering)

`ExpandedObject` lived in `types-fmgr`, which depends on `types-tuple` — so
`types-tuple` could not name it without a cycle. Relocated the trait (and a
shared `flatten_expanded` helper) DOWN to `crates/types-datum/src/expandeddatum.rs`
(both `types-tuple` and `types-fmgr` already depend on `types-datum`).
`types-fmgr::boundary` now `pub use types_datum::ExpandedObject;` so the public
path `types_fmgr::ExpandedObject` and all `RefPayload` code keep working
unchanged.

### Accessors / constructors (additive, mirroring DatumGetX/XGetDatum)

Pre-existing `from_*`/`as_*` by-value codec family (int4/oid/bool/int8/float8/
float4/etc., bit-cast floats + sign-extend signed) left intact. Added:
`from_cstring`/`as_cstring`, `as_composite`, `is_expanded`/`as_expanded`/
`as_expanded_mut`, `as_internal`.

### Derives obstacle — RESOLVED by hand-impl (the predicted real blocker)

`Box<dyn ExpandedObject>` and `Box<dyn Any>` are not `Clone`/`Eq`, so the
`#[derive(Clone, Debug, Eq, PartialEq)]` could not hold. Resolved exactly like
`RefPayload` (option (a) of the keystone brief):

- `Clone`: flat arms clone normally; `Composite` clones the `FormedTuple`;
  `Expanded`/`Internal` panic (no `Mcx` to re-home a flattened image in bare
  `Clone`; use `clone_in`). Unreachable today (no producer) — sanctioned panic.
- `clone_in(mcx)`: flat arms re-home; `Expanded` flattens into `ByRef` (mirrors
  `RefPayload::clone_flat`); `Internal` panics (C never `datumCopy`s internal).
- `PartialEq`/`Eq`: by-word / flat-byte / flatten-then-compare for `Expanded`;
  `Composite` compares data+t_len; `Internal` is non-comparable (panic) — mirrors
  `RefPayload`.
- `Debug`: hand-rolled per arm (`Expanded` shows flat_size; `Internal` opaque).

## Workspace match-site fan-out

Every exhaustive `match` on `Datum` (and its aliases `TupleDatum`/`DatumV`/
`CanonDatum`) now handles the 4 new arms. ~50 sites across ~30 crates (lib +
test code). Rule applied per site:

- Default: route the 4 unproduced arms into the site's EXISTING rejection arm
  (`panic!`/`Err`/default `false`/`0`/`InvalidOid`/`null()`), or a fresh
  `"... not yet produced — wave 2"` panic. This is the sanctioned
  mirror-and-panic: the arms have NO producer yet (later waves), so they are
  genuinely unreachable.
- Real handling where trivially correct: `DatumGetHeapTupleHeader` returns a
  `Composite`'s tuple directly via `clone_in`; predicate fns
  (`VARATT_IS_EXTERNAL`, `varatt_is_external_ondisk`) return `false` for the new
  arms (none is an on-disk external varlena); fmgr-core marshalers map `Cstring`
  → `RefPayload::Cstring`.

No new contract divergences introduced (purely additive). CONTRACT_RECONCILE
ledger untouched.

## Gates

- `cargo check --workspace` — GREEN.
- `cargo check --workspace --tests` — GREEN (except pre-existing, unrelated
  `backend-commands-explain` test E0063 `missing field initPlan`, confirmed
  failing at the base before this change).
- `cargo test -p no-todo-guard -- --ignored` — PASS (strict gate; tree clean).
- `cargo test -p seams-init` — PASS (both recurrence guards).

## Producer waves NEXT

This F0 only makes the arms EXPRESSIBLE. The composite-Datum bridges that named
the missing `Composite` arm as a blocker (`record_from_values`,
`get_expr_result_type_node`, `DatumGetHeapTupleHeader`/`HeapTupleGetDatum`, the
misc2 `make_expanded_object_read_only_internal` byte-image divergence) are now
type-level unblocked but still need their producer/consumer seams ported.
