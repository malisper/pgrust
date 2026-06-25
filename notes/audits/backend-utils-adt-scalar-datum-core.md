# Audit: backend-utils-adt-scalar-datum-core (`utils/adt/datum.c`)

Unit / CATALOG row: `probe-adt-scalar-datum` (`*/datum.c`).
Branch: `port/backend-utils-adt-scalar-datum-core`.
Sources: `../pgrust/postgres-18.3/src/backend/utils/adt/datum.c`,
`../pgrust/c2rust-runs/probe-adt-scalar-datum/src/*.rs`.

datum.c serves the SAME byte-dispatch logic through TWO already-consumed,
deliberately non-unified seam contracts:

* the **byte model** (`backend-utils-adt-scalar-seams::datum_copy`, over
  `types-tuple::TupleValue` ByVal/ByRef), consumed by brin-tuple;
* the **bare-Datum machine-word model** (`backend-utils-adt-datum-seams`:
  `datum_copy` / `datum_estimate_space` / `datum_serialize` / `datum_restore`
  / `datum_image_hash` / `datum_image_eq`), consumed by nbtree, nodeMemoize,
  nodes-core `copyParamList`, misc2 rowtypes. By-ref values cross as bare
  `Datum` pointer words; the length is recovered from the pointed-at bytes via
  `unsafe` reads mirroring C's `DatumGetPointer` + `VARSIZE_ANY`/`strlen`.
  Opacity INHERITED from C's `Datum` contract.

## Function inventory (10 fns, matches C and c2rust exactly)

| C fn (datum.c) | C line | Port location | Verdict | Notes |
|---|---|---|---|---|
| `datumGetSize` | 65 | `datum_get_size_bytes` (byte), `datum_get_size_word` (word) | MATCH | byval→typLen; by-ref typLen>0→typLen; -1→`VARSIZE_ANY`; -2→`strlen+1`; else `Err("invalid typLen")`. NULL-ptr `ereport(ERRCODE_DATA_EXCEPTION,"invalid Datum pointer")` preserved in the byte lane (`invalid_datum_pointer`). Byval `Assert(typLen 1..=8)` is debug-only in C; omitting it is behaviour-preserving. |
| `datumCopy` | 132 | `datum_copy` (byte), `datum_copy_word` (word) | MATCH | byval verbatim; typLen==-1 → expanded-flatten (`EOH_get_flat_size`/`EOH_flatten_into` misc2 seams) vs verbatim `VARSIZE_ANY` copy; else `datumGetSize` copy. Word lane `palloc` modelled as a leaked boxed slice (owned as a `palloc`'d chunk); byte lane copies into caller `mcx`. |
| `datumTransfer` | 194 | `datum_transfer` (word) | MATCH | **Was MISSING; added during this audit.** Dispatch ported: `!typByVal && typLen==-1 && VARATT_IS_EXTERNAL_EXPANDED_RW(ptr)` → reparent leg; else `datumCopy`. The reparent leg's callee `TransferExpandedObject` is mirror-and-panic in its owner (misc2 `expandeddatum`, `-> !` at the `MemoryContextSetParent` mcx-ownership boundary); no seam exists to delegate to, so this leg mirrors that panic with the same rationale (unreachable on this unit's serial/copy consumers). `EXPANDED_RW` tag == 3 verified against `varatt.h`. |
| `datumIsEqual` | 223 | `datum_is_equal` (byte) | MATCH | byval word `==`; else `datumGetSize` both, length check, `memcmp`. No bare-Datum consumer; `pub fn`. |
| `datum_image_eq` | 266 | `datum_image_eq_bytes` (byte), `datum_image_eq_word` (word) | MATCH | byval `==`; typLen>0 `memcmp(typLen)`; -1 logical-payload compare after logical-length check; -2 `strlen+1` compare after length check; else `elog`→`Err`. Detoast (`toast_raw_datum_size`/`PG_DETOAST_DATUM_PACKED`) served over the already-detoasted in-line image (same convention as rowtypes/brin; the detoast owner is a cyclic-unported dep). |
| `datum_image_hash` | 338 | `datum_image_hash_bytes` (byte), `datum_image_hash_word` (word) | MATCH | byval `hash_bytes(&value, sizeof(Datum))`; typLen>0 `hash_bytes(ptr,typLen)`; -1 `hash_bytes(VARDATA_ANY,len-VARHDRSZ)`; -2 `hash_bytes(s,strlen+1)`; else `elog`→`Err`. `hash_bytes` is the non-cyclic `common-hashfn` direct dep. |
| `btequalimage` | 397 | `btequalimage` | MATCH | unconditional `true` (`PG_RETURN_BOOL(true)`), `opcintype` unused. |
| `datumEstimateSpace` | 412 | `datum_estimate_space` (word) | MATCH | `sizeof(int)` base; if !isnull: +`sizeof(Datum)` byval, +`EOH_get_flat_size` for external-expanded varlena, else +`datumGetSize`. |
| `datumSerialize` | 459 | `datum_serialize` (word) | MATCH | header `-2`(null)/`-1`(byval)/flat-size(expanded)/`datumGetSize`; byval writes `sizeof(Datum)` bytes; expanded flattened through a separate scratch buffer then memcpy'd (C's maxalign requirement); else memcpy payload. Cursor `*mut u8` models C `char **start_address`. |
| `datumRestore` | 521 | `datum_restore` (word) | MATCH | header `-2`→NULL; `-1`→read `sizeof(Datum)` bytes byval; else copy `header` bytes into leaked storage (`Assert(header>0)`→`debug_assert!`). |

### varatt.h byte helpers (re-derived, verified against headers)

`VARHDRSZ`=4, `VARHDRSZ_SHORT`=1, `VARHDRSZ_EXTERNAL`=2; `VARTAG_SIZE`:
INDIRECT(1)→8 (`sizeof(varatt_indirect)`=ptr), EXPANDED_RO(2)/RW(3)→8
(`sizeof(varatt_expanded)`=ptr), ONDISK(18)→16 (`sizeof(varatt_external)`=
i32+u32+Oid+Oid). `VARTAG_IS_EXPANDED(tag)`=`(tag & ~1)==2`. `VARSIZE_1B`/
`VARSIZE_4B` shift/mask bits (`>>1 &0x7F`, `>>2 &0x3FFFFFFF`) match. All
correct.

## Seam audit

Owned seam crates (by C-source coverage of `datum.c`):

* `backend-utils-adt-datum-seams` — declares `datum_copy`, `datum_estimate_space`,
  `datum_serialize`, `datum_restore`, `datum_image_hash`, `datum_image_eq`. All
  6 installed in `init_seams()`.
* `backend-utils-adt-scalar-seams` — datum.c's `datum_copy` decl (the bool.c
  `parse_bool` decl in the same crate is owned by `probe-adt-scalar-bool`, not
  this unit). The datum.c `datum_copy` decl is installed in `init_seams()`.

`init_seams()` is `set()` calls only; `seams-init::init_all()` wires this crate
(recurrence_guard: both `every_seam_installing_crate_is_wired_into_init_all` and
`every_declared_seam_is_installed_by_its_owner` pass). No uninstalled owned
seam, no `set()` outside the owner. Outward seam calls (`EOH_get_flat_size`,
`EOH_flatten_into` into misc2; `hash_bytes` direct) are thin marshal+delegate
over a genuine cyclic dep (expanded-object subsystem) / non-cyclic direct dep
(hashfn). No branching/computation hidden in any seam path.

## Design conformance

No invented opacity: the bare-Datum pointer-word + length-from-bytes recovery is
exactly C's `Datum` contract (inherited). `palloc`-shaped allocations on the
no-`Mcx` infallible seams are leaked boxed slices, owned as `palloc` chunks by
the caller per the declared contracts (`copyParamList`, nbtree array restore).
Expanded-object / `TransferExpandedObject` panics sit at the same misc2
mcx-ownership boundary the owner already flags mirror-and-panic. No shared
statics, no ambient-global seams, no locks across `?`, no `todo!`/`unimplemented!`.

## Verdict: PASS

Initial pass found `datumTransfer` **MISSING** (FAIL). Fixed by adding
`datum_transfer` (dispatch ported, reparent leg mirror-and-panics on the
unported `TransferExpandedObject` callee, common path delegates to the ported
`datum_copy_word`) + 2 tests. Re-audited from scratch: all 10 functions MATCH,
zero seam findings, zero residual own-logic stubs.

Gate: `cargo check --workspace` clean (only pre-existing unrelated
printtup warnings); `cargo test -p backend-utils-adt-scalar-datum-core` 20/20;
`cargo test -p seams-init` 2/2 (recurrence_guard green).
