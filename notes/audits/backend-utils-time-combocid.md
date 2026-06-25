# Audit: backend-utils-time-combocid (catalog unit backend-utils-time-small)

- C source: `src/backend/utils/time/combocid.c` (postgres-18.3, 364 lines)
- c2rust rendering: `c2rust-runs/probe-utils-time-combocid/src/combocid.rs`
- Port: `crates/backend-utils-time-combocid/src/lib.rs`
- Auditor: independent re-derivation from C + c2rust; constants verified
  against `src/include/access/htup_details.h`, `src/backend/storage/ipc/shmem.c`,
  and `src/backend/utils/errcodes.txt`.

## Function inventory and verdicts

Every function definition in combocid.c, including statics. The c2rust
rendering contains the same ten functions (asserts compiled out, as in the
production build); no `#if` branches exist in the C file.

| C function (combocid.c) | Port location (lib.rs) | Verdict | Notes |
|---|---|---|---|
| `HeapTupleHeaderGetCmin` (L103) | `HeapTupleHeaderGetCmin` (L88) | MATCH | Raw-cid fetch, `HEAP_COMBOCID` test, `GetRealCmin` dispatch identical. `Assert(!(t_infomask & HEAP_MOVED))` kept as `debug_assert!`; the `TransactionIdIsCurrentTransactionId(GetXmin)` Assert is a debug-only cross-subsystem check, absent (c2rust build also compiles asserts out); release behavior identical. |
| `HeapTupleHeaderGetCmax` (L117) | `HeapTupleHeaderGetCmax` (L106) | MATCH | Same structure. The `CritSectionCount > 0 \|\| TransactionIdIsCurrentTransactionId(GetUpdateXid)` Assert omitted (debug-only, cross-subsystem); release behavior identical. |
| `HeapTupleHeaderAdjustCmax` (L152) | `HeapTupleHeaderAdjustCmax` (L129) | MATCH | `!XminCommitted && TransactionIdIsCurrentTransactionId(RawXmin)` predicate identical, including the C "test XminCommitted first because it's cheaper" ordering (`&&` short-circuit). Combo branch computes cmin via `GetCmin` then `GetComboCommandId`; out-params become `PgResult<(CommandId, bool)>`, `Err` carrying the OOM `ereport(ERROR)` surface. `TransactionIdIsCurrentTransactionId` goes through the xact seam (see seam audit). |
| `AtEOXact_ComboCid` (L181) | `AtEOXact_ComboCid` (L154) | MATCH | C nulls `comboHash`/`comboCids` and zeroes the counters, relying on TopTransactionContext reset to free. Port sets `combo_hash = None` and replaces the vec with an empty one (drop frees); same observable post-state (empty, hash "not created"). |
| `GetComboCommandId` (static, L203) | `GetComboCommandId` (L163) | MATCH | Lazy first-use init (`comboHash == NULL` ⇔ `combo_hash.is_none()`): array of `CCID_ARRAY_SIZE`=100 made first, then hash of `CCID_HASH_SIZE`=100, both in the transaction Mcx (C: `TopTransactionContext` via `MemoryContextAlloc` + `HASH_CONTEXT`). Grow-before-hash-enter ordering preserved (doubling: `try_reserve_exact(capacity)` on `len >= capacity` ⇔ `newsize = sizeComboCids * 2`), so a failed grow cannot leave a dangling hash entry — the C comment's invariant holds. Found-entry reuse returns existing combocid; otherwise new combocid = `usedComboCids` ⇔ `combo_cids.len()`, array slot written, counter bumped (push), hash entry set. `hash_search(HASH_ENTER)` OOM `ereport` ⇔ `try_reserve(1)` before insert. `HASH_BLOBS` byte-keying ⇔ derived `Hash`/`Eq` on the two u32 fields (no padding; same value space). All allocation failures surface as `Err(mcx.oom(...))` ⇔ C OOM `ereport(ERROR)`. |
| `GetRealCmin` (static, L278) | `GetRealCmin` (L229) | MATCH | `Assert(combocid < usedComboCids)` + array read. Port's vec index panics on out-of-range in release too, where C release would read garbage — strictly tighter on a path that is UB in C; identical on all valid inputs. |
| `GetRealCmax` (static, L285) | `GetRealCmax` (L235) | MATCH | Same as above for `.cmax`. |
| `EstimateComboCIDStateSpace` (L296) | `EstimateComboCIDStateSpace` (L250) | MATCH | `sizeof(int)`=4 + `mul_size(sizeof(ComboCidKeyData)=8, used)` via `add_size`. The shmem.c helpers are mirrored locally as `checked_add`/`checked_mul` raising the exact C error: message "requested shared memory size overflows size_t", SQLSTATE 54000 `ERRCODE_PROGRAM_LIMIT_EXCEEDED` (verified against shmem.c L498-521 and errcodes.txt). `checked_mul` returns 0 for zero operands, matching mul_size's early return. |
| `SerializeComboCIDState` (L315) | `SerializeComboCIDState` (L270) | MATCH | Same layout: native-endian 4-byte int count then packed `(cmin,cmax)` u32 pairs (struct has no padding). Size check `needed > buf.len()` ⇔ `endptr > start_address + maxsize` (the `endptr < start_address` arm guards pointer overflow, impossible with usize arithmetic at these magnitudes); `elog(ERROR, "not enough space to serialize ComboCID state")` ⇔ `PgError::error(...)` (ERROR / XX000, matching elog's internal-error default — verified `default_sqlstate_for_level`). C writes the count before the size check; the port checks first — unobservable, since the C error path longjmps out of the serialize anyway and the buffer contents are then meaningless. |
| `RestoreComboCIDState` (L341) | `RestoreComboCIDState` (L305) | MATCH | `Assert(!comboCids && !comboHash)` ⇔ `debug_assert!`. Count read as native-endian i32; negative count yields an empty loop in both (C `i < num_elements` / Rust empty range). Per-element `GetComboCommandId` then `cid != i` check with the exact C message "unexpected command ID while restoring combo CIDs" (ERROR / XX000 ⇔ `elog(ERROR)`); allocation failure propagates as in C. Short-buffer reads (UB in C, which trusts the producer) are bounds-checked and surface as the same restore error — tighter only where C is undefined. |

Port-local helpers (not in combocid.c): `add_size`, `mul_size`,
`size_overflow` — private mirrors of the unported `storage/ipc/shmem.c`
functions used by `EstimateComboCIDStateSpace`; verified line-for-line against
shmem.c above. `ComboCidState::new` replaces the C module statics
(`comboHash`, `comboCids`, `usedComboCids`, `sizeComboCids`) with an owned
value threaded from the transaction machinery; initial state (`None`, empty,
0, 0) matches the statics' initializers, and `usedComboCids`/`sizeComboCids`
are carried by `PgVec::len()`/`capacity()`.

## Constants verified against headers

| Constant | C value | Port | OK |
|---|---|---|---|
| `HEAP_COMBOCID` | 0x0020 (htup_details.h L195) | types-tuple 0x0020 | yes |
| `HEAP_XMIN_COMMITTED` | 0x0100 (L204) | 0x0100 | yes |
| `HEAP_MOVED` | 0x4000\|0x8000 = 0xC000 (L211-217) | 0xC000 | yes |
| `CCID_HASH_SIZE` | 100 | 100 | yes |
| `CCID_ARRAY_SIZE` | 100 | 100 | yes |
| `sizeof(int)` / `sizeof(ComboCidKeyData)` | 4 / 8 | `SIZEOF_INT`=4 / `SIZEOF_COMBO_CID_KEY_DATA`=8 | yes |
| `ERRCODE_PROGRAM_LIMIT_EXCEEDED` | 54000 (errcodes.txt) | `make_sqlstate(*b"54000")` | yes |
| `elog(ERROR)` sqlstate | XX000 internal error | `PgError::error` default XX000 | yes |

Tuple accessors (`HeapTupleHeaderGetRawCommandId`, `HeapTupleHeaderGetRawXmin`,
`HeapTupleHeaderXminCommitted`) verified in types-tuple against
htup_details.h: same fields, same mask tests.

## Seam audit

- One outward seam call: `backend_access_transam_xact_seams::
  transaction_id_is_current_transaction_id::call(...)` in
  `HeapTupleHeaderAdjustCmax`. Justified: xact.c is unported and is mutually
  entangled with this unit (xact.c calls `AtEOXact_ComboCid`/serialize
  functions). The call site is a bare predicate call inside the C `if` —
  marshal-free delegate, no logic in the seam path.
- The seam declaration lives in `crates/backend-access-transam-xact-seams`
  (owned by the future xact unit); it is correctly *not* installed by this
  crate — calling it before xact lands panics loudly, which is the sanctioned
  behavior for unported callees.
- `backend-utils-time-combocid::init_seams()` is an empty no-op (the unit owns
  no seams) and is called from `seams-init::init_all()` (lib.rs L22). No
  `set()` calls outside an owner. No function body was replaced by a seam.

## Build and tests

`cargo test -p backend-utils-time-combocid`: 7 passed, 0 failed. Full
workspace `cargo build` clean.

## Verdict

**PASS** — all 10 C functions MATCH (one seam-delegated callee per the seam
rules); constants verified against headers; seam wiring clean.
