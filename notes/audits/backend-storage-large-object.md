# Audit: backend-storage-large-object

C source: `src/backend/storage/large_object/inv_api.c` (PG 18.3) — the
server-side inversion-fs large-object byte API. Independently re-derived from the
C, the c2rust rendering (`c2rust-runs/backend-storage-large-object/src/inv_api.rs`),
the src-idiomatic port
(`src-idiomatic/crates/backend-storage-large-object/src/lib.rs`), and the headers
`storage/large_object.h`, `catalog/pg_largeobject.h`, `libpq/libpq-fs.h`.

`LargeObjectDesc` and the open-descriptor lifecycle that the header places in
`storage/large_object.h` live in/around this TU; they are ported into
`types-storage::large_object`.

## Function inventory

| Function | C location | Port | Verdict | Notes |
|---|---|---|---|---|
| `open_lo_relation` (static) | inv_api.c:72-91 | lib.rs `open_lo_relation` | RE-MODELED | C caches `lo_heap_r`/`lo_index_r` in file statics + transfers ownership to `TopTransactionResourceOwner`. That static `Relation` cache + the resowner kludge cannot be a `'mcx`-bound static, and resowner is unported. Re-modeled as a per-operation `table_open(LargeObjectRelationId, RowExclusiveLock)` + `index_open(LargeObjectLOidPNIndexId, RowExclusiveLock)` in the op's `Mcx` (the repo's direct-call idiom, cf. merged pg_largeobject/pg_namespace/pg_depend). Each op closes both with `NoLock`, so the lock is retained till xact end exactly as C does; the relcache makes the per-op open cheap → behavior-equivalent. |
| `close_lo_relation` | inv_api.c:96-122 | lib.rs `close_lo_relation` (pub) | RE-MODELED | No surviving static reference to release (the per-op `NoLock` closes already released the relcache pins; locks held to xact end). Kept as the no-op xact-end entry point for the `xact.c` caller's contract. Installed as an outward seam. |
| `getdatafield` (static) | inv_api.c:130-157 | indexing seam `deform_lo_page` | SEAMED | The `HeapTupleHasNulls` "null field found" paranoia + `GETSTRUCT(Form_pg_largeobject)` pageno access + detoast of the `data` bytea + `VARSIZE - VARHDRSZ` length-sanity (`0..=LOBLKSIZE`, else `ERRCODE_DATA_CORRUPTED` "...has invalid data field size") is the catalog/heapam value layer → NEW `backend_catalog_indexing_seams::deform_lo_page` (owner indexing.c unported → panics, mirror-pg-and-panic). |
| `inv_create` | inv_api.c:172-202 | lib.rs `inv_create` | MATCH | `LargeObjectCreate(lobjId)` direct into merged crate; `recordDependencyOnOwner(LargeObjectRelationId, lobjId_new, GetUserId())` (shdepend-seams + miscinit-seams); `InvokeObjectPostCreateHook(LargeObjectRelationId, lobjId_new, 0)` (`object_access_hook_present` guard + `run_object_post_create_hook(..., false)`, direct into merged objectaccess); `CommandCounterIncrement()` (xact-seams). LO dependency under the **heap** class id preserved. |
| `inv_open` | inv_api.c:214-292 | lib.rs `inv_open` | MATCH | flag→descflags: `INV_WRITE` ⇒ `IFS_WRLOCK|IFS_RDLOCK`, `INV_READ` ⇒ `IFS_RDLOCK`; `descflags==0` ⇒ `ERRCODE_INVALID_PARAMETER_VALUE` "invalid flags ...". Snapshot: write ⇒ `None` (instantaneous), read ⇒ `GetActiveSnapshot()`. `LargeObjectExistsWithSnapshot(lobjId, snapshot)` direct; absent ⇒ `ERRCODE_UNDEFINED_OBJECT`. SELECT then UPDATE permission branches, each `!lo_compat_privileges && pg_largeobject_aclcheck_snapshot(...) != ACLCHECK_OK` ⇒ `ERRCODE_INSUFFICIENT_PRIVILEGE` "permission denied ...". Descriptor `{id, offset=0, flags=descflags, subid=InvalidSubTransactionId, snapshot}`, returned `Box` (C `MemoryContextAlloc`). Branch order verbatim. |
| `inv_close` | inv_api.c:298-303 | lib.rs `inv_close` | MATCH | Consumes the owned `Box<LargeObjectDesc>` (C `pfree` → `Drop`). |
| `inv_drop` | inv_api.c:310-331 | lib.rs `inv_drop` | MATCH | `performDeletion({LargeObjectRelationId, lobjId, 0}, DROP_CASCADE, 0)` (dependency-seams; the `ObjectAddress` is passed as the seam's class/object/subid scalars); `CommandCounterIncrement()`; returns 1. |
| `inv_getsize` (static) | inv_api.c:339-385 | lib.rs `inv_getsize` | MATCH | `open_lo_relation`; scankey `(loid, BTEqual, F_OIDEQ, id)`; `systable_beginscan_ordered`; ONE `systable_getnext_ordered(BackwardScanDirection)` (last page = max byte) ⇒ `deform_lo_page` ⇒ `lastbyte = pageno*LOBLKSIZE + len`; `endscan`. |
| `inv_seek` | inv_api.c:387-434 | lib.rs `inv_seek` | MATCH | `SEEK_SET`/`SEEK_CUR`/`SEEK_END(inv_getsize+offset)`; default ⇒ `ERRCODE_INVALID_PARAMETER_VALUE` "invalid whence ..."; `newoffset<0 || >MAX_LARGE_OBJECT_SIZE` ⇒ `errmsg_internal` "invalid large object seek target". |
| `inv_tell` | inv_api.c:436-447 | lib.rs `inv_tell` | MATCH | returns `offset`. |
| `inv_read` | inv_api.c:449-540 | lib.rs `inv_read` | MATCH | `IFS_RDLOCK` check ⇒ `ERRCODE_INSUFFICIENT_PRIVILEGE`; `nbytes<=0` ⇒ 0; 2-key scan `(loid==id, pageno>=pageno)` (`F_OIDEQ`/`F_INT4GE`); forward ordered loop: hole-zero (`pageoff>offset`), then partial-page `memcpy` from `data[off..]`, both clamped to `nbytes-nread`; `break` at `nread>=nbytes`. Byte arithmetic + clamps 1:1. |
| `inv_write` | inv_api.c:542-737 | lib.rs `inv_write` | MATCH | `IFS_WRLOCK` check; `nbytes<=0` ⇒ 0; `nbytes+offset > MAX_LARGE_OBJECT_SIZE` ⇒ `ERRCODE_INVALID_PARAMETER_VALUE`; `CatalogOpenIndexes`; 2-key ordered scan; `neednextpage`/`olddata` loop. Existing page (`olddata.pageno==pageno`): load old into `workb`, fill hole (`off>len`), copy new `LOBLKSIZE-off` clamped, recompute `len`, `catalog_tuple_update_with_info_pg_largeobject(tid, workb[..len])`. Brand-new page: fill leading hole (`off>0`), copy, `len=off+n`, `catalog_tuple_insert_with_info_pg_largeobject(id, pageno, workb[..len])`. `pageno++`; `endscan`; `CatalogCloseIndexes`; `CommandCounterIncrement`. |
| `inv_truncate` | inv_api.c:739-915 | lib.rs `inv_truncate` | MATCH | `IFS_WRLOCK` check; `len<0 || >MAX` ⇒ `errmsg_internal` "invalid large object truncation target"; `CatalogOpenIndexes`; 2-key ordered scan; first page. Found cut page: load, fill hole (`off>pagelen`), update with `workb[..off]`. Else: if a later page exists delete it (`pageno>pageno` assert), then fill hole + insert `workb[..off]`. Then if `olddata` was found, delete every remaining page. `endscan`; `CatalogCloseIndexes`; `CommandCounterIncrement`. |

## Seam crossings

INWARD (this crate owns + installs via `init_seams`, in
`backend-storage-large-object-seams`, consumed by `be-fsstubs` / the LO SQL
functions / `xact.c` — all unported): `close_lo_relation`, `inv_create`,
`inv_open`, `inv_close`, `inv_drop`, `inv_seek`, `inv_tell`, `inv_read`,
`inv_write`, `inv_truncate`.

OUTWARD (called here, installed by their real owners):
- direct calls (no seam): `LargeObjectCreate` / `LargeObjectExistsWithSnapshot`
  (merged `backend-catalog-pg-largeobject`), `object_access_hook_present` /
  `run_object_post_create_hook` (merged `backend-catalog-objectaccess`),
  `table_open` (merged `backend-access-table-table`), `ScanKeyInit` (merged
  `backend-access-common-scankey`).
- seams (owner unported → panic = mirror-pg-and-panic): `index_open`
  (indexam-seams); `systable_beginscan_ordered` / `systable_getnext_ordered` /
  `systable_endscan_ordered` (genam-seams); `recordDependencyOnOwner`
  (shdepend-seams); `perform_deletion` (dependency-seams); `get_user_id`
  (miscinit-seams); `get_active_snapshot` (snapmgr-seams);
  `command_counter_increment` (xact-seams); `pg_largeobject_aclcheck_snapshot`
  (aclchk-seams); `catalog_open_indexes` / `catalog_close_indexes` /
  `catalog_tuple_delete` (existing indexing-seams); and the 3 NEW LO-page
  value-layer seams in indexing-seams: `deform_lo_page`,
  `catalog_tuple_insert_with_info_pg_largeobject`,
  `catalog_tuple_update_with_info_pg_largeobject`.

## Constants / types added

- `types-storage::large_object`: `LargeObjectDesc` (id/snapshot/subid/offset/
  flags), `IFS_RDLOCK=1`, `IFS_WRLOCK=2`, `LOBLKSIZE = BLCKSZ/4 = 2048`,
  `MAX_LARGE_OBJECT_SIZE = INT_MAX * LOBLKSIZE` (verified vs large_object.h).
- `types-catalog`: `ANUM_PG_LARGEOBJECT_PAGENO=2`, `ANUM_PG_LARGEOBJECT_DATA=3`
  (pg_largeobject column order: loid/pageno/data).
- `types-core::fmgr`: `F_INT4GE=150` (int4ge, pg_proc.dat oid 150).
- In-crate constants spelled as the C macros: `INV_WRITE=0x00020000`,
  `INV_READ=0x00040000` (libpq-fs.h), `SEEK_SET/CUR/END=0/1/2` (stdio).

## Deferrals / divergences (all sanctioned)

- `lo_compat_privileges` GUC ⇒ `false` (boot value; GUC owner unported). When the
  GUC lands this becomes a real `bool`.
- The static-relation cache + `TopTransactionResourceOwner` kludge re-modeled as
  per-op open/close (behavior-equivalent; see `open_lo_relation`).
- `workbuf` union (`bytea hdr` / `char data[LOBLKSIZE+VARHDRSZ]`) ⇒ owned
  `Vec<u8>` of `LOBLKSIZE`; the `SET_VARSIZE` varlena framing is performed inside
  the insert/update LO-page seam.

ZERO `todo!()`/`unimplemented!()`. 13 in-crate unit tests cover the seam-free
paths (flag mapping, seek/tell + bounds, zero-length no-ops, permission/param/
size error SQLSTATEs, constant parity).
