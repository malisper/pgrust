# Audit: backend-commands-sequence (sequence.c)

Independent function-by-function logic audit against postgres-18.3
`src/backend/commands/sequence.c` (1965 lines) + `sequence.h` + `pg_sequence.h`.
Re-derived from C; port comments/self-review not trusted.

## Per-function verdicts

| C function | C loc | Port loc (lib.rs) | Verdict | Notes |
|---|---|---|---|---|
| DefineSequence | 120-247 | 263-340 | MATCH | if_not_exists pre-check, init_params, DefineRelation (via tablecmds seam), fill_seq_with_data, OWNED BY, pg_sequence insert (via indexing seam). value[]/null[] column build + heap_form_tuple folded into the catalog-insert seam owner — faithful. |
| ResetSequence | 261-329 | 347-394 | MATCH | read old tuple, fetch seqstart from syscache, copy tuple → set last_value=startv/is_called=false/log_cnt=0, RelationSetNewRelfilenumber, fill_seq_with_data, elm->cached=elm->last. relfrozenxid/relminmxid Asserts dropped (debug-only, harmless). |
| fill_seq_with_data | 337-353 | 403-420 | **DIVERGES (MISSING ops)** | Unlogged path omits `smgrcreate(INIT_FORKNUM)`, `log_smgrcreate(&rd_locator, INIT_FORKNUM)`, `FlushRelationBuffers(rel)`, `smgrclose(srel)`. Port only re-calls `fill_seq_fork_with_data(INIT_FORKNUM)`; comments hand-wave the rest as "performed as part of writing the init fork", but the code does not perform them and no seam carries them. See Finding 1. |
| fill_seq_fork_with_data | 358-429 | 424-504 | MATCH (mostly) | ExtendBufferedRel, PageInit(special=4), magic poke, frozen-xmin/xmax-invalid header pokes, PageAddItem at FirstOffsetNumber, GetTopTransactionId-if-WAL, MarkBufferDirty, XLOG when `RelationNeedsWAL || forkNum==INIT_FORKNUM`. Byte image built via build_seq_item is faithful. Minor: double content-lock (ExtendBufferedRel + explicit lock_buffer_exclusive) — see Finding 4 (observation). |
| AlterSequence | 436-538 | 607-697 | MATCH | RangeVarGetRelidExtended-owns callback, init_sequence, syscache copy, read_seq_tuple→copy, init_params, conditional rewrite (GetTopTransactionId/RelationSetNewRelfilenumber/fill), elm->cached=elm->last, OWNED BY, CatalogTupleUpdate + InvokeObjectPostAlterHook (folded into update seam). |
| SequenceChangePersistence | 540-567 | 704-731 | MATCH | LockRelationOid(AccessExclusiveLock), init_sequence, GetTopTransactionId-if-WAL, read_seq_tuple, RelationSetNewRelfilenumber(newpersistence), fill_seq_with_data, UnlockReleaseBuffer ordering preserved. |
| DeleteSequenceTuple | 569-585 | 738-744 | MATCH (SEAMED) | open/search/CatalogTupleDelete/release/close folded into catalog_delete_pg_sequence seam; `found==false` → cache-lookup-failed elog. Faithful. |
| nextval | 592-612 | 751-766 | MATCH | text→qualified-name-list→RangeVarGetRelid(NoLock,false)→nextval_internal(true). |
| nextval_oid | 614-620 | 769-774 | MATCH | |
| nextval_internal | 622-863 | 777-984 | MATCH | Cached-fast-path, aclcheck, read-only/parallel guards, syscache params, read_seq_tuple, fetch/log/cycle loop, all four limit branches (incby>0/<0 × maxv/minv sign), rescnt/break/cycle wrap, `log -= fetch`, local-cache save, logit WAL of post-fetch state (last_value=next/is_called/log_cnt=0), final state write. Error paths release the buffer before returning (correct adaptation, no resowner). The C `fetch = log = fetch + SEQ_LOG_VALS` rendered as `fetch += SEQ_LOG_VALS; log = fetch;` — equivalent. |
| currval_oid | 865-894 | 1020-1057 | MATCH | aclcheck ACL_SELECT\|ACL_USAGE, last_valid check, returns elm->last. |
| lastval | 896-929 | 1064-1110 | MATCH | LAST_USED_SEQ none→error, SearchSysCacheExists(RELOID) drop check, lock_and_open, aclcheck, elm->last. |
| do_setval | 944-1042 | 1117-1206 | MATCH | aclcheck ACL_UPDATE, syscache min/max, read-only/parallel guards, read_seq_tuple, bounds check (releases buffer before error), iscalled→last/last_valid, cached=last, GetTopTransactionId-if-WAL, write state, MarkBufferDirty, XLOG, close. |
| setval_oid | 1048-1057 | 1209-1217 | MATCH | |
| setval3_oid | 1063-1073 | 1220-1229 | MATCH | |
| lock_and_open_sequence | 1084-1107 | 1237-1254 | MATCH | lxid compare, LockRelationOid(RowExclusiveLock) when not held, set lxid, sequence_open(NoLock). Resowner switch to TopTransactionResourceOwner not modeled (comment notes it is the lmgr owner's concern) — acceptable given no resowner model. |
| create_seq_hashtable | 1112-1122 | (inlined in init_sequence 1264-1280) | MATCH | HashMap::new() lazily; key=Oid. |
| init_sequence | 1128-1177 | 1262-1298 | MATCH | find-or-create entry (filenumber=Invalid, lxid=Invalid, last_valid=false, last=cached=0), lock_and_open, relfilenode-change → cached=last. Note `increment` not explicitly initialized on create in C either (left 0 by hash alloc); port sets increment:0 explicitly — equivalent. |
| read_seq_tuple | 1189-1234 | 1307-1372 | MATCH | ReadBuffer(0)+exclusive lock, magic check (elog with %08X), FirstOffsetNumber item, XMAX_IS_MULTI assert, non-frozen-xmax hint-bit cleanup (clear XMAX_COMMITTED, set XMAX_INVALID, MarkBufferDirtyHint), decode three columns. |
| init_params | 1256-1582 | 1379-1714 | MATCH | All option-parse branches + conflict detection, log_cnt resets, AS-type INT2/4/8 check + old-min/max reset logic, INCREMENT!=0, CYCLE, MAXVALUE/MINVALUE (null-arg = NO MAX/MIN + ascending/descending defaults), int2/int4 range validation, min<max crosscheck, START defaults+crosscheck, RESTART, CACHE>0. Bounds constants correct. Faithful branch-for-branch. |
| process_owned_by | 1592-1700 | 1784-1924 | MATCH | deptype INTERNAL vs AUTO, OWNED BY NONE vs table.col, relkind allow-set (RELATION/FOREIGN/VIEW/PARTITIONED), same owner+namespace, get_attnum, identity-sequence AUTO guard (sequenceIsOwned), deleteDependencyRecordsForClass, recordDependencyOn, relation_close(NoLock). |
| sequence_options | 1706-1735 | 1932-1955 | MATCH | makeFloat(INT64) for cache/increment/maxvalue/minvalue/start, makeBoolean for cycle, same order. |
| pg_sequence_parameters | 1740-1777 | 2004-2049 | MATCH | aclcheck ACL_SELECT\|UPDATE\|USAGE, 7-col record start/min/max/increment/cycle/cache/typid. get_call_result_type composite check folded into record_from_values seam. |
| pg_get_sequence_data | 1786-1838 | 2056-2098 | **DIVERGES** | C uses `try_relation_open` (NULL only for missing rel); port uses `table_open(...).ok()`, which swallows ALL errors into the all-NULL branch, not just relation-missing. See Finding 2. Otherwise the relkind/aclcheck/other-temp/permanent-or-not-recovery gate + 2-col record is faithful. |
| pg_sequence_last_value | 1846-1888 | 2105-2137 | MATCH | init_sequence, aclcheck ACL_SELECT\|USAGE + !other_temp + (permanent \|\| !RecoveryInProgress), read tuple, is_called?INT64:NULL. |
| seq_redo | 1891-1939 | 2144-2198 | MATCH | info check vs XLOG_SEQ_LOG (PANIC), XLogInitBufferForRedo, local-page build (PageInit special=4, magic, PageAddItem at FirstOffsetNumber, PageSetLSN), memcpy into buffer page, MarkBufferDirty, UnlockReleaseBuffer. itemsz = data_len - sizeof(xl_seq_rec) (=12) and item slice match. |
| ResetSequenceCaches | 1944-1954 | 2213-2217 | MATCH | drop hash + last_used_seq. (C hash_destroy → set None; equivalent.) |
| seq_mask | 1959-1965 | 2224-2228 | MATCH | mask_page_lsn_and_checksum + mask_unused_space. |

No C function is missing from the port. (seq_desc / seq_identify are declared in sequence.h but defined in `access/rmgr` desc files, not sequence.c — correctly out of scope.)

## Constants check

SEQ_MAGIC=0x1717 ✓, SEQ_LOG_VALS=32 ✓, XLOG_SEQ_LOG=0x00 ✓, RM_SEQ_ID=15 ✓,
REGBUF_WILL_INIT (re-exported) ✓, XLR_INFO_MASK=0x0F ✓, sizeof(xl_seq_rec)=12 ✓,
PG_INT16/32/64 MIN/MAX ✓, InvalidAttrNumber=0 ✓, RelationRelationId=1259 ✓,
DEPENDENCY_INTERNAL vs AUTO ✓, ACL_USAGE/UPDATE/SELECT ✓, special-area size 4 ✓.
FormData_pg_sequence (types-catalog/pg_sequence.rs) is a real struct with all 8
fields in catalog order (seqrelid, seqtypid, seqstart, seqincrement, seqmax,
seqmin, seqcache, seqcycle) — no alias/blob, no invented opacity. ✓

## Seam audit

Owned seams (backend-commands-sequence-seams): seq_redo, seq_mask,
reset_sequence_caches, DeleteSequenceTuple. All four are installed by
`init_seams()` (lib.rs 2306-2311) and `backend_commands_sequence::init_seams()`
is wired into seams-init/src/lib.rs:111. ✓

Outward seam CALLS are thin marshal+delegate (catalog insert/update/delete,
DefineRelation, bufmgr, xloginsert, syscache, aclchk, namespace, dependency,
funcapi). The new catalog-indexing seam decls (catalog_insert/update/delete_pg_sequence)
and tablecmds define_sequence_relation are thin declarations delegating the
open/form/insert/close cycle to the owner — acceptable (that logic is the
indexing/tablecmds owner's, not sequence.c's). ✓

**Seam Finding A — real algorithm in a bridge installer (namespace crate).**
`backend-catalog-namespace/src/lib.rs` adds `RangeVarCallbackOwnsRelation`
(the tablecmds.c callback) as a full implementation inside the namespace crate:
syscache pg_class lookup, `object_ownercheck`/`aclcheck_error`, and the
`allow_system_table_mods` + `is_system_class` system-catalog protection with a
hand-built ERRCODE_INSUFFICIENT_PRIVILEGE ereport. This is substantive
tablecmds.c logic placed in a namespace bridge installer rather than a thin
bridge. (The three namespace seam wrappers themselves —
range_var_get_relid_from_text / _and_check_creation_namespace / _owns_seq — are
thin and fine; the embedded callback is the issue.) See Finding 3.

The parse-type bridge (`seam_typename_type_id_from_defelem` + `raw_typename_to_parse`)
is a node-model converter feeding the owner's `typenameTypeId`; it is marshalling
(K1 raw TypeName → resolver TypeName), not sequence algorithm — acceptable.

## Design conformance

- thread_local (not static) for SEQHASHTAB + LAST_USED_SEQ ✓
- allocating fns take Mcx + return PgResult ✓; OOM via mcx (alloc_in/vec_with_capacity_in returns PgResult) ✓
- no invented opacity; FormData_pg_sequence real struct, correct field order ✓
- locks/buffers not held across `?` without release: error paths in
  nextval_internal (limit), do_setval (bounds), read_seq_tuple all release the
  buffer before returning Err ✓
- LAST_USED_SEQ modeled as the relid key (entry lives in SEQHASHTAB) — faithful ✓

## FINDINGS

### Finding 1 (DIVERGES — unlogged init-fork creation/logging/flush MISSING)
`fill_seq_with_data` (lib.rs 403-420): for RELPERSISTENCE_UNLOGGED, C does
`smgropen` → `smgrcreate(srel, INIT_FORKNUM, false)` → `log_smgrcreate(&rd_locator,
INIT_FORKNUM)` → fill init fork → `FlushRelationBuffers(rel)` → `smgrclose(srel)`.
The port performs only `fill_seq_fork_with_data(INIT_FORKNUM)`. The smgrcreate,
log_smgrcreate, FlushRelationBuffers, and smgrclose are absent, and no seam carries
them. Consequence: an unlogged sequence's init fork is not explicitly created /
its creation not WAL-logged / its buffers not flushed to disk as in C. (The
INIT_FORKNUM tuple WAL record IS emitted inside fill_seq_fork_with_data, but that
is the page contents, not the relation-fork creation log or the flush.) The
in-code comments asserting these are "performed as part of writing the init fork"
are inaccurate. Fix: add the smgrcreate/log_smgrcreate/FlushRelationBuffers/
smgrclose calls (via bufmgr/smgr seams) in the unlogged branch.

### Finding 2 (DIVERGES — try_relation_open semantics)
`pg_get_sequence_data` (lib.rs 2067): C calls `try_relation_open(relid,
AccessShareLock)`, which returns NULL **only** when the relation does not exist
and otherwise opens/locks normally (raising on genuine errors). The port uses
`table_open(relid, AccessShareLock).ok()`, converting **every** error
(lock failure, etc.) — not just relation-missing — into the all-NULL result
branch. This widens the "return NULLs" behavior beyond the missing-relation case.
Fix: introduce/use a `try_relation_open` (missing_ok) seam that returns
`Option<Relation>` for the not-found case while still propagating other errors.

### Finding 3 (SEAM — real algorithm leaked into a non-owner bridge installer)
`backend-catalog-namespace/src/lib.rs` `RangeVarCallbackOwnsRelation`: the
tablecmds.c ownership/system-catalog-protection callback is implemented in full
inside the namespace crate (ownercheck, aclcheck_error, allow_system_table_mods +
is_system_class guard, ereport). The instruction "no real algorithm in bridge
installers in other crates the port modified" is violated. This logic belongs to
its real owner (tablecmds.c `RangeVarCallbackOwnsRelation`) and should be invoked
via a seam, not re-implemented in the namespace bridge.

### Finding 4 (OBSERVATION — possible double content-lock)
`fill_seq_fork_with_data` (lib.rs 431-434) calls `extend_buffered_rel` then
`lock_buffer_exclusive`. C's `ExtendBufferedRel(EB_LOCK_FIRST | ...)` returns the
buffer already content-locked; the `extend_buffered_rel` seam takes no flags, so
whether this is a correct single lock or a double-lock depends on the seam's
implementation. Not provably a defect from this crate alone, but flagged for the
bufmgr owner to confirm the seam does not lock (else this self-deadlocks/asserts).

## VERDICT: FAIL

Two behavioral divergences (Finding 1 — missing unlogged init-fork
creation/WAL-log/flush; Finding 2 — try_relation_open over-broad error
swallowing) and one seam finding (Finding 3 — tablecmds ownership callback
re-implemented in the namespace bridge installer). PASS requires zero divergence
and zero seam findings. Finding 4 is an observation to confirm, not counted.

---

## RE-AUDIT (post-fix) — VERDICT: PASS

All findings resolved and re-verified against sequence.c:

- **Finding 1 (RESOLVED):** `fill_seq_with_data` now performs the full unlogged
  init-fork sequence in C order (sequence.c:342-352): `smgr_create_init_fork_and_log`
  (smgropen + smgrcreate(INIT_FORKNUM,false) + log_smgrcreate, owned by storage.c,
  thin delegate to real smgr fns) → `fill_seq_fork_with_data(INIT_FORKNUM)` →
  `flush_relation_buffers` (delegates to the real `FlushRelationBuffers`) →
  `relation_close_smgr` (smgrclose).
- **Finding 2 (RESOLVED):** `pg_get_sequence_data` uses
  `backend_access_common_relation_seams::try_relation_open` (Ok(None) only for a
  missing relation; other errors propagate via `?`); closes with AccessShareLock.
- **Finding 3 (RESOLVED):** the `RangeVarCallbackOwnsRelation` re-implementation
  is removed from the namespace bridge; the bridge callback now thin-delegates to
  `backend_commands_tablecmds_seams::range_var_callback_owns_relation` (owner
  tablecmds, unported → panic-until-landed, not installed here). namespace's own
  `RangeVarGetRelidExtended` owner-check logic (legitimately namespace.c) retained.
- **Finding 4 (RESOLVED):** confirmed the `extend_buffered_rel` seam returns an
  already write-locked buffer (`EB_LOCK_FIRST`); removed the redundant
  `lock_buffer_exclusive` call — matches sequence.c:368-372 (no second lock).

Gate: cargo check --workspace GREEN; no-todo-guard GREEN; seams-init GREEN (all 4
owned seams installed + wired; newly declared neighbor seams owned by their real
owners). Full workspace test: only allowed flakes (range_pair_*, hashfunc
text_nondeterministic) fail. All 28 functions MATCH or legitimately SEAMED.
