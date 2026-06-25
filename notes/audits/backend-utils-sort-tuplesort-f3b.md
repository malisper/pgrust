# Audit: backend-utils-sort-tuplesort F3b (index sort variants, #320)

Scope: the index-tuple sort variants of tuplesortvariants.c added on top of the
landed F0/F1/F2 engine + F3a (heap/datum). Source:
postgres-18.3/src/backend/utils/sort/tuplesortvariants.c. This is a
divergence-resolution lane: it installs the 5 previously-allowlisted seams and
removes their CONTRACT_RECONCILE_PENDING entries.

## Seams installed (init_seams, wired in seams-init::init_all at line 370)

- tuplesort_begin_index_btree  -> seam_begin_index_btree -> tuplesort_begin_index_btree_state
- tuplesort_begin_index_hash   -> seam_begin_index_hash  -> tuplesort_begin_index_hash_state
- tuplesort_begin_index_gist   -> seam_begin_index_gist  -> tuplesort_begin_index_gist_state
- tuplesort_putindextuplevalues-> seam_putindextuplevalues-> tuplesort_putindextuplevalues_impl
- tuplesort_getindextuple      -> seam_getindextuple     -> tuplesort_getindextuple_impl

Recurrence guards (seams-init): both pass
(`every_seam_installing_crate_is_wired_into_init_all`,
`every_declared_seam_is_installed_by_its_owner`). The 5 allowlist tuples were
deleted from CONTRACT_RECONCILE_PENDING; the second guard confirms they are now
genuinely installed.

## Per-function parity table

| C function (tuplesortvariants.c) | Rust | Parity notes |
|---|---|---|
| tuplesort_begin_index_btree | tuplesort_begin_index_btree_state | nKeys=indnkeyatts; _bt_mkscankey(indexRel,NULL); per-col SortSupport: ssup_collation=sk_collation, ssup_nulls_first=(sk_flags&SK_BT_NULLS_FIRST)!=0, ssup_attno=sk_attno, abbreviate=(i==0&&haveDatum1), reverse=(sk_flags&SK_BT_DESC)!=0, PrepareSortSupportFromIndexRel. arg=IndexBtree{enforceUnique,uniqueNullsNotDistinct}. trace_sort elog omitted (logging, no behavior). pfree(indexScanKey) is arena drop. |
| tuplesort_begin_index_hash | tuplesort_begin_index_hash_state | nKeys=1; no SortSupport array (comparetup uses masks); arg=IndexHash{high_mask,low_mask,max_buckets}. trace_sort omitted. |
| tuplesort_begin_index_gist | tuplesort_begin_index_gist_state | nKeys=indnkeyatts; per-col SortSupport: ssup_collation=rd_indcollation[i], ssup_nulls_first=false, ssup_attno=i+1, abbreviate=(i==0&&haveDatum1), PrepareSortSupportFromGistIndexRel. Shares IndexBtree arg with enforceUnique=uniqueNullsNotDistinct=false and SortVariantKind::IndexBtree (same comparetup/writetup/readtup as btree, per C). |
| tuplesort_putindextuplevalues | tuplesort_putindextuplevalues_impl | index_form_tuple_context(RelationGetDescr(rel),values,isnull)+t_tid=*self via the index_form_tuple seam (forms + stamps TID, returns on-disk image). datum1=index_getattr(tuple,1,RelationGetDescr(indexRel)). tuplen=byte len (the bump-cxt fast path = MAXALIGN(IndexTupleSize); GetMemoryChunkSpace path collapses to the same stored size in the owned model). use_abbrev=sortKeys[0].abbrev_converter&&!isnull1 (sortKeys empty for hash => false, matching base->sortKeys NULL check). |
| tuplesort_getindextuple | tuplesort_getindextuple_impl | gettuple_common; returns (IndexTuple)stup.tuple bytes or NULL. |
| removeabbrev_index | removeabbrev_index | for each memtuple: datum1=index_getattr(tuple,1,RelationGetDescr(indexRel),&isnull1). |
| comparetup_index_btree | comparetup_index_btree | ApplySortComparator on leading key; then comparetup_index_btree_tiebreak. |
| comparetup_index_btree_tiebreak | comparetup_index_btree_tiebreak | abbrev_converter => ApplySortAbbrevFullComparator on attr1; equal_hasnull from a->isnull1; loop nkey=2..keysz with index_getattr+ApplySortComparator (equal_hasnull on isnull1); uniqueness: enforceUnique && !(!uniqueNullsNotDistinct && equal_hasnull) => index_deform_tuple + BuildIndexValueDescription => ERROR (errcode UNIQUE_VIOLATION, "could not create unique index", key_desc detail or "Duplicate keys exist."); then heap-TID block#/offset# ItemPointer tiebreak; final Assert(false)=>debug_assert. |
| comparetup_index_hash | comparetup_index_hash | bucket=_hash_hashkey2bucket(DatumGetUInt32(datum1),max_buckets,high_mask,low_mask); compare bucket, then hash (DatumGetUInt32(datum1)), then ItemPointer block#/offset#; Assert(!isnull1)=>debug_assert; final Assert(false). |
| comparetup_index_hash_tiebreak | (none) | C body is Assert(false)+return 0; hash sort has 1 key so never reached — not a dispatchable arm here (comparetup_index_hash returns directly). Faithful (the C fn is "only here for consistency"). |
| writetup_index | writetup_index | tuplen=IndexTupleSize(tuple)+sizeof(tuplen); write len word + IndexTupleSize bytes (== PgVec len); trailing len word if TUPLESORT_RANDOMACCESS. |
| readtup_index | readtup_index | tuplen=len-sizeof(uint); read tuplen bytes; trailing len word if RANDOMACCESS; datum1=index_getattr(tuple,1,RelationGetDescr(indexRel)). |

## Constant verification (against postgres-18.3 headers)

- SK_BT_INDOPTION_SHIFT = 24 (access/nbtree.h:1146). types_scan::scankey matches.
- SK_BT_DESC = INDOPTION_DESC(0x0001, pg_index.h:89) << 24. types_scan: 0x0001<<24. OK.
- SK_BT_NULLS_FIRST = INDOPTION_NULLS_FIRST(0x0002, pg_index.h:90) << 24. types_scan: 0x0002<<24. OK.
- INDEX_SIZE_MASK = 0x1FFF (itup.h:65) = IndexTupleSize. The TupleBody::Index PgVec<u8> is exactly the IndexTuple on-disk image of length IndexTupleSize (on_disk_image() == size()); writetup uses tuple.len() == IndexTupleSize. OK.
- ItemPointerData byte layout: ip_blkid.bi_hi[0..2], bi_lo[2..4], ip_posid[4..6], t_info[6..8] — matches FormedIndexTuple::on_disk_image and itup_block_number/itup_offset_number.
- _hash_hashkey2bucket (hashutil.c:124): bucket=hashkey&highmask; if bucket>maxbucket bucket&=lowmask. Inlined verbatim (pure bit arithmetic, no catalog/state).

## Model / divergence notes

- IndexTuple carrier: reuses the existing TupleBody::Index(PgVec<u8>) on-disk
  byte image (the model F3a established). No new carrier.
- index_getattr(tuple,1,..): routed through the nocache_index_getattr seam (the C
  macro's fallback; identical result for any attr). index_deform_tuple seam used
  for the uniqueness-violation key_desc.
- _hash_hashkey2bucket inlined (4-line pure function) rather than a cross-unit
  seam, per the sanctioned pure-arithmetic-inline convention.
- Relation carrying into the self-owned engine (OwnedSort for<'sx> bundle): the
  index descriptor is deep-cloned (TupleDescSnapshot) for the hot paths; the two
  Relation handles are carried via a sound lifetime-extending alias
  (relation_into_engine: alias() bumps the relcache cell Rc refcount, pinning the
  allocation for the engine's whole life — mirrors the established
  seam_puttupleslot/seam_putdatum transmute pattern). Used only by the cold
  uniqueness-violation error path (BuildIndexValueDescription + relname).
- SortSupport built against the live caller relation outside the build closure,
  then snapshot lifetime-free (SortSupportSnapshot: every field except ssup_cxt is
  a Copy registry token/scalar) and rebuilt in the engine arena. Behaviour-
  preserving deep copy of the C MemoryContextSwitchTo(maincontext) setup.
- trace_sort elog(LOG,...) lines omitted (logging only, no behavior), consistent
  with F3a.
- BRIN/GIN variants and the parallel SortCoordinate legs and CLUSTER variant
  remain out of scope (sanctioned seam-panic / not-this-task), as before.

## Gate

- cargo check --workspace: clean.
- cargo test -p backend-utils-sort-tuplesort: 15 passed.
- cargo test -p seams-init: 2 recurrence guards pass.
- residual_own_todos: 0 (no todo!/unimplemented!; index comparetup/writetup/readtup
  arms now have real bodies, CLUSTER arm remains a sanctioned mirror-and-panic).
