//! Seam declarations for the `backend-access-index-genam` unit
//! (`access/index/genam.c`), the system-table scan facility.
//!
//! The owning unit installs these from its `init_seams()` when it lands; until
//! then a call panics loudly.
//!
//! The API mirrors C's iterator (`systable_beginscan*` /
//! `systable_getnext*` / `systable_endscan*`): the caller opens the catalog
//! (and, for the ordered variant, the index) itself, exactly as in C.
//! Relations cross as borrows of the caller's open
//! `types_rel::RelationData` carriers; snapshots as trimmed
//! `types_snapshot::SnapshotData`; the live scan state is the trimmed
//! `types_scan::genam::SysScanDescData`, held by a [`SysScanGuard`] so the
//! scan is closed on every early return (AGENTS.md "Locks and held
//! resources"). C returns a `HeapTuple` owned by the scan (valid until the
//! next call); the owned model copies each result tuple out into `mcx`.

use types_core::primitive::{AttrNumber, Oid};
use types_error::PgResult;
use types_scan::genam::SysScanDescData;

/* ==========================================================================
 * High-level relcache catalog-scan seams.
 *
 * `RelationGetIndexList`/`RelationGetStatExtList`/`RelationGetFKeyList`/
 * `RelationGetExclusionInfo` (relcache.c) each open a system catalog,
 * `systable_beginscan` it under the conrelid/indrelid/stxrelid key, and
 * `GETSTRUCT`/`heap_getattr`-decode every matching tuple. The whole scan +
 * per-row decode is genam-owned catalog vocabulary; the relcache caller only
 * consumes the decoded rows. These seams package the scan-and-decode that the
 * genam owner performs, returning plain owner-vocabulary rows (no relcache
 * types — the relcache caller marshals them into its owned entry fields).
 * Panic until the genam owner installs them.
 * ======================================================================== */

/// One decoded `pg_class` row as `ScanPgRelation` (relcache.c) consumes it: the
/// `Form_pg_class` scalar columns the relcache copies into `rd_rel`, in
/// owner-vocabulary form (no relcache types — the relcache caller marshals this
/// into its owned `FormPgClass`). The variable-length tail columns (`relacl`,
/// `reloptions`, `relpartbound`) are not part of the fixed-width form the
/// relcache caches in `rd_rel`; `reloptions` is consumed separately by
/// `RelationParseRelOptions` (its own primitive). `oid` is the row's OID
/// (`pg_class.oid`), which the relcache uses as `rd_id`.
#[derive(Clone, Debug)]
pub struct ScannedPgClass {
    pub oid: Oid,
    pub relname: String,
    pub relnamespace: Oid,
    pub reltype: Oid,
    pub reloftype: Oid,
    pub relowner: Oid,
    pub relam: Oid,
    pub relfilenode: Oid,
    pub reltablespace: Oid,
    pub relpages: i32,
    pub reltuples: f32,
    pub relallvisible: i32,
    pub reltoastrelid: Oid,
    pub relhasindex: bool,
    pub relisshared: bool,
    pub relpersistence: i8,
    pub relkind: i8,
    pub relnatts: i16,
    pub relchecks: i16,
    pub relhasrules: bool,
    pub relhastriggers: bool,
    pub relhassubclass: bool,
    pub relrowsecurity: bool,
    pub relforcerowsecurity: bool,
    pub relispopulated: bool,
    pub relreplident: i8,
    pub relispartition: bool,
    pub relrewrite: Oid,
    pub relfrozenxid: u32,
    pub relminmxid: u32,
}

/// One decoded `pg_attribute` row as `RelationBuildTupleDesc` (relcache.c)
/// consumes it: the fixed-layout `Form_pg_attribute` columns it copies into the
/// tuple descriptor's attribute array, in owner-vocabulary form (no relcache
/// types — the relcache caller marshals this into its owned `OwnedAttr`). The
/// `attmissingval` / `attacl` / `attoptions` / `attfdwoptions` / `attstattarget`
/// tail columns are not part of the fixed descriptor data the relcache builds
/// from here (`attmissingval` is fetched separately when `atthasmissing`).
#[derive(Clone, Debug)]
pub struct ScannedPgAttribute {
    pub attname: String,
    pub atttypid: Oid,
    pub attlen: i16,
    pub attnum: AttrNumber,
    pub atttypmod: i32,
    pub attndims: i16,
    pub attbyval: bool,
    pub attalign: i8,
    pub attstorage: i8,
    pub attcompression: i8,
    pub attnotnull: bool,
    pub atthasdef: bool,
    pub atthasmissing: bool,
    pub attidentity: i8,
    pub attgenerated: i8,
    pub attisdropped: bool,
    pub attislocal: bool,
    pub attinhcount: i16,
    pub attcollation: Oid,
}

/// One decoded `pg_index` row as `RelationGetIndexList` consumes it: the
/// `Form_pg_index` flags + `int2vector indkey` it needs, plus whether the
/// `indpred` attribute is null (`heap_attisnull(Anum_pg_index_indpred)`).
#[derive(Clone, Debug)]
pub struct ScannedPgIndex {
    pub indexrelid: Oid,
    pub indnatts: i16,
    pub indnkeyatts: i16,
    pub indisunique: bool,
    pub indnullsnotdistinct: bool,
    pub indisprimary: bool,
    pub indisexclusion: bool,
    pub indimmediate: bool,
    pub indisclustered: bool,
    pub indisvalid: bool,
    pub indcheckxmin: bool,
    pub indisready: bool,
    pub indislive: bool,
    pub indisreplident: bool,
    /// `int2vector indkey` — table column numbers of the index columns.
    pub indkey: Vec<AttrNumber>,
    /// `heap_attisnull(rd_indextuple, Anum_pg_index_indpred)`.
    pub indpred_isnull: bool,
}

/// One decoded `pg_constraint` foreign-key row as `RelationGetFKeyList`
/// consumes it (the `ForeignKeyCacheInfo` payload built by
/// `DeconstructFkConstraintRow`). Opaque to the relcache caller (it only caches
/// the list and the presence flag), so only the constraint OID is exposed.
#[derive(Clone, Debug)]
pub struct ScannedFkInfo {
    pub conoid: Oid,
}

/// One decoded `pg_rewrite` row as `RelationBuildRuleLock` consumes it: the
/// `Form_pg_rewrite` scalar fields plus the two node-string `text` columns
/// (`ev_qual`/`ev_action`), returned as their raw `nodeToString` text so the
/// relcache builder can `stringToNode` them into the process-lifetime
/// CacheMemoryContext arena (keeping the cached `Query` trees in cache memory,
/// not in the catalog-scan `mcx`). The scan order is the
/// `RewriteRelRulesIndexId` order (by `rulename`).
#[derive(Clone, Debug)]
pub struct ScannedPgRewrite {
    /// `Form_pg_rewrite.oid` — the rule OID (`RewriteRule.ruleId`).
    pub ruleid: Oid,
    /// `char ev_type` — `'1'`..`'4'`; `RelationBuildRuleLock` does
    /// `rule->event = ev_type - '0'` to get the `CmdType`.
    pub ev_type: u8,
    /// `char ev_enabled` — `'O'`/`'D'`/`'R'`/`'A'` (`RewriteRule.enabled`).
    pub ev_enabled: u8,
    /// `bool is_instead` (`RewriteRule.isInstead`).
    pub is_instead: bool,
    /// `text ev_qual` — the rule qualification's `nodeToString` text, or `None`
    /// if the attribute is NULL (`heap_attisnull`). `stringToNode`'d by the
    /// relcache builder into the cache arena.
    pub ev_qual: Option<String>,
    /// `text ev_action` — the action `List<Query>`'s `nodeToString` text, or
    /// `None` if NULL. `stringToNode`'d into the cache arena.
    pub ev_action: Option<String>,
}

/// One key column's resolved exclusion info as `RelationGetExclusionInfo`
/// consumes it: the operator OID (`conexclop`), its underlying procedure OID
/// (`get_opcode`), and its opfamily strategy number
/// (`get_op_opfamily_strategy`).
#[derive(Clone, Copy, Debug)]
pub struct ExclusionKeyInfo {
    pub op: Oid,
    pub proc: Oid,
    pub strat: u16,
}

seam_core::seam!(
    /// `ScanPgRelation(targetRelId, indexOK, force_non_historic)` (relcache.c):
    /// `table_open(RelationRelationId)`, `systable_beginscan(ClassOidIndexId,
    /// oid = targetRelId)` then a single `systable_getnext` +
    /// `GETSTRUCT(Form_pg_class)` deform. Returns the found row's decoded
    /// `pg_class` form, `Ok(None)` for the C NULL (no matching row). The
    /// relcache caller marshals this into its owned `FormPgClass` and
    /// `rd_id`/`rd_rel`. Can `ereport(ERROR)` (catalog read failure), carried on
    /// `Err`. (`index_ok` toggles the index-vs-heap scan; the relcache passes
    /// it straight through to `systable_beginscan`.)
    pub fn scan_pg_class(reloid: Oid, index_ok: bool) -> PgResult<Option<ScannedPgClass>>
);

seam_core::seam!(
    /// `RelationBuildTupleDesc(relation)`'s `pg_attribute` scan (relcache.c):
    /// `table_open(AttributeRelationId)`, `systable_beginscan(
    /// AttributeRelidNumIndexId, attrelid = relid, attnum > 0)` then a
    /// `systable_getnext` loop + `GETSTRUCT(Form_pg_attribute)` deform for each
    /// of the `natts` user columns. Returns the decoded rows (the relcache
    /// caller marshals each into its owned `OwnedAttr`, fetches `attmissingval`
    /// separately when `atthasmissing`, and fills the descriptor). `natts` is
    /// `relation->rd_rel->relnatts`, used to size the scan and detect a short
    /// catalog. Can `ereport(ERROR)` (catalog read failure / missing column),
    /// carried on `Err`.
    pub fn scan_pg_attribute(reloid: Oid, natts: i16) -> PgResult<Vec<ScannedPgAttribute>>
);

seam_core::seam!(
    /// `RelationGetIndexList`'s scan (relcache.c): `systable_beginscan(pg_index,
    /// IndexIndrelidIndexId, indrelid = relid)` then `systable_getnext` +
    /// `GETSTRUCT(Form_pg_index)` for each row. Returns every matching decoded
    /// row. Can `ereport(ERROR)` (catalog read failure), carried on `Err`.
    pub fn relcache_scan_pg_index(relid: Oid) -> PgResult<Vec<ScannedPgIndex>>
);

seam_core::seam!(
    /// `RelationBuildRuleLock`'s scan (relcache.c):
    /// `systable_beginscan(pg_rewrite, RewriteRelRulesIndexId, ev_class =
    /// relid)` then `systable_getnext` + `GETSTRUCT(Form_pg_rewrite)` and the
    /// two `heap_getattr` node-string columns (`ev_qual`/`ev_action`) for each
    /// row. Returns every matching decoded row in scan (`rulename`) order; the
    /// relcache builder `stringToNode`s the node strings into the cache arena
    /// and sorts the rules by `ruleId`. Can `ereport(ERROR)` (catalog read
    /// failure), carried on `Err`.
    pub fn relcache_scan_pg_rewrite(relid: Oid) -> PgResult<Vec<ScannedPgRewrite>>
);

seam_core::seam!(
    /// `RelationGetStatExtList`'s scan (relcache.c):
    /// `systable_beginscan(pg_statistic_ext, StatisticExtRelidIndexId,
    /// stxrelid = relid)` then `systable_getnext`, collecting each object OID.
    /// Can `ereport(ERROR)`, carried on `Err`.
    pub fn relcache_scan_pg_statistic_ext(relid: Oid) -> PgResult<Vec<Oid>>
);

seam_core::seam!(
    /// `RelationGetFKeyList`'s scan (relcache.c):
    /// `systable_beginscan(pg_constraint, conrelid = relid)`, keeping the
    /// foreign keys, then `DeconstructFkConstraintRow` to build each
    /// `ForeignKeyCacheInfo`. Returns the assembled rows. Can
    /// `ereport(ERROR)`, carried on `Err`.
    pub fn relcache_scan_pg_constraint_fkeys(relid: Oid) -> PgResult<Vec<ScannedFkInfo>>
);

seam_core::seam!(
    /// `RelationGetExclusionInfo`'s scan (relcache.c):
    /// `systable_beginscan(pg_constraint, conrelid = indrelid)`, matching the
    /// constraint whose `conindid` is `index_relid` and decoding its
    /// `conexclop` array, then `get_opcode`/`get_op_opfamily_strategy` per key
    /// column (lsyscache). Returns one [`ExclusionKeyInfo`] per key column
    /// (`indnkeyatts` long, in column order). Can `ereport(ERROR)`, carried on
    /// `Err`.
    pub fn relcache_exclusion_info(
        index_relid: Oid,
        indrelid: Oid,
        indnkeyatts: usize,
    ) -> PgResult<Vec<ExclusionKeyInfo>>
);

seam_core::seam!(
    /// `systable_beginscan(heapRelation, indexId, indexOK, snapshot, nkeys,
    /// key)` (genam.c): begin a scan of a system(-like) table. `index_ok`
    /// false forces a heap scan; `snapshot` `None` is the C NULL (use the
    /// catalog snapshot, registered by the owner and recorded in the
    /// descriptor for unregistration at end of scan). The `keys` slice
    /// carries `nkeys`. `Err` carries the scan-setup error surface (fmgr
    /// lookup of the key procedures, AM begin-scan).
    pub fn systable_beginscan(
        heap_relation: &types_rel::RelationData<'_>,
        index_id: types_core::primitive::Oid,
        index_ok: bool,
        snapshot: Option<&types_snapshot::SnapshotData>,
        keys: &[types_scan::scankey::ScanKeyData],
    ) -> types_error::PgResult<SysScanGuard>
);

seam_core::seam!(
    /// `systable_getnext(sysscan)` (genam.c): the next tuple of the scan,
    /// copied into `mcx`, or `None` at the end. `Err` carries the index/heap
    /// fetch error surface.
    pub fn systable_getnext<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        sysscan: &mut types_scan::genam::SysScanDescData,
    ) -> types_error::PgResult<
        Option<types_tuple::backend_access_common_heaptuple::FormedTuple<'mcx>>,
    >
);

seam_core::seam!(
    /// `systable_endscan(sysscan)` (genam.c): finish the scan, releasing
    /// the AM scan state and unregistering the descriptor's snapshot.
    /// Reached only through [`SysScanGuard`] (`end()` or `Drop`); consumers
    /// never call it directly. `Err` carries the AM end-scan error surface.
    pub fn systable_endscan(
        sysscan: types_scan::genam::SysScanDescData,
    ) -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `systable_recheck_tuple(sysscan, tup)` (genam.c): recheck visibility of
    /// the most-recently-fetched tuple under a fresh catalog snapshot,
    /// returning whether it is still live. The C `tup` argument only asserts
    /// it matches `sysscan->slot`; the recheck itself reads the scan's live
    /// slot, so the owned model passes only the scan descriptor (the caller
    /// invokes this immediately after the `systable_getnext` that produced the
    /// current row). `Err` carries the snapshot-acquisition / heap-fetch error
    /// surface as well as any concurrent-abort handling.
    pub fn systable_recheck_tuple(
        sysscan: &mut types_scan::genam::SysScanDescData,
    ) -> types_error::PgResult<bool>
);

seam_core::seam!(
    /// `systable_beginscan_ordered(heapRelation, indexRelation, snapshot,
    /// nkeys, key)` (genam.c): begin an index scan on a system(-like) table,
    /// ordered by the index. The caller has the index open (`index_open`),
    /// as in C. `snapshot` `None` is the C NULL (use the catalog snapshot).
    /// The `keys` slice carries `nkeys`. `Err` carries the index-scan-setup
    /// error surface.
    pub fn systable_beginscan_ordered(
        heap_relation: &types_rel::RelationData<'_>,
        index_relation: &types_rel::RelationData<'_>,
        snapshot: Option<&types_snapshot::SnapshotData>,
        keys: &[types_scan::scankey::ScanKeyData],
    ) -> types_error::PgResult<SysScanGuard>
);

seam_core::seam!(
    /// `systable_getnext_ordered(sysscan, direction)` (genam.c): the next
    /// tuple of the ordered scan, copied into `mcx`, or `None` at the end.
    /// `Err` carries the index/heap fetch error surface.
    pub fn systable_getnext_ordered<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        sysscan: &mut types_scan::genam::SysScanDescData,
        direction: types_scan::sdir::ScanDirection,
    ) -> types_error::PgResult<
        Option<types_tuple::backend_access_common_heaptuple::FormedTuple<'mcx>>,
    >
);

seam_core::seam!(
    /// `systable_endscan_ordered(sysscan)` (genam.c): finish the ordered
    /// scan. Reached only through [`SysScanGuard`] (`end()` or `Drop`);
    /// consumers never call it directly. `Err` carries the AM end-scan
    /// error surface.
    pub fn systable_endscan_ordered(
        sysscan: types_scan::genam::SysScanDescData,
    ) -> types_error::PgResult<()>
);

seam_core::seam!(
    /// The combined `systable_inplace_update_begin` → mutate → finish standard
    /// flow (genam.c), specialized to the "overwrite a fixed-size header field"
    /// use the catalog callers (`dropdb` marking `datconnlimit` invalid,
    /// `vac_update_datfrozenxid`, etc.) need. C exposes three primitives
    /// (`_begin` returns an exclusive-locked buffer + the live `oldtup`, the
    /// caller mutates `GETSTRUCT(tup)`, then `_finish` calls
    /// `heap_inplace_update_and_unlock`); the locked buffer + `SysScanDesc` are
    /// scan-internal state that cannot cross a seam without leaking the buffer
    /// lock across the consumer's `?`, so the owner runs the whole flow and the
    /// per-row mutation is supplied as a callback (AGENTS.md "shared-state
    /// access goes through a callback shape").
    ///
    /// The owner: `systable_inplace_update_begin(relation, index_id, index_ok,
    /// NULL, keys)` (the buffer-locking retry loop) → if no live tuple, returns
    /// `Ok(None)` (the C `*oldtupcopy = NULL`); else builds a writable copy of
    /// the tuple's user-data area, invokes `mutate(&mut new_data)` (the C
    /// `datform->field = ...` in-place edit — the area cannot change size), then
    /// `systable_inplace_update_finish` (`heap_inplace_update_and_unlock`) and
    /// `systable_endscan`. Returns the updated tuple's `t_self` (so a caller can
    /// follow with a transactional `CatalogTupleDelete`, as `dropdb` does), or
    /// `None` when the key found no live tuple. `Err` carries the
    /// parallel-mode / retry-exhaustion / buffer-lock / WAL `ereport(ERROR)`
    /// surface. The owning genam unit installs this from its `init_seams()`.
    ///
    /// `mutate` returns a "dirty" flag. The C callers run
    /// `systable_inplace_update_begin` → mutate `GETSTRUCT(tup)` → conditionally
    /// `systable_inplace_update_finish` *or* `systable_inplace_update_cancel`
    /// (e.g. `index_update_stats` cancels — never WAL-logs — when no column
    /// actually changed). The callback may both *read* the existing column bytes
    /// (to compute that decision) and *write* the new image in place; returning
    /// `Ok(true)` makes the owner run `_finish` (WAL + inplace cache inval),
    /// `Ok(false)` makes it run `_cancel` (`heap_inplace_unlock`, no WAL). The
    /// returned `t_self` (`Some` when a live tuple was found) is supplied in both
    /// cases, so a caller that cancels can still issue its own
    /// `CacheInvalidateRelcacheByTuple`.
    pub fn systable_inplace_update<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        relation: &types_rel::RelationData<'mcx>,
        index_id: Oid,
        index_ok: bool,
        keys: &[types_scan::scankey::ScanKeyData],
        mutate: &mut dyn FnMut(&mut [u8]) -> types_error::PgResult<bool>,
    ) -> types_error::PgResult<Option<types_tuple::heaptuple::ItemPointerData>>
);

seam_core::seam!(
    /// `BuildIndexValueDescription(indexRelation, values, isnull)` (genam.c):
    /// build a "(key_names) = (key_values)" description of an index entry,
    /// or `Ok(None)` when the current user lacks rights to see the key values
    /// (the C NULL). `values`/`isnull` are `FormIndexDatum` outputs (the raw
    /// index-AM input). The string is allocated in `mcx`; key out-functions
    /// can `ereport(ERROR)`, carried on `Err`.
    pub fn build_index_value_description<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        index_relation: &types_rel::Relation<'_>,
        values: &[types_tuple::backend_access_common_heaptuple::Datum<'mcx>],
        isnull: &[bool],
    ) -> types_error::PgResult<Option<mcx::PgString<'mcx>>>
);

/// One deformed `pg_attrdef` row, as produced by [`scan_pg_attrdef`]: the
/// `adnum` plus the `adbin` default-expression node-tree text already run
/// through `TextDatumGetCString` (`None` is the C `isnull`). The owner does
/// the `table_open(AttrDefaultRelationId)`, the `systable_beginscan` on
/// `adrelid = relid`, the per-row `GETSTRUCT(Form_pg_attrdef)` deform, and the
/// `adbin` text detoast; this DTO carries exactly the two fields
/// `AttrDefaultFetch` consumes.
#[derive(Debug, Clone)]
pub struct PgAttrdefRow {
    /// `attrdef->adnum`.
    pub adnum: types_core::primitive::AttrNumber,
    /// `TextDatumGetCString(adbin)`, or `None` for the C `isnull`.
    pub adbin: Option<String>,
}

/// One deformed `pg_constraint` row, as produced by
/// [`scan_pg_constraint_nncheck`]: the `contype` plus the per-kind fields
/// `CheckNNConstraintFetch` consumes. For a NOT NULL constraint,
/// `!convalidated` and `extractNotNullColumn(htup)`; for a CHECK constraint,
/// the enforced/valid/noinherit flags, the name, and the `conbin` node-tree
/// text already run through `TextDatumGetCString` (`None` is the C `isnull`).
/// The owner does the `table_open(ConstraintRelationId)`, the
/// `systable_beginscan` on `conrelid = relid`, the per-row
/// `GETSTRUCT(Form_pg_constraint)` deform, `extractNotNullColumn`, and the
/// `conbin` text detoast.
#[derive(Debug, Clone)]
pub struct PgConstraintNnCheckRow {
    /// `conform->contype` (`CONSTRAINT_NOTNULL`/`CONSTRAINT_CHECK`/other).
    pub contype: i8,
    /// NOT NULL only: `!conform->convalidated`.
    pub notnull_invalid: bool,
    /// NOT NULL only: `extractNotNullColumn(htup)`.
    pub notnull_attnum: types_core::primitive::AttrNumber,
    /// CHECK only: `conform->conenforced`.
    pub ccenforced: bool,
    /// CHECK only: `conform->convalidated`.
    pub ccvalid: bool,
    /// CHECK only: `conform->connoinherit`.
    pub ccnoinherit: bool,
    /// CHECK only: `NameStr(conform->conname)`.
    pub ccname: String,
    /// CHECK only: `TextDatumGetCString(conbin)`, or `None` for the C `isnull`.
    pub ccbin: Option<String>,
}

seam_core::seam!(
    /// `AttrDefaultFetch`'s `pg_attrdef` scan (relcache.c): `table_open`,
    /// `systable_beginscan(AttrDefaultIndexId, adrelid = relid)`, then a
    /// `systable_getnext` loop deforming `Form_pg_attrdef` and running
    /// `TextDatumGetCString(adbin)`. Returns every matching row in scan order;
    /// the caller does the per-attribute accounting/sort/install. `Err`
    /// carries the scan-setup / index-or-heap fetch / detoast error surface.
    pub fn scan_pg_attrdef(
        relid: types_core::primitive::Oid,
    ) -> types_error::PgResult<Vec<PgAttrdefRow>>
);

seam_core::seam!(
    /// `CheckNNConstraintFetch`'s `pg_constraint` scan (relcache.c):
    /// `table_open`, `systable_beginscan(ConstraintRelidTypidNameIndexId,
    /// conrelid = relid)`, then a `systable_getnext` loop deforming
    /// `Form_pg_constraint`, calling `extractNotNullColumn(htup)` for NOT NULL
    /// rows and `TextDatumGetCString(conbin)` for CHECK rows. Returns every
    /// matching row in scan order; the caller does the per-kind
    /// accounting/sort/install + not-null attnullability fixup. `Err` carries
    /// the scan-setup / fetch / detoast error surface.
    pub fn scan_pg_constraint_nncheck(
        relid: types_core::primitive::Oid,
    ) -> types_error::PgResult<Vec<PgConstraintNnCheckRow>>
);

/// The live-scan token returned by [`systable_beginscan`] /
/// [`systable_beginscan_ordered`]: owns the `SysScanDescData`. `Drop` ends
/// the scan silently (the abort path); [`Self::end`] is the explicit
/// `systable_endscan(_ordered)` at the C call site, surfacing its error.
#[derive(Debug)]
pub struct SysScanGuard {
    desc: Option<SysScanDescData>,
    ordered: bool,
}

impl SysScanGuard {
    /// Wrap a just-begun scan (`ordered` records which begin-scan flavor
    /// created it, so release dispatches to the matching end-scan). Called
    /// by the owner's installed implementation (and test fixtures);
    /// consumers only ever receive one.
    pub fn new(desc: SysScanDescData, ordered: bool) -> Self {
        SysScanGuard {
            desc: Some(desc),
            ordered,
        }
    }

    /// The scan descriptor, as `systable_getnext*` consumes it.
    pub fn desc_mut(&mut self) -> &mut SysScanDescData {
        self.desc.as_mut().expect("SysScanGuard already ended")
    }

    /// `systable_endscan(sysscan)` / `systable_endscan_ordered(sysscan)` at
    /// the C call site, consuming the guard.
    pub fn end(mut self) -> PgResult<()> {
        let desc = self.desc.take().expect("SysScanGuard ended twice");
        if self.ordered {
            systable_endscan_ordered::call(desc)
        } else {
            systable_endscan::call(desc)
        }
    }
}

impl Drop for SysScanGuard {
    fn drop(&mut self) {
        if let Some(desc) = self.desc.take() {
            // The abort path: end silently (C reaches the equivalent
            // releases through error-recovery resource cleanup).
            let _ = if self.ordered {
                systable_endscan_ordered::call(desc)
            } else {
                systable_endscan::call(desc)
            };
        }
    }
}
