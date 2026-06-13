//! Seam declarations for the `backend-executor-execMain` unit
//! (`executor/execMain.c`).
//!
//! The owning unit installs these from its `init_seams()` when it lands; until
//! then a call panics loudly.

#![allow(non_snake_case)]

seam_core::seam!(
    /// `InitResultRelInfo(resultRelInfo, resultRelationDesc,
    /// resultRelationIndex, partition_root_rri, instrument_options)`
    /// (execMain.c): fill a `ResultRelInfo` for the given target relation
    /// (an alias handle of the relation `es_relations` owns, stored into
    /// `ri_RelationDesc`). Allocates trigger bookkeeping arrays in `mcx`
    /// when the relation has triggers (fallible on OOM); reads the
    /// relation's trigdesc through the relcache.
    pub fn init_result_rel_info<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        result_rel_info: &mut types_nodes::ResultRelInfo<'mcx>,
        relation: types_rel::Relation<'mcx>,
        result_relation_index: types_core::primitive::Index,
        partition_root_rri: Option<types_nodes::RriId>,
        instrument_options: i32,
    ) -> types_error::PgResult<()>
);

/// Outcome of the EvalPlanQual branch of `ExecScanFetch` (execScan.h).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum EpqScanFetch {
    /// The EPQ branch did not apply for this rel (the `scanrelid == 0` /
    /// not-a-pushed-join-descendant fall-through): run the node's access
    /// method instead.
    FallThrough,
    /// The EPQ branch produced a result: return this slot (the node's scan
    /// slot id), or `None` for the C `NULL` (empty result).
    Result(Option<types_nodes::SlotId>),
    /// The EPQ branch wants the caller to apply its access-method recheck to
    /// the node's scan slot, then (if the recheck fails) clear the slot; the
    /// `bool` mirrors the C "would not be returned by scan" clear flag. The
    /// caller returns the scan slot id when recheck passes, else the cleared
    /// slot per the embedded directive.
    Recheck { clear_on_fail: bool },
}

seam_core::seam!(
    /// The EvalPlanQual branch of `ExecScanFetch` (execScan.h): with an active
    /// `EPQState` (`estate.es_epq_active`), decide what the scan should return
    /// for this rel (`scanrelid`) — the replacement/test tuple, an empty slot,
    /// or a fall-through to the access method — performing the
    /// `relsubs_done`/`relsubs_slot`/`relsubs_rowmark` bookkeeping and any
    /// rowmark fetch. Returns an [`EpqScanFetch`] directive; the access-method
    /// recheck stays with the calling node (it owns `recheckMtd`). Fallible on
    /// `ereport(ERROR)`.
    pub fn exec_scan_fetch_epq<'mcx>(
        scanstate: &mut types_nodes::execnodes::ScanStateData<'mcx>,
        estate: &mut types_nodes::EStateData<'mcx>,
        scanrelid: types_core::primitive::Index,
    ) -> types_error::PgResult<EpqScanFetch>
);

seam_core::seam!(
    /// `ExecBuildSlotValueDescription(reloid, slot, tupdesc, modifiedCols,
    /// maxfieldlen)` (execMain.c): build a "(col, ...) = (val, ...)"
    /// description of the slot's contents, limited to the columns the current
    /// user has SELECT rights on (all columns when `modified_cols` names only
    /// accessible ones); `Ok(None)` when permissions allow no column (the C
    /// NULL). The string is allocated in `mcx`; column out-functions can
    /// `ereport(ERROR)`, carried on `Err`.
    pub fn exec_build_slot_value_description<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        reloid: types_core::Oid,
        slot: &types_nodes::TupleTableSlot,
        tupdesc: &types_tuple::heaptuple::TupleDescData<'_>,
        modified_cols: Option<&types_nodes::Bitmapset<'_>>,
        maxfieldlen: i32,
    ) -> types_error::PgResult<Option<mcx::PgString<'mcx>>>
);
