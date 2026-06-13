//! Seam declarations for the `backend-executor-execMain` unit
//! (`executor/execMain.c`).
//!
//! The owning unit installs these from its `init_seams()` when it lands; until
//! then a call panics loudly.

#![allow(non_snake_case)]

seam_core::seam!(
    /// `epqstate->relsubs_done[scanrelid - 1]` (execMain.c / EvalPlanQual):
    /// whether the EPQ test tuple for this scan relation has already been
    /// returned. Pure read of the EPQ state.
    pub fn epq_relsubs_done(
        epqstate: types_nodes::EPQStateHandle,
        scanrelid_minus_1: u32,
    ) -> bool
);

seam_core::seam!(
    /// `epqstate->relsubs_slot[scanrelid - 1] != NULL` — is there a
    /// replacement-slot EPQ source for this scan relation?
    pub fn epq_relsubs_slot_present(
        epqstate: types_nodes::EPQStateHandle,
        scanrelid_minus_1: u32,
    ) -> bool
);

seam_core::seam!(
    /// `epqstate->relsubs_rowmark[scanrelid - 1] != NULL` — is there a
    /// non-locking-rowmark EPQ source for this scan relation?
    pub fn epq_relsubs_rowmark_present(
        epqstate: types_nodes::EPQStateHandle,
        scanrelid_minus_1: u32,
    ) -> bool
);

seam_core::seam!(
    /// `epqstate->relsubs_done[scanrelid - 1] = value` (execMain.c): mark
    /// whether the EPQ test tuple has been returned.
    pub fn epq_set_relsubs_done(
        epqstate: types_nodes::EPQStateHandle,
        scanrelid_minus_1: u32,
        value: bool,
    )
);

seam_core::seam!(
    /// Load the EPQ replacement slot (`epqstate->relsubs_slot[scanrelid - 1]`)
    /// into the scan node's scan slot (`ExecCopySlot`-shape), returning whether
    /// a (non-empty) tuple was loaded. Fallible on OOM.
    pub fn epq_load_relsubs_slot<'mcx>(
        epqstate: types_nodes::EPQStateHandle,
        estate: &mut types_nodes::EStateData<'mcx>,
        scanrelid_minus_1: u32,
        dest_slot: types_nodes::SlotId,
    ) -> types_error::PgResult<bool>
);

seam_core::seam!(
    /// `EvalPlanQualFetchRowMark(epqstate, scanrelid, slot)` (execMain.c):
    /// fetch the EPQ replacement tuple for a non-locking rowmark into the scan
    /// slot, returning whether a tuple was produced. Fallible on
    /// `ereport(ERROR)`.
    pub fn eval_plan_qual_fetch_row_mark<'mcx>(
        epqstate: types_nodes::EPQStateHandle,
        estate: &mut types_nodes::EStateData<'mcx>,
        scanrelid: u32,
        dest_slot: types_nodes::SlotId,
    ) -> types_error::PgResult<bool>
);

seam_core::seam!(
    /// For a `scanrelid == 0` Foreign/Custom scan that pushed a join down,
    /// whether the node's `extParam` set overlaps the EPQ relation set — the
    /// `bms_overlap` test in `ExecScanFetch`'s `scanrelid == 0` branch.
    pub fn epq_param_is_member_of_ext_param(
        epqstate: types_nodes::EPQStateHandle,
        node_ext_param: Option<&types_nodes::Bitmapset<'_>>,
    ) -> bool
);

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
