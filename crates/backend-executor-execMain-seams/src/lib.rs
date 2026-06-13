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

seam_core::seam!(
    /// `ExecCheckPermissions(rangeTable, rteperminfos, ereport_on_violation)`
    /// (execMain.c) for `RI_Initial_Check`'s SELECT probe on two relations.
    /// Each entry is `(relid, relkind, selected_col_attnums)` for an
    /// `RTE_RELATION` requiring `ACL_SELECT` on the listed columns
    /// (`AccessShareLock`); the owner builds the `RangeTblEntry` /
    /// `RTEPermissionInfo` nodes and runs the access-method-level permission
    /// check. Returns `true` if every check passes; with
    /// `ereport_on_violation = false` a denial is `Ok(false)`. Catalog/ACL
    /// lookups can `ereport(ERROR)`.
    pub fn exec_check_permissions_select(
        rels: &[(types_core::Oid, u8, &[i16])],
        ereport_on_violation: bool,
    ) -> types_error::PgResult<bool>
);
