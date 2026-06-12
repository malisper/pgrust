//! Seam declarations for the `backend-parser-relation` unit
//! (`parser/parse_relation.c`).
//!
//! The owning unit installs these from its `init_seams()` when it lands; until
//! then a call panics loudly.

seam_core::seam!(
    /// `getRTEPermissionInfo(rteperminfos, rte)` (parse_relation.c): fetch
    /// the `RTEPermissionInfo` the RTE's `perminfoindex` points at. C returns
    /// the list cell's pointer; the owned model returns the 0-based index
    /// into `rteperminfos`. `Err` carries the C `elog(ERROR)`s (invalid
    /// `perminfoindex`, relid mismatch).
    pub fn get_rte_permission_info(
        rteperminfos: &[types_nodes::RTEPermissionInfo<'_>],
        rte: &types_nodes::RangeTblEntry,
    ) -> types_error::PgResult<usize>
);
