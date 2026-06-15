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
        rte: &types_nodes::RangeTblEntry<'_>,
    ) -> types_error::PgResult<usize>
);

seam_core::seam!(
    /// `GetNSItemByRangeTablePosn(pstate, varno, sublevels_up)` followed by
    /// `scanNSItemForColumn(pstate, nsitem, sublevels_up, colname, location)`
    /// (parse_relation.c), as used by `ParseComplexProjection` (parse_func.c)
    /// for the whole-row-Var fast path (`(foo.*).bar`).
    ///
    /// The C first resolves the namespace item the whole-row Var refers to
    /// (`GetNSItemByRangeTablePosn`, which `elog(ERROR)`s if not found), then
    /// asks it for the named column, returning a freshly-built `Var` node or
    /// `NULL` if the column name does not match. Because the resolved
    /// `ParseNamespaceItem *` is an internal pointer into `pstate`, the seam
    /// crosses the item by its `(varno, sublevels_up)` identity — exactly the
    /// key `GetNSItemByRangeTablePosn` looks up — and the owner re-resolves it
    /// and performs the column scan. `Ok(None)` is the C `NULL` (no match).
    pub fn scan_ns_item_for_column_by_posn<'mcx>(
        pstate: &mut types_nodes::parsestmt::ParseState<'mcx>,
        varno: i32,
        sublevels_up: i32,
        colname: &str,
        location: i32,
    ) -> types_error::PgResult<Option<types_nodes::primnodes::Expr>>
);

seam_core::seam!(
    /// `expandRecordVariable(pstate, var, levelsup)` (parse_relation.c): find
    /// the tuple descriptor a Var of type `RECORD` ultimately refers to, by
    /// chasing it back to its defining subquery/join/RECORD function. Used by
    /// `ParseComplexProjection` (parse_func.c). The returned descriptor is
    /// owned in `mcx`; `Ok(None)` is the C `NULL` (unresolvable RECORD type).
    pub fn expand_record_variable<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        pstate: &mut types_nodes::parsestmt::ParseState<'mcx>,
        var: &types_nodes::primnodes::Var,
        levelsup: i32,
    ) -> types_error::PgResult<types_tuple::heaptuple::TupleDesc<'mcx>>
);
