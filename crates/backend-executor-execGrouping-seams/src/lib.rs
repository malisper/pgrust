//! Seam declarations for the `backend-executor-execGrouping` unit
//! (`executor/execGrouping.c`).
//!
//! The owning unit installs these from its `init_seams()` when it lands; until
//! then a call panics loudly.

#![allow(non_snake_case)]

seam_core::seam!(
    /// `execTuplesMatchPrepare(desc, numCols, keyColIdx, eqOperators,
    /// collations, parent)` (execGrouping.c): build the `ExprState` that tests
    /// two tuples of the given descriptor for equality on the named key
    /// columns (used for `LIMIT ... WITH TIES` peer detection, `DISTINCT`,
    /// etc.). A zero-column key compiles to `None` (the C `NULL` ExprState).
    /// The compiled state is allocated in the EState's per-query context
    /// (fallible on OOM); preparation can also `ereport(ERROR)`.
    pub fn exec_tuples_match_prepare<'mcx>(
        desc: types_tuple::heaptuple::TupleDesc<'mcx>,
        num_cols: i32,
        key_col_idx: &[types_core::primitive::AttrNumber],
        eq_operators: &[types_core::primitive::Oid],
        collations: &[types_core::primitive::Oid],
        parent: &mut types_nodes::execnodes::PlanStateData<'mcx>,
        estate: &mut types_nodes::EStateData<'mcx>,
    ) -> types_error::PgResult<Option<mcx::PgBox<'mcx, types_nodes::execexpr::ExprState>>>
);
