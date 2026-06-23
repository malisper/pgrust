//! Seam declarations for the `backend-executor-execCurrent` unit
//! (`executor/execCurrent.c`): the `WHERE CURRENT OF cursor` resolver.
//!
//! The owning unit installs these from its `init_seams()` when it lands; until
//! then a call panics loudly.

#![allow(non_snake_case)]

seam_core::seam!(
    /// `execCurrentOf(cexpr, econtext, table_oid, &current_tid)`
    /// (execCurrent.c): resolve the cursor named by `cexpr` to the row it is
    /// currently positioned on in the relation `table_oid`. Returns
    /// `Some(tid)` when the cursor yields a TID for this table (the C `true`,
    /// with `*current_tid` set), `None` otherwise (the C `false`). The
    /// `econtext` (id into the EState pool) supplies the param context for a
    /// REFCURSOR-parameter cursor. Fallible on `ereport(ERROR)` (cursor not
    /// found / not positioned on a row / not a simple scan).
    pub fn exec_current_of<'mcx>(
        cexpr: &nodes::primnodes::CurrentOfExpr,
        econtext: nodes::EcxtId,
        table_oid: types_core::Oid,
        estate: &mut nodes::EStateData<'mcx>,
    ) -> types_error::PgResult<Option<types_tuple::heaptuple::ItemPointerData>>
);
