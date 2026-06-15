//! Seam declarations for the SP-GiST core's typed opclass support-procedure
//! dispatch (`access/spgist/`).
//!
//! The SP-GiST AM dispatches its five opclass support procedures
//! (`config`/`choose`/`picksplit`/`inner_consistent`/`leaf_consistent`) by OID,
//! exactly as the BRIN AM dispatches `OpcInfo`/`AddValue`/`Consistent`/`Union`
//! through `backend-access-brin-entry-seams`. In C this is
//!
//! ```c
//! FunctionCall2Coll(procinfo, collation,
//!                   PointerGetDatum(&in), PointerGetDatum(&out));
//! ```
//!
//! over the internal-typed by-pointer `spg*In` / `spg*Out` structs — *not* a
//! generic fmgr-by-pointer-Datum path. We model it as five per-AM typed seams,
//! each taking the proc OID (resolved by the SP-GiST core via
//! `index_getprocinfo(rel, 1, SPGIST_*_PROC).fn_oid`) plus the typed input and
//! a `&mut` output borrowing the owned [`types_spgist`] vocabulary structs.
//!
//! The opclass crates (`backend-access-spg-quadtree` / `-kdtree`, text in F5)
//! INSTALL their typed bodies into these seams from their `init_seams()`, keyed
//! on their `pg_proc.dat` support-proc OIDs (`F_SPG_QUAD_*` / `F_SPG_KD_*`).
//! Until an opclass installs its arm, a dispatch to an unrecognized OID
//! `ereport(ERROR)`s ("unrecognized SP-GiST support function OID"), and an
//! uninstalled seam panics loudly (mirror-PG-and-panic).

#![allow(non_snake_case)]

use types_core::primitive::Oid;
use types_error::PgResult;
use types_spgist::{
    spgChooseIn, spgChooseOut, spgConfigIn, spgConfigOut, spgInnerConsistentIn,
    spgInnerConsistentOut, spgLeafConsistentIn, spgLeafConsistentOut, spgPickSplitIn,
    spgPickSplitOut,
};

seam_core::seam!(
    /// `FunctionCall2Coll(configProc, ..., &cfgin, &cfgout)`
    /// (spgutils.c `spgGetCache`): the opclass `SPGIST_CONFIG_PROC` support
    /// procedure — fill `out` (prefix/label/leaf type, can-return-data,
    /// long-values-OK) for the indexed `in.attType`. `proc_oid` is
    /// `index_getprocinfo(index, 1, SPGIST_CONFIG_PROC).fn_oid`; the opclass
    /// owns the body and installs its arm keyed on that OID. `Err` carries the
    /// opclass' `ereport(ERROR)` surface.
    pub fn spg_config(
        proc_oid: Oid,
        cfgin: &spgConfigIn,
        cfgout: &mut spgConfigOut,
    ) -> PgResult<()>
);

seam_core::seam!(
    /// `FunctionCall2Coll(chooseProc, ..., &in, &out)` (spgdoinsert.c): the
    /// opclass `SPGIST_CHOOSE_PROC` support procedure — decide how to place a new
    /// value (descend into a node, add a node, or split the inner tuple), writing
    /// the tagged result into `out`. `Err` carries the opclass' `ereport(ERROR)`.
    pub fn spg_choose<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        proc_oid: Oid,
        in_: &spgChooseIn<'mcx>,
        out: &mut spgChooseOut<'mcx>,
    ) -> PgResult<()>
);

seam_core::seam!(
    /// `FunctionCall2Coll(picksplitProc, ..., &in, &out)` (spgdoinsert.c): the
    /// opclass `SPGIST_PICKSPLIT_PROC` support procedure — split a set of leaf
    /// tuples into child nodes, writing the new inner tuple's prefix, node
    /// labels and per-leaf node assignments into `out`. `Err` carries the
    /// opclass' `ereport(ERROR)`.
    pub fn spg_picksplit<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        proc_oid: Oid,
        in_: &spgPickSplitIn<'mcx>,
        out: &mut spgPickSplitOut<'mcx>,
    ) -> PgResult<()>
);

seam_core::seam!(
    /// `FunctionCall2Coll(innerConsistentProc, ..., &in, &out)` (spgscan.c): the
    /// opclass `SPGIST_INNER_CONSISTENT_PROC` support procedure — given the
    /// scankeys and orderbys, decide which child nodes of the inner tuple to
    /// descend into, writing them (plus level increments, reconstructed values,
    /// traversal values and distances) into `out`. `Err` carries the opclass'
    /// `ereport(ERROR)`.
    pub fn spg_inner_consistent<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        proc_oid: Oid,
        in_: &spgInnerConsistentIn<'mcx>,
        out: &mut spgInnerConsistentOut<'mcx>,
    ) -> PgResult<()>
);

seam_core::seam!(
    /// `FunctionCall2Coll(leafConsistentProc, ..., &in, &out)` (spgscan.c): the
    /// opclass `SPGIST_LEAF_CONSISTENT_PROC` support procedure — test the leaf
    /// datum against the scankeys; returns the C `DatumGetBool(result)` (whether
    /// the leaf matches), writing recheck flags, reconstructed leaf value and
    /// distances into `out`. `Err` carries the opclass' `ereport(ERROR)`.
    pub fn spg_leaf_consistent<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        proc_oid: Oid,
        in_: &spgLeafConsistentIn<'mcx>,
        out: &mut spgLeafConsistentOut<'mcx>,
    ) -> PgResult<bool>
);

seam_core::seam!(
    /// `getBaseType(exprType((Node *) lfirst(indexpr_item)))` for the
    /// polymorphic + EXPRESSION-key branch of `GetIndexInputType` (spgutils.c:146)
    /// — the nominal input type of an SP-GiST index whose (single) key column is
    /// an expression over a polymorphic opclass.
    ///
    /// This path needs `RelationGetIndexExpressions(index)` (the relcache's
    /// cached index-expression list, built in relcache `derived.rs` but not yet
    /// seamed) and `exprType` over that node — neither reachable from the
    /// SP-GiST core, so the leg is seam-and-panic per repo idiom. `index_oid` is
    /// `RelationGetRelid(index)`; `indexcol` is the 1-based key column. The
    /// SP-GiST simple-column polymorphic path (`indkey != 0`) is handled inline
    /// in the core and never reaches this seam. relcache/plancat installs the
    /// body once `RelationGetIndexExpressions` lands. `Err` carries the
    /// `elog(ERROR, "wrong number of index expressions")` surface.
    pub fn get_index_input_type_expr(
        index_oid: Oid,
        indexcol: types_core::primitive::AttrNumber,
    ) -> PgResult<Oid>
);
