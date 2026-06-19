//! Seam declarations for the PL/pgSQL executor unit (`pl_exec.c`).
//!
//! The compiler (`pl_comp.c`, `backend-pl-plpgsql-comp`) calls back into the
//! executor at compile time from `make_datum_param`
//! (`plpgsql_exec_get_datum_type_info`) to learn the type/typmod/collation of a
//! `PLpgSQL_datum` so it can stamp a `Param` node. That callee lives in
//! `pl_exec.c` (this unit, `backend-pl-plpgsql-exec`), which depends on the
//! compiler — a cycle. The compiler therefore reaches it through this seam; the
//! executor unit installs it from its `init_seams()` when it lands. Until then
//! a call panics loudly (mirror-PG-and-panic).
//!
//! ## Modeling the C out-parameter contract
//!
//! ```c
//! void plpgsql_exec_get_datum_type_info(PLpgSQL_execstate *estate,
//!                                       PLpgSQL_datum *datum,
//!                                       Oid *typeId, int32 *typMod, Oid *collation);
//! ```
//!
//! The three out-parameters are returned as a [`DatumTypeInfo`] value. The
//! `estate`/`datum` pair is identified by the datum's `dno` against the live
//! execstate the compiler is currently building an expression for
//! (`expr->func->cur_estate`), which the executor owns; the seam carries the
//! `dno` plus the executor's opaque execstate handle.

use types_core::Oid;
use types_error::PgResult;
use types_plpgsql::int32;

/// The `(typeId, typMod, collation)` triple filled by
/// `plpgsql_exec_get_datum_type_info`.
#[derive(Clone, Copy, Debug)]
pub struct DatumTypeInfo {
    pub type_id: Oid,
    pub typmod: int32,
    pub collation: Oid,
}

seam_core::seam!(
    /// `plpgsql_exec_get_datum_type_info(estate, datum, &typeId, &typMod, &collation)`
    /// (`pl_exec.c`): report the type/typmod/collation of the datum identified
    /// by `dno` in the execstate identified by `estate_handle`
    /// (`expr->func->cur_estate`). The compiler calls this from
    /// `make_datum_param` while building a `Param` node.
    pub fn plpgsql_exec_get_datum_type_info(
        estate_handle: u64,
        dno: int32,
    ) -> PgResult<DatumTypeInfo>
);

/// One PL/pgSQL scalar datum value bound into a `Param` for expression
/// evaluation: the bare-word value, its is-null flag, and its type OID
/// (`estate->datums[dno]` projected to what `setup_param_list` binds).
#[derive(Clone, Copy, Debug)]
pub struct EvalParamValue {
    pub value: usize,
    pub isnull: bool,
    pub typeid: Oid,
}

/// The raw result of evaluating a PL/pgSQL expression to a single value (the
/// first row's first column, `SPI_getbinval(tuptab->vals[0], tupdesc, 1)`).
#[derive(Clone, Copy, Debug)]
pub struct EvalExprResult {
    /// The bare-word result datum (`0` when null).
    pub value: usize,
    pub isnull: bool,
    /// The result column's type OID (`SPI_gettypeid(tupdesc, 1)`).
    pub typeid: Oid,
    /// `SPI_processed` — the number of rows the expression produced.
    pub processed: u64,
}

seam_core::seam!(
    /// `exec_eval_expr(estate, expr)` slow path (`pl_exec.c`'s `exec_run_select`):
    /// prepare the PL/pgSQL expression `query` (in its `parse_mode`) with the
    /// PL/pgSQL parser hooks installed (so variable barewords resolve to
    /// `$dno+1` `Param`s), bind the referenced scalar datums from
    /// `datum_snapshot` (indexed by `dno`), run the one-row SELECT, and return
    /// the first row's first-column raw datum.
    ///
    /// The executor (`pl_exec.c`, this unit) cannot reach the SPI plan surface
    /// directly (it is layered below SPI), so the SPI owner installs this from
    /// the handler's `init_seams()`. `datum_snapshot[dno]` carries the current
    /// `(value, isnull, typeid)` of each scalar datum (the
    /// `setup_param_list`/`plpgsql_param_fetch` value); a `None` entry is a
    /// non-scalar datum that cannot be a simple-expr Param.
    pub fn exec_eval_expr_via_spi(
        query: std::string::String,
        parse_mode: types_parsenodes::RawParseMode,
        parse_state: types_nodes::parsestmt::PlpgsqlExprParseState,
        datum_snapshot: std::vec::Vec<Option<EvalParamValue>>,
        maxtuples: i64,
    ) -> PgResult<EvalExprResult>
);
