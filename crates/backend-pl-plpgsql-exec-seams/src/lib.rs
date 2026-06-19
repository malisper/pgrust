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
