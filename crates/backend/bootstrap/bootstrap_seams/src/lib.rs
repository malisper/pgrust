//! Seam declarations for the `backend-bootstrap-bootstrap` unit
//! (`bootstrap/bootstrap.c`).
//!
//! The owning unit installs these from its `init_seams()`; until then a call
//! panics loudly. Only `bootstrap.c`'s own exported functions that are reached
//! across a dependency cycle live here. In particular `boot_get_type_io_data`
//! is called by `lsyscache.c`'s `get_type_io_data` while in bootstrap mode, but
//! `bootstrap.c` itself depends on `lsyscache.c`; the call therefore crosses a
//! cycle and must route through this seam.

use types_core::Oid;
use types_error::PgResult;

/// Result of [`boot_get_type_io_data`] — mirrors the out-params of the C
/// `boot_get_type_io_data(typid, typlen, typbyval, typalign, typdelim,
/// typioparam, typinput, typoutput)`.
#[derive(Clone, Copy, Debug)]
pub struct BootTypeIoData {
    pub typlen: i16,
    pub typbyval: bool,
    pub typalign: i8,
    pub typdelim: i8,
    pub typioparam: Oid,
    pub typinput: Oid,
    pub typoutput: Oid,
}

seam_core::seam!(
    /// `boot_get_type_io_data(typid, ...)` (bootstrap.c): obtain type I/O
    /// information at bootstrap time. Almost the same API as lsyscache.c's
    /// `get_type_io_data`, except only `typinput`/`typoutput` are supported
    /// (not the binary I/O routines). Exported so `array_in`/`array_out` work
    /// during early bootstrap. `Err` carries the `elog(ERROR, "type OID %u not
    /// found ...")` / hard-wired-TypInfo miss paths.
    pub fn boot_get_type_io_data(typid: Oid) -> PgResult<BootTypeIoData>
);

seam_core::seam!(
    /// `index_register(heap, ind, indexInfo)` (bootstrap.c): stash the index's
    /// `IndexInfo` on the bootstrap "indexes to build later" list so
    /// `build_indices` can fill it once bootstrapping is finishing. Reached only
    /// from `index_create` (catalog/index.c) in bootstrap mode. The C copies the
    /// `IndexInfo` into the process-lifetime no-gc bootstrap context; this seam
    /// passes the live `IndexInfo<'mcx>` by reference and the owner deep-copies
    /// it (`makeIndexInfo` + the attribute/expression/predicate copy) into its
    /// own list. `Err` carries OOM on the copy.
    pub fn index_register<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        heap: Oid,
        ind: Oid,
        index_info: &nodes::execnodes::IndexInfo<'mcx>,
    ) -> PgResult<()>
);
