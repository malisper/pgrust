//! contrib/bloom (18.3): the signature-file index AM. Lossy by contract:
//! amgetbitmap only, every hit rechecked against the heap, equality the only
//! operator. Crash safety rides generic WAL exactly as in C.

pub use types_bloom as layout;

pub mod insert;
pub mod scan;
pub mod state;
pub mod vacuum;
pub mod validate;

pub use insert::blinsert;
pub use scan::{blbeginscan, blendscan, blgetbitmap, blrescan};
pub use vacuum::{blbulkdelete, blbulkdelete_collect, blvacuumcleanup};
pub use validate::blvalidate;

use types_fmgr::PGFunction;

const LIBRARY: &str = "bloom";

fn lookup(function: &str) -> Option<PGFunction> {
    Some(match function {
        "blhandler" => fc_blhandler,
        _ => return None,
    })
}

// blutils.c:103-165 blhandler: the handler proc's fmgr entry. C makeNode()s
// an IndexAmRoutine in CurrentMemoryContext and PG_RETURN_POINTERs it; for
// the closed AM set the routine IS the IndexAmKind (amapi::GetIndexAmRoutine
// resolves this proc by name and never comes through here), so the datum is
// a pointer to the kind allocated in the call's result context. Reachable
// from SQL: bloom--1.0.sql declares the proc non-strict, so an aggregate
// with STYPE = internal and FINALFUNC = blhandler calls it (with a NULL or
// the transition state), and the value is only ever seen by
// index_am_handler_out (0A000), IS NULL and pg_typeof.
fn fc_blhandler(
    _f: Option<&mut types_fmgr::FmgrInfo>,
    fcinfo: &mut types_fmgr::FunctionCallInfoBaseData,
) -> types_error::PgResult<datum::Datum> {
    let amroutine: &types_relscan::IndexAmKind =
        mcx::alloc_leak_in(fcinfo.result_mcx(), types_relscan::IndexAmKind::Bloom)?;
    Ok(datum::Datum::from_usize(amroutine as *const types_relscan::IndexAmKind as usize))
}

pub fn init_seams() {
    dfmgr::register_builtin_library(dfmgr::BuiltinLibraryEntry {
        name: LIBRARY,
        lookup,
        pg_init: None,
    });
}
