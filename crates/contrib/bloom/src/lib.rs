//! contrib/bloom (18.3): the signature-file index access method.
//!
//! Plain terms: instead of storing column values, a bloom index stores one
//! small bit-signature per heap row; each indexed column sets a few
//! pseudo-random bits. A search builds the same signature from its keys and
//! returns every row whose stored bits cover it — sometimes wrongly ("maybe",
//! when bits collide), never missing a real match. So the scan is a lossy
//! bitmap scan (every hit rechecked against the heap) and equality is the
//! only supported operator.
//!
//! C sources: blutils.c blinsert.c blscan.c blvacuum.c blcost.c blvalidate.c
//! bloom.h. Crash safety rides generic WAL (GenericXLogStart/Register/Finish)
//! exactly as in C.

pub use types_bloom as layout;

use types_fmgr::PGFunction;

const LIBRARY: &str = "bloom";

fn lookup(function: &str) -> Option<PGFunction> {
    Some(match function {
        "blhandler" => fc_blhandler,
        _ => return None,
    })
}

// CREATE FUNCTION validation target only; the closed AM set dispatches via
// IndexAmKind, never through fmgr (same shape as pgvector's fc_hnswhandler).
fn fc_blhandler(
    _f: Option<&mut types_fmgr::FmgrInfo>,
    _fcinfo: &mut types_fmgr::FunctionCallInfoBaseData,
) -> types_error::PgResult<datum::Datum> {
    panic!("blhandler: the closed AM set dispatches via IndexAmKind, never through fmgr")
}

pub fn init_seams() {
    dfmgr::register_builtin_library(dfmgr::BuiltinLibraryEntry {
        name: LIBRARY,
        lookup,
        pg_init: None,
    });
}
