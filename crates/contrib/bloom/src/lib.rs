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

// CREATE FUNCTION validation target only; the closed AM set dispatches via
// IndexAmKind, never through fmgr (fc_hnswhandler precedent).
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
