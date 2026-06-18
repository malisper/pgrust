//! The fmgr builtin layer (`Datum fn(PG_FUNCTION_ARGS)`) for `version.c`.
//!
//! The single SQL-callable function is `version()` (catalog OID 89, prosrc
//! `pgsql_version`). It takes no arguments and returns `text` — pass-by-
//! reference — so its `fc_` adapter calls the crate's real value core
//! [`crate::pgsql_version`] (which builds the `text` varlena through the
//! varlena owner's `cstring_to_text`) and sets the resulting varlena bytes on
//! the fmgr call frame's by-reference lane (C: `PG_RETURN_TEXT_P`).
//!
//! [`register_version_builtins`] registers the row into the fmgr-core builtin
//! table (C: `fmgr_builtins[]`), so by-OID dispatch resolves it. The OID /
//! nargs / strict / retset are transcribed exactly from `pg_proc.dat`
//! (`{ oid => '89', proname => 'version', provolatile => 's', prorettype =>
//! 'text', proargtypes => '', prosrc => 'pgsql_version' }` — no `proisstrict`
//! key, so strict defaults to `false`; not `proretset`, so retset is `false`).

use types_datum::Datum;
use types_fmgr::boundary::RefPayload;
use types_fmgr::{BuiltinFunction, FunctionCallInfoBaseData};

/// Raise a builtin's `ereport(ERROR)` through the one dispatch point every
/// builtin crosses (`invoke_pgfunction`'s `catch_unwind`).
fn raise(err: types_error::PgError) -> ! {
    let chars = types_error::unpack_sqlstate(err.sqlstate());
    let code = core::str::from_utf8(&chars).unwrap_or("XX000");
    std::panic::panic_any(format!("PGRUST-SQLSTATE:{code}:{}", err.message()));
}

/// `version()` (version.c `pgsql_version`) — `PG_RETURN_TEXT_P(cstring_to_text(
/// PG_VERSION_STR))`. No arguments; the `text` result rides the by-reference
/// lane as a `text` varlena (header + payload).
fn fc_version(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    // The varlena `text` result is allocated through a scratch `Mcx` (C: the
    // caller's current memory context); its flat varlena bytes are then handed
    // to the by-reference result lane (which owns them as a `Vec<u8>`).
    let m = mcx::MemoryContext::new("version fmgr scratch");
    let bytes = match crate::pgsql_version_v(m.mcx()) {
        Ok(d) => d.as_ref_bytes().to_vec(),
        Err(e) => raise(e),
    };
    fcinfo.set_ref_result(RefPayload::Varlena(bytes));
    Datum::from_usize(0)
}

// ---------------------------------------------------------------------------
// Registration.
// ---------------------------------------------------------------------------

fn builtin(
    foid: u32,
    name: &str,
    nargs: i16,
    strict: bool,
    retset: bool,
    func: fn(&mut FunctionCallInfoBaseData) -> Datum,
) -> BuiltinFunction {
    BuiltinFunction {
        foid,
        name: name.to_string(),
        nargs,
        strict,
        retset,
        func: Some(func),
    }
}

/// Register `version.c`'s SQL-callable builtin (C: its `fmgr_builtins[]` row).
/// Called from this crate's `init_seams()`. OID / nargs / strict / retset
/// transcribed exactly from `pg_proc.dat` (OID 89, 0 args, not strict, not
/// retset).
pub fn register_version_builtins() {
    backend_utils_fmgr_core::register_builtins([builtin(
        89, "pgsql_version", 0, true, false, fc_version,
    )]);
}
