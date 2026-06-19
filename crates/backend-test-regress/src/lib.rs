//! Regression-test support library (`src/test/regress/regress.c`).
//!
//! `regress.c` is built into the loadable module `regress` (`$libdir/regress`)
//! and the regression `test_setup.sql` script creates SQL functions from it with
//! `CREATE FUNCTION ... LANGUAGE C AS '$libdir/regress', '<symbol>'`. The Rust
//! backend exposes no C ABI, so the real `regress.so` cannot be `dlopen`ed;
//! instead the C bodies the regression suite depends on are ported here and
//! registered with the dynamic-loader unit's in-process ported-library registry
//! ([`backend_utils_fmgr_dfmgr_seams::builtin_library_present`] /
//! [`backend_utils_fmgr_dfmgr_seams::resolve_builtin_library_function`]). When
//! `dfmgr`'s `load_external_function` / `load_file` is asked to resolve a symbol
//! from library `regress`, it consults this registry rather than the OS loader,
//! so `CREATE FUNCTION ... LANGUAGE C AS '$libdir/regress'` validates and the
//! resulting function is callable.
//!
//! Each ported symbol is a plain fmgr-1 `PGFunction` exactly as the
//! `PG_FUNCTION_INFO_V1` macro would expose it (api_version 1); the registry hands
//! the function manager the same `(user_fn, api_version)` pair the OS loader's
//! `fetch_finfo_record` would have produced.

use types_datum::Datum;
use types_error::PgError;
use types_fmgr::{FunctionCallInfoBaseData, LoadedExternalFunc, PGFunction};

/// The simple (suffix-free, directory-free) name of the regression-test loadable
/// module — `$libdir/regress` reduces to this for the registry.
const LIBRARY: &str = "regress";

/// Raise a builtin's `ereport(ERROR)` through the one dispatch point every
/// `PGFunction` crosses (`invoke_pgfunction`'s `catch_unwind`), which downcasts
/// the panic payload back to the structured [`PgError`].
fn raise(err: PgError) -> ! {
    std::panic::panic_any(err);
}

/// `PG_GETARG_OID(i)` — argument `i`'s word as an `Oid`.
#[inline]
fn arg_oid(fcinfo: &FunctionCallInfoBaseData, i: usize) -> types_core::Oid {
    fcinfo
        .arg(i)
        .expect("regress fn: missing oid arg")
        .value
        .as_oid()
}

/* ===========================================================================
 * binary_coercible(oid, oid) RETURNS bool  (regress.c)
 *
 *   Datum
 *   binary_coercible(PG_FUNCTION_ARGS)
 *   {
 *       Oid srctype = PG_GETARG_OID(0);
 *       Oid targettype = PG_GETARG_OID(1);
 *       PG_RETURN_BOOL(IsBinaryCoercible(srctype, targettype));
 *   }
 *
 * Provides SQL access to IsBinaryCoercible(); used by the opr_sanity /
 * type_sanity regression tests.
 * ========================================================================= */

/// `binary_coercible(oid, oid) -> bool` — SQL access to `IsBinaryCoercible`.
fn fc_binary_coercible(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let srctype = arg_oid(fcinfo, 0);
    let targettype = arg_oid(fcinfo, 1);
    match backend_parser_coerce_seams::is_binary_coercible::call(srctype, targettype) {
        Ok(result) => Datum::from_bool(result),
        Err(err) => raise(err),
    }
}

/// Resolve a symbol of the `regress` module to its ported `PGFunction` (the
/// `PG_FUNCTION_INFO_V1`-exposed `(user_fn, api_version=1)` pair). Returns `None`
/// for an unported / unknown symbol, exactly as the OS loader would fail to find
/// it in `regress.so`.
fn lookup(function: &str) -> Option<LoadedExternalFunc> {
    let user_fn: PGFunction = match function {
        "binary_coercible" => Some(fc_binary_coercible),
        _ => return None,
    };
    Some(LoadedExternalFunc {
        user_fn,
        // PG_FUNCTION_INFO_V1 declares api_version 1 (the only version fmgr
        // accepts for a C-language function).
        api_version: 1,
    })
}

/// Install this unit's inward seams: register the `regress` module with the
/// dynamic-loader unit's ported-library registry.
pub fn init_seams() {
    backend_utils_fmgr_dfmgr_seams::builtin_library_present::set(|library| library == LIBRARY);
    backend_utils_fmgr_dfmgr_seams::resolve_builtin_library_function::set(|library, function| {
        if library == LIBRARY {
            Ok(lookup(function))
        } else {
            Ok(None)
        }
    });
}
