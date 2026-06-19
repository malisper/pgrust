//! The fmgr builtin layer (`Datum fn(PG_FUNCTION_ARGS)`) for the `cstring`
//! pseudo-type's working I/O functions, whose argument/result types are
//! expressible at the current fmgr boundary.
//!
//! `cstring` is marked a pseudo-type only so people don't use it in tables, but
//! it carries a full working set of I/O functions (pseudotypes.c:100-141). Its
//! arg (`cstring`) and results (`cstring` / `bytea`) all ride the by-ref lane.
//! Each entry is a `fc_<name>` adapter that reads its argument off the fmgr call
//! frame, calls the matching value core, and writes back the by-reference
//! payload. [`register_pseudotypes_builtins`] registers every row into the
//! fmgr-core builtin table (C: `fmgr_builtins[]`). OIDs / nargs / strict /
//! retset are transcribed exactly from `pg_proc.dat`.
//!
//! `cstring_in` / `cstring_out` / `cstring_send` are registered here, along with
//! the `void` working I/O (`void_in` / `void_out` / `void_send`) and the `shell`
//! type dummies (`shell_in` / `shell_out`) — all of whose arg/result types are
//! expressible at the current fmgr boundary (`cstring` on the by-ref lane, the
//! 0-width by-value `void`, the `bytea` send result on the by-ref lane).
//!
//! The remaining pseudo-type I/O functions are either the `ereport(ERROR)`
//! dummies (no SQL-callable value), or `recv`/delegating outputs over `Datum`
//! arms (array / enum / range / multirange) whose arg/result types are not
//! expressible at the current fmgr boundary.

extern crate std;

use alloc::format;
use alloc::string::{String, ToString};
use alloc::vec::Vec;

use types_datum::Datum;
use types_fmgr::boundary::RefPayload;
use types_fmgr::{BuiltinFunction, FunctionCallInfoBaseData};
use types_stringinfo::StringInfo;

// ---------------------------------------------------------------------------
// Argument readers / result writers.
// ---------------------------------------------------------------------------

/// `PG_GETARG_CSTRING(i)`: the input text on the by-ref lane.
#[inline]
fn arg_cstring<'a>(fcinfo: &'a FunctionCallInfoBaseData, i: usize) -> &'a str {
    fcinfo
        .ref_arg(i)
        .and_then(|p| p.as_cstring())
        .expect("pseudotypes fn: cstring arg missing from by-ref lane")
}

/// Set a `cstring` result on the by-ref lane and return the dummy word.
#[inline]
fn ret_cstring(fcinfo: &mut FunctionCallInfoBaseData, s: String) -> Datum {
    fcinfo.set_ref_result(RefPayload::Cstring(s));
    Datum::from_usize(0)
}

/// Set a `bytea` (`_send`) result on the by-ref lane and return the dummy word.
#[inline]
fn ret_varlena(fcinfo: &mut FunctionCallInfoBaseData, bytes: Vec<u8>) -> Datum {
    fcinfo.set_ref_result(RefPayload::Varlena(bytes));
    Datum::from_usize(0)
}

/// A scratch context for cores that allocate their result through `Mcx`.
fn scratch_mcx() -> mcx::MemoryContext {
    mcx::MemoryContext::new("pseudotypes fmgr scratch")
}

/// A by-ref wire arg's verbatim image, built into a `StringInfo` for `_recv`
/// cores (there is no varlena header to skip — the bytes are the raw message).
/// The image bytes are first copied into `image`, so the returned `StringInfo`
/// no longer borrows `fcinfo` and the caller may set a result afterward.
#[inline]
fn arg_image(fcinfo: &FunctionCallInfoBaseData) -> Vec<u8> {
    fcinfo
        .ref_arg(0)
        .and_then(|p| p.as_varlena())
        .unwrap_or(&[])
        .to_vec()
}
#[inline]
fn buf_from<'a>(image: &[u8], m: &'a mcx::MemoryContext) -> StringInfo<'a> {
    let mut data = mcx::PgVec::new_in(m.mcx());
    if data.try_reserve(image.len()).is_err() {
        raise(types_error::PgError::error("out of memory"));
    }
    data.extend_from_slice(image);
    StringInfo::from_vec(data)
}

/// `PG_GETARG_DATUM(0)`: the by-value word for an `_out`/`_send` dummy (unread
/// by the always-throwing core, but read off the frame for faithfulness).
#[inline]
fn arg_datum(fcinfo: &FunctionCallInfoBaseData) -> Datum {
    fcinfo.arg(0).map(|a| a.value).unwrap_or_else(Datum::null)
}

/// Raise a builtin's `ereport(ERROR)` through the one dispatch point every
/// builtin crosses (`invoke_pgfunction`'s `catch_unwind`).
fn raise(err: types_error::PgError) -> ! {
    std::panic::panic_any(err);
}

// ---------------------------------------------------------------------------
// fc_ adapters.
// ---------------------------------------------------------------------------

/// `cstring_in` (pseudotypes.c:101): `PG_RETURN_CSTRING(pstrdup(str))`.
fn fc_cstring_in(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let m = scratch_mcx();
    let s = arg_cstring(fcinfo, 0);
    let owned = match crate::cstring_in(m.mcx(), s) {
        Ok(out) => out.as_str().to_string(),
        Err(e) => raise(e),
    };
    ret_cstring(fcinfo, owned)
}

/// `cstring_out` (pseudotypes.c:110): `PG_RETURN_CSTRING(pstrdup(str))`.
fn fc_cstring_out(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let m = scratch_mcx();
    let s = arg_cstring(fcinfo, 0);
    let owned = match crate::cstring_out(m.mcx(), s) {
        Ok(out) => out.as_str().to_string(),
        Err(e) => raise(e),
    };
    ret_cstring(fcinfo, owned)
}

/// `cstring_send` (pseudotypes.c:130): `PG_RETURN_BYTEA_P(pq_endtypsend(&buf))`.
fn fc_cstring_send(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let m = scratch_mcx();
    let s = arg_cstring(fcinfo, 0);
    let bytes = match crate::cstring_send(m.mcx(), s) {
        Ok(bytea) => bytea.as_bytes().to_vec(),
        Err(e) => raise(e),
    };
    ret_varlena(fcinfo, bytes)
}

/// `void_in` (pseudotypes.c:263): `PG_RETURN_VOID()`. Accepts any cstring and
/// returns the 0-width by-value `void` word.
fn fc_void_in(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let _s = arg_cstring(fcinfo, 0);
    match crate::void_in(_s) {
        Ok(d) => d,
        Err(e) => raise(e),
    }
}

/// `void_out` (pseudotypes.c:269): `PG_RETURN_CSTRING(pstrdup(""))`. The `void`
/// argument is a 0-width by-value word that carries no payload.
fn fc_void_out(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let m = scratch_mcx();
    let owned = match crate::void_out(m.mcx()) {
        Ok(out) => out.as_str().to_string(),
        Err(e) => raise(e),
    };
    ret_cstring(fcinfo, owned)
}

/// `void_send` (pseudotypes.c:285): send an empty string,
/// `PG_RETURN_BYTEA_P(pq_endtypsend(&buf))`.
fn fc_void_send(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let m = scratch_mcx();
    let bytes = match crate::void_send(m.mcx()) {
        Ok(bytea) => bytea.as_bytes().to_vec(),
        Err(e) => raise(e),
    };
    ret_varlena(fcinfo, bytes)
}

/// `shell_in` (pseudotypes.c:303): `errmsg("cannot accept a value of a shell
/// type")` — always raises.
fn fc_shell_in(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let s = arg_cstring(fcinfo, 0);
    match crate::shell_in(s) {
        Ok(d) => d,
        Err(e) => raise(e),
    }
}

/// `shell_out` (pseudotypes.c:313): `errmsg("cannot display a value of a shell
/// type")` — always raises. Its `opaque` argument is an unread by-value word.
fn fc_shell_out(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let value = fcinfo.arg(0).map(|a| a.value).unwrap_or_else(Datum::null);
    let owned = match crate::shell_out(value) {
        Ok(out) => out.as_str().to_string(),
        Err(e) => raise(e),
    };
    ret_cstring(fcinfo, owned)
}

// ---------------------------------------------------------------------------
// Dummy I/O adapters (PSEUDOTYPE_DUMMY_* — always-throwing).
//
// Each `_in` reads a `cstring` arg and forwards to the throwing core; each
// `_out`/`_send` reads the unread by-value word; each `_recv` builds a
// `StringInfo` over the raw message. All raise the core's `ereport(ERROR)`.
// ---------------------------------------------------------------------------

/// `PG_GETARG_CSTRING(0)` for an always-throwing dummy `_in`: the core never
/// reads the text (it raises unconditionally), and the constant may not ride the
/// by-ref lane as a cstring, so an absent/non-cstring arg degrades to `""`
/// rather than panicking ahead of the core's faithful `ereport(ERROR)`.
#[inline]
fn arg_cstring_opt<'a>(fcinfo: &'a FunctionCallInfoBaseData) -> &'a str {
    fcinfo.ref_arg(0).and_then(|p| p.as_cstring()).unwrap_or("")
}

/// A throwing-or-Datum core: `_in`/`_recv` cores return `PgResult<Datum>`.
macro_rules! fc_in {
    ($adapter:ident, $core:path) => {
        fn $adapter(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
            match $core(arg_cstring_opt(fcinfo)) {
                Ok(d) => d,
                Err(e) => raise(e),
            }
        }
    };
}

/// `_out` dummy: reads the by-value word, forwards to the throwing core (which
/// returns `PgResult<PgString>`), and on success writes the cstring result.
macro_rules! fc_out {
    ($adapter:ident, $core:path) => {
        fn $adapter(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
            let value = arg_datum(fcinfo);
            match $core(value) {
                Ok(out) => ret_cstring(fcinfo, out.as_str().to_string()),
                Err(e) => raise(e),
            }
        }
    };
}

/// `_recv` dummy: builds a `StringInfo` over the raw message, forwards to the
/// throwing core (`PgResult<Datum>`).
macro_rules! fc_recv {
    ($adapter:ident, $core:path) => {
        fn $adapter(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
            let m = scratch_mcx();
            let image = arg_image(fcinfo);
            let mut buf = buf_from(&image, &m);
            match $core(&mut buf) {
                Ok(d) => d,
                Err(e) => raise(e),
            }
        }
    };
}

// --- the PSEUDOTYPE_DUMMY_IO_FUNCS family (in: cstring, out: by-value) ---
fc_in!(fc_any_in, crate::any_in);
fc_out!(fc_any_out, crate::any_out);
fc_in!(fc_trigger_in, crate::trigger_in);
fc_out!(fc_trigger_out, crate::trigger_out);
fc_in!(fc_event_trigger_in, crate::event_trigger_in);
fc_out!(fc_event_trigger_out, crate::event_trigger_out);
fc_in!(fc_language_handler_in, crate::language_handler_in);
fc_out!(fc_language_handler_out, crate::language_handler_out);
fc_in!(fc_fdw_handler_in, crate::fdw_handler_in);
fc_out!(fc_fdw_handler_out, crate::fdw_handler_out);
fc_in!(fc_table_am_handler_in, crate::table_am_handler_in);
fc_out!(fc_table_am_handler_out, crate::table_am_handler_out);
fc_in!(fc_index_am_handler_in, crate::index_am_handler_in);
fc_out!(fc_index_am_handler_out, crate::index_am_handler_out);
fc_in!(fc_tsm_handler_in, crate::tsm_handler_in);
fc_out!(fc_tsm_handler_out, crate::tsm_handler_out);
fc_in!(fc_internal_in, crate::internal_in);
fc_out!(fc_internal_out, crate::internal_out);
fc_in!(fc_anyelement_in, crate::anyelement_in);
fc_out!(fc_anyelement_out, crate::anyelement_out);
fc_in!(fc_anynonarray_in, crate::anynonarray_in);
fc_out!(fc_anynonarray_out, crate::anynonarray_out);
fc_in!(fc_anycompatible_in, crate::anycompatible_in);
fc_out!(fc_anycompatible_out, crate::anycompatible_out);
fc_in!(fc_anycompatiblenonarray_in, crate::anycompatiblenonarray_in);
fc_out!(fc_anycompatiblenonarray_out, crate::anycompatiblenonarray_out);

// --- dummy INPUT funcs whose OUTPUT is real (out not registered here) ---
fc_in!(fc_anyarray_in, crate::anyarray_in);
fc_recv!(fc_anyarray_recv, crate::anyarray_recv);
fc_in!(fc_anycompatiblearray_in, crate::anycompatiblearray_in);
fc_recv!(fc_anycompatiblearray_recv, crate::anycompatiblearray_recv);
fc_in!(fc_anyenum_in, crate::anyenum_in);
fc_in!(fc_anyrange_in, crate::anyrange_in);
fc_in!(fc_anycompatiblerange_in, crate::anycompatiblerange_in);
fc_in!(fc_anymultirange_in, crate::anymultirange_in);
fc_in!(fc_anycompatiblemultirange_in, crate::anycompatiblemultirange_in);

// --- pg_node_tree (in/recv throw; out/send are real) ---
fc_in!(fc_pg_node_tree_in, crate::pg_node_tree_in);
fc_recv!(fc_pg_node_tree_recv, crate::pg_node_tree_recv);

/// Read a `text` argument's payload off the by-ref lane: the full varlena image
/// with its (1- or 4-byte) header stripped by `VARDATA_ANY`. The `pg_node_tree`
/// value is stored as a `text`; `pg_node_tree_out`/`_send` are `return
/// textout/textsend(fcinfo)`, which consume that payload.
#[inline]
fn arg_text_bytes<'a>(fcinfo: &'a FunctionCallInfoBaseData, i: usize) -> &'a [u8] {
    let image = fcinfo
        .ref_arg(i)
        .and_then(|p| p.as_varlena())
        .expect("pseudotypes fn: text arg missing from by-ref lane");
    backend_utils_adt_varlena::vardata_any_slice(image)
}

/// `pg_node_tree_out` (pseudotypes.c:338): `return textout(fcinfo)` — emit the
/// node-tree `text` payload as a `cstring` on the by-ref lane.
fn fc_pg_node_tree_out(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let m = scratch_mcx();
    let out = match crate::pg_node_tree_out(m.mcx(), arg_text_bytes(fcinfo, 0)) {
        Ok(v) => v,
        Err(e) => raise(e),
    };
    // `text_to_cstring` returns a NUL-terminated cstring (`pstrdup`); the by-ref
    // cstring lane carries the logical string, so drop one trailing NUL.
    let bytes = out.as_slice();
    let body = match bytes.last() {
        Some(0) => &bytes[..bytes.len() - 1],
        _ => bytes,
    };
    ret_cstring(fcinfo, String::from_utf8_lossy(body).into_owned())
}

/// `pg_node_tree_send` (pseudotypes.c:344): `return textsend(fcinfo)` — emit the
/// node-tree `text` payload as a header-ful `bytea` on the by-ref lane.
fn fc_pg_node_tree_send(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let m = scratch_mcx();
    let bytea = match crate::pg_node_tree_send(m.mcx(), arg_text_bytes(fcinfo, 0)) {
        Ok(b) => b,
        Err(e) => raise(e),
    };
    ret_varlena(fcinfo, bytea.as_bytes().to_vec())
}

// --- pg_ddl_command: all four throw ---
fc_in!(fc_pg_ddl_command_in, crate::pg_ddl_command_in);
fc_out!(fc_pg_ddl_command_out, crate::pg_ddl_command_out);
fc_recv!(fc_pg_ddl_command_recv, crate::pg_ddl_command_recv);
/// `pg_ddl_command_send` (pseudotypes.c:359): reads the by-value word, forwards
/// to the throwing core (`PgResult<Bytea>`).
fn fc_pg_ddl_command_send(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let value = arg_datum(fcinfo);
    match crate::pg_ddl_command_send(value) {
        Ok(bytea) => ret_varlena(fcinfo, bytea.as_bytes().to_vec()),
        Err(e) => raise(e),
    }
}

// --- working recv funcs (return a real value, not throwers) ---
/// `void_recv` (pseudotypes.c:275): `PG_RETURN_VOID()`.
fn fc_void_recv(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let m = scratch_mcx();
    let image = arg_image(fcinfo);
    let mut buf = buf_from(&image, &m);
    match crate::void_recv(&mut buf) {
        Ok(d) => d,
        Err(e) => raise(e),
    }
}
/// `cstring_recv` (pseudotypes.c:119): read the remaining message text, return a
/// `cstring` on the by-ref lane.
fn fc_cstring_recv(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let m = scratch_mcx();
    let image = arg_image(fcinfo);
    let mut buf = buf_from(&image, &m);
    let s = match crate::cstring_recv(m.mcx(), &mut buf) {
        Ok(bytes) => String::from_utf8_lossy(bytes.as_slice()).into_owned(),
        Err(e) => raise(e),
    };
    ret_cstring(fcinfo, s)
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

/// Register the `cstring` pseudo-type's working I/O builtins (C: their
/// `fmgr_builtins[]` rows). Called from this crate's `init_seams()`. OIDs /
/// nargs from `pg_proc.dat`; all default `proisstrict => 't'`, none retset.
pub fn register_pseudotypes_builtins() {
    backend_utils_fmgr_core::register_builtins([
        builtin(2292, "cstring_in", 1, true, false, fc_cstring_in),
        builtin(2293, "cstring_out", 1, true, false, fc_cstring_out),
        builtin(2501, "cstring_send", 1, true, false, fc_cstring_send),
        // ---- void working I/O ----
        builtin(2298, "void_in", 1, true, false, fc_void_in),
        builtin(2299, "void_out", 1, true, false, fc_void_out),
        builtin(3121, "void_send", 1, true, false, fc_void_send),
        // ---- shell type dummies ----
        builtin(2398, "shell_in", 1, false, false, fc_shell_in),
        builtin(2399, "shell_out", 1, true, false, fc_shell_out),
        // ---- working recv funcs (return real values) ----
        builtin(2500, "cstring_recv", 1, true, false, fc_cstring_recv),
        builtin(3120, "void_recv", 1, true, false, fc_void_recv),
        // ---- PSEUDOTYPE_DUMMY_IO_FUNCS (in: cstring thrower, out: by-value thrower) ----
        builtin(2294, "any_in", 1, true, false, fc_any_in),
        builtin(2295, "any_out", 1, true, false, fc_any_out),
        builtin(2300, "trigger_in", 1, false, false, fc_trigger_in),
        builtin(2301, "trigger_out", 1, true, false, fc_trigger_out),
        builtin(3594, "event_trigger_in", 1, false, false, fc_event_trigger_in),
        builtin(3595, "event_trigger_out", 1, true, false, fc_event_trigger_out),
        builtin(2302, "language_handler_in", 1, false, false, fc_language_handler_in),
        builtin(2303, "language_handler_out", 1, true, false, fc_language_handler_out),
        builtin(3116, "fdw_handler_in", 1, false, false, fc_fdw_handler_in),
        builtin(3117, "fdw_handler_out", 1, true, false, fc_fdw_handler_out),
        builtin(267, "table_am_handler_in", 1, false, false, fc_table_am_handler_in),
        builtin(268, "table_am_handler_out", 1, true, false, fc_table_am_handler_out),
        builtin(326, "index_am_handler_in", 1, false, false, fc_index_am_handler_in),
        builtin(327, "index_am_handler_out", 1, true, false, fc_index_am_handler_out),
        builtin(3311, "tsm_handler_in", 1, false, false, fc_tsm_handler_in),
        builtin(3312, "tsm_handler_out", 1, true, false, fc_tsm_handler_out),
        builtin(2304, "internal_in", 1, false, false, fc_internal_in),
        builtin(2305, "internal_out", 1, true, false, fc_internal_out),
        builtin(2312, "anyelement_in", 1, true, false, fc_anyelement_in),
        builtin(2313, "anyelement_out", 1, true, false, fc_anyelement_out),
        builtin(2777, "anynonarray_in", 1, true, false, fc_anynonarray_in),
        builtin(2778, "anynonarray_out", 1, true, false, fc_anynonarray_out),
        builtin(5086, "anycompatible_in", 1, true, false, fc_anycompatible_in),
        builtin(5087, "anycompatible_out", 1, true, false, fc_anycompatible_out),
        builtin(5092, "anycompatiblenonarray_in", 1, true, false, fc_anycompatiblenonarray_in),
        builtin(5093, "anycompatiblenonarray_out", 1, true, false, fc_anycompatiblenonarray_out),
        // ---- dummy INPUT (output is real, registered by its real owner) ----
        builtin(2296, "anyarray_in", 1, true, false, fc_anyarray_in),
        builtin(2502, "anyarray_recv", 1, true, false, fc_anyarray_recv),
        builtin(5088, "anycompatiblearray_in", 1, true, false, fc_anycompatiblearray_in),
        builtin(5090, "anycompatiblearray_recv", 1, true, false, fc_anycompatiblearray_recv),
        builtin(3504, "anyenum_in", 1, true, false, fc_anyenum_in),
        builtin(3832, "anyrange_in", 3, true, false, fc_anyrange_in),
        builtin(5094, "anycompatiblerange_in", 3, true, false, fc_anycompatiblerange_in),
        builtin(4229, "anymultirange_in", 3, true, false, fc_anymultirange_in),
        builtin(4226, "anycompatiblemultirange_in", 3, true, false, fc_anycompatiblemultirange_in),
        // ---- pg_node_tree (in/recv throw; out/send delegate to text I/O) ----
        builtin(195, "pg_node_tree_in", 1, true, false, fc_pg_node_tree_in),
        builtin(196, "pg_node_tree_out", 1, true, false, fc_pg_node_tree_out),
        builtin(197, "pg_node_tree_recv", 1, true, false, fc_pg_node_tree_recv),
        builtin(198, "pg_node_tree_send", 1, true, false, fc_pg_node_tree_send),
        // ---- pg_ddl_command (all four throw) ----
        builtin(86, "pg_ddl_command_in", 1, true, false, fc_pg_ddl_command_in),
        builtin(87, "pg_ddl_command_out", 1, true, false, fc_pg_ddl_command_out),
        builtin(88, "pg_ddl_command_recv", 1, true, false, fc_pg_ddl_command_recv),
        builtin(90, "pg_ddl_command_send", 1, true, false, fc_pg_ddl_command_send),
    ]);
}
