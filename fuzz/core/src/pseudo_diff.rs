//! Differential fuzz driver: adt/pseudotypes shipped Rust vs vendored
//! PostgreSQL C (csrc/pg_pseudotypes.c, Stamp 18.3 62d6c7d3df).
//!
//! One composite target for the whole input-language family (the
//! `float_in_diff` selector-byte pattern): the first byte picks an op, the
//! remainder is the payload. Every case compares value bytes/images,
//! error-verdict, and the full MAKE_SQLSTATE errcode (both sides use the
//! identical six-bit packing, so the comparison is numeric equality).
//! Message text is out of the fuzz comparator; the stable unit tests below
//! pin the exact C message templates instead.
//!
//! Both the value cores (crate::cstring_in etc.) and the shipped fmgr
//! wrappers (builtins::fc_*) run on the Rust side, so the fc_ dispatch
//! layer is under the same differential. The unported-delegate panics
//! (anyarray_out and friends) are deliberately NOT called: they are named
//! unported-delegate panics carried as coverage exception rows.
//!
//! ENCODING CARVE (documented, deliberate): the process is pinned to the
//! single-encoding identity configuration (client = database = SQL_ASCII,
//! the mbutils thread-local default; the pq_sendtext seam is set to the
//! identity arm below). The C oracle models the same arm, including
//! SQL_ASCII's embedded-NUL rejection on the receive path (22021). Other
//! client encodings route through mbutils conversion machinery that is out
//! of this crate's scope.

use core::ffi::{c_char, CStr};

use datum::Datum;
use mcx::MemoryContext;
use types_error::{PgError, ERRCODE_FEATURE_NOT_SUPPORTED};
use types_fmgr::{FmgrInfo, LocalFcinfo};

use adt_pseudotypes::builtins;

extern "C" {
    fn pg_pseudo_errcode_get() -> i32;
    fn pg_pseudo_errcode_reset();

    fn pg_cstring_in(s: *const c_char, out: *mut c_char) -> i32;
    fn pg_cstring_out(s: *const c_char, out: *mut c_char) -> i32;
    fn pg_cstring_recv(payload: *const u8, plen: i32, out: *mut c_char) -> i32;
    fn pg_cstring_send(s: *const c_char, out: *mut u8) -> i32;

    fn pg_void_in() -> u64;
    fn pg_void_out(out: *mut c_char) -> i32;
    fn pg_void_recv() -> u64;
    fn pg_void_send(out: *mut u8) -> i32;

    fn pg_pg_node_tree_out(payload: *const u8, plen: i32, out: *mut c_char) -> i32;
    fn pg_pg_node_tree_send(payload: *const u8, plen: i32, out: *mut u8) -> i32;
}

/// The 43 ereport-only stubs (26 accept + 15 display + shell pair are listed
/// separately below): Rust core fn, Rust fc_ wrapper, C oracle fn.
macro_rules! declare_stub_oracles {
    ($( $c:ident ),* $(,)?) => {
        extern "C" { $( fn $c() -> i32; )* }
    };
}

declare_stub_oracles!(
    pg_anyarray_in,
    pg_anyarray_recv,
    pg_anycompatiblearray_in,
    pg_anycompatiblearray_recv,
    pg_anyenum_in,
    pg_anyrange_in,
    pg_anycompatiblerange_in,
    pg_anymultirange_in,
    pg_anycompatiblemultirange_in,
    pg_pg_node_tree_in,
    pg_pg_node_tree_recv,
    pg_pg_ddl_command_in,
    pg_pg_ddl_command_out,
    pg_pg_ddl_command_recv,
    pg_pg_ddl_command_send,
    pg_any_in,
    pg_any_out,
    pg_trigger_in,
    pg_trigger_out,
    pg_event_trigger_in,
    pg_event_trigger_out,
    pg_language_handler_in,
    pg_language_handler_out,
    pg_fdw_handler_in,
    pg_fdw_handler_out,
    pg_table_am_handler_in,
    pg_table_am_handler_out,
    pg_index_am_handler_in,
    pg_index_am_handler_out,
    pg_tsm_handler_in,
    pg_tsm_handler_out,
    pg_internal_in,
    pg_internal_out,
    pg_anyelement_in,
    pg_anyelement_out,
    pg_anynonarray_in,
    pg_anynonarray_out,
    pg_anycompatible_in,
    pg_anycompatible_out,
    pg_anycompatiblenonarray_in,
    pg_anycompatiblenonarray_out,
    pg_shell_in,
    pg_shell_out,
);

fn c_errcode_of(f: unsafe extern "C" fn() -> i32) -> i32 {
    unsafe {
        pg_pseudo_errcode_reset();
        let r = f();
        assert_eq!(r, 1, "C pseudotype stub returned without ereporting");
        pg_pseudo_errcode_get()
    }
}

fn c_reset() {
    unsafe { pg_pseudo_errcode_reset() }
}

fn c_code() -> i32 {
    unsafe { pg_pseudo_errcode_get() }
}

/// Value-plane byte comparison; a mismatch is a divergence reproducer.
fn compare_bytes(what: &str, rust: &[u8], c: &[u8]) {
    assert!(
        rust == c,
        "{what} DIVERGENCE: Rust={rust:?} C={c:?}"
    );
}

fn rust_err_code(e: &PgError) -> i32 {
    e.sqlstate().0
}

/// Identity server->client conversion: the single-encoding configuration
/// (see module doc). Set once per process.
fn init_seams() {
    static ONCE: std::sync::Once = std::sync::Once::new();
    ONCE.call_once(|| {
        // Tolerate another module (name_diff installs the REAL mbutils
        // seams) having installed first: with the client encoding at its
        // SQL_ASCII default the real seam is the same identity conversion
        // (returns None), so either install order satisfies this driver.
        let _ = std::panic::catch_unwind(|| {
            mbutils_seams::pg_server_to_client::set(|_, _| Ok(None));
        });
    });
}

/// A 4-byte-header text varlena image (SET_VARSIZE_4B little-endian arm,
/// this host), 8-aligned so the header read is always aligned. Payload may
/// contain NULs — text is length-delimited.
fn text_varlena(payload: &[u8]) -> Vec<u64> {
    let total = 4 + payload.len();
    let mut buf = vec![0u64; total.div_ceil(8)];
    let bytes = unsafe {
        core::slice::from_raw_parts_mut(buf.as_mut_ptr() as *mut u8, buf.len() * 8)
    };
    bytes[..4].copy_from_slice(&((total as u32) << 2).to_le_bytes());
    bytes[4..total].copy_from_slice(payload);
    buf
}

// ---------------------------------------------------------------------------
// Per-op cases
// ---------------------------------------------------------------------------

fn case_cstring_in(payload: &[u8]) {
    let mut cbuf = payload.to_vec();
    cbuf.push(0);
    let mut c_out = vec![0u8; cbuf.len()];
    c_reset();
    let clen = unsafe { pg_cstring_in(cbuf.as_ptr().cast(), c_out.as_mut_ptr().cast()) };
    assert_eq!(c_code(), 0, "C cstring_in unexpectedly ereported");
    let clen = clen as usize;

    let ctx = MemoryContext::new("pseudo_fuzz");
    let mcx = ctx.mcx();
    let r = adt_pseudotypes::cstring_in(mcx, payload).expect("Rust cstring_in errored; C cannot");
    compare_bytes("cstring_in", &r[..], &c_out[..clen + 1]);

    let mut fcinfo = LocalFcinfo::<1>::new(0);
    // SAFETY: ctx outlives the call.
    unsafe { fcinfo.set_result_mcx(mcx) };
    fcinfo.set_arg(0, Datum::from_usize(cbuf.as_ptr() as usize));
    let d = builtins::fc_cstring_in(None, &mut fcinfo).expect("fc_cstring_in errored");
    // SAFETY: fc_cstring_in returns a fresh NUL-terminated cstring.
    let fc = unsafe { CStr::from_ptr(d.as_usize() as *const c_char) };
    compare_bytes("fc_cstring_in", fc.to_bytes_with_nul(), &c_out[..clen + 1]);
}

fn case_cstring_out(payload: &[u8]) {
    let mut cbuf = payload.to_vec();
    cbuf.push(0);
    let mut c_out = vec![0u8; cbuf.len()];
    c_reset();
    let clen = unsafe { pg_cstring_out(cbuf.as_ptr().cast(), c_out.as_mut_ptr().cast()) };
    assert_eq!(c_code(), 0, "C cstring_out unexpectedly ereported");
    let clen = clen as usize;

    let ctx = MemoryContext::new("pseudo_fuzz");
    let mcx = ctx.mcx();
    let r = adt_pseudotypes::cstring_out(mcx, payload).expect("Rust cstring_out errored; C cannot");
    compare_bytes("cstring_out", &r[..], &c_out[..clen + 1]);

    let mut flinfo = FmgrInfo::unresolved();
    let mut fcinfo = LocalFcinfo::<1>::new(0);
    fcinfo.set_arg(0, Datum::from_usize(cbuf.as_ptr() as usize));
    let d = builtins::fc_cstring_out(Some(&mut flinfo), &mut fcinfo).expect("fc_cstring_out errored");
    // SAFETY: fc_cstring_out returns a NUL-terminated cstring in scratch.
    let fc = unsafe { CStr::from_ptr(d.as_usize() as *const c_char) };
    compare_bytes("fc_cstring_out", fc.to_bytes_with_nul(), &c_out[..clen + 1]);
}

fn case_cstring_recv(rest: &[u8]) {
    let Some((&cur, payload)) = rest.split_first() else {
        return;
    };
    let cursor = (cur as usize) % (payload.len() + 1);
    let unread = &payload[cursor..];

    let mut c_out = vec![0u8; unread.len() + 1];
    c_reset();
    let clen = unsafe {
        pg_cstring_recv(unread.as_ptr(), unread.len() as i32, c_out.as_mut_ptr().cast())
    };

    let ctx = MemoryContext::new("pseudo_fuzz");
    let mcx = ctx.mcx();
    let mut si = stringinfo::StringInfo::new_in(mcx).expect("StringInfo alloc");
    si.append_bytes(payload).expect("append");
    si.cursor = cursor;
    match adt_pseudotypes::cstring_recv(mcx, &mut si) {
        Ok(r) => {
            assert!(
                clen >= 0,
                "cstring_recv DIVERGENCE: C err {} vs Rust Ok({:?})",
                c_code(),
                &r[..]
            );
            compare_bytes("cstring_recv", &r[..], &c_out[..clen as usize + 1]);
            assert_eq!(si.cursor, si.len(), "cstring_recv cursor not fully advanced");
        }
        Err(e) => {
            assert!(
                clen < 0 && c_code() == rust_err_code(&e),
                "cstring_recv DIVERGENCE: C (len {clen}, err {}) vs Rust err {} ({})",
                c_code(),
                rust_err_code(&e),
                e.message()
            );
        }
    }

    // fc plane: fresh StringInfo, same bytes.
    let mut si2 = stringinfo::StringInfo::new_in(mcx).expect("StringInfo alloc");
    si2.append_bytes(payload).expect("append");
    si2.cursor = cursor;
    let mut fcinfo = LocalFcinfo::<1>::new(0);
    // SAFETY: ctx outlives the call.
    unsafe { fcinfo.set_result_mcx(mcx) };
    fcinfo.set_arg(
        0,
        Datum::from_usize(&mut si2 as *mut stringinfo::StringInfo as usize),
    );
    match builtins::fc_cstring_recv(None, &mut fcinfo) {
        Ok(d) => {
            assert!(clen >= 0, "fc_cstring_recv verdict skew vs C");
            // SAFETY: fc_cstring_recv returns a fresh NUL-terminated cstring.
            let fc = unsafe { CStr::from_ptr(d.as_usize() as *const c_char) };
            // CStr observability stops at the first NUL; unread is NUL-free
            // in every Ok case (SQL_ASCII verify), so this is byte-exact.
            compare_bytes("fc_cstring_recv", fc.to_bytes_with_nul(), &c_out[..clen as usize + 1]);
        }
        Err(e) => {
            assert!(
                clen < 0 && c_code() == rust_err_code(&e),
                "fc_cstring_recv DIVERGENCE: C (len {clen}, err {}) vs Rust err {}",
                c_code(),
                rust_err_code(&e)
            );
        }
    }
}

fn case_cstring_send(payload: &[u8]) {
    let mut cbuf = payload.to_vec();
    cbuf.push(0);
    let nul = payload.iter().position(|&b| b == 0).unwrap_or(payload.len());
    let mut c_out = vec![0u8; 4 + nul];
    c_reset();
    let clen = unsafe { pg_cstring_send(cbuf.as_ptr().cast(), c_out.as_mut_ptr()) };
    assert_eq!(c_code(), 0, "C cstring_send unexpectedly ereported");
    let clen = clen as usize;

    let ctx = MemoryContext::new("pseudo_fuzz");
    let mcx = ctx.mcx();
    let r = adt_pseudotypes::cstring_send(mcx, payload).expect("Rust cstring_send errored; C cannot");
    compare_bytes("cstring_send", r.as_bytes(), &c_out[..clen]);

    let mut fcinfo = LocalFcinfo::<1>::new(0);
    // SAFETY: ctx outlives the call.
    unsafe { fcinfo.set_result_mcx(mcx) };
    fcinfo.set_arg(0, Datum::from_usize(cbuf.as_ptr() as usize));
    let d = builtins::fc_cstring_send(None, &mut fcinfo).expect("fc_cstring_send errored");
    // SAFETY: fc_cstring_send returns a live 4B-header varlena in ctx.
    let v = unsafe { datum::VarlenaRef::from_ptr(d.as_usize() as *const u8) };
    compare_bytes("fc_cstring_send", v.as_bytes(), &c_out[..clen]);
}

fn case_void() {
    c_reset();
    assert_eq!(adt_pseudotypes::void_in().as_usize(), unsafe { pg_void_in() } as usize);
    assert_eq!(adt_pseudotypes::void_recv().as_usize(), unsafe { pg_void_recv() } as usize);

    let mut c_cstr = [0u8; 2];
    let c_cstr_len = unsafe { pg_void_out(c_cstr.as_mut_ptr().cast()) } as usize;
    let ctx = MemoryContext::new("pseudo_fuzz");
    let mcx = ctx.mcx();
    let r = adt_pseudotypes::void_out(mcx).expect("void_out");
    compare_bytes("void_out", &r[..], &c_cstr[..c_cstr_len + 1]);

    let mut c_img = [0u8; 4];
    let c_img_len = unsafe { pg_void_send(c_img.as_mut_ptr()) } as usize;
    let r = adt_pseudotypes::void_send(mcx).expect("void_send");
    compare_bytes("void_send", r.as_bytes(), &c_img[..c_img_len]);
    assert_eq!(c_code(), 0, "C void family unexpectedly ereported");

    // fc plane (fixed outputs).
    let mut fcinfo = LocalFcinfo::<1>::new(0);
    // SAFETY: ctx outlives the call.
    unsafe { fcinfo.set_result_mcx(mcx) };
    fcinfo.set_arg(0, Datum::null());
    assert_eq!(builtins::fc_void_in(None, &mut fcinfo).expect("fc_void_in").as_usize(), 0);
    assert_eq!(builtins::fc_void_recv(None, &mut fcinfo).expect("fc_void_recv").as_usize(), 0);
    let d = builtins::fc_void_out(None, &mut fcinfo).expect("fc_void_out");
    // SAFETY: fc_void_out returns a static NUL-terminated cstring.
    let fc = unsafe { CStr::from_ptr(d.as_usize() as *const c_char) };
    compare_bytes("fc_void_out", fc.to_bytes_with_nul(), &c_cstr[..c_cstr_len + 1]);
    let d = builtins::fc_void_send(None, &mut fcinfo).expect("fc_void_send");
    // SAFETY: fc_void_send returns a live 4B-header varlena in ctx.
    let v = unsafe { datum::VarlenaRef::from_ptr(d.as_usize() as *const u8) };
    compare_bytes("fc_void_send", v.as_bytes(), &c_img[..c_img_len]);
}

fn case_node_tree_out(payload: &[u8]) {
    let mut c_out = vec![0u8; payload.len() + 1];
    c_reset();
    let clen =
        unsafe { pg_pg_node_tree_out(payload.as_ptr(), payload.len() as i32, c_out.as_mut_ptr().cast()) };
    assert_eq!(c_code(), 0, "C pg_node_tree_out unexpectedly ereported");
    let clen = clen as usize;

    let ctx = MemoryContext::new("pseudo_fuzz");
    let mcx = ctx.mcx();
    let r = adt_pseudotypes::pg_node_tree_out(mcx, payload).expect("pg_node_tree_out");
    compare_bytes("pg_node_tree_out", &r[..], &c_out[..clen + 1]);

    // fc plane: 4B-header text varlena arg; CStr observability stops at the
    // first NUL, exactly like C's returned cstring — compare that prefix.
    let varl = text_varlena(payload);
    let mut flinfo = FmgrInfo::unresolved();
    let mut fcinfo = LocalFcinfo::<1>::new(0);
    fcinfo.set_arg(0, Datum::from_usize(varl.as_ptr() as usize));
    let d = builtins::fc_pg_node_tree_out(Some(&mut flinfo), &mut fcinfo).expect("fc_pg_node_tree_out");
    // SAFETY: textout scratch holds a NUL-terminated cstring.
    let fc = unsafe { CStr::from_ptr(d.as_usize() as *const c_char) };
    let c_cstr_view = &c_out[..c_out.iter().position(|&b| b == 0).unwrap() + 1];
    compare_bytes("fc_pg_node_tree_out", fc.to_bytes_with_nul(), c_cstr_view);
}

fn case_node_tree_send(payload: &[u8]) {
    let mut c_out = vec![0u8; payload.len() + 4];
    c_reset();
    let clen =
        unsafe { pg_pg_node_tree_send(payload.as_ptr(), payload.len() as i32, c_out.as_mut_ptr()) };
    assert_eq!(c_code(), 0, "C pg_node_tree_send unexpectedly ereported");
    let clen = clen as usize;

    let ctx = MemoryContext::new("pseudo_fuzz");
    let mcx = ctx.mcx();
    let r = adt_pseudotypes::pg_node_tree_send(mcx, payload).expect("pg_node_tree_send");
    compare_bytes("pg_node_tree_send", r.as_bytes(), &c_out[..clen]);

    let varl = text_varlena(payload);
    let mut fcinfo = LocalFcinfo::<1>::new(0);
    // SAFETY: ctx outlives the call.
    unsafe { fcinfo.set_result_mcx(mcx) };
    fcinfo.set_arg(0, Datum::from_usize(varl.as_ptr() as usize));
    let d = builtins::fc_pg_node_tree_send(None, &mut fcinfo).expect("fc_pg_node_tree_send");
    // SAFETY: fc_pg_node_tree_send returns a live 4B-header varlena in ctx.
    let v = unsafe { datum::VarlenaRef::from_ptr(d.as_usize() as *const u8) };
    compare_bytes("fc_pg_node_tree_send", v.as_bytes(), &c_out[..clen]);
}

/// Every ereport-only stub, both Rust planes vs the C oracle's errcode.
macro_rules! check_stubs {
    ($fcinfo:expr; $( $core:ident / $fc:ident => $c:ident ),* $(,)?) => {$(
        let e = adt_pseudotypes::$core().expect_err(concat!(
            "Rust ", stringify!($core), " returned Ok; C always ereports"
        ));
        let ccode = c_errcode_of($c);
        assert_eq!(
            rust_err_code(&e), ccode,
            concat!(stringify!($core), " DIVERGENCE: sqlstate {} vs C {}"),
            rust_err_code(&e), ccode
        );
        let e = builtins::$fc(None, $fcinfo).expect_err(concat!(
            "Rust ", stringify!($fc), " returned Ok; C always ereports"
        ));
        assert_eq!(rust_err_code(&e), ccode, concat!(stringify!($fc), " sqlstate skew"));
    )*};
}

fn case_stubs() {
    let mut fcinfo = LocalFcinfo::<1>::new(0);
    fcinfo.set_arg(0, Datum::null());
    check_stubs!(&mut fcinfo;
        anyarray_in / fc_anyarray_in => pg_anyarray_in,
        anyarray_recv / fc_anyarray_recv => pg_anyarray_recv,
        anycompatiblearray_in / fc_anycompatiblearray_in => pg_anycompatiblearray_in,
        anycompatiblearray_recv / fc_anycompatiblearray_recv => pg_anycompatiblearray_recv,
        anyenum_in / fc_anyenum_in => pg_anyenum_in,
        anyrange_in / fc_anyrange_in => pg_anyrange_in,
        anycompatiblerange_in / fc_anycompatiblerange_in => pg_anycompatiblerange_in,
        anymultirange_in / fc_anymultirange_in => pg_anymultirange_in,
        anycompatiblemultirange_in / fc_anycompatiblemultirange_in => pg_anycompatiblemultirange_in,
        pg_node_tree_in / fc_pg_node_tree_in => pg_pg_node_tree_in,
        pg_node_tree_recv / fc_pg_node_tree_recv => pg_pg_node_tree_recv,
        pg_ddl_command_in / fc_pg_ddl_command_in => pg_pg_ddl_command_in,
        pg_ddl_command_out / fc_pg_ddl_command_out => pg_pg_ddl_command_out,
        pg_ddl_command_recv / fc_pg_ddl_command_recv => pg_pg_ddl_command_recv,
        pg_ddl_command_send / fc_pg_ddl_command_send => pg_pg_ddl_command_send,
        any_in / fc_any_in => pg_any_in,
        any_out / fc_any_out => pg_any_out,
        trigger_in / fc_trigger_in => pg_trigger_in,
        trigger_out / fc_trigger_out => pg_trigger_out,
        event_trigger_in / fc_event_trigger_in => pg_event_trigger_in,
        event_trigger_out / fc_event_trigger_out => pg_event_trigger_out,
        language_handler_in / fc_language_handler_in => pg_language_handler_in,
        language_handler_out / fc_language_handler_out => pg_language_handler_out,
        fdw_handler_in / fc_fdw_handler_in => pg_fdw_handler_in,
        fdw_handler_out / fc_fdw_handler_out => pg_fdw_handler_out,
        table_am_handler_in / fc_table_am_handler_in => pg_table_am_handler_in,
        table_am_handler_out / fc_table_am_handler_out => pg_table_am_handler_out,
        index_am_handler_in / fc_index_am_handler_in => pg_index_am_handler_in,
        index_am_handler_out / fc_index_am_handler_out => pg_index_am_handler_out,
        tsm_handler_in / fc_tsm_handler_in => pg_tsm_handler_in,
        tsm_handler_out / fc_tsm_handler_out => pg_tsm_handler_out,
        internal_in / fc_internal_in => pg_internal_in,
        internal_out / fc_internal_out => pg_internal_out,
        anyelement_in / fc_anyelement_in => pg_anyelement_in,
        anyelement_out / fc_anyelement_out => pg_anyelement_out,
        anynonarray_in / fc_anynonarray_in => pg_anynonarray_in,
        anynonarray_out / fc_anynonarray_out => pg_anynonarray_out,
        anycompatible_in / fc_anycompatible_in => pg_anycompatible_in,
        anycompatible_out / fc_anycompatible_out => pg_anycompatible_out,
        anycompatiblenonarray_in / fc_anycompatiblenonarray_in => pg_anycompatiblenonarray_in,
        anycompatiblenonarray_out / fc_anycompatiblenonarray_out => pg_anycompatiblenonarray_out,
        shell_in / fc_shell_in => pg_shell_in,
        shell_out / fc_shell_out => pg_shell_out,
    );
}

// ---------------------------------------------------------------------------
// Entry point
// ---------------------------------------------------------------------------

pub fn pseudotypes_diff(data: &[u8]) {
    let _oracle = crate::oracle_serial(); // one-thread-at-a-time through the C oracles (process-global statics)
    let Some((&sel, rest)) = data.split_first() else {
        return;
    };
    if rest.len() > 1024 {
        return;
    }
    init_seams();
    match sel % 8 {
        0 => case_cstring_in(rest),
        1 => case_cstring_out(rest),
        2 => case_cstring_recv(rest),
        3 => case_cstring_send(rest),
        4 => case_void(),
        5 => case_node_tree_out(rest),
        6 => case_node_tree_send(rest),
        _ => case_stubs(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The exact C message templates (pseudotypes.c) — text is out of the
    /// fuzz comparator, pinned here instead.
    #[test]
    fn stub_messages_match_c_templates() {
        let _serial = crate::c_oracle_serial();
        for (e, want) in [
            (adt_pseudotypes::any_in().unwrap_err(), "cannot accept a value of type any"),
            (adt_pseudotypes::anyarray_in().unwrap_err(), "cannot accept a value of type anyarray"),
            (adt_pseudotypes::anyarray_recv().unwrap_err(), "cannot accept a value of type anyarray"),
            (adt_pseudotypes::trigger_out().unwrap_err(), "cannot display a value of type trigger"),
            (
                adt_pseudotypes::pg_ddl_command_send().unwrap_err(),
                "cannot display a value of type pg_ddl_command",
            ),
            (adt_pseudotypes::shell_in().unwrap_err(), "cannot accept a value of a shell type"),
            (adt_pseudotypes::shell_out().unwrap_err(), "cannot display a value of a shell type"),
        ] {
            assert_eq!(e.message(), want);
            assert_eq!(e.sqlstate(), ERRCODE_FEATURE_NOT_SUPPORTED);
        }
        // The Rust 0A000 packing equals the C MAKE_SQLSTATE packing.
        assert_eq!(ERRCODE_FEATURE_NOT_SUPPORTED.0, c_errcode_of(pg_any_in));
    }

    #[test]
    fn recv_rejects_embedded_nul_both_sides_22021() {
        let _serial = crate::c_oracle_serial();
        // [sel=2][cursor=0][payload "a\0b"]: SQL_ASCII verify rejects the NUL
        // on both sides with character_not_in_repertoire.
        pseudotypes_diff(&[2, 0, b'a', 0, b'b']);
        let mut out = [0u8; 8];
        c_reset();
        let r = unsafe { pg_cstring_recv([b'a', 0, b'b'].as_ptr(), 3, out.as_mut_ptr().cast()) };
        assert_eq!(r, -1);
        assert_eq!(c_code(), types_error::ERRCODE_CHARACTER_NOT_IN_REPERTOIRE.0);
    }

    /// Deterministic sweep: every selector, assorted payload shapes. The
    /// driver must not panic on any of these (a panic = divergence).
    #[test]
    fn selector_sweep_no_divergence() {
        let _serial = crate::c_oracle_serial();
        let payloads: &[&[u8]] = &[
            b"",
            b"a",
            b"blah",
            b"\0",
            b"a\0b",
            b"\xff\x80 high bits",
            b"{QUERY :junk 1}",
            &[0u8; 64],
            &[0xffu8; 256],
        ];
        for sel in 0u8..=15 {
            for p in payloads {
                let mut input = vec![sel];
                input.extend_from_slice(p);
                pseudotypes_diff(&input);
            }
        }
    }

    /// Must-disagree control: the comparator has to FIRE on skewed bytes —
    /// proves the divergence assert is live, not vacuously green.
    #[test]
    #[should_panic(expected = "DIVERGENCE")]
    fn comparator_control_fires_on_skew() {
        let _serial = crate::c_oracle_serial();
        compare_bytes("control", b"a", b"b");
    }

    /// Executable claim for the no_flinfo defensive arm (pgrust-shell; C's
    /// fmgr always resolves flinfo before an out-function call, so there is
    /// no C counterpart to diff). Carried as a coverage exception row; this
    /// test pins that the arm fires, loudly, with the documented message.
    #[test]
    #[should_panic(expected = "cstring result needs a resolved FmgrInfo")]
    fn no_flinfo_arm_fires() {
        let _serial = crate::c_oracle_serial();
        let mut fcinfo = LocalFcinfo::<1>::new(0);
        fcinfo.set_arg(0, Datum::from_usize(b"x\0".as_ptr() as usize));
        let _ = builtins::fc_cstring_out(None, &mut fcinfo);
    }
}
