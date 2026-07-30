//! Kani C-vs-Rust equivalence harnesses for the pg_lsn function family
//! against vendored PostgreSQL C (c/pg_pg_lsn.c — provenance + shims there).
//!
//! Run: timeout 30 cargo kani -Z c-ffi -Z stubbing --c-lib c/pg_pg_lsn.c \
//!        --solver kissat --harness <h>     (or ./run-all.sh)
//! EXCEPT the negative control (control_pg_lsn_out_mismatch): DEFAULT
//! solver, validated by an explicit counterexample ("VERIFICATION:- FAILED"),
//! never by timeout — kissat does not terminate on failing harnesses.
//!
//! Shipped Rust under proof (never copied):
//!   adt_pg_lsn::pg_lsn_in_internal   (lib.rs:25) — pg_lsn_in core
//!   adt_pg_lsn::pg_lsn_out_into      (lib.rs:53) — pg_lsn_out core
//!   adt_pg_lsn::pg_lsn_cmp_internal  (lib.rs:91) — pg_lsn_cmp core
//! The six boolean operators' shipped bodies are the bare u64 operator
//! applied by the fc_lsn_cmp! macro (builtins.rs:67-82:
//! `arg_lsn(fcinfo,0) $op arg_lsn(fcinfo,1)`); the fmgr wrapper cannot be
//! invoked without a live Fcinfo, so each operator harness states that
//! one-token body directly AND cross-checks it against the shipped
//! pg_lsn_cmp_internal, so the proof is anchored to shipped code.
#![cfg(kani)]

use adt_pg_lsn::{pg_lsn_cmp_internal, pg_lsn_in_internal, pg_lsn_out_into, MAXPG_LSNLEN};

extern "C" {
    fn pg_pg_lsn_in_safe(str_: *const u8, have_error: *mut i32) -> u64;
    fn pg_pg_lsn_larger(lsn1: u64, lsn2: u64) -> u64;
    fn pg_pg_lsn_smaller(lsn1: u64, lsn2: u64) -> u64;
    fn pg_pg_lsn_out_rel18(lsn: u64, buf: *mut u8) -> i32;
    fn pg_pg_lsn_out_master(lsn: u64, buf: *mut u8) -> i32;
    fn pg_pg_lsn_eq(a: u64, b: u64) -> i32;
    fn pg_pg_lsn_ne(a: u64, b: u64) -> i32;
    fn pg_pg_lsn_lt(a: u64, b: u64) -> i32;
    fn pg_pg_lsn_gt(a: u64, b: u64) -> i32;
    fn pg_pg_lsn_le(a: u64, b: u64) -> i32;
    fn pg_pg_lsn_ge(a: u64, b: u64) -> i32;
    fn pg_pg_lsn_cmp(a: u64, b: u64) -> i32;
}

/// pg_lsn_out: full symbolic u64, byte-for-byte %X/%X output (REL_18
/// format — the one pgrust ships; see the drift witness below).
#[kani::proof]
#[kani::unwind(20)]
fn eq_pg_lsn_out() {
    let lsn: u64 = kani::any();
    let mut cbuf = [0u8; MAXPG_LSNLEN + 1];
    let clen = unsafe { pg_pg_lsn_out_rel18(lsn, cbuf.as_mut_ptr()) } as usize;
    let mut rbuf = [0u8; MAXPG_LSNLEN + 1];
    let rlen = pg_lsn_out_into(lsn, &mut rbuf);
    assert_eq!(clen, rlen);
    assert_eq!(cbuf[..clen], rbuf[..rlen]);
}

/// Upstream-drift witness (PROVED DIVERGENCE, intentional): master (PG19
/// devel) changed pg_lsn_out to %X/%08X. Whenever the low half has fewer
/// than 8 significant hex digits, master's output differs from the shipped
/// Rust (%X/%X) — and whenever it has exactly 8, they agree. This harness
/// proves that characterization over all u64, pinning the divergence to
/// exactly the zero-padding of the low word.
#[kani::proof]
#[kani::unwind(20)]
fn drift_pg_lsn_out_master_format() {
    let lsn: u64 = kani::any();
    let mut cbuf = [0u8; MAXPG_LSNLEN + 1];
    let clen = unsafe { pg_pg_lsn_out_master(lsn, cbuf.as_mut_ptr()) } as usize;
    let mut rbuf = [0u8; MAXPG_LSNLEN + 1];
    let rlen = pg_lsn_out_into(lsn, &mut rbuf);
    if (lsn as u32) >= 0x1000_0000 {
        assert_eq!(clen, rlen);
        assert_eq!(cbuf[..clen], rbuf[..rlen]);
    } else {
        assert!(clen > rlen); // padded low word is strictly longer
    }
}

/// pg_lsn_cmp: full symbolic u64 pair.
#[kani::proof]
fn eq_pg_lsn_cmp() {
    let a: u64 = kani::any();
    let b: u64 = kani::any();
    let c = unsafe { pg_pg_lsn_cmp(a, b) };
    assert_eq!(c, pg_lsn_cmp_internal(a, b));
}

macro_rules! op_harness {
    ($($h:ident: $cfn:ident, $op:tt, $cmp_pred:expr;)*) => {$(
        /// C operator core vs the shipped macro body (`a $op b`), cross-
        /// anchored to shipped pg_lsn_cmp_internal.
        #[kani::proof]
        fn $h() {
            let a: u64 = kani::any();
            let b: u64 = kani::any();
            let c = unsafe { $cfn(a, b) } != 0;
            let rust_op = a $op b; // fc_lsn_cmp! body, builtins.rs:67-82
            let via_cmp: fn(i32) -> bool = $cmp_pred;
            assert_eq!(c, rust_op);
            assert_eq!(c, via_cmp(pg_lsn_cmp_internal(a, b)));
        }
    )*};
}

op_harness! {
    eq_pg_lsn_eq: pg_pg_lsn_eq, ==, |o| o == 0;
    eq_pg_lsn_ne: pg_pg_lsn_ne, !=, |o| o != 0;
    eq_pg_lsn_lt: pg_pg_lsn_lt, <,  |o| o < 0;
    eq_pg_lsn_gt: pg_pg_lsn_gt, >,  |o| o > 0;
    eq_pg_lsn_le: pg_pg_lsn_le, <=, |o| o <= 0;
    eq_pg_lsn_ge: pg_pg_lsn_ge, >=, |o| o >= 0;
}

/// pg_lsn_larger / pg_lsn_smaller (oids 4187/4188): full symbolic u64
/// pairs, WRAPPER-LEVEL (datetime-cmp precedent) — the shipped bodies live
/// in the fc_* wrappers (`arg_lsn(0).max/min(arg_lsn(1)) as i64`,
/// builtins.rs:85-91), so the harness builds a real LocalFcinfo frame via
/// proof_support and invokes the shipped wrapper: datum unwrap (arg_i64 as
/// u64) → max/min → from_i64 pack are all inside the theorem.
macro_rules! minmax_harness {
    ($($h:ident: $fc:ident, $cfn:ident;)*) => {$(
        #[kani::proof]
        fn $h() {
            let a: u64 = kani::any();
            let b: u64 = kani::any();
            let r = proof_support::call2_ok(adt_pg_lsn::builtins::$fc, a, b);
            let c = unsafe { $cfn(a, b) };
            assert!(r.as_i64() as u64 == c);
        }
    )*};
}

minmax_harness! {
    eq_pg_lsn_larger: fc_pg_lsn_larger, pg_pg_lsn_larger;
    eq_pg_lsn_smaller: fc_pg_lsn_smaller, pg_pg_lsn_smaller;
}

/// NEGATIVE CONTROL — must FAIL with a decodable counterexample: feeds the
/// C formatter a different LSN than the Rust one. A pass here means the
/// rig is vacuous (gate-blindness class).
#[kani::proof]
#[kani::unwind(20)]
fn control_pg_lsn_out_mismatch() {
    let lsn: u64 = kani::any();
    let mut cbuf = [0u8; MAXPG_LSNLEN + 1];
    let clen = unsafe { pg_pg_lsn_out_rel18(lsn, cbuf.as_mut_ptr()) } as usize;
    let mut rbuf = [0u8; MAXPG_LSNLEN + 1];
    let rlen = pg_lsn_out_into(lsn ^ 1, &mut rbuf);
    assert!(clen == rlen && cbuf[..clen] == rbuf[..rlen]);
}

// ---------------------------------------------------------------------------
// pg_lsn_in — case-split per the escalation ladder (step 3).
//
// A single full-symbolic harness walls: CBMC unwinds std's
// run_utf8_validation (data-dependent word-skip) and from_ascii_radix over
// symbolic-length slices at ~1s+/iteration of symex. The domain is therefore
// range-partitioned by the harness-side `shape` predicate below, with the
// MANDATORY union-coverage harness (cover_pg_lsn_in_partition) proving the
// partition is total, so the gate does not silently weaken:
//   - eq_pg_lsn_in_reject: shape() == None (symbolic len <= 19). On this
//     domain neither implementation reaches its number-parsing calls; std's
//     from_utf8 / from_str_radix are stubbed to PANIC, so if the accept path
//     were reachable the proof fails loudly (no silent vacuity).
//   - eq_pg_lsn_in_accept_<P1>_<P2> (64 harnesses, P1,P2 = 1..=8): shape()
//     == Some((P1, P2)) with concrete split points (concrete slice lengths
//     keep the std loops concretely bounded; one pair per harness keeps
//     each run inside the solver budget). std from_utf8 is stubbed to from_utf8_unchecked —
//     sound: the call sites receive slices the preceding take_while proved
//     all-ASCII-hexdigit, and ASCII is valid UTF-8. from_str_radix runs real.
// Stub scope note: stubs replace std library internals only, never pgrust
// code under proof. Runs need `-Z stubbing` in addition to `-Z c-ffi`.
// ---------------------------------------------------------------------------

/// Harness-side partition predicate — mirrors the C shape checks (leading
/// hex run, '/', trailing hex run to end-of-string). Used ONLY to partition
/// the input space; both implementations' verdicts are still compared inside
/// every partition.
fn shape(s: &[u8]) -> Option<(usize, usize)> {
    let p1 = s.iter().take_while(|b| b.is_ascii_hexdigit()).count();
    if p1 < 1 || p1 > 8 || s.get(p1) != Some(&b'/') {
        return None;
    }
    let rest = &s[p1 + 1..];
    let p2 = rest.iter().take_while(|b| b.is_ascii_hexdigit()).count();
    if p2 < 1 || p2 > 8 || p2 != rest.len() {
        return None;
    }
    Some((p1, p2))
}

fn stub_from_utf8_unchecked(v: &[u8]) -> Result<&str, core::str::Utf8Error> {
    // Sound at the pg_lsn_in_internal call sites: slices are all-ASCII-hex.
    Ok(unsafe { core::str::from_utf8_unchecked(v) })
}

fn stub_from_utf8_unreachable(_v: &[u8]) -> Result<&str, core::str::Utf8Error> {
    panic!("from_utf8 reached in reject domain — partition predicate wrong");
}

fn stub_from_str_radix_unreachable(
    _s: &str,
    _radix: u32,
) -> Result<u32, core::num::ParseIntError> {
    panic!("from_str_radix reached in reject domain — partition predicate wrong");
}

/// Reject side: every string (len <= 19, no interior NULs) whose shape is
/// invalid is rejected by BOTH implementations. The parse calls are stubbed
/// to panic: reachability of the accept path here would fail the proof.
#[kani::proof]
#[kani::stub(core::str::from_utf8, stub_from_utf8_unreachable)]
#[kani::stub(u32::from_str_radix, stub_from_str_radix_unreachable)]
#[kani::unwind(22)]
fn eq_pg_lsn_in_reject() {
    const CAP: usize = 19;
    let mut buf: [u8; CAP + 1] = kani::any();
    let len: usize = kani::any();
    kani::assume(len <= CAP);
    for i in 0..CAP {
        kani::assume(i >= len || buf[i] != 0);
    }
    buf[len] = 0;
    kani::assume(shape(&buf[..len]).is_none());

    let mut have_error: i32 = 0;
    let c = unsafe { pg_pg_lsn_in_safe(buf.as_ptr(), &mut have_error) };
    let r = pg_lsn_in_internal(&buf[..len]);
    assert_eq!(have_error, 1);
    assert_eq!(c, 0); // InvalidXLogRecPtr
    assert!(r.is_none());
}

/// Accept side, one concrete split (P1 hex digits, '/', P2 hex digits,
/// NUL): both implementations accept and parse the same u64.
fn check_accept<const P1: usize, const P2: usize>() {
    let mut buf: [u8; 20] = kani::any();
    let len = P1 + 1 + P2;
    for i in 0..P1 {
        kani::assume(buf[i].is_ascii_hexdigit());
    }
    buf[P1] = b'/';
    for i in P1 + 1..len {
        kani::assume(buf[i].is_ascii_hexdigit());
    }
    buf[len] = 0;

    let mut have_error: i32 = 0;
    let c = unsafe { pg_pg_lsn_in_safe(buf.as_ptr(), &mut have_error) };
    let r = pg_lsn_in_internal(&buf[..len]);
    assert_eq!(have_error, 0);
    assert_eq!(r, Some(c));
}

macro_rules! accept_harness {
    ($($h:ident: $p1:literal $p2:literal;)*) => {$(
        #[kani::proof]
        #[kani::stub(core::str::from_utf8, stub_from_utf8_unchecked)]
        #[kani::unwind(19)]
        fn $h() {
            check_accept::<$p1, $p2>();
        }
    )*};
}

accept_harness! {
    eq_pg_lsn_in_accept_1_1: 1 1;
    eq_pg_lsn_in_accept_1_2: 1 2;
    eq_pg_lsn_in_accept_1_3: 1 3;
    eq_pg_lsn_in_accept_1_4: 1 4;
    eq_pg_lsn_in_accept_1_5: 1 5;
    eq_pg_lsn_in_accept_1_6: 1 6;
    eq_pg_lsn_in_accept_1_7: 1 7;
    eq_pg_lsn_in_accept_1_8: 1 8;
    eq_pg_lsn_in_accept_2_1: 2 1;
    eq_pg_lsn_in_accept_2_2: 2 2;
    eq_pg_lsn_in_accept_2_3: 2 3;
    eq_pg_lsn_in_accept_2_4: 2 4;
    eq_pg_lsn_in_accept_2_5: 2 5;
    eq_pg_lsn_in_accept_2_6: 2 6;
    eq_pg_lsn_in_accept_2_7: 2 7;
    eq_pg_lsn_in_accept_2_8: 2 8;
    eq_pg_lsn_in_accept_3_1: 3 1;
    eq_pg_lsn_in_accept_3_2: 3 2;
    eq_pg_lsn_in_accept_3_3: 3 3;
    eq_pg_lsn_in_accept_3_4: 3 4;
    eq_pg_lsn_in_accept_3_5: 3 5;
    eq_pg_lsn_in_accept_3_6: 3 6;
    eq_pg_lsn_in_accept_3_7: 3 7;
    eq_pg_lsn_in_accept_3_8: 3 8;
    eq_pg_lsn_in_accept_4_1: 4 1;
    eq_pg_lsn_in_accept_4_2: 4 2;
    eq_pg_lsn_in_accept_4_3: 4 3;
    eq_pg_lsn_in_accept_4_4: 4 4;
    eq_pg_lsn_in_accept_4_5: 4 5;
    eq_pg_lsn_in_accept_4_6: 4 6;
    eq_pg_lsn_in_accept_4_7: 4 7;
    eq_pg_lsn_in_accept_4_8: 4 8;
    eq_pg_lsn_in_accept_5_1: 5 1;
    eq_pg_lsn_in_accept_5_2: 5 2;
    eq_pg_lsn_in_accept_5_3: 5 3;
    eq_pg_lsn_in_accept_5_4: 5 4;
    eq_pg_lsn_in_accept_5_5: 5 5;
    eq_pg_lsn_in_accept_5_6: 5 6;
    eq_pg_lsn_in_accept_5_7: 5 7;
    eq_pg_lsn_in_accept_5_8: 5 8;
    eq_pg_lsn_in_accept_6_1: 6 1;
    eq_pg_lsn_in_accept_6_2: 6 2;
    eq_pg_lsn_in_accept_6_3: 6 3;
    eq_pg_lsn_in_accept_6_4: 6 4;
    eq_pg_lsn_in_accept_6_5: 6 5;
    eq_pg_lsn_in_accept_6_6: 6 6;
    eq_pg_lsn_in_accept_6_7: 6 7;
    eq_pg_lsn_in_accept_6_8: 6 8;
    eq_pg_lsn_in_accept_7_1: 7 1;
    eq_pg_lsn_in_accept_7_2: 7 2;
    eq_pg_lsn_in_accept_7_3: 7 3;
    eq_pg_lsn_in_accept_7_4: 7 4;
    eq_pg_lsn_in_accept_7_5: 7 5;
    eq_pg_lsn_in_accept_7_6: 7 6;
    eq_pg_lsn_in_accept_7_7: 7 7;
    eq_pg_lsn_in_accept_7_8: 7 8;
    eq_pg_lsn_in_accept_8_1: 8 1;
    eq_pg_lsn_in_accept_8_2: 8 2;
    eq_pg_lsn_in_accept_8_3: 8 3;
    eq_pg_lsn_in_accept_8_4: 8 4;
    eq_pg_lsn_in_accept_8_5: 8 5;
    eq_pg_lsn_in_accept_8_6: 8 6;
    eq_pg_lsn_in_accept_8_7: 8 7;
    eq_pg_lsn_in_accept_8_8: 8 8;
}

/// MANDATORY union coverage: every input (len <= 19) falls in the reject
/// partition or in exactly the accept partition some
/// eq_pg_lsn_in_accept_<P1>_<P2> covers (1 <= P1,P2 <= 8 with
/// len == P1 + 1 + P2 and the exact byte-class layout check_accept assumes).
#[kani::proof]
#[kani::unwind(25)]
fn cover_pg_lsn_in_partition() {
    const CAP: usize = 19;
    let buf: [u8; CAP] = kani::any();
    let len: usize = kani::any();
    kani::assume(len <= CAP);
    let s = &buf[..len];
    match shape(s) {
        None => {} // eq_pg_lsn_in_reject's domain
        Some((p1, p2)) => {
            assert!((1..=8).contains(&p1));
            assert!((1..=8).contains(&p2));
            assert_eq!(len, p1 + 1 + p2);
            for i in 0..len {
                if i == p1 {
                    assert_eq!(s[i], b'/');
                } else {
                    assert!(s[i].is_ascii_hexdigit());
                }
            }
        }
    }
}

// ===========================================================================
// WAVE 5 (2026-07-28): pg_lsn_recv (3238) / pg_lsn_send (3239).
// C side: the WAVE 5 wire section appended to c/pg_pg_lsn.c.
//
//   - recv: CORE-level (adt_pg_lsn::pg_lsn_recv on a directly-held
//     StringInfo — the datum round-trip walls symex per the int-arith recv
//     lesson; fc datum plumbing out of proof).  Value + cursor + verdict +
//     sqlstate 08P01 parity over full symbolic bytes/dlen/cursor.
//   - send: WRAPPER-level over a real result-mcx frame (proof_support
//     mcx-stubs recipe; theorem "modulo static-buffer allocator model");
//     full 12-byte wire image compared.  control_pg_lsn_send_skew (C fed
//     lsn^1) MUST FAIL.
//
// SKIPPED wave-5 pg_lsn rows (honest walls, see runqueue.txt/report):
//   3237 pg_lsn_mi, 5022 pg_lsn_pli, 5024 pg_lsn_mii — the shipped cores
//   route through adt_numeric numeric_in/add/sub (allocating DigitBuf
//   digit-loop arithmetic = the TRIAGE numeric-arithmetic wall; needs the
//   fixed-buffer numeric core refactor first).  6103 numeric_pg_lsn —
//   packed-numeric decode + var_to_uint64 accumulate; parked with the same
//   family until the numeric-probe image-builder rig is shared (cheap
//   follow-up, not attempted here to keep the C surface verbatim).
// pg_lsn_hash (3252) / pg_lsn_hash_extended (3413) live in the hash-rows
// rig (proofs/hash-rows) with the other hash pg_proc rows.
// ===========================================================================

#[cfg(kani)]
mod wave5 {
    use datum::{Datum, NullableDatum};
    use proof_support::{mcx_stubs, stubs};
    use types_error::{ERRCODE_PROTOCOL_VIOLATION, ERROR};
    use types_fmgr::LocalFcinfo;

    extern "C" {
        fn pg_pg_lsn_recv(
            data: *const u8,
            len: i32,
            cursor: *mut i32,
            out: *mut u64,
        ) -> std::os::raw::c_int;
        fn pg_pg_lsn_send(lsn: u64, out: *mut u8) -> i32;
    }

    #[kani::proof]
    #[kani::unwind(16)] // copy loops <= CAP+1
    #[kani::stub(mcx::Mcx::allocate, mcx_stubs::stub_mcx_allocate)]
    // grow/deallocate stubs: RVR lesson — mandatory whenever the core can
    // reach vec_append_bytes' try_reserve/grow branch (si.append_bytes here);
    // without them real arena + Acct::subtree_sum recursion enters symex.
    #[kani::stub(mcx::Mcx::grow, mcx_stubs::stub_mcx_grow)]
    #[kani::stub(mcx::Mcx::deallocate, mcx_stubs::stub_mcx_deallocate)]
    #[kani::stub(std::env::var, stubs::stub_env_var_zero)]
    #[kani::stub(std::sync::OnceLock::get_or_init, stubs::stub_once_lock_get_or_init)]
    #[kani::stub(types_error::PgError::error, stubs::stub_pg_error_error)]
    #[kani::stub(std::fmt::format, stubs::stub_format)]
    fn eq_pg_lsn_recv_core() {
        const CAP: usize = 12;
        let data: [u8; CAP] = kani::any();
        let dlen: usize = kani::any();
        kani::assume(dlen <= CAP);
        let cur: usize = kani::any();
        kani::assume(cur <= CAP);

        let mut ccur: i32 = cur as i32;
        let mut cout: u64 = 0;
        let cst = unsafe { pg_pg_lsn_recv(data.as_ptr(), dlen as i32, &mut ccur, &mut cout) };

        let ctx = mcx::MemoryContext::new_bump("kani-lsn-recv");
        let mut si = match stringinfo::StringInfo::with_capacity_in(ctx.mcx(), CAP + 2) {
            Ok(s) => s,
            Err(e) => {
                core::mem::forget(e);
                panic!("stub alloc failed")
            }
        };
        if let Err(e) = si.append_bytes(&data[..dlen]) {
            core::mem::forget(e);
            panic!("append within capacity failed");
        }
        si.cursor = cur;
        match adt_pg_lsn::pg_lsn_recv(&mut si) {
            Ok(v) => {
                assert!(cst == 0);
                assert!(v == cout);
                assert!(si.cursor == ccur as usize);
                kani::cover!(true, "pg_lsn_recv Ok arm reachable");
            }
            Err(e) => {
                assert!(cst == 4);
                assert!(e.sqlstate == ERRCODE_PROTOCOL_VIOLATION);
                assert!(e.level == ERROR);
                core::mem::forget(e);
                kani::cover!(true, "pg_lsn_recv Err arm reachable");
            }
        }
        core::mem::forget(si);
        core::mem::forget(ctx);
    }

    /// Width-probe / per-length cell rig for the recv core: concrete dlen,
    /// symbolic bytes + symbolic cursor. Mirrors eq_pg_lsn_recv_core.
    macro_rules! recv_cell {
        ($($name:ident: $dlen:expr;)*) => {$(
            #[kani::proof]
            #[kani::unwind(16)]
            #[kani::stub(mcx::Mcx::allocate, mcx_stubs::stub_mcx_allocate)]
            #[kani::stub(mcx::Mcx::grow, mcx_stubs::stub_mcx_grow)]
            #[kani::stub(mcx::Mcx::deallocate, mcx_stubs::stub_mcx_deallocate)]
            #[kani::stub(std::env::var, stubs::stub_env_var_zero)]
            #[kani::stub(std::sync::OnceLock::get_or_init, stubs::stub_once_lock_get_or_init)]
            #[kani::stub(types_error::PgError::error, stubs::stub_pg_error_error)]
            #[kani::stub(std::fmt::format, stubs::stub_format)]
            fn $name() {
                const CAP: usize = 12;
                let data: [u8; CAP] = kani::any();
                let dlen: usize = $dlen;
                let cur: usize = kani::any();
                kani::assume(cur <= CAP);

                let mut ccur: i32 = cur as i32;
                let mut cout: u64 = 0;
                let cst = unsafe { pg_pg_lsn_recv(data.as_ptr(), dlen as i32, &mut ccur, &mut cout) };

                let ctx = mcx::MemoryContext::new_bump("kani-lsn-recv");
                let mut si = match stringinfo::StringInfo::with_capacity_in(ctx.mcx(), CAP + 2) {
                    Ok(s) => s,
                    Err(e) => {
                        core::mem::forget(e);
                        panic!("stub alloc failed")
                    }
                };
                if let Err(e) = si.append_bytes(&data[..dlen]) {
                    core::mem::forget(e);
                    panic!("append within capacity failed");
                }
                si.cursor = cur;
                match adt_pg_lsn::pg_lsn_recv(&mut si) {
                    Ok(v) => {
                        assert!(cst == 0);
                        assert!(v == cout);
                        assert!(si.cursor == ccur as usize);
                        kani::cover!(true, "cell Ok arm reachable");
                    }
                    Err(e) => {
                        assert!(cst == 4);
                        assert!(e.sqlstate == ERRCODE_PROTOCOL_VIOLATION);
                        assert!(e.level == ERROR);
                        core::mem::forget(e);
                        kani::cover!(true, "cell Err arm reachable");
                    }
                }
                core::mem::forget(si);
                core::mem::forget(ctx);
            }
        )*};
    }

    recv_cell! {
        eq_pg_lsn_recv_core_d12: 12;
        eq_pg_lsn_recv_core_d11: 11;
        eq_pg_lsn_recv_core_d10: 10;
        eq_pg_lsn_recv_core_d9: 9;
        eq_pg_lsn_recv_core_d8: 8;
        eq_pg_lsn_recv_core_d7: 7;
        eq_pg_lsn_recv_core_d6: 6;
        eq_pg_lsn_recv_core_d5: 5;
        eq_pg_lsn_recv_core_d4: 4;
        eq_pg_lsn_recv_core_d3: 3;
        eq_pg_lsn_recv_core_d2: 2;
        eq_pg_lsn_recv_core_d1: 1;
        eq_pg_lsn_recv_core_d0: 0;
    }

    #[kani::proof]
    #[kani::unwind(16)]
    #[kani::stub(mcx::Mcx::allocate, mcx_stubs::stub_mcx_allocate)]
    #[kani::stub(std::env::var, stubs::stub_env_var_zero)]
    #[kani::stub(std::sync::OnceLock::get_or_init, stubs::stub_once_lock_get_or_init)]
    #[kani::stub(std::fmt::format, stubs::stub_format)]
    fn eq_pg_lsn_send() {
        let lsn: u64 = kani::any();
        let mut cbuf = [0u8; 12];
        let clen = unsafe { pg_pg_lsn_send(lsn, cbuf.as_mut_ptr()) };

        let ctx = mcx::MemoryContext::new_bump("kani-lsn-send");
        let mut f = LocalFcinfo::<1>::new(0);
        // SAFETY: ctx outlives the call (forgotten, never freed).
        unsafe { f.set_result_mcx(ctx.mcx()) };
        f.args[0] = NullableDatum::value(Datum::from_i64(lsn as i64));
        let d = match adt_pg_lsn::builtins::fc_pg_lsn_send(None, &mut f) {
            Ok(d) => d,
            Err(e) => {
                core::mem::forget(e);
                panic!("pg_lsn_send errored")
            }
        };
        let img = unsafe { core::slice::from_raw_parts(d.as_usize() as *const u8, 12) };
        assert!(clen == 12);
        let mut i = 0;
        while i < 12 {
            assert!(img[i] == cbuf[i]);
            i += 1;
        }
        core::mem::forget(ctx);
    }

    /// MUST FAIL (wire-section control): C is fed lsn^1. DEFAULT solver.
    #[kani::proof]
    #[kani::unwind(16)]
    #[kani::stub(mcx::Mcx::allocate, mcx_stubs::stub_mcx_allocate)]
    #[kani::stub(std::env::var, stubs::stub_env_var_zero)]
    #[kani::stub(std::sync::OnceLock::get_or_init, stubs::stub_once_lock_get_or_init)]
    #[kani::stub(std::fmt::format, stubs::stub_format)]
    fn control_pg_lsn_send_skew() {
        let lsn: u64 = kani::any();
        let mut cbuf = [0u8; 12];
        let _ = unsafe { pg_pg_lsn_send(lsn ^ 1, cbuf.as_mut_ptr()) };
        let ctx = mcx::MemoryContext::new_bump("kani-lsn-send-ctl");
        let mut f = LocalFcinfo::<1>::new(0);
        // SAFETY: ctx outlives the call.
        unsafe { f.set_result_mcx(ctx.mcx()) };
        f.args[0] = NullableDatum::value(Datum::from_i64(lsn as i64));
        let d = match adt_pg_lsn::builtins::fc_pg_lsn_send(None, &mut f) {
            Ok(d) => d,
            Err(e) => {
                core::mem::forget(e);
                panic!("send errored")
            }
        };
        let img = unsafe { core::slice::from_raw_parts(d.as_usize() as *const u8, 12) };
        let mut i = 0;
        while i < 12 {
            assert!(img[i] == cbuf[i]); // expected failure at the last byte
            i += 1;
        }
        core::mem::forget(ctx);
    }
}
