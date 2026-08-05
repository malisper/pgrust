//! WAVE-8 varlena byte kernels (extends the COMPLETE text-slice lane; this
//! module is self-contained — it shares the crate, the inline-varlena image
//! recipe, and c/pg_text_slice.c's substring/catenate sections, but touches
//! nothing in the crate-root `proofs` module).
//!
//! Ledger rows hosted here (screened against USER_FACING_FUNCTIONS.tsv,
//! all `untriaged` as of 2026-07-28):
//!   FULL symbolic harnesses (scalar-verdict or EXPLICIT-length images):
//!     6370/6371/6372 bytea_int2/int4/int8 (value+verdict+sqlstate class;
//!       int8 uses a CAP=9 image so its len>8 error plane is reachable),
//!     6367/6368/6369 int2/int4/int8_bytea (fixed-width 2/4/8 BE images,
//!       full symbolic i16/i32/i64),
//!     6382 bytea_reverse (out-len == in-len, len<=8 fully symbolic),
//!     6393/6394 bytea_larger/smaller (C pointer-identity selector),
//!     456/6413 hashvarlena+hashbytea, 772/6414 hashvarlena/byteaextended
//!       (one fmgr body per pair; len<=8 fully symbolic, symbolic seed),
//!     6163 bytea_bit_count (LEN<=7 FENCE: the shipped pg_popcount routes
//!       len>=8 through NEON intrinsics on aarch64 = Kani
//!       unsupported_construct; len>=8 arm excluded(blocked:simd)),
//!     47 textout (out-len == in-len + NUL; wrapper-level with a real
//!       FmgrInfo OutBuf scratch),
//!     2413 byteasend (identity copy), 2415 textsend (payload image;
//!       ENCODING-CONVERSION FENCE: pg_server_to_client seam pinned to
//!       identity == ClientEncoding==ServerEncoding arm, both sides),
//!     749/752 byteaoverlay[_no_len] (ladder tier: composed
//!       substr+substr+cat+cat; sp/sl full symbolic i32 incl overflow edge).
//!   LADDER symbolic image harnesses with a WALL RISK note (derived-length
//!   copy class — the result length is computed by the code under proof,
//!   TRIAGE 2026-07-28; the spot grids below are the fallback of record):
//!     6330/6331 to_bin32/64, 6332/6333 to_oct32/64, 2089/2090 to_hex32/64.
//!   SPOT grids (one-symbolic-index-into-concrete-table, geo-cmp pattern)
//!   for the derived-length copy class (pstrdup/strlen shapes — the exact
//!   pseudotypes-lane wall; symbolic harnesses are DOOMED per TRIAGE, so
//!   these rows get boundary-grid coverage + a runqueue wall note):
//!     46 textin, 109 unknownin, 110 unknownout, 2417 unknownsend.
//!   DEFERRED (no harness; see runqueue-wave8.txt):
//!     2412 bytearecv / 2414 textrecv / 2416 unknownrecv — wrapper level is
//!     the measured POINTER-DATUM ROUND-TRIP WALL (arg_stringinfo datum
//!     recovery, TRIAGE int-arith wave 3); core level needs the
//!     pq_getmsgtext + pg_client_to_server validating-conversion stack
//!     vendored (its own lane per the hstore_recv lesson).
//!
//! Fences and claims (mirror into the ledger at run time):
//!  - Bounds: payload len <= 8 (<=9 for bytea_int8's error plane; <=7 for
//!    bytea_bit_count) fully symbolic bytes; int args full-domain.
//!  - Error parity: value-space + verdict + sqlstate class (PgError::error
//!    and format machinery stubbed via proof_support — message text and
//!    Location leave the proof). Classes: 1=22011 substring, 5=22003
//!    numeric out of range.
//!  - Allocation: "modulo static-buffer allocator model" — Mcx::allocate/
//!    grow/deallocate + vec_with_capacity_in/vec_append_bytes stubbed to
//!    the proof heap; all bytes written through allocations stay in the
//!    theorem. textin/textout ride the shipped OutBuf std-Vec scratch
//!    (global allocator, Kani-native).
//!  - textsend/unknownsend: conversion seam pinned to identity (encoding
//!    conversion out of scope); byteasend has no conversion leg.
//!  - hash rows: C side is ../hash/pg_hashfn.c (REL_18-conformant per
//!    proofs/PROVENANCE-AUDIT.md); harness images put the payload at an
//!    odd offset (1-byte header), i.e. the UNALIGNED hash_bytes arm — the
//!    aligned arm is proved in proofs/hash over both alignments.
//!  - bytea_larger/smaller: theorem includes C's pointer-identity result
//!    convention (returned datum IS one of the argument datums).
//!
//! Negative controls (MUST FAIL, run with the DEFAULT solver):
//!  - control_bytea_int4_short: C sees a one-byte-shorter payload
//!    (scalar-verdict path non-vacuity for the new C section).
//!  - control_bytea_reverse_short: C sees a one-byte-shorter payload
//!    (image-compare path non-vacuity for the new C section).
//!
//! Run: see proofs/text-slice/runqueue-wave8.txt (exact commands, unwind
//! derivations, tiers, expected verdicts). All three C files link together:
//!   cargo kani -Z c-ffi -Z stubbing --solver kissat \
//!     --c-lib c/pg_text_slice.c c/pg_varlena_wave8.c ../hash/pg_hashfn.c \
//!     --harness wave8::<name> --exact

use datum::{Datum, NullableDatum};
use mcx::{Mcx, MemoryContext, PgVec};
use proof_support::mcx_stubs;
use std::os::raw::c_int;
use types_core::C_COLLATION_OID;
use types_error::{PgError, PgResult};
use types_fmgr::{FmgrInfo, FunctionCallInfoBaseData, LocalFcinfo};
use types_tuple::varatt;

extern "C" {
    fn pg8_take_err() -> c_int;

    fn pg8_textin(s: *const u8, out: *mut u8) -> c_int;
    fn pg8_textout(d: *const u8, len: c_int, out: *mut u8) -> c_int;
    fn pg8_unknownin(s: *const u8, out: *mut u8) -> c_int;
    fn pg8_unknownout(s: *const u8, out: *mut u8) -> c_int;
    fn pg8_byteasend(d: *const u8, len: c_int, out: *mut u8) -> c_int;
    fn pg8_textsend(d: *const u8, len: c_int, out: *mut u8) -> c_int;
    fn pg8_unknownsend(s: *const u8, out: *mut u8) -> c_int;
    fn pg8_int2_bytea(arg1: i16, out: *mut u8) -> c_int;
    fn pg8_int4_bytea(arg1: i32, out: *mut u8) -> c_int;
    fn pg8_int8_bytea(arg1: i64, out: *mut u8) -> c_int;
    fn pg8_bytea_int2(d: *const u8, len: c_int) -> i16;
    fn pg8_bytea_int4(d: *const u8, len: c_int) -> c_int;
    fn pg8_bytea_int8(d: *const u8, len: c_int) -> i64;
    fn pg8_bytea_reverse(d: *const u8, len: c_int, out: *mut u8) -> c_int;
    fn pg8_bytea_larger_pick(d1: *const u8, len1: c_int, d2: *const u8, len2: c_int) -> c_int;
    fn pg8_bytea_smaller_pick(d1: *const u8, len1: c_int, d2: *const u8, len2: c_int) -> c_int;
    fn pg8_bytea_bit_count(d: *const u8, len: c_int) -> i64;
    fn pg8_to_bin32(arg: c_int, out: *mut u8) -> c_int;
    fn pg8_to_bin64(arg: i64, out: *mut u8) -> c_int;
    fn pg8_to_oct32(arg: c_int, out: *mut u8) -> c_int;
    fn pg8_to_oct64(arg: i64, out: *mut u8) -> c_int;
    fn pg8_to_hex32(arg: c_int, out: *mut u8) -> c_int;
    fn pg8_to_hex64(arg: i64, out: *mut u8) -> c_int;
    fn pg8_bytea_overlay(
        d1: *const u8,
        len1: c_int,
        d2: *const u8,
        len2: c_int,
        sp: c_int,
        sl: c_int,
        out: *mut u8,
    ) -> c_int;

    // ../hash/pg_hashfn.c (REL_18-conformant vendored hashfn.c)
    fn pg_hash_bytes(k: *const u8, keylen: c_int) -> u32;
    fn pg_hash_bytes_extended(k: *const u8, keylen: c_int, seed: u64) -> u64;
}

const CAP: usize = 8;
const COLL_C: u32 = C_COLLATION_OID;
/// C-side ereport sentinel (shim W3 in c/pg_varlena_wave8.c).
const PG_CERR: c_int = -2_100_000_000;

// C errflag classes (shim W3) <-> Rust sqlstates.
fn err_class8(e: &PgError) -> c_int {
    use types_error::*;
    if e.sqlstate == ERRCODE_SUBSTRING_ERROR {
        1
    } else if e.sqlstate == ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE {
        5
    } else {
        -1
    }
}

/// One inline 1-byte-header varlena image over symbolic payload bytes +
/// symbolic length, built with the SHIPPED header encoder (text-cmp /
/// crate-root proofs pattern). `T` is the whole-image size, so payload cap
/// = T - VARHDRSZ_SHORT; generic so bytea_int8 can reach its len == 9
/// error plane.
struct VarImgN<const T: usize> {
    img: [u8; T],
    len: usize,
}

fn sym_varlena_n<const T: usize>() -> VarImgN<T> {
    let payload: [u8; T] = kani::any();
    let len: usize = kani::any();
    kani::assume(len <= T - varatt::VARHDRSZ_SHORT);
    let mut img = [0u8; T];
    // SAFETY: img is writable and len + 1 <= VARATT_SHORT_MAX.
    unsafe { varatt::set_varsize_short(img.as_mut_ptr(), len + varatt::VARHDRSZ_SHORT) };
    let mut i = varatt::VARHDRSZ_SHORT;
    while i < T {
        img[i] = payload[i];
        i += 1;
    }
    VarImgN { img, len }
}

type VarImg = VarImgN<{ CAP + varatt::VARHDRSZ_SHORT }>;
type VarImg9 = VarImgN<{ 9 + varatt::VARHDRSZ_SHORT }>;

fn sym_varlena() -> VarImg {
    sym_varlena_n()
}

impl<const T: usize> VarImgN<T> {
    fn datum(&self) -> Datum {
        Datum::from_usize(self.img.as_ptr() as usize)
    }
    fn data(&self) -> *const u8 {
        self.img[varatt::VARHDRSZ_SHORT..].as_ptr()
    }
    fn clen(&self) -> c_int {
        self.len as c_int
    }
}

/// The shipped fc_* wrapper shape.
type FcFn = fn(Option<&mut FmgrInfo>, &mut FunctionCallInfoBaseData) -> PgResult<Datum>;

/// Run a shipped fc_* wrapper on a real N-arg frame with an armed result
/// mcx (the new-by-ref result convention).
fn call<const N: usize>(fc: FcFn, args: [Datum; N], collid: u32, mcx: Mcx<'_>) -> PgResult<Datum> {
    let mut f = LocalFcinfo::<N>::new(collid);
    for (slot, d) in f.args.iter_mut().zip(args) {
        *slot = NullableDatum::value(d);
    }
    // SAFETY: the context outlives the call (harness stack frame).
    unsafe { f.set_result_mcx(mcx) };
    fc(None, &mut f)
}

/// [`call`] without an armed result mcx, for scalar-result wrappers.
fn call_scalar<const N: usize>(fc: FcFn, args: [Datum; N], collid: u32) -> PgResult<Datum> {
    let mut f = LocalFcinfo::<N>::new(collid);
    for (slot, d) in f.args.iter_mut().zip(args) {
        *slot = NullableDatum::value(d);
    }
    fc(None, &mut f)
}

/// [`call_scalar`] with a real (unresolved) FmgrInfo, for the OutBuf-scratch
/// cstring/text wrappers (textin/textout/unknownout). The caller LEAKS the
/// FmgrInfo after reading the result (its fn_extra Box/Vec teardown is
/// allocator machinery outside every claim).
fn call_flinfo<const N: usize>(
    fc: FcFn,
    fi: &mut FmgrInfo,
    args: [Datum; N],
    collid: u32,
) -> PgResult<Datum> {
    let mut f = LocalFcinfo::<N>::new(collid);
    for (slot, d) in f.args.iter_mut().zip(args) {
        *slot = NullableDatum::value(d);
    }
    fc(Some(fi), &mut f)
}

/// Payload bytes of a shipped 4B-header result image.
unsafe fn out_payload<'a>(d: Datum) -> &'a [u8] {
    let p = d.as_usize() as *const u8;
    let total = varatt::varsize_any(p);
    core::slice::from_raw_parts(p.add(varatt::VARHDRSZ), total - varatt::VARHDRSZ)
}

/// Assert C == Rust for a scalar result: value + error verdict + sqlstate
/// class (the C value is a dead 0 whenever pg8_errflag is set, mirroring
/// C's longjmp unwind). Single property (external kissat re-solves the
/// whole formula per property). The Result is FORGOTTEN, never dropped
/// (Box<PgError> drop glue is a measured symex wall — TRIAGE error-drop
/// trap).
fn check_scalar(r: PgResult<i64>, c: i64) {
    let cerr = unsafe { pg8_take_err() };
    let ok = match &r {
        Ok(v) => cerr == 0 && *v == c,
        Err(e) => cerr != 0 && err_class8(e) == cerr,
    };
    core::mem::forget(r);
    assert!(ok, "C/Rust divergence (value or error verdict/class)");
}

/// Assert C (out buffer + returned length) == Rust (result image payload),
/// or matching error verdict + class. Single property; Result forgotten
/// (see check_scalar).
fn check_bytes(r: PgResult<Datum>, c: c_int, out: &[u8]) {
    let cerr = unsafe { pg8_take_err() };
    let ok = match &r {
        Ok(d) => {
            let rp = unsafe { out_payload(*d) };
            let mut same = c != PG_CERR && cerr == 0 && rp.len() == c as usize;
            let mut i = 0usize;
            while i < rp.len() && i < out.len() {
                same = same && rp[i] == out[i];
                i += 1;
            }
            same && rp.len() <= out.len()
        }
        Err(e) => c == PG_CERR && err_class8(e) == cerr,
    };
    core::mem::forget(r);
    assert!(ok, "C/Rust divergence (bytes or error verdict/class)");
}

/// Assert C (cstring payload + NUL) == Rust cstring result datum. Compares
/// clen bytes AND the terminator on both sides — both bodies are straight
/// memcpy+NUL, so raw-byte parity (stronger than first-NUL-observable
/// parity) is the intended claim. Result forgotten (see check_scalar).
fn check_cstring(r: PgResult<Datum>, clen: c_int, out: &[u8]) {
    let cerr = unsafe { pg8_take_err() };
    let ok = match &r {
        Ok(d) => {
            let p = d.as_usize() as *const u8;
            let rp = unsafe { core::slice::from_raw_parts(p, clen as usize + 1) };
            let mut same = cerr == 0;
            let mut i = 0usize;
            while i < clen as usize {
                same = same && rp[i] == out[i];
                i += 1;
            }
            same && rp[clen as usize] == 0 && out[clen as usize] == 0
        }
        Err(e) => err_class8(e) == cerr,
    };
    core::mem::forget(r);
    assert!(ok, "C/Rust divergence (cstring bytes or error verdict/class)");
}

/// Stub for `mcx::vec_append_bytes` under proof (same contract as the
/// crate-root proofs copy): identical semantics for within-capacity appends;
/// a capacity overrun fails the proof loudly instead of re-entering the
/// real allocator's grow path.
pub fn stub_vec_append_bytes(v: &mut PgVec<'_, u8>, bytes: &[u8]) -> PgResult<()> {
    assert!(
        v.len() + bytes.len() <= v.capacity(),
        "proof stub: append beyond reserved capacity"
    );
    let old = v.len();
    // SAFETY: capacity checked above; src/dst disjoint; set_len covers
    // exactly the bytes written.
    unsafe {
        core::ptr::copy_nonoverlapping(bytes.as_ptr(), v.as_mut_ptr().add(old), bytes.len());
        v.set_len(old + bytes.len());
    }
    Ok(())
}

/// Stub for `mcx::local_pool_on` (OnceLock/std-sync walls symex; pool
/// SELECTION is allocation strategy, out of every equivalence claim).
pub fn stub_local_pool_on() -> bool {
    false
}

/// Stub for `std::env::var` (mcx pool-stripe read on the context
/// construction path; environment state is harness scaffolding).
pub fn stub_env_var<K: AsRef<std::ffi::OsStr>>(_key: K) -> Result<String, std::env::VarError> {
    Err(std::env::VarError::NotPresent)
}

/// Reachability-canary stubs for the pglz decompress arms of
/// detoast_attr_slice (byteaoverlay's raw-image substring fetches): harness
/// images are inline uncompressed, so these MUST be unreachable — panic
/// loudly and keep the (dead) pglz loops out of the unwound formula.
pub fn stub_decompress_unreachable<'mcx>(
    _mcx: Mcx<'mcx>,
    _attr: &[u8],
) -> PgResult<mcx::PgVec<'mcx, u8>> {
    panic!("toast_decompress_datum reached — harness images are inline");
}

pub fn stub_decompress_slice_unreachable<'mcx>(
    _mcx: Mcx<'mcx>,
    _attr: &[u8],
    _slicelength: i32,
) -> PgResult<mcx::PgVec<'mcx, u8>> {
    panic!("toast_decompress_datum_slice reached — harness images are inline");
}

/// ENCODING-CONVERSION FENCE (shim W5's Rust half): pg_server_to_client
/// identity arm — Ok(None) = "caller's bytes stand", exactly C's
/// ClientEncoding == ServerEncoding return-src-pointer arm. Conversion
/// proper is out of scope.
fn server_to_client_identity<'mcx>(
    _mcx: Mcx<'mcx>,
    _s: &[u8],
) -> PgResult<Option<PgVec<'mcx, u8>>> {
    Ok(None)
}

fn install_send_env() {
    mbutils_seams::pg_server_to_client::set(server_to_client_identity);
}

fn install_detoast() {
    detoast_seams::detoast_attr_slice::set(detoast::detoast_attr_slice);
}

// Common stub set: allocation -> proof heap (Mcx::allocate/grow/deallocate
// level, so StringInfo construction and PgVec::push grow branches stay out
// of the real arena — TRIAGE mcx-stubs rounds 1+2); error message plumbing
// out of the proof (value/verdict/sqlstate stay in).
macro_rules! wave8_proof {
    ($(#[$attr:meta])* fn $name:ident() $body:block) => {
        #[kani::proof]
        $(#[$attr])*
        #[kani::stub(mcx::Mcx::allocate, mcx_stubs::stub_mcx_allocate)]
        #[kani::stub(mcx::Mcx::grow, mcx_stubs::stub_mcx_grow)]
        #[kani::stub(mcx::Mcx::deallocate, mcx_stubs::stub_mcx_deallocate)]
        #[kani::stub(mcx::vec_with_capacity_in, mcx_stubs::stub_vec_with_capacity_in)]
        #[kani::stub(mcx::vec_append_bytes, stub_vec_append_bytes)]
        #[kani::stub(types_error::PgError::error, proof_support::stub_pg_error_error)]
        #[kani::stub(alloc::fmt::format, proof_support::stub_format)]
        #[kani::stub(std::env::var, stub_env_var)]
        #[kani::stub(mcx::local_pool_on, stub_local_pool_on)]
        #[kani::stub(detoast::toast_decompress_datum, stub_decompress_unreachable)]
        #[kani::stub(detoast::toast_decompress_datum_slice, stub_decompress_slice_unreachable)]
        fn $name() $body
    };
}

// ================= bytea -> int casts (6370/6371/6372) =================
// Scalar-verdict class: full harnesses. Error plane (len > width, sqlstate
// 22003) in-theorem. Unwind: fold loop <= 8 (9 for int8) iterations + image
// init loops <= 9 -> 12/13.

wave8_proof! {
    #[kani::unwind(12)]
    fn eq_bytea_int2() {
        let a = sym_varlena();
        let c = unsafe { pg8_bytea_int2(a.data(), a.clen()) };
        let r = call_scalar(varlena::builtins::fc_bytea_int2, [a.datum()], COLL_C);
        check_scalar(r.map(|d| d.as_i16() as i64), c as i64);
    }
}

wave8_proof! {
    #[kani::unwind(12)]
    fn eq_bytea_int4() {
        let a = sym_varlena();
        let c = unsafe { pg8_bytea_int4(a.data(), a.clen()) };
        let r = call_scalar(varlena::builtins::fc_bytea_int4, [a.datum()], COLL_C);
        check_scalar(r.map(|d| d.as_i32() as i64), c as i64);
    }
}

wave8_proof! {
    #[kani::unwind(13)]
    fn eq_bytea_int8() {
        // CAP = 9 so the len > 8 error plane is reachable.
        let a: VarImg9 = sym_varlena_n();
        let c = unsafe { pg8_bytea_int8(a.data(), a.clen()) };
        let r = call_scalar(varlena::builtins::fc_bytea_int8, [a.datum()], COLL_C);
        check_scalar(r.map(|d| d.as_i64()), c);
    }
}

/// Err-arm reachability witnesses for the fallible bytea->int rows, hoisted
/// into one harness (each inline cover = one extra SAT pass under kissat).
/// Run with the DEFAULT solver.
wave8_proof! {
    #[kani::unwind(13)]
    fn cover_bytea_int_err_arms() {
        let a = sym_varlena();
        let r2 = call_scalar(varlena::builtins::fc_bytea_int2, [a.datum()], COLL_C);
        kani::cover!(r2.is_err());
        let r4 = call_scalar(varlena::builtins::fc_bytea_int4, [a.datum()], COLL_C);
        kani::cover!(r4.is_err());
        let b: VarImg9 = sym_varlena_n();
        let r8 = call_scalar(varlena::builtins::fc_bytea_int8, [b.datum()], COLL_C);
        kani::cover!(r8.is_err());
        core::mem::forget(r2);
        core::mem::forget(r4);
        core::mem::forget(r8);
    }
}

// ================= int -> bytea casts (6367/6368/6369) =================
// Explicit-length images (2/4/8 BE bytes), full symbolic ints.

wave8_proof! {
    #[kani::unwind(12)]
    fn eq_int2_bytea() {
        let v: i16 = kani::any();
        let ctx = MemoryContext::new_bump("proof");
        let mut out = [0u8; 8];
        let c = unsafe { pg8_int2_bytea(v, out.as_mut_ptr()) };
        let r = call(varlena::builtins::fc_int2_bytea, [Datum::from_i16(v)], COLL_C, ctx.mcx());
        core::mem::forget(ctx);
        check_bytes(r, c, &out);
    }
}

wave8_proof! {
    #[kani::unwind(12)]
    fn eq_int4_bytea() {
        let v: i32 = kani::any();
        let ctx = MemoryContext::new_bump("proof");
        let mut out = [0u8; 8];
        let c = unsafe { pg8_int4_bytea(v, out.as_mut_ptr()) };
        let r = call(varlena::builtins::fc_int4_bytea, [Datum::from_i32(v)], COLL_C, ctx.mcx());
        core::mem::forget(ctx);
        check_bytes(r, c, &out);
    }
}

wave8_proof! {
    #[kani::unwind(12)]
    fn eq_int8_bytea() {
        let v: i64 = kani::any();
        let ctx = MemoryContext::new_bump("proof");
        let mut out = [0u8; 16];
        let c = unsafe { pg8_int8_bytea(v, out.as_mut_ptr()) };
        let r = call(varlena::builtins::fc_int8_bytea, [Datum::from_i64(v)], COLL_C, ctx.mcx());
        core::mem::forget(ctx);
        check_bytes(r, c, &out);
    }
}

// ================= bytea_reverse (6382) =================
// out-len == in-len (explicit-length image class). The shipped core's
// per-byte PgVec::push loop stays within reserved capacity; its grow branch
// resolves to the loud proof-heap stub, never the real arena.

wave8_proof! {
    #[kani::unwind(12)]
    fn eq_bytea_reverse() {
        let a = sym_varlena();
        let ctx = MemoryContext::new_bump("proof");
        let mut out = [0u8; CAP];
        let c = unsafe { pg8_bytea_reverse(a.data(), a.clen(), out.as_mut_ptr()) };
        let r = call(varlena::builtins::fc_bytea_reverse, [a.datum()], COLL_C, ctx.mcx());
        core::mem::forget(ctx);
        check_bytes(r, c, &out);
    }
}

// ================= bytea_larger / bytea_smaller (6393/6394) ============
// C returns arg1 or arg2 BY POINTER (no copy); theorem = the shipped
// wrapper returns the SAME argument's datum (pointer identity, shim W7).
// Ties (equal bytes + equal len) pick arg2 on both sides.

wave8_proof! {
    #[kani::unwind(12)]
    fn eq_bytea_larger() {
        let (a, b) = (sym_varlena(), sym_varlena());
        let pick = unsafe { pg8_bytea_larger_pick(a.data(), a.clen(), b.data(), b.clen()) };
        let r = call_scalar(
            varlena::builtins::fc_bytea_larger,
            [a.datum(), b.datum()],
            COLL_C,
        );
        let want = if pick == 0 { a.datum() } else { b.datum() };
        let ok = match &r {
            Ok(d) => d.as_usize() == want.as_usize(),
            Err(_) => false,
        };
        core::mem::forget(r);
        assert!(ok, "C/Rust divergence (larger pick)");
    }
}

wave8_proof! {
    #[kani::unwind(12)]
    fn eq_bytea_smaller() {
        let (a, b) = (sym_varlena(), sym_varlena());
        let pick = unsafe { pg8_bytea_smaller_pick(a.data(), a.clen(), b.data(), b.clen()) };
        let r = call_scalar(
            varlena::builtins::fc_bytea_smaller,
            [a.datum(), b.datum()],
            COLL_C,
        );
        let want = if pick == 0 { a.datum() } else { b.datum() };
        let ok = match &r {
            Ok(d) => d.as_usize() == want.as_usize(),
            Err(_) => false,
        };
        core::mem::forget(r);
        assert!(ok, "C/Rust divergence (smaller pick)");
    }
}

// ================= hashvarlena family (456/772/6413/6414) ==============
// One fmgr body serves hashvarlena==hashbytea and the extended pair
// (VARLENA_BUILTINS registers both oids on fc_hashvarlena[extended]).
// len <= 8 => the C 12-byte block loop runs 0 times; unwind 12 covers the
// trailing switch + the Rust chunk handling.

wave8_proof! {
    #[kani::unwind(12)]
    fn eq_hashvarlena() {
        let a = sym_varlena();
        let c = unsafe { pg_hash_bytes(a.data(), a.clen()) };
        let r = call_scalar(varlena::builtins::fc_hashvarlena, [a.datum()], COLL_C);
        let ok = match &r {
            Ok(d) => d.as_u32() == c,
            Err(_) => false,
        };
        core::mem::forget(r);
        assert!(ok, "C/Rust divergence (hashvarlena)");
    }
}

wave8_proof! {
    #[kani::unwind(12)]
    fn eq_hashvarlena_extended() {
        let a = sym_varlena();
        let seed: u64 = kani::any();
        let c = unsafe { pg_hash_bytes_extended(a.data(), a.clen(), seed) };
        let r = call_scalar(
            varlena::builtins::fc_hashvarlenaextended,
            [a.datum(), Datum::from_u64(seed)],
            COLL_C,
        );
        let ok = match &r {
            Ok(d) => d.as_u64() == c,
            Err(_) => false,
        };
        core::mem::forget(r);
        assert!(ok, "C/Rust divergence (hashvarlenaextended)");
    }
}

// ================= bytea_bit_count (6163) =================
// LEN <= 7 FENCE: shipped pg_popcount routes len >= 8 through NEON
// intrinsics on aarch64 (Kani unsupported_construct; TRIAGE popcount row)
// — the len >= 8 arm is excluded(blocked:simd); len <= 7 is the whole
// scalar arm on this host.

wave8_proof! {
    #[kani::unwind(12)]
    fn eq_bytea_bit_count() {
        let a = sym_varlena();
        kani::assume(a.len <= 7);
        let c = unsafe { pg8_bytea_bit_count(a.data(), a.clen()) };
        let r = call_scalar(varlena::builtins::fc_bytea_bit_count, [a.datum()], COLL_C);
        check_scalar(r.map(|d| d.as_i64()), c);
    }
}

// ================= textout (47) =================
// out-len == in-len + NUL (explicit-length cstring image); wrapper-level
// with a real FmgrInfo (the shipped OutBuf std-Vec scratch is in-theorem;
// std global allocator is Kani-native).

wave8_proof! {
    #[kani::unwind(12)]
    fn eq_textout() {
        let a = sym_varlena();
        let mut out = [0u8; CAP + 1];
        let c = unsafe { pg8_textout(a.data(), a.clen(), out.as_mut_ptr()) };
        let mut fi = FmgrInfo::unresolved();
        let r = call_flinfo(varlena::builtins::fc_textout, &mut fi, [a.datum()], COLL_C);
        check_cstring(r, c, &out);
        // OutBuf teardown (Box<dyn Any> + Vec drop) is allocator machinery
        // outside the claim — leak it.
        core::mem::forget(fi);
    }
}

// ================= send family (2413/2415) =================

wave8_proof! {
    #[kani::unwind(12)]
    fn eq_byteasend() {
        let a = sym_varlena();
        let ctx = MemoryContext::new_bump("proof");
        let mut out = [0u8; CAP];
        let c = unsafe { pg8_byteasend(a.data(), a.clen(), out.as_mut_ptr()) };
        let r = call(varlena::builtins::fc_byteasend, [a.datum()], COLL_C, ctx.mcx());
        core::mem::forget(ctx);
        check_bytes(r, c, &out);
    }
}

wave8_proof! {
    #[kani::unwind(12)]
    fn eq_textsend() {
        install_send_env();
        let a = sym_varlena();
        let ctx = MemoryContext::new_bump("proof");
        let mut out = [0u8; 64];
        let c = unsafe { pg8_textsend(a.data(), a.clen(), out.as_mut_ptr()) };
        let r = call(varlena::builtins::fc_textsend, [a.datum()], COLL_C, ctx.mcx());
        core::mem::forget(ctx);
        check_bytes(r, c, &out);
    }
}

// ================= byteaoverlay (749/752) — LADDER tier ===============
// Composed substr+substr+cat+cat both sides (C reuses the verbatim
// pg_bytea_substring/pg_bytea_catenate sections of c/pg_text_slice.c, shim
// W8); sp/sl fully symbolic i32 (overflow edge + sp<=0 error plane
// in-theorem). t1 rides as the raw image through detoast_attr_slice
// (fc_bytea_substr convention). Unwind 20 covers the crate-root substr
// bound (14) + the second catenate pass over <=16+8 bytes.

wave8_proof! {
    #[kani::unwind(20)]
    fn eq_byteaoverlay() {
        install_detoast();
        let (a, b) = (sym_varlena(), sym_varlena());
        let sp: i32 = kani::any();
        let sl: i32 = kani::any();
        let ctx = MemoryContext::new_bump("proof");
        let mut out = [0u8; 4 * CAP];
        let c = unsafe {
            pg8_bytea_overlay(a.data(), a.clen(), b.data(), b.clen(), sp, sl, out.as_mut_ptr())
        };
        let r = call(
            varlena::builtins::fc_byteaoverlay,
            [a.datum(), b.datum(), Datum::from_i32(sp), Datum::from_i32(sl)],
            COLL_C,
            ctx.mcx(),
        );
        core::mem::forget(ctx);
        check_bytes(r, c, &out);
    }
}

wave8_proof! {
    #[kani::unwind(20)]
    fn eq_byteaoverlay_no_len() {
        install_detoast();
        let (a, b) = (sym_varlena(), sym_varlena());
        let sp: i32 = kani::any();
        let ctx = MemoryContext::new_bump("proof");
        let mut out = [0u8; 4 * CAP];
        // byteaoverlay_no_len: sl = VARSIZE_ANY_EXHDR(t2), then the shared
        // bytea_overlay body.
        let c = unsafe {
            pg8_bytea_overlay(
                a.data(),
                a.clen(),
                b.data(),
                b.clen(),
                sp,
                b.clen(),
                out.as_mut_ptr(),
            )
        };
        let r = call(
            varlena::builtins::fc_byteaoverlay_no_len,
            [a.datum(), b.datum(), Datum::from_i32(sp)],
            COLL_C,
            ctx.mcx(),
        );
        core::mem::forget(ctx);
        check_bytes(r, c, &out);
    }
}

/// Overlay regime witnesses (proper interior splice + both error arms),
/// hoisted out of the equality harnesses. Run with the DEFAULT solver.
wave8_proof! {
    #[kani::unwind(20)]
    fn cover_byteaoverlay_regimes() {
        let (a, b) = (sym_varlena(), sym_varlena());
        let sp: i32 = kani::any();
        let sl: i32 = kani::any();
        let mut out = [0u8; 4 * CAP];
        let c = unsafe {
            pg8_bytea_overlay(a.data(), a.clen(), b.data(), b.clen(), sp, sl, out.as_mut_ptr())
        };
        let cerr = unsafe { pg8_take_err() };
        kani::cover!(sp > 1 && c > 0 && cerr == 0); // interior splice
        kani::cover!(cerr == 1); // sp <= 0 substring error
        kani::cover!(cerr == 5); // sp + sl overflow
    }
}

// ================= to_bin/to_oct/to_hex (6330-6333, 2089/2090) ========
// LADDER (wall-risk) symbolic image harnesses: convert_to_base's result
// length is computed by the digit loop = the DERIVED-LENGTH COPY class
// (TRIAGE 2026-07-28) — every store lands at a symbolic offset. Kept
// because the emission buffer is tiny (64B) and the divides are by powers
// of two; if these wall, the spot grids below are the coverage of record.
// Unwind = exact worst-case digit count + 2 (unwind slack is catastrophic
// near emission loops — TRIAGE intout lesson):
//   bin32: 32 digits -> 34; oct32: 11 -> 13; hex32: 8 -> 10;
//   bin64: 64 digits -> 66; oct64: 22 -> 24; hex64: 16 -> 18.

macro_rules! to_base_eq {
    ($name:ident, $unwind:literal, $cfn:ident, $fc:ident, $ty:ty, $from:ident) => {
        wave8_proof! {
            #[kani::unwind($unwind)]
            fn $name() {
                let v: $ty = kani::any();
                let ctx = MemoryContext::new_bump("proof");
                let mut out = [0u8; 64];
                let c = unsafe { $cfn(v, out.as_mut_ptr()) };
                let r = call(
                    varlena::builtins::$fc,
                    [Datum::$from(v)],
                    COLL_C,
                    ctx.mcx(),
                );
                core::mem::forget(ctx);
                check_bytes(r, c, &out);
            }
        }
    };
}

to_base_eq!(eq_to_bin32, 34, pg8_to_bin32, fc_to_bin32, i32, from_i32);
to_base_eq!(eq_to_oct32, 13, pg8_to_oct32, fc_to_oct32, i32, from_i32);
to_base_eq!(eq_to_hex32, 10, pg8_to_hex32, fc_to_hex32, i32, from_i32);
to_base_eq!(eq_to_bin64, 66, pg8_to_bin64, fc_to_bin64, i64, from_i64);
to_base_eq!(eq_to_oct64, 24, pg8_to_oct64, fc_to_oct64, i64, from_i64);
to_base_eq!(eq_to_hex64, 18, pg8_to_hex64, fc_to_hex64, i64, from_i64);

// Spot grids (fallback of record for the derived-length class): ONE
// SYMBOLIC INDEX into a concrete boundary table (geo-cmp spot-grid law —
// never a loop through the wrapper). Boundary set: zero, one, digit-count
// transitions, sign boundary (MIN), all-ones (-1), MAX, mixed patterns.

static T32: [i32; 10] = [0, 1, 7, 8, 15, 16, i32::MAX, i32::MIN, -1, 0x1ead_beef];
static T64: [i64; 10] = [
    0,
    1,
    255,
    256,
    i64::MAX,
    i64::MIN,
    -1,
    0x0123_4567_89ab_cdef,
    i32::MAX as i64 + 1,
    -0x1ead_beef,
];

macro_rules! to_base_spot {
    ($name:ident, $unwind:literal, $cfn:ident, $fc:ident, $tbl:ident, $ty:ty, $from:ident) => {
        wave8_proof! {
            #[kani::unwind($unwind)]
            fn $name() {
                let idx: usize = kani::any();
                kani::assume(idx < $tbl.len());
                let v: $ty = $tbl[idx];
                let ctx = MemoryContext::new_bump("proof");
                let mut out = [0u8; 64];
                let c = unsafe { $cfn(v, out.as_mut_ptr()) };
                let r = call(
                    varlena::builtins::$fc,
                    [Datum::$from(v)],
                    COLL_C,
                    ctx.mcx(),
                );
                core::mem::forget(ctx);
                check_bytes(r, c, &out);
            }
        }
    };
}

to_base_spot!(spot_to_bin32, 34, pg8_to_bin32, fc_to_bin32, T32, i32, from_i32);
to_base_spot!(spot_to_oct32, 13, pg8_to_oct32, fc_to_oct32, T32, i32, from_i32);
to_base_spot!(spot_to_hex32, 10, pg8_to_hex32, fc_to_hex32, T32, i32, from_i32);
to_base_spot!(spot_to_bin64, 66, pg8_to_bin64, fc_to_bin64, T64, i64, from_i64);
to_base_spot!(spot_to_oct64, 24, pg8_to_oct64, fc_to_oct64, T64, i64, from_i64);
to_base_spot!(spot_to_hex64, 18, pg8_to_hex64, fc_to_hex64, T64, i64, from_i64);

// ============ derived-length cstring rows: spot grids only ============
// textin (46), unknownin (109), unknownout (110), unknownsend (2417): the
// length is strlen/NUL-scan-derived INSIDE the code under proof — the
// exact pstrdup wall the pseudotypes lane measured (6.7-9.7 GiB at every
// cap/solver combo; input case-splits don't help). Symbolic harnesses are
// deliberately NOT written; grid = NUL-position boundary set incl. empty
// string, 1 byte, mid-NUL with trailing garbage, near-max, high bytes.

/// Concrete NUL-terminated grid entries (8 bytes each; every entry has a
/// NUL within the array — CStr reads stay in-bounds).
static CSTR_GRID: [[u8; 8]; 6] = [
    [0, 0, 0, 0, 0, 0, 0, 0],                      // ""
    [b'a', 0, 0, 0, 0, 0, 0, 0],                   // "a"
    [b'a', b'b', b'c', 0, b'z', b'z', 0, 0],       // NUL mid, bytes after
    [b'x', b'y', b'z', b'w', b'v', b'u', 0, 0],    // len 6
    [b'q', b'q', b'q', b'q', b'q', b'q', b'q', 0], // len 7 (array max)
    [0xff, 0x80, b' ', 0x7f, 0, 0xff, 0, 0],       // high bytes, NUL mid
];

fn grid_cstr() -> *const u8 {
    let idx: usize = kani::any();
    kani::assume(idx < CSTR_GRID.len());
    CSTR_GRID[idx].as_ptr()
}

wave8_proof! {
    #[kani::unwind(12)]
    fn spot_textin() {
        let s = grid_cstr();
        let mut out = [0u8; 8];
        let c = unsafe { pg8_textin(s, out.as_mut_ptr()) };
        let mut fi = FmgrInfo::unresolved();
        let r = call_flinfo(
            varlena::builtins::fc_textin,
            &mut fi,
            [Datum::from_usize(s as usize)],
            COLL_C,
        );
        check_bytes(r, c, &out);
        core::mem::forget(fi);
    }
}

wave8_proof! {
    #[kani::unwind(12)]
    fn spot_unknownin() {
        let s = grid_cstr();
        let mut out = [0u8; 9];
        let c = unsafe { pg8_unknownin(s, out.as_mut_ptr()) };
        let ctx = MemoryContext::new_bump("proof");
        let r = call(
            varlena::builtins::fc_unknownin,
            [Datum::from_usize(s as usize)],
            COLL_C,
            ctx.mcx(),
        );
        core::mem::forget(ctx);
        check_cstring(r, c, &out);
    }
}

wave8_proof! {
    #[kani::unwind(12)]
    fn spot_unknownout() {
        let s = grid_cstr();
        let mut out = [0u8; 9];
        let c = unsafe { pg8_unknownout(s, out.as_mut_ptr()) };
        let mut fi = FmgrInfo::unresolved();
        let r = call_flinfo(
            varlena::builtins::fc_unknownout,
            &mut fi,
            [Datum::from_usize(s as usize)],
            COLL_C,
        );
        check_cstring(r, c, &out);
        core::mem::forget(fi);
    }
}

wave8_proof! {
    #[kani::unwind(12)]
    fn spot_unknownsend() {
        install_send_env();
        let s = grid_cstr();
        let mut out = [0u8; 64];
        let c = unsafe { pg8_unknownsend(s, out.as_mut_ptr()) };
        let ctx = MemoryContext::new_bump("proof");
        let r = call(
            varlena::builtins::fc_unknownsend,
            [Datum::from_usize(s as usize)],
            COLL_C,
            ctx.mcx(),
        );
        core::mem::forget(ctx);
        check_bytes(r, c, &out);
    }
}

// ============ negative controls (MUST FAIL: rig non-vacuity) ============
// One per new-C-section shape; run with the DEFAULT solver (kissat never
// terminates on failing harnesses). assert! not assert_eq! (Debug
// machinery on the deliberately-failing path).

/// Scalar path: C sees a one-byte-shorter payload than the wrapper.
/// Expected: VERIFICATION FAILED with a decodable counterexample.
wave8_proof! {
    #[kani::unwind(12)]
    fn control_bytea_int4_short() {
        let a = sym_varlena();
        kani::assume(a.len >= 1);
        let c = unsafe { pg8_bytea_int4(a.data(), a.clen() - 1) };
        let r = call_scalar(varlena::builtins::fc_bytea_int4, [a.datum()], COLL_C);
        check_scalar(r.map(|d| d.as_i32() as i64), c as i64);
    }
}

/// Image path: C reverses a one-byte-shorter payload.
/// Expected: VERIFICATION FAILED with a decodable counterexample.
wave8_proof! {
    #[kani::unwind(12)]
    fn control_bytea_reverse_short() {
        let a = sym_varlena();
        kani::assume(a.len >= 1);
        let ctx = MemoryContext::new_bump("proof");
        let mut out = [0u8; CAP];
        let c = unsafe { pg8_bytea_reverse(a.data(), a.clen() - 1, out.as_mut_ptr()) };
        let r = call(varlena::builtins::fc_bytea_reverse, [a.datum()], COLL_C, ctx.mcx());
        core::mem::forget(ctx);
        check_bytes(r, c, &out);
    }
}
