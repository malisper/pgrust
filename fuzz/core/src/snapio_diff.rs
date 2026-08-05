//! snapio_diff: differential fuzz driver — shipped Rust `xid8funcs` vs vendored
//! PostgreSQL 18.3 (Stamp-18.3, upstream sha 62d6c7d3df) C
//! (csrc/pg_snapio_io.c). Crate under test: crates/backend/utils/adt/xid8funcs.
//!
//! Comparison planes (float_in_diff conventions): value bytes/bits,
//! error-verdict, and errcode/sqlstate class. Message text is out of scope.
//!
//! Input layout: [selector][payload]; selector % 7 picks the arm:
//!   0 pg_snapshot_in   (oid 5055) — payload = the input text (cstring).
//!   1 pg_snapshot_out  (oid 5056) — payload decodes a CONSTRUCTED snapshot
//!     image: [nxip u8][xmin 8][xmax 8][xip 8 × nxip] (le); out never
//!     validates, so fields are arbitrary u64s.
//!   2 pg_snapshot_recv (oid 5057) — payload = raw wire bytes.
//!   3 pg_snapshot_send (oid 5058) — payload = constructed image, arm-1 form.
//!   4 pg_snapshot_xmin (oid 5062) — payload = constructed image, arm-1 form.
//!   5 pg_snapshot_xmax (oid 5063) — payload = constructed image, arm-1 form.
//!   6 pg_visible_in_snapshot (oid 5065) — payload = [fxid 8] + constructed
//!     image; the xip list is SORTED by the driver before both sides (real
//!     snapshots are always sorted — parse/recv enforce it, current_snapshot
//!     sorts; on UNSORTED xips libc bsearch and the Rust binary search may
//!     legitimately probe differently, an unreachable-state false diff).
//!
//! FC-WRAPPER PLANE: each arm additionally routes its (already core-vs-C
//! checked) input through the crate's builtins.rs fc_* wrapper via a native
//! types_fmgr::LocalFcinfo frame and asserts wrapper == core (Datum value /
//! returned bytes / error verdict + sqlstate). Arm 0 also drives the
//! soft-error (ErrorSaveNode) shape: soft failure must report the same
//! sqlstate class and return the wrapper's `1:1:` placeholder image.
//!
//! SKIPPED rows (state carve, claims row of record):
//!   pg_current_xact_id / pg_current_xact_id_if_assigned / pg_current_snapshot
//!   (live-xact/snapmgr session state), pg_snapshot_xip (SRF protocol),
//!   pg_xact_status (clog/procarray state), pg_export_snapshot (snapmgr
//!   state). All are `excluded(state)`/SRF ledger rows — out of the lane's
//!   claimed scope, not coverable by a pure differential harness.

use std::ffi::{c_char, CString};

use datum::{Datum, NullableDatum};
use mcx::MemoryContext;
use stringinfo::StringInfo;
use types_error::{
    PgError, PgResult, ERRCODE_INVALID_BINARY_REPRESENTATION, ERRCODE_INVALID_TEXT_REPRESENTATION,
    ERRCODE_PROTOCOL_VIOLATION,
};
use types_fmgr::{ErrorSaveNode, LocalFcinfo, PGFunction};

extern "C" {
    // Shared TLS errcode accessor (defined in csrc/pg_float_io.c).
    fn pg_diff_errcode_get() -> i32;
    fn pg_diff_pg_snapshot_in(str_: *const c_char, out: *mut u8, cap: i32, outlen: *mut i32)
        -> i32;
    fn pg_diff_pg_snapshot_out(img: *const u8, out: *mut c_char, cap: i32, outlen: *mut i32)
        -> i32;
    fn pg_diff_pg_snapshot_recv(
        wire: *const u8,
        wirelen: i32,
        out: *mut u8,
        cap: i32,
        outlen: *mut i32,
    ) -> i32;
    fn pg_diff_pg_snapshot_send(img: *const u8, out: *mut u8, cap: i32, outlen: *mut i32) -> i32;
    fn pg_diff_pg_snapshot_xmin(img: *const u8) -> u64;
    fn pg_diff_pg_snapshot_xmax(img: *const u8) -> u64;
    fn pg_diff_pg_visible_in_snapshot(fxid: u64, img: *const u8) -> i32;
}

/// Oracle error classes (csrc/pg_snapio_io.c header).
const C_ERR_INVALID_TEXT: i32 = 1; /* 22P02 */
const C_ERR_INVALID_BINARY: i32 = 5; /* 22P03 */
const C_ERR_PROTOCOL: i32 = 6; /* 08P01 */

fn c_errcode() -> i32 {
    unsafe { pg_diff_errcode_get() }
}

fn rust_err_class(e: &PgError) -> i32 {
    if e.sqlstate() == ERRCODE_INVALID_TEXT_REPRESENTATION {
        C_ERR_INVALID_TEXT
    } else if e.sqlstate() == ERRCODE_INVALID_BINARY_REPRESENTATION {
        C_ERR_INVALID_BINARY
    } else if e.sqlstate() == ERRCODE_PROTOCOL_VIOLATION {
        C_ERR_PROTOCOL
    } else {
        99
    }
}

/// Caps keep a single exec cheap; 64 xips reaches both the linear
/// (nxip <= 30) and bsearch (nxip > 30) visibility arms.
const MAX_TEXT: usize = 1024;
const MAX_WIRE: usize = 4096;
const MAX_NXIP: usize = 64;
/// Image cap: recv can build at most (MAX_WIRE-20)/8 xips from a full wire;
/// text-in at most ~MAX_TEXT/2. One generous cap covers every arm.
const IMG_CAP: usize = 4 + 20 + 8 * MAX_WIRE;

// ---------------------------------------------------------------------------
// fc-wrapper plane plumbing (native LocalFcinfo, real mcx).
// ---------------------------------------------------------------------------

/// Invoke an fc_* wrapper over non-null args; returns (result, isnull flag).
fn fc_call<const N: usize>(
    f: PGFunction,
    m: mcx::Mcx<'_>,
    args: [Datum; N],
) -> (PgResult<Datum>, bool) {
    let mut fcinfo = LocalFcinfo::<N>::new(0);
    // SAFETY: the context owning `m` outlives this single call (caller scope).
    unsafe { fcinfo.set_result_mcx(m) };
    for (i, a) in args.into_iter().enumerate() {
        fcinfo.args[i] = NullableDatum::value(a);
    }
    let r = f(None, &mut fcinfo);
    (r, fcinfo.isnull)
}

/// fc_call with an armed ErrorSaveNode (soft-error shape).
fn fc_call_soft<const N: usize>(
    f: PGFunction,
    m: mcx::Mcx<'_>,
    args: [Datum; N],
) -> (PgResult<Datum>, ErrorSaveNode) {
    let mut esn = ErrorSaveNode::new(true);
    let mut fcinfo = LocalFcinfo::<N>::new(0);
    // SAFETY: the context owning `m` outlives this single call (caller scope).
    unsafe { fcinfo.set_result_mcx(m) };
    fcinfo.context = esn.fm_node_ptr();
    for (i, a) in args.into_iter().enumerate() {
        fcinfo.args[i] = NullableDatum::value(a);
    }
    let r = f(None, &mut fcinfo);
    fcinfo.context = None;
    (r, esn)
}

/// Full varlena image (4B header + payload) behind a by-ref result Datum.
fn datum_varlena_image<'a>(d: Datum) -> &'a [u8] {
    // SAFETY: d came from a wrapper returning a live 4B-uncompressed varlena.
    unsafe {
        let p = d.as_usize() as *const u8;
        let hdr = p.cast::<u32>().read_unaligned();
        core::slice::from_raw_parts(p, (hdr >> 2) as usize)
    }
}

fn datum_cstring<'a>(d: Datum) -> &'a [u8] {
    // SAFETY: d came from a wrapper returning a live NUL-terminated cstring.
    unsafe { core::ffi::CStr::from_ptr(d.as_usize() as *const c_char).to_bytes() }
}

/// A StringInfo image over `bytes` in `m` (None = alloc failure: skip plane).
fn make_si<'a>(m: mcx::Mcx<'a>, bytes: &[u8]) -> Option<StringInfo<'a>> {
    let mut vec = mcx::vec_with_capacity_in::<u8>(m, bytes.len()).ok()?;
    mcx::vec_append_bytes(&mut vec, bytes).ok()?;
    StringInfo::from_vec(vec).ok()
}

/// Decode the constructed-image payload form: [nxip u8][xmin 8][xmax 8]
/// [xip 8 × nxip] (le), returning the Rust-built varlena image. None =
/// payload too short (fuzzer will grow it).
fn decode_image<'m>(
    m: mcx::Mcx<'m>,
    payload: &[u8],
    sort_xips: bool,
) -> Option<datum::Varlena<'m>> {
    let (&nxip_b, rest) = payload.split_first()?;
    if rest.len() < 16 {
        return None;
    }
    let xmin = u64::from_le_bytes(rest[0..8].try_into().unwrap());
    let xmax = u64::from_le_bytes(rest[8..16].try_into().unwrap());
    let avail = (rest.len() - 16) / 8;
    let nxip = (nxip_b as usize).min(MAX_NXIP).min(avail);
    let mut xips: Vec<u64> = (0..nxip)
        .map(|i| u64::from_le_bytes(rest[16 + 8 * i..24 + 8 * i].try_into().unwrap()))
        .collect();
    if sort_xips {
        xips.sort_unstable();
    }
    xid8funcs::snapshot_image(m, xmin, xmax, &xips).ok()
}

// ---------------------------------------------------------------------------
// Dispatch
// ---------------------------------------------------------------------------

pub fn snapio_diff(data: &[u8]) {
    // one-thread-at-a-time through the C oracles (process-global statics) —
    // the fuzz TARGET's own frame stack needs the lock, same driver-entry
    // idiom as every other pub *_diff (task #144 addendum, trgm precedent).
    let _oracle = crate::oracle_serial();

    let Some((&sel, payload)) = data.split_first() else {
        return;
    };
    match sel % 7 {
        0 => pg_snapshot_in_diff(payload),
        1 => pg_snapshot_out_diff(payload),
        2 => pg_snapshot_recv_diff(payload),
        3 => pg_snapshot_send_diff(payload),
        4 => pg_snapshot_xminmax_diff(payload, false),
        5 => pg_snapshot_xminmax_diff(payload, true),
        _ => pg_visible_in_snapshot_diff(payload),
    }
}

// ---------------------------------------------------------------------------
// Arm: pg_snapshot_in (oid 5055).
// ---------------------------------------------------------------------------

fn pg_snapshot_in_diff(payload: &[u8]) {
    if payload.len() > MAX_TEXT || payload.contains(&0) {
        return;
    }
    let Ok(s) = std::str::from_utf8(payload) else {
        // fc_pg_snapshot_in goes through from_utf8_lossy; the core &str API
        // can't see non-UTF-8, so parity there is vacuous. Skip.
        return;
    };
    let cs = CString::new(payload).unwrap();
    let mut cimg = vec![0u8; IMG_CAP];
    let mut clen: i32 = 0;
    let cst = unsafe {
        pg_diff_pg_snapshot_in(cs.as_ptr(), cimg.as_mut_ptr(), IMG_CAP as i32, &mut clen)
    };
    assert!(
        cst != -1,
        "pg_snapshot_in C oracle capacity overflow (driver bug) input={s:?}"
    );
    let cerr = c_errcode();

    let ctx = MemoryContext::new("snapio_diff");
    let m = ctx.mcx();
    match xid8funcs::parse_snapshot(m, s, None) {
        Ok(Some(v)) => {
            assert!(
                cst == 0,
                "pg_snapshot_in DIVERGENCE input={s:?}: C err {cerr} vs Rust Ok"
            );
            assert!(
                v.as_bytes() == &cimg[..clen as usize],
                "pg_snapshot_in DIVERGENCE input={s:?}: image C={:02x?} Rust={:02x?}",
                &cimg[..clen as usize],
                v.as_bytes()
            );
            // fc-wrapper plane (hard-error shape).
            let (r, isnull) = fc_call::<1>(
                xid8funcs::builtins::fc_pg_snapshot_in,
                m,
                [Datum::from_usize(cs.as_ptr() as usize)],
            );
            let d = r.expect("fc_pg_snapshot_in: wrapper Err where core Ok");
            assert!(!isnull, "fc_pg_snapshot_in: null result on ok input {s:?}");
            assert!(
                datum_varlena_image(d) == v.as_bytes(),
                "fc_pg_snapshot_in wrapper!=core image input={s:?}"
            );
        }
        Ok(None) => unreachable!("parse_snapshot Ok(None) without escontext"),
        Err(e) => {
            let rerr = rust_err_class(&e);
            assert!(
                cst != 0 && cerr == rerr,
                "pg_snapshot_in DIVERGENCE input={s:?}: C=(st {cst}, err {cerr}) vs Rust err {rerr} ({})",
                e.message()
            );
            // fc-wrapper plane, hard shape: same sqlstate class.
            let (r, _) = fc_call::<1>(
                xid8funcs::builtins::fc_pg_snapshot_in,
                m,
                [Datum::from_usize(cs.as_ptr() as usize)],
            );
            let we = r.expect_err("fc_pg_snapshot_in: wrapper Ok where core Err");
            assert!(
                rust_err_class(&we) == rerr,
                "fc_pg_snapshot_in wrapper sqlstate!=core input={s:?}"
            );
            // Soft-error shape: error captured in the node, `1:1:` placeholder.
            let (r, esn) = fc_call_soft::<1>(
                xid8funcs::builtins::fc_pg_snapshot_in,
                m,
                [Datum::from_usize(cs.as_ptr() as usize)],
            );
            match r {
                Ok(d) => {
                    assert!(
                        esn.ctx.error_occurred(),
                        "fc_pg_snapshot_in soft: no error recorded input={s:?}"
                    );
                    let placeholder =
                        xid8funcs::snapshot_image(m, 1, 1, &[]).expect("placeholder image");
                    assert!(
                        datum_varlena_image(d) == placeholder.as_bytes(),
                        "fc_pg_snapshot_in soft: placeholder image mismatch input={s:?}"
                    );
                }
                Err(he) => panic!(
                    "fc_pg_snapshot_in soft: hard error {} despite escontext input={s:?}",
                    he.message()
                ),
            }
        }
    };
}

// ---------------------------------------------------------------------------
// Arm: pg_snapshot_out (oid 5056). Result-image wall route: exact text bytes.
// ---------------------------------------------------------------------------

fn pg_snapshot_out_diff(payload: &[u8]) {
    let ctx = MemoryContext::new("snapio_diff");
    let m = ctx.mcx();
    let Some(v) = decode_image(m, payload, false) else {
        return;
    };
    let mut cbuf = vec![0u8; 64 + 21 * (MAX_NXIP + 2)];
    let mut clen: i32 = 0;
    let cst = unsafe {
        pg_diff_pg_snapshot_out(
            v.as_bytes().as_ptr(),
            cbuf.as_mut_ptr() as *mut c_char,
            cbuf.len() as i32,
            &mut clen,
        )
    };
    assert!(cst == 0, "pg_snapshot_out C oracle st={cst} (out never errors)");

    let snap = xid8funcs::SnapView::new(v.data());
    let rout = xid8funcs::snapshot_out_bytes(m, &snap).expect("snapshot_out_bytes alloc");
    let cbytes: &[u8] = &cbuf[..clen as usize];
    assert!(
        &rout[..] == cbytes,
        "pg_snapshot_out DIVERGENCE image={:02x?}: C={:?} Rust={:?}",
        v.as_bytes(),
        std::str::from_utf8(cbytes),
        std::str::from_utf8(&rout)
    );

    // fc-wrapper plane: cstring result == core bytes.
    let (r, _) = fc_call::<1>(
        xid8funcs::builtins::fc_pg_snapshot_out,
        m,
        [Datum::from_usize(v.as_bytes().as_ptr() as usize)],
    );
    let d = r.expect("fc_pg_snapshot_out: wrapper Err on valid image");
    assert!(
        datum_cstring(d) == &rout[..],
        "fc_pg_snapshot_out wrapper!=core"
    );
}

// ---------------------------------------------------------------------------
// Arm: pg_snapshot_recv (oid 5057). Raw wire bytes, all planes.
// ---------------------------------------------------------------------------

fn pg_snapshot_recv_diff(payload: &[u8]) {
    if payload.len() > MAX_WIRE {
        return;
    }
    let mut cimg = vec![0u8; IMG_CAP];
    let mut clen: i32 = 0;
    let cst = unsafe {
        pg_diff_pg_snapshot_recv(
            payload.as_ptr(),
            payload.len() as i32,
            cimg.as_mut_ptr(),
            IMG_CAP as i32,
            &mut clen,
        )
    };
    assert!(
        cst != -1,
        "pg_snapshot_recv C oracle capacity overflow (driver bug)"
    );
    let cerr = c_errcode();

    let ctx = MemoryContext::new("snapio_diff");
    let m = ctx.mcx();
    let Some(mut si) = make_si(m, payload) else {
        return;
    };
    match xid8funcs::snapshot_recv(m, &mut si) {
        Ok(v) => {
            assert!(
                cst == 0,
                "pg_snapshot_recv DIVERGENCE wire={payload:02x?}: C err {cerr} vs Rust Ok"
            );
            assert!(
                v.as_bytes() == &cimg[..clen as usize],
                "pg_snapshot_recv DIVERGENCE wire={payload:02x?}: image C={:02x?} Rust={:02x?}",
                &cimg[..clen as usize],
                v.as_bytes()
            );
            // fc-wrapper plane: recv ABI takes the live StringInfo pointer.
            let Some(mut si2) = make_si(m, payload) else {
                return;
            };
            let (r, _) = fc_call::<1>(
                xid8funcs::builtins::fc_pg_snapshot_recv,
                m,
                [Datum::from_usize(&mut si2 as *mut StringInfo as usize)],
            );
            let d = r.expect("fc_pg_snapshot_recv: wrapper Err where core Ok");
            assert!(
                datum_varlena_image(d) == v.as_bytes(),
                "fc_pg_snapshot_recv wrapper!=core wire={payload:02x?}"
            );
        }
        Err(e) => {
            let rerr = rust_err_class(&e);
            assert!(
                cst != 0 && cerr == rerr,
                "pg_snapshot_recv DIVERGENCE wire={payload:02x?}: C=(st {cst}, err {cerr}) vs Rust err {rerr} ({})",
                e.message()
            );
        }
    };
}

// ---------------------------------------------------------------------------
// Arm: pg_snapshot_send (oid 5058). Constructed image -> exact wire bytes.
// ---------------------------------------------------------------------------

fn pg_snapshot_send_diff(payload: &[u8]) {
    let ctx = MemoryContext::new("snapio_diff");
    let m = ctx.mcx();
    let Some(v) = decode_image(m, payload, false) else {
        return;
    };
    let mut cwire = vec![0u8; 4 + 20 + 8 * MAX_NXIP];
    let mut clen: i32 = 0;
    let cst = unsafe {
        pg_diff_pg_snapshot_send(
            v.as_bytes().as_ptr(),
            cwire.as_mut_ptr(),
            cwire.len() as i32,
            &mut clen,
        )
    };
    assert!(cst == 0, "pg_snapshot_send C oracle st={cst} (send never errors)");

    // The Rust send logic lives in the fc wrapper itself (pq_send calls).
    let (r, _) = fc_call::<1>(
        xid8funcs::builtins::fc_pg_snapshot_send,
        m,
        [Datum::from_usize(v.as_bytes().as_ptr() as usize)],
    );
    let d = r.expect("fc_pg_snapshot_send: wrapper Err on valid image");
    let rimg = datum_varlena_image(d);
    // bytea result: compare wire payload after the 4B varlena header.
    assert!(
        &rimg[4..] == &cwire[..clen as usize],
        "pg_snapshot_send DIVERGENCE image={:02x?}: C={:02x?} Rust={:02x?}",
        v.as_bytes(),
        &cwire[..clen as usize],
        &rimg[4..]
    );
}

// ---------------------------------------------------------------------------
// Arms: pg_snapshot_xmin / pg_snapshot_xmax (oids 5062/5063).
// ---------------------------------------------------------------------------

fn pg_snapshot_xminmax_diff(payload: &[u8], want_xmax: bool) {
    let ctx = MemoryContext::new("snapio_diff");
    let m = ctx.mcx();
    let Some(v) = decode_image(m, payload, false) else {
        return;
    };
    let img = v.as_bytes().as_ptr();
    let cval = unsafe {
        if want_xmax {
            pg_diff_pg_snapshot_xmax(img)
        } else {
            pg_diff_pg_snapshot_xmin(img)
        }
    };
    let snap = xid8funcs::SnapView::new(v.data());
    let rval = if want_xmax { snap.xmax() } else { snap.xmin() };
    assert!(
        cval == rval,
        "pg_snapshot_{} DIVERGENCE image={:02x?}: C={cval} Rust={rval}",
        if want_xmax { "xmax" } else { "xmin" },
        v.as_bytes()
    );
    let f: PGFunction = if want_xmax {
        xid8funcs::builtins::fc_pg_snapshot_xmax
    } else {
        xid8funcs::builtins::fc_pg_snapshot_xmin
    };
    let (r, _) = fc_call::<1>(f, m, [Datum::from_usize(v.as_bytes().as_ptr() as usize)]);
    let d = r.expect("fc_pg_snapshot_xmin/xmax: wrapper Err on valid image");
    assert!(d.as_u64() == rval, "fc xmin/xmax wrapper!=core");
}

// ---------------------------------------------------------------------------
// Arm: pg_visible_in_snapshot (oid 5065). Sorted xips (see module header).
// ---------------------------------------------------------------------------

fn pg_visible_in_snapshot_diff(payload: &[u8]) {
    if payload.len() < 8 {
        return;
    }
    let fxid = u64::from_le_bytes(payload[..8].try_into().unwrap());
    let ctx = MemoryContext::new("snapio_diff");
    let m = ctx.mcx();
    let Some(v) = decode_image(m, &payload[8..], true) else {
        return;
    };
    let cval = unsafe { pg_diff_pg_visible_in_snapshot(fxid, v.as_bytes().as_ptr()) } != 0;
    let snap = xid8funcs::SnapView::new(v.data());
    let rval = xid8funcs::is_visible_fxid(fxid, &snap);
    assert!(
        cval == rval,
        "pg_visible_in_snapshot DIVERGENCE fxid={fxid} image={:02x?}: C={cval} Rust={rval}",
        v.as_bytes()
    );
    let (r, _) = fc_call::<2>(
        xid8funcs::builtins::fc_pg_visible_in_snapshot,
        m,
        [
            Datum::from_u64(fxid),
            Datum::from_usize(v.as_bytes().as_ptr() as usize),
        ],
    );
    let d = r.expect("fc_pg_visible_in_snapshot: wrapper Err on valid args");
    assert!(d.as_bool() == rval, "fc visible wrapper!=core");
}

// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    /// Replay every checked-in seed (catches shim/link errors before the
    /// nightly fuzz campaign).
    #[test]
    fn seed_corpus_replays_clean() {
        let _g = crate::c_oracle_serial();
        let dir = concat!(env!("CARGO_MANIFEST_DIR"), "/../corpus/snapio_diff");
        let mut n = 0;
        for e in std::fs::read_dir(dir).expect("corpus/snapio_diff missing") {
            let p = e.unwrap().path();
            if p.is_file() {
                snapio_diff(&std::fs::read(&p).unwrap());
                n += 1;
            }
        }
        assert!(n >= 30, "expected >=30 seeds, found {n}");
    }

    /// Fleet LSan artifact leak-99e971cb (campaign -1785479986-1afc-68969):
    /// "12:13:0" hits parse_snapshot's xip-out-of-range bad_format AFTER
    /// buf_init allocated — the C shim's palloc->malloc leaked 24 bytes on
    /// that exit until the allocation registry landed. Replay keeps the
    /// path exercised; the registry makes it leak-free by construction
    /// (LSan itself only runs on the Linux fleet build).
    #[test]
    fn lsan_leak_99e971cb_error_path_replays() {
        let _g = crate::c_oracle_serial();
        snapio_diff(b"\x0012:13:0");
        // Sibling error-after-alloc exits: in-loop order/range/format arms
        // and the recv longjmp-past-palloc path.
        snapio_diff(b"\x0012:16:14,13");
        snapio_diff(b"\x0012:16:14,,16");
        snapio_diff(b"\x0012:13:14");
        let mut recv_short = vec![2u8];
        recv_short.extend_from_slice(&2u32.to_be_bytes());
        recv_short.extend_from_slice(&10u64.to_be_bytes());
        recv_short.extend_from_slice(&20u64.to_be_bytes());
        recv_short.extend_from_slice(&11u64.to_be_bytes()[..4]); // truncated xip
        snapio_diff(&recv_short);
    }

    fn arm(sel: u8, payload: &[u8]) -> Vec<u8> {
        let mut v = vec![sel];
        v.extend_from_slice(payload);
        v
    }

    /// Constructed-image payload: nxip byte + le u64 fields.
    fn img_payload(xmin: u64, xmax: u64, xips: &[u64]) -> Vec<u8> {
        let mut v = vec![xips.len() as u8];
        v.extend_from_slice(&xmin.to_le_bytes());
        v.extend_from_slice(&xmax.to_le_bytes());
        for x in xips {
            v.extend_from_slice(&x.to_le_bytes());
        }
        v
    }

    #[test]
    fn arms_smoke() {
        let _g = crate::c_oracle_serial();
        // in: ok + all error shapes
        for s in [
            "10:20:",
            "10:20:10,14,15",
            "12:16:14,14",
            "8589934593:8589934595:8589934594",
            "18446744073709551615:18446744073709551615:",
            "",
            ":",
            "5:3:",
            "0:1:",
            "12:16:14,13",
            "12:16:14,,16",
            "12:13:0",
            "12:13:14",
            " 10:20:",
            "+10:20:",
            "-1:-1:",
            "10:20:19,",
        ] {
            snapio_diff(&arm(0, s.as_bytes()));
        }
        // out/send/xmin/xmax over the same constructed images
        for sel in [1u8, 3, 4, 5] {
            snapio_diff(&arm(sel, &img_payload(10, 20, &[11, 15, 19])));
            snapio_diff(&arm(sel, &img_payload(u64::MAX, 0, &[])));
            snapio_diff(&arm(
                sel,
                &img_payload(1, u64::MAX, &(0..40).collect::<Vec<u64>>()),
            ));
        }
        // recv: valid wire + short + bad nxip + xmax<xmin + empty
        let mut wire = Vec::new();
        wire.extend_from_slice(&2u32.to_be_bytes());
        wire.extend_from_slice(&10u64.to_be_bytes());
        wire.extend_from_slice(&20u64.to_be_bytes());
        wire.extend_from_slice(&11u64.to_be_bytes());
        wire.extend_from_slice(&15u64.to_be_bytes());
        snapio_diff(&arm(2, &wire));
        snapio_diff(&arm(2, &wire[..10]));
        snapio_diff(&arm(2, &[]));
        let mut bad = wire.clone();
        bad[0..4].copy_from_slice(&0xffff_ffffu32.to_be_bytes());
        snapio_diff(&arm(2, &bad));
        // visible: below xmin / above xmax / in xip / not in xip / bsearch arm
        for fx in [5u64, 25, 11, 12] {
            let mut p = fx.to_le_bytes().to_vec();
            p.extend_from_slice(&img_payload(10, 20, &[11, 15]));
            snapio_diff(&arm(6, &p));
        }
        let mut p = 40u64.to_le_bytes().to_vec();
        p.extend_from_slice(&img_payload(2, 100, &(3..40).collect::<Vec<u64>>()));
        snapio_diff(&arm(6, &p));
    }
}
