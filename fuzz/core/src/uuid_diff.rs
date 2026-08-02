//! uuid_diff: differential fuzz driver — shipped Rust adt_uuid vs vendored
//! PostgreSQL 18.3 C (csrc/pg_uuid_io.c, upstream sha 62d6c7d3df).
//!
//! Comparison planes (float_in_diff conventions): value bytes/bits,
//! error-verdict, and errcode class. Message text is out of scope.
//!
//! Input layout: [selector][payload]; selector % 8 picks the arm:
//!   0 uuid_in        payload = text bytes (interior-NUL-free; need not be
//!                    UTF-8 — the shipped API takes &[u8]); hard-error AND
//!                    soft-error (SoftErrorContext) shapes both compared.
//!   1 uuid_out       payload = 16 raw bytes; exact 36-byte image + NUL.
//!   2 cmp family     payload = 32 raw bytes; cmp signum + all six bool ops.
//!   3 hash           payload = 16 raw bytes (+8 seed bytes); uuid_hash,
//!                    uuid_hash_extended(0) and uuid_hash_extended(seed).
//!   4 extract        payload = 16 raw bytes; uuid_extract_version +
//!                    uuid_extract_timestamp (null-verdict + value).
//!   5 recv/send      payload = wire bytes; uuid_recv over a StringInfo vs
//!                    the C wire triple (verdict + 16 bytes + cursor), then
//!                    uuid_send image parity (20-byte varlena image).
//!   6 generate_uuidv7 payload = 8B ts_ms + 4B sub_ms; deterministic planes
//!                    only (bytes 0..=6 exact; byte 7 masked 0xFC on macOS
//!                    where C's SUBMS 10-bit arm xors in random bits — the C
//!                    oracle gets a zero rand8, shipped Rust real entropy;
//!                    byte 8 variant bits).
//!   7 abbrev         payload = [mode][8B seed]; UuidAbbrevState::convert
//!                    key vs the C uuid_abbrev_convert pure kernel per call,
//!                    plus uuid_abbrev_abort gate execution/invariants.
//!
//! FC-WRAPPER PLANE: each arm additionally routes its (already core-vs-C
//! checked) input through the crate's builtins.rs fc_* wrapper via a native
//! types_fmgr::LocalFcinfo frame and asserts wrapper ≡ core (Datum value /
//! returned bytes / error verdict + sqlstate). C-parity keeps being carried
//! by the core comparison; the plane makes the wrapper lines execute every
//! iteration with an in-harness oracle. Executed: fc_uuid_in (hard + soft
//! ErrorSaveNode shapes), fc_uuid_out, fc_uuid_lt/le/eq/ge/gt/ne/cmp,
//! fc_uuid_hash, fc_uuid_hash_extended, fc_uuid_extract_version,
//! fc_uuid_extract_timestamp, fc_uuid_recv, fc_uuid_send.
//!
//! SKIPPED (stateful, excluded(state)-class rows; no differential possible):
//!   - gen_random_uuid / uuidv7 / uuidv7_interval /
//!     get_real_time_ns_ascending — and their fc_gen_random_uuid / fc_uuidv7
//!     / fc_uuidv7_interval wrappers: PRNG (pg_strong_random) and wall-clock
//!     + backend-private monotonic TLS. The pure core behind uuidv7* —
//!     generate_uuidv7 — IS covered (arm 6) via the C oracle's rand8 seam.
//!     uuidv7_interval additionally needs adt_datetime::Interval, not a
//!     decoder_fuzz dependency.
//!   - abbrev abort's HyperLogLog estimate is state accumulated across
//!     calls; the abort gates are executed and checked against the
//!     deterministic expectations of fixed corpora (not a C differential —
//!     vendoring PG's hyperloglog.c is out of this target's scope). No fc
//!     wrapper exists for abbrev (uuid_sortsupport is unregistered).

use std::ffi::{c_char, CString};

use adt_uuid::abbrev::UuidAbbrevState;
use adt_uuid::{PgUuid, UUID_LEN, UUID_OUT_LEN};
use datum::{Datum, NullableDatum};
use mcx::MemoryContext;
use stringinfo::StringInfo;
use types_error::{PgResult, SoftErrorContext, ERRCODE_INVALID_TEXT_REPRESENTATION};
use types_fmgr::{ErrorSaveNode, LocalFcinfo, PGFunction};

extern "C" {
    fn pg_diff_uuid_in(source: *const c_char, out: *mut u8) -> i32;
    fn pg_diff_uuid_out(data: *const u8, buf: *mut c_char) -> i32;
    fn pg_diff_uuid_cmpop(op: i32, a: *const u8, b: *const u8) -> i32;
    fn pg_diff_uuid_hash(data: *const u8) -> u32;
    fn pg_diff_uuid_hash_extended(data: *const u8, seed: u64) -> u64;
    fn pg_diff_uuid_extract_version(data: *const u8, isnull: *mut i32) -> u16;
    fn pg_diff_uuid_extract_timestamp(data: *const u8, isnull: *mut i32) -> i64;
    fn pg_diff_uuid_recv(data: *const u8, len: i32, cursor: *mut i32, out: *mut u8) -> i32;
    fn pg_diff_uuid_send(data: *const u8, out: *mut u8) -> i32;
    fn pg_diff_uuid_generate_v7(
        unix_ts_ms: u64,
        sub_ms: u32,
        rand8: *const u8,
        out: *mut u8,
    ) -> i32;
    fn pg_diff_uuid_abbrev_key(data: *const u8) -> u64;
    // Shared TLS errcode accessor (defined in csrc/pg_float_io.c).
    fn pg_diff_errcode_get() -> i32;
}

/// Oracle errcode class for 22P02 (see csrc/pg_uuid_io.c section 4).
const C_ERR_INVALID_TEXT: i32 = 1;

// ---------------------------------------------------------------------------
// fc-wrapper plane plumbing (native LocalFcinfo, real mcx — the proofs
// wrapper-level pattern run without kani).
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

/// First `n` bytes behind a by-ref result Datum. Caller contract: `d` came
/// from a wrapper that returned an `n`-byte-or-longer allocation still live
/// in the arming context (or thread-local out scratch).
fn datum_bytes<'a>(d: Datum, n: usize) -> &'a [u8] {
    // SAFETY: caller contract above.
    unsafe { core::slice::from_raw_parts(d.as_usize() as *const u8, n) }
}

/// A StringInfo image over `bytes` in `m` (None = alloc failure: skip plane).
fn make_si<'a>(m: mcx::Mcx<'a>, bytes: &[u8]) -> Option<StringInfo<'a>> {
    let mut vec = mcx::vec_with_capacity_in::<u8>(m, bytes.len()).ok()?;
    mcx::vec_append_bytes(&mut vec, bytes).ok()?;
    StringInfo::from_vec(vec).ok()
}

fn take16(payload: &[u8]) -> Option<PgUuid> {
    let head = payload.get(..UUID_LEN)?;
    let mut u = [0u8; UUID_LEN];
    u.copy_from_slice(head);
    Some(u)
}

pub fn uuid_diff(data: &[u8]) {
    let _oracle = crate::oracle_serial(); // one-thread-at-a-time through the C oracles (process-global statics)
    let Some((&sel, payload)) = data.split_first() else {
        return;
    };
    match sel % 8 {
        0 => in_diff(payload),
        1 => out_diff(payload),
        2 => cmp_diff(payload),
        3 => hash_diff(payload),
        4 => extract_diff(payload),
        5 => recv_send_diff(payload),
        6 => v7_diff(payload),
        _ => abbrev_diff(payload),
    }
}

// ---------------------------------------------------------------------------
// Arm 0: uuid_in (parse: canonical / braces / hyphen-free / error arms).
// ---------------------------------------------------------------------------

fn in_diff(payload: &[u8]) {
    if payload.len() > 1024 || payload.contains(&0) {
        return;
    }
    let cs = CString::new(payload).unwrap();
    let mut cval = [0u8; UUID_LEN];
    let cst = unsafe { pg_diff_uuid_in(cs.as_ptr(), cval.as_mut_ptr()) };
    let cerr = unsafe { pg_diff_errcode_get() };

    // Hard-error shape (escontext = None), the fc_uuid_in default.
    match adt_uuid::uuid_in(payload, None) {
        Ok(r) => {
            assert!(
                cst == 0 && r == cval,
                "uuid_in DIVERGENCE input={:?}: C=(st {cst}, {cval:02x?}) Rust=Ok({r:02x?})",
                String::from_utf8_lossy(payload)
            );
        }
        Err(e) => {
            assert!(
                cst == 1
                    && cerr == C_ERR_INVALID_TEXT
                    && e.sqlstate == ERRCODE_INVALID_TEXT_REPRESENTATION,
                "uuid_in DIVERGENCE input={:?}: C=(st {cst}, err {cerr}) Rust=Err(sqlstate {:?})",
                String::from_utf8_lossy(payload),
                e.sqlstate
            );
        }
    }

    // Soft-error shape (ereturn saves into the context and returns Ok).
    let mut sc = SoftErrorContext::new(true);
    let soft = adt_uuid::uuid_in(payload, Some(&mut sc));
    let soft = soft.expect("uuid_in with escontext must not hard-error");
    assert_eq!(
        sc.error_occurred(),
        cst != 0,
        "uuid_in soft-error verdict DIVERGENCE input={:?}",
        String::from_utf8_lossy(payload)
    );
    if cst == 0 {
        assert_eq!(soft, cval, "uuid_in soft-path value DIVERGENCE");
    } else {
        let saved = sc.error().expect("details_wanted context must save the error");
        assert_eq!(saved.sqlstate, ERRCODE_INVALID_TEXT_REPRESENTATION);
    }

    // fc-wrapper plane: fc_uuid_in, hard shape (no escontext) ...
    let cx = MemoryContext::new("uuid_fc");
    let m = cx.mcx();
    let din = Datum::from_usize(cs.as_ptr() as usize);
    match fc_call::<1>(adt_uuid::builtins::fc_uuid_in, m, [din]).0 {
        Ok(d) => assert!(
            cst == 0 && datum_bytes(d, UUID_LEN) == cval,
            "fc_uuid_in vs core DIVERGENCE input={:?}",
            String::from_utf8_lossy(payload)
        ),
        Err(e) => assert!(
            cst == 1 && e.sqlstate == ERRCODE_INVALID_TEXT_REPRESENTATION,
            "fc_uuid_in error-shape DIVERGENCE input={:?} sqlstate={:?}",
            String::from_utf8_lossy(payload),
            e.sqlstate
        ),
    }
    // ... then the soft shape through a real ErrorSaveNode on the frame.
    let mut node = ErrorSaveNode::new(true);
    let mut fcinfo = LocalFcinfo::<1>::new(0);
    // SAFETY: `cx` outlives this single call.
    unsafe { fcinfo.set_result_mcx(m) };
    fcinfo.context = node.fm_node_ptr();
    fcinfo.args[0] = NullableDatum::value(din);
    let d = adt_uuid::builtins::fc_uuid_in(None, &mut fcinfo)
        .expect("fc_uuid_in with escontext must not hard-error");
    assert_eq!(
        node.ctx.error_occurred(),
        cst != 0,
        "fc_uuid_in soft verdict DIVERGENCE input={:?}",
        String::from_utf8_lossy(payload)
    );
    if cst == 0 {
        assert_eq!(datum_bytes(d, UUID_LEN), cval, "fc_uuid_in soft value DIVERGENCE");
    } else {
        let saved = node.ctx.error().expect("fc soft context must save the error");
        assert_eq!(saved.sqlstate, ERRCODE_INVALID_TEXT_REPRESENTATION);
    }
}

// ---------------------------------------------------------------------------
// Arm 1: uuid_out — exact 36-byte image.
// ---------------------------------------------------------------------------

fn out_diff(payload: &[u8]) {
    let Some(u) = take16(payload) else { return };
    let mut cbuf = [0u8; UUID_OUT_LEN + 4];
    unsafe { pg_diff_uuid_out(u.as_ptr(), cbuf.as_mut_ptr() as *mut c_char) };
    let mut rbuf = [0u8; UUID_OUT_LEN];
    let rlen = adt_uuid::uuid_out_into(&u, &mut rbuf);
    assert!(
        rlen == UUID_OUT_LEN && rbuf == cbuf[..UUID_OUT_LEN] && cbuf[UUID_OUT_LEN] == 0,
        "uuid_out DIVERGENCE uuid={u:02x?}: C={:?} Rust={:?}",
        String::from_utf8_lossy(&cbuf[..UUID_OUT_LEN]),
        String::from_utf8_lossy(&rbuf[..rlen])
    );

    // fc-wrapper plane: fc_uuid_out (thread-local cstring scratch Datum).
    let cx = MemoryContext::new("uuid_fc");
    let d = fc_call::<1>(
        adt_uuid::builtins::fc_uuid_out,
        cx.mcx(),
        [Datum::from_usize(u.as_ptr() as usize)],
    )
    .0
    .expect("fc_uuid_out cannot fail");
    let img = datum_bytes(d, UUID_OUT_LEN + 1);
    assert!(
        img[..UUID_OUT_LEN] == rbuf && img[UUID_OUT_LEN] == 0,
        "fc_uuid_out vs core DIVERGENCE uuid={u:02x?}: fc={:?}",
        String::from_utf8_lossy(&img[..UUID_OUT_LEN])
    );
}

// ---------------------------------------------------------------------------
// Arm 2: uuid_cmp + lt/le/eq/ge/gt/ne (signum on cmp, exact on bools).
// ---------------------------------------------------------------------------

fn cmp_diff(payload: &[u8]) {
    let Some(a) = take16(payload) else { return };
    let Some(b) = take16(&payload[UUID_LEN..]) else { return };
    let c = |op: i32| unsafe { pg_diff_uuid_cmpop(op, a.as_ptr(), b.as_ptr()) };
    // memcmp magnitude is implementation-defined; compare signum (btree
    // comparator contract, same as the proofs harnesses).
    let (ccmp, rcmp) = (c(0).signum(), adt_uuid::uuid_internal_cmp(&a, &b).signum());
    assert_eq!(ccmp, rcmp, "uuid_cmp DIVERGENCE a={a:02x?} b={b:02x?}");
    let rust: [bool; 6] = [
        adt_uuid::uuid_lt(&a, &b),
        adt_uuid::uuid_le(&a, &b),
        adt_uuid::uuid_eq(&a, &b),
        adt_uuid::uuid_ge(&a, &b),
        adt_uuid::uuid_gt(&a, &b),
        adt_uuid::uuid_ne(&a, &b),
    ];
    for (i, r) in rust.iter().enumerate() {
        let cop = c(i as i32 + 1) != 0;
        assert_eq!(cop, *r, "uuid bool-op {i} DIVERGENCE a={a:02x?} b={b:02x?}");
    }

    // fc-wrapper plane: the six bool wrappers + fc_uuid_cmp (exact value —
    // wrapper and core share the same memcmp result on this build).
    let cx = MemoryContext::new("uuid_fc");
    let m = cx.mcx();
    let (da, db) = (
        Datum::from_usize(a.as_ptr() as usize),
        Datum::from_usize(b.as_ptr() as usize),
    );
    use adt_uuid::builtins as fcb;
    let fc_ops: [(&str, PGFunction); 6] = [
        ("lt", fcb::fc_uuid_lt),
        ("le", fcb::fc_uuid_le),
        ("eq", fcb::fc_uuid_eq),
        ("ge", fcb::fc_uuid_ge),
        ("gt", fcb::fc_uuid_gt),
        ("ne", fcb::fc_uuid_ne),
    ];
    for ((name, f), expect) in fc_ops.into_iter().zip(rust) {
        let d = fc_call::<2>(f, m, [da, db]).0.expect("bool wrapper cannot fail");
        assert_eq!(
            d.as_bool(),
            expect,
            "fc_uuid_{name} vs core DIVERGENCE a={a:02x?} b={b:02x?}"
        );
    }
    let d = fc_call::<2>(fcb::fc_uuid_cmp, m, [da, db]).0.expect("cmp wrapper cannot fail");
    assert_eq!(
        d.as_i32(),
        adt_uuid::uuid_internal_cmp(&a, &b),
        "fc_uuid_cmp vs core DIVERGENCE a={a:02x?} b={b:02x?}"
    );
}

// ---------------------------------------------------------------------------
// Arm 3: uuid_hash / uuid_hash_extended (seed 0 and fuzzed seed).
// ---------------------------------------------------------------------------

fn hash_diff(payload: &[u8]) {
    let Some(u) = take16(payload) else { return };
    let ch = unsafe { pg_diff_uuid_hash(u.as_ptr()) };
    assert_eq!(ch, adt_uuid::uuid_hash(&u), "uuid_hash DIVERGENCE uuid={u:02x?}");

    // fc-wrapper plane: fc_uuid_hash / fc_uuid_hash_extended per seed.
    let cx = MemoryContext::new("uuid_fc");
    let m = cx.mcx();
    let du = Datum::from_usize(u.as_ptr() as usize);
    let d = fc_call::<1>(adt_uuid::builtins::fc_uuid_hash, m, [du])
        .0
        .expect("fc_uuid_hash cannot fail");
    assert_eq!(d.as_u32(), ch, "fc_uuid_hash vs core DIVERGENCE uuid={u:02x?}");

    let mut seeds = vec![0u64];
    if let Some(sb) = payload.get(UUID_LEN..UUID_LEN + 8) {
        seeds.push(u64::from_le_bytes(sb.try_into().unwrap()));
    }
    for seed in seeds {
        let ce = unsafe { pg_diff_uuid_hash_extended(u.as_ptr(), seed) };
        assert_eq!(
            ce,
            adt_uuid::uuid_hash_extended(&u, seed),
            "uuid_hash_extended DIVERGENCE uuid={u:02x?} seed={seed:#x}"
        );
        let d = fc_call::<2>(
            adt_uuid::builtins::fc_uuid_hash_extended,
            m,
            [du, Datum::from_u64(seed)],
        )
        .0
        .expect("fc_uuid_hash_extended cannot fail");
        assert_eq!(
            d.as_u64(),
            ce,
            "fc_uuid_hash_extended vs core DIVERGENCE uuid={u:02x?} seed={seed:#x}"
        );
    }
}

// ---------------------------------------------------------------------------
// Arm 4: uuid_extract_version / uuid_extract_timestamp.
// ---------------------------------------------------------------------------

fn extract_diff(payload: &[u8]) {
    let Some(u) = take16(payload) else { return };
    let mut isnull = 0i32;
    let cv = unsafe { pg_diff_uuid_extract_version(u.as_ptr(), &mut isnull) };
    let rv = adt_uuid::uuid_extract_version(&u);
    match rv {
        Some(v) => assert!(
            isnull == 0 && v == cv,
            "uuid_extract_version DIVERGENCE uuid={u:02x?}: C=(null {isnull}, {cv}) Rust={v}"
        ),
        None => assert!(
            isnull == 1,
            "uuid_extract_version DIVERGENCE uuid={u:02x?}: C non-null {cv}, Rust NULL"
        ),
    }
    let mut isnull = 0i32;
    let ct = unsafe { pg_diff_uuid_extract_timestamp(u.as_ptr(), &mut isnull) };
    let rt = adt_uuid::uuid_extract_timestamp(&u);
    match rt {
        Some(t) => assert!(
            isnull == 0 && t == ct,
            "uuid_extract_timestamp DIVERGENCE uuid={u:02x?}: C=(null {isnull}, {ct}) Rust={t}"
        ),
        None => assert!(
            isnull == 1,
            "uuid_extract_timestamp DIVERGENCE uuid={u:02x?}: C non-null {ct}, Rust NULL"
        ),
    }

    // fc-wrapper plane: fc_uuid_extract_version / _timestamp (null verdicts
    // travel through fcinfo.isnull; the value through the Datum).
    let cx = MemoryContext::new("uuid_fc");
    let m = cx.mcx();
    let du = Datum::from_usize(u.as_ptr() as usize);
    let (r, fcnull) = fc_call::<1>(adt_uuid::builtins::fc_uuid_extract_version, m, [du]);
    let d = r.expect("fc_uuid_extract_version cannot fail");
    match rv {
        Some(v) => assert!(
            !fcnull && d.as_u16() == v,
            "fc_uuid_extract_version vs core DIVERGENCE uuid={u:02x?}"
        ),
        None => assert!(fcnull, "fc_uuid_extract_version null-verdict DIVERGENCE uuid={u:02x?}"),
    }
    let (r, fcnull) = fc_call::<1>(adt_uuid::builtins::fc_uuid_extract_timestamp, m, [du]);
    let d = r.expect("fc_uuid_extract_timestamp cannot fail");
    match rt {
        Some(t) => assert!(
            !fcnull && d.as_i64() == t,
            "fc_uuid_extract_timestamp vs core DIVERGENCE uuid={u:02x?}"
        ),
        None => assert!(fcnull, "fc_uuid_extract_timestamp null-verdict DIVERGENCE uuid={u:02x?}"),
    }
}

// ---------------------------------------------------------------------------
// Arm 5: uuid_recv (wire) + uuid_send (varlena image).
// ---------------------------------------------------------------------------

fn recv_send_diff(payload: &[u8]) {
    if payload.len() > 4096 {
        return;
    }
    // C side: wire triple.
    let mut cursor = 0i32;
    let mut cval = [0u8; UUID_LEN];
    let cst = unsafe {
        pg_diff_uuid_recv(
            payload.as_ptr(),
            payload.len() as i32,
            &mut cursor,
            cval.as_mut_ptr(),
        )
    };

    // Rust side: uuid_recv over a StringInfo built on the same bytes.
    let cx = MemoryContext::new("uuid_fuzz");
    let mcx = cx.mcx();
    let mut vec = match mcx::vec_with_capacity_in::<u8>(mcx, payload.len()) {
        Ok(v) => v,
        Err(_) => return,
    };
    if mcx::vec_append_bytes(&mut vec, payload).is_err() {
        return;
    }
    let mut msg = match StringInfo::from_vec(vec) {
        Ok(m) => m,
        Err(_) => return,
    };
    match adt_uuid::uuid_recv(&mut msg) {
        Ok(r) => {
            assert!(
                cst == 0 && r == cval && cursor == UUID_LEN as i32,
                "uuid_recv DIVERGENCE len={}: C=(st {cst}, cur {cursor}, {cval:02x?}) Rust=Ok({r:02x?})",
                payload.len()
            );
            // Round-trip through uuid_send: exact 20-byte varlena image.
            let mut cimg = [0u8; 20];
            let clen = unsafe { pg_diff_uuid_send(r.as_ptr(), cimg.as_mut_ptr()) };
            let rimg = adt_uuid::uuid_send(mcx, &r).expect("uuid_send cannot fail on 16 bytes");
            assert!(
                clen == 20 && rimg.as_bytes() == cimg,
                "uuid_send DIVERGENCE uuid={r:02x?}: C={cimg:02x?} Rust={:02x?}",
                rimg.as_bytes()
            );

            // fc-wrapper plane: fc_uuid_recv over a fresh StringInfo image,
            // then fc_uuid_send on the decoded value.
            if let Some(mut msg2) = make_si(mcx, payload) {
                let d = fc_call::<1>(
                    adt_uuid::builtins::fc_uuid_recv,
                    mcx,
                    [Datum::from_usize(core::ptr::from_mut(&mut msg2) as usize)],
                )
                .0
                .expect("fc_uuid_recv must succeed where core did");
                assert_eq!(datum_bytes(d, UUID_LEN), r, "fc_uuid_recv vs core DIVERGENCE");
                let ds = fc_call::<1>(adt_uuid::builtins::fc_uuid_send, mcx, [d])
                    .0
                    .expect("fc_uuid_send cannot fail");
                assert_eq!(
                    datum_bytes(ds, 20),
                    rimg.as_bytes(),
                    "fc_uuid_send vs core DIVERGENCE uuid={r:02x?}"
                );
            }
        }
        Err(_) => {
            // Insufficient data: C status 4. (Errcode class: both sides are
            // the 08P01 protocol-violation arm; the C shim has no finer
            // granularity — verdict parity only, matching the wave-5 shim.)
            assert!(
                cst == 4,
                "uuid_recv DIVERGENCE len={}: C st {cst} but Rust errored",
                payload.len()
            );

            // fc-wrapper plane: the wrapper must error exactly where core did.
            if let Some(mut msg2) = make_si(mcx, payload) {
                let rr = fc_call::<1>(
                    adt_uuid::builtins::fc_uuid_recv,
                    mcx,
                    [Datum::from_usize(core::ptr::from_mut(&mut msg2) as usize)],
                )
                .0;
                assert!(
                    rr.is_err(),
                    "fc_uuid_recv verdict vs core DIVERGENCE len={}",
                    payload.len()
                );
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Arm 6: generate_uuidv7 — deterministic planes through the C rand8 seam.
// ---------------------------------------------------------------------------

fn v7_diff(payload: &[u8]) {
    let Some(ts_b) = payload.get(..8) else { return };
    let Some(sub_b) = payload.get(8..12) else { return };
    let ts = u64::from_le_bytes(ts_b.try_into().unwrap());
    let sub_ms = u32::from_le_bytes(sub_b.try_into().unwrap());

    let rand8 = [0u8; 8];
    let mut cval = [0u8; UUID_LEN];
    let cst = unsafe { pg_diff_uuid_generate_v7(ts, sub_ms, rand8.as_ptr(), cval.as_mut_ptr()) };
    assert_eq!(cst, 0);
    let r = adt_uuid::generate_uuidv7(ts, sub_ms).expect("entropy source unavailable");

    // Bytes 0..=5: unix_ts_ms big-endian. Byte 6: version nibble (7, forced
    // by both sides) | precision high nibble (deterministic).
    assert_eq!(
        &r[..7],
        &cval[..7],
        "generate_uuidv7 time/version DIVERGENCE ts={ts} sub_ms={sub_ms}"
    );
    // Byte 7: precision low byte. On macOS (SUBMS_MINIMAL_STEP_BITS == 10,
    // both sides) its low 2 bits are xored with data[8] >> 6 — random on the
    // Rust side, zero in the C oracle — so mask them there.
    #[cfg(target_os = "macos")]
    let m = 0xfcu8;
    #[cfg(not(target_os = "macos"))]
    let m = 0xffu8;
    assert_eq!(
        r[7] & m,
        cval[7] & m,
        "generate_uuidv7 sub-ms DIVERGENCE ts={ts} sub_ms={sub_ms}: C={cval:02x?} Rust={r:02x?}"
    );
    // Byte 8: RFC 9562 variant bits (10xx xxxx) forced by uuid_set_version.
    assert_eq!(r[8] & 0xc0, 0x80);
    assert_eq!(cval[8] & 0xc0, 0x80);
    // And the result must classify as version 7 through the shipped readers.
    assert_eq!(adt_uuid::uuid_extract_version(&r), Some(7));
}

// ---------------------------------------------------------------------------
// Arm 7: abbrev sortsupport kernels (convert key vs C; abort gate machine).
// ---------------------------------------------------------------------------

fn splitmix64(x: &mut u64) -> u64 {
    *x = x.wrapping_add(0x9e3779b97f4a7c15);
    let mut z = *x;
    z = (z ^ (z >> 30)).wrapping_mul(0xbf58476d1ce4e5b9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94d049bb133111eb);
    z ^ (z >> 31)
}

fn check_convert(st: &mut UuidAbbrevState, u: &PgUuid) -> u64 {
    let rk = st.convert(u);
    let ck = unsafe { pg_diff_uuid_abbrev_key(u.as_ptr()) };
    assert_eq!(rk, ck, "uuid_abbrev_convert key DIVERGENCE uuid={u:02x?}");
    rk
}

fn abbrev_diff(payload: &[u8]) {
    let Some((&mode, rest)) = payload.split_first() else {
        return;
    };
    let mut seed = match rest.get(..8) {
        Some(b) => u64::from_le_bytes(b.try_into().unwrap()),
        None => 0,
    };

    if mode == 0xa5 {
        // Rare heavy mode: >100k distinct abbrevs drives the abort machine
        // through the "stop estimating" arm (card > 100000.0 =>
        // estimating = false), then the !estimating early return.
        let mut st = UuidAbbrevState::new();
        for _ in 0..110_000 {
            let x = splitmix64(&mut seed);
            let mut u = [0u8; UUID_LEN];
            u[..8].copy_from_slice(&x.to_be_bytes());
            st.convert(&u);
        }
        // 110k distinct: cardinality estimate far above the 100k ceiling.
        assert!(!st.abort(110_000), "abort must commit (not abort) at >100k distinct");
        assert!(!st.abort(110_000), "!estimating early-return arm must be false");
        return;
    }

    match mode % 3 {
        0 => {
            // Key-parity sweep over pseudo-random uuids + below-threshold
            // abort gates (memtupcount / input_count < 10000).
            let mut st = UuidAbbrevState::default();
            let mut prev: Option<(u64, PgUuid)> = None;
            for _ in 0..256 {
                let x = splitmix64(&mut seed);
                let y = splitmix64(&mut seed);
                let mut u = [0u8; UUID_LEN];
                u[..8].copy_from_slice(&x.to_be_bytes());
                u[8..].copy_from_slice(&y.to_le_bytes());
                let k = check_convert(&mut st, &u);
                // Abbrev contract: unsigned key order agrees with the
                // authoritative comparator whenever keys differ.
                if let Some((pk, pu)) = prev {
                    if pk != k {
                        let ord = if pk < k { -1 } else { 1 };
                        assert_eq!(
                            ord,
                            adt_uuid::uuid_internal_cmp(&pu, &u).signum(),
                            "abbrev key order DIVERGENCE {pu:02x?} vs {u:02x?}"
                        );
                    }
                }
                prev = Some((k, u));
            }
            assert!(!st.abort(256), "abort below memtupcount threshold must be false");
            assert!(!st.abort(20_000), "abort below input_count threshold must be false");
        }
        1 => {
            // Pathological single-value run: cardinality 1 => abort fires.
            let x = splitmix64(&mut seed);
            let mut u = [0u8; UUID_LEN];
            u[..8].copy_from_slice(&x.to_be_bytes());
            let mut st = UuidAbbrevState::new();
            for _ in 0..10_000 {
                check_convert(&mut st, &u);
            }
            assert!(!st.abort(9_999), "memtupcount gate");
            assert!(st.abort(10_000), "cardinality-1 run must abort abbreviation");
        }
        _ => {
            // Small mixed run: convert parity only (fast arm for mutation).
            let mut st = UuidAbbrevState::new();
            for chunk in rest.chunks(UUID_LEN).take(64) {
                let mut u = [0u8; UUID_LEN];
                u[..chunk.len()].copy_from_slice(chunk);
                check_convert(&mut st, &u);
            }
            assert!(!st.abort(0));
        }
    }
}

// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    /// Replay every checked-in seed (catches shim/link errors before the
    /// nightly fuzz campaign).
    #[test]
    fn seed_corpus_replays_clean() {
        let _serial = crate::c_oracle_serial();
        let dir = concat!(env!("CARGO_MANIFEST_DIR"), "/../corpus/uuid_diff");
        let mut n = 0;
        for e in std::fs::read_dir(dir).expect("corpus/uuid_diff missing") {
            let p = e.unwrap().path();
            if p.is_file() {
                uuid_diff(&std::fs::read(&p).unwrap());
                n += 1;
            }
        }
        assert!(n >= 30, "expected >=30 seeds, found {n}");
    }

    #[test]
    fn in_out_roundtrip_arms() {
        let _serial = crate::c_oracle_serial();
        // Canonical, braced, hyphen-free, upper-case, mixed-hyphen forms.
        for s in [
            "11111111-1111-1111-1111-111111111111",
            "{22222222-2222-2222-2222-222222222222}",
            "3f3e3c3b3a3039383736353433a2313e",
            "C232AB00-9414-11EC-B3C8-9F6BDECED846",
            "0123-4567-89ab-cdef-0123-4567-89ab-cdef",
            "{a0eebc99-9c0b4ef8-bb6d6bb9-bd380a11}",
        ] {
            let mut d = vec![0u8];
            d.extend_from_slice(s.as_bytes());
            uuid_diff(&d);
            let u = adt_uuid::uuid_in(s.as_bytes(), None).unwrap();
            let mut d = vec![1u8];
            d.extend_from_slice(&u);
            uuid_diff(&d);
        }
        // Error arms: truncation, bad hex, stray hyphen, unclosed/garbage
        // braces, trailing junk, empty, non-UTF8 bytes.
        for s in [
            &b"11111111-1111-1111-1111-11111111111"[..],
            b"11111111-1111-1111-G111-111111111111",
            b"111-11111-1111-1111-1111-111111111111",
            b"{11111111-1111-1111-1111-11111111111}",
            b"{22222222-2222-2222-2222-222222222222 ",
            b"11111111-1111-1111-1111-111111111111F",
            b"",
            b"-",
            b"\xff\xfe1111111-1111-1111-1111-111111111111",
        ] {
            let mut d = vec![0u8];
            d.extend_from_slice(s);
            uuid_diff(&d);
        }
    }

    #[test]
    fn binary_arms() {
        let _serial = crate::c_oracle_serial();
        let a: PgUuid = [0x11; UUID_LEN];
        let b_: PgUuid = [0x22; UUID_LEN];
        let mut v7 = 0x017f22e279b0u64.to_le_bytes().to_vec();
        v7.extend_from_slice(&500_000u32.to_le_bytes());
        let mut ab0 = vec![0u8];
        ab0.extend_from_slice(&7u64.to_le_bytes());
        let mut ab1 = vec![1u8];
        ab1.extend_from_slice(&9u64.to_le_bytes());
        for (sel, payload) in [
            (2u8, [a, b_].concat()),
            (2u8, [a, a].concat()),
            (2u8, [b_, a].concat()),
            (3u8, {
                let mut v = a.to_vec();
                v.extend_from_slice(&0xdeadbeefdeadbeefu64.to_le_bytes());
                v
            }),
            (4u8, a.to_vec()),
            (5u8, a.to_vec()),      // exact 16 wire bytes
            (5u8, a[..7].to_vec()), // short read
            (6u8, v7),
            (7u8, ab0),
            (7u8, ab1),
        ] {
            let mut d = vec![sel];
            d.extend_from_slice(&payload);
            uuid_diff(&d);
        }
    }

    /// Version/timestamp extraction over the RFC 9562 variant lattice.
    #[test]
    fn extract_lattice() {
        let _serial = crate::c_oracle_serial();
        for ver in 0u8..16 {
            for variant in [0x00u8, 0x40, 0x80, 0xc0] {
                let mut u = [0x5au8; UUID_LEN];
                u[6] = (ver << 4) | 0x0c;
                u[8] = variant | 0x1f;
                let mut d = vec![4u8];
                d.extend_from_slice(&u);
                uuid_diff(&d);
            }
        }
    }

    /// fc-wrapper plane smoke: drive every selector whose arm carries fc_*
    /// wrapper checks (0 in hard+soft ok/err, 1 out, 2 cmp family, 3 hash +
    /// hash_extended, 4 extract_version/timestamp incl. NULL verdicts,
    /// 5 recv ok/err + send), so every executed wrapper runs at least once
    /// under `cargo test` on stable.
    #[test]
    fn fc_plane_smoke() {
        let _serial = crate::c_oracle_serial();
        // Arm 0: fc_uuid_in Ok and Err shapes (hard + soft each).
        for s in [&b"01020304-0506-7008-800a-0b0c0d0e0f10"[..], b"bogus", b""] {
            let mut d = vec![0u8];
            d.extend_from_slice(s);
            uuid_diff(&d);
        }
        // A v7 uuid (version nibble 7, RFC variant): non-NULL extract arms.
        let mut v7: PgUuid = [0x5a; UUID_LEN];
        v7[6] = 0x71;
        v7[8] = 0x8f;
        // A version-0/variant-0 uuid: NULL extract verdict arms.
        let nil: PgUuid = [0u8; UUID_LEN];
        for u in [v7, nil] {
            for sel in [1u8, 3, 4, 5] {
                let mut d = vec![sel];
                d.extend_from_slice(&u);
                if sel == 3 {
                    d.extend_from_slice(&0x1234_5678_9abc_def0u64.to_le_bytes());
                }
                uuid_diff(&d);
            }
        }
        // Arm 2: cmp family on <, ==, > orderings.
        for (a, b) in [(v7, nil), (v7, v7), (nil, v7)] {
            let mut d = vec![2u8];
            d.extend_from_slice(&a);
            d.extend_from_slice(&b);
            uuid_diff(&d);
        }
        // Arm 5 short-read: fc_uuid_recv error verdict.
        let mut d = vec![5u8];
        d.extend_from_slice(&v7[..9]);
        uuid_diff(&d);
    }

    /// The heavy >100k-distinct abbrev mode (0xa5) — run once here so the
    /// estimating-shutoff arm is exercised even if the fuzzer never mutates
    /// into the magic byte.
    #[test]
    fn abbrev_heavy_mode() {
        let _serial = crate::c_oracle_serial();
        let mut d = vec![7u8, 0xa5];
        d.extend_from_slice(&42u64.to_le_bytes());
        uuid_diff(&d);
    }
}
