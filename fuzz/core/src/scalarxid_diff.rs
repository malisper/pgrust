//! scalarxid_diff: differential fuzz driver — shipped Rust `adt_scalar` vs
//! vendored PostgreSQL 18.3 (Stamp-18.3, upstream sha 62d6c7d3df) C
//! (csrc/pg_scalarxid_io.c). Crate under test: crates/backend/utils/adt/scalar.
//!
//! Comparison planes (float_in_diff conventions): value bytes/bits,
//! error-verdict, and errcode/sqlstate class. Message text is out of scope.
//!
//! Input layout: [selector][payload]; selector % 35 picks the arm:
//!   0  tidin        payload = tid literal text (e.g. b"(12,34)")
//!   1  tidout       payload = 6 bytes: blk u32 LE + off u16 LE
//!   2..=7 tid bool cmps (eq,ne,lt,gt,le,ge)
//!                   payload = 12 bytes: (blk u32, off u16) LE pair x2
//!   8  bttidcmp     payload = 12 bytes as above
//!   9  tidlarger / 10 tidsmaller
//!                   payload = 12 bytes as above
//!   11 xidout       payload = 4 bytes: xid u32 LE
//!   12 xideq / 13 xidneq
//!                   payload = 8 bytes: u32 LE x2
//!   14 xid8cmp, 15..=20 xid8 bool cmps (eq,ne,lt,gt,le,ge),
//!   21 xid8_larger / 22 xid8_smaller (xid8out value plane rides 21/22)
//!                   payload = 16 bytes: u64 LE x2
//!   23..=28 oid bool cmps (eq,ne,lt,le,ge,gt), 29 oidlarger / 30 oidsmaller
//!                   payload = 8 bytes: u32 LE x2
//!   31 oidin        payload = oid literal text (strtoul base 0: hex/octal
//!                   accepted); the uint64in_subr (xid8in core) plane rides
//!                   this arm on the same text
//!   32 oidout       payload = 4 bytes: u32 LE
//!   33 oidvectorin  payload = "num num ..." text
//!   34 oidvectorout payload = 1 byte n (%17) + n x u32 LE
//!   -- coverage-extension arms (round 2; fleet round 1 = 10M/0 div) --
//!   35 xidin+cidin  payload = text (uint32in_subr wrappers; value+verdict+
//!                   errcode class vs pg_diff_uint32in)
//!   36 cidout       payload = 4 bytes: u32 LE
//!   37 cideq        payload = 8 bytes: u32 LE x2
//!   38 xid8in       payload = text (fc wrapper plane over uint64in_subr +
//!                   FullTransactionIdFromU64 = identity on the u64 datum)
//!   39 oidsend      payload = 4 bytes  (bytea wire image vs BE bytes)
//!   40 xidsend+cidsend (shared fc)   payload = 4 bytes
//!   41 xid8send     payload = 8 bytes
//!   42 tidsend      payload = 6 bytes: blk u32 LE + off u16 LE
//!   43 xid8toxid    payload = 8 bytes (epoch truncation)
//!   44 hashoid/hashxid/hashcid + extended (shared fc_hash_uint32*)
//!                   payload = 12 bytes: k u32 LE + seed u64 LE
//!   45 hashxid8 + extended          payload = 16 bytes: v u64 + seed u64
//!   46 hashtid + extended           payload = 14 bytes: tid image + seed
//!   47 oidvector cmp family (eq,ne,lt,le,ge,gt over btoidvectorcmp)
//!                   payload = 1 byte na (%9) + na x u32 + 1 byte nb (%9)
//!                   + nb x u32
//!   48 hashoidvector + extended     payload = 1 byte n (%17) + n x u32
//!                   + 8 bytes seed
//!   -- round-3 datum_ops arms (the scalar-datum Kani family walled on
//!      serialize/copy/restore at fleet 24GB/600s, flipping these lines to
//!      the differential-fuzz route; C oracle = pg_scalarxid_datum.c,
//!      cribbed from proofs/scalar-datum/c/pg_datum.c) --
//!   49 datum_get_size + datum_is_equal + datum_copy + datum_transfer over
//!      a decoded (typbyval,typlen) datum PAIR; payload = 1 kind byte +
//!      1 aux byte + rest split in half for the two datum images
//!   50 datum_estimate_space + datum_serialize + datum_restore roundtrip;
//!      payload = 1 kind + 1 aux + 1 flags (bit0 = isnull) + image bytes;
//!      images compared byte-exact, then cross-restored (Rust restores the
//!      C image, C restores the Rust image) and compared to the original
//!   51 datum error arms: NULL pointer with typlen -1/-2 (data-exception
//!      class) and invalid typlen (0, -3..) with byref (internal class),
//!      driven through get_size/copy/is_equal/estimate/serialize on the
//!      verdict + errclass plane
//!
//!   Datum construction plane (arms 49-51): kind%7 selects byval typlen
//!   {1,2,4,8} | fixed byref (typlen 1 + aux%32) | varlena (aux bit0: 1-byte
//!   vs 4-byte header, headers constructed WELL-FORMED with size == backing
//!   length) | cstring (interior NULs stripped). EXPANDED-OBJECT arms are
//!   out of scope: generated headers can never spell VARATT_IS_1B_E
//!   (1B header byte is (total<<1)|1 with total>=1, so never 0x01; 4B
//!   headers have low bit 0), and every C driver call asserts the EOH trap
//!   stub never fired (vacuity insurance, matching the proof family fence).
//!   datum_restore's corrupt-header arm (Rust release-asserts where C's
//!   Assert compiles out — documented hardening, proofs/scalar-datum
//!   README) is fenced by only restoring images serialize produced.
//!
//! ORACLE PLATFORM CARVE — tidin empty fields (ledger oid 48, ruling
//! pending): C tidin checks `errno || *badp != DELIM` with NO endptr==start
//! guard, so on glibc (errno untouched, endptr reset to the field start on
//! no-conversion) "(,5)" parses as (0,5) while BSD/macOS libc sets EINVAL
//! and rejects; shipped Rust deliberately matches the BSD arm. The tidin arm
//! therefore SKIPS inputs where a tid field is empty AND the byte at the
//! field start is the terminating delimiter itself (s[coord0]==',' or
//! s[coord1]==')'), the exact glibc-accepts class; every other malformed
//! shape still compares strictly. Host-independent: banded on macOS and
//! Linux alike so fleet campaigns hunt real divergences.
//!
//! SCOPE NOTES (not divergences):
//!   - text arms (tidin/oidin/oidvectorin) compare only valid-UTF-8,
//!     NUL-free inputs <=512 bytes: the shipped Rust cores are `&str`/lossy
//!     typed at the fmgr boundary (a pgrust API shape, not value logic);
//!     C-side behavior on non-UTF-8 bytes has no Rust counterpart to diff.
//!   - oidvectorout's DATATYPE_MISMATCH arm (check_valid_oidvector) fires
//!     only on header-corrupted vectors; both drivers here build the
//!     header-valid SQL-boundary shape. The Rust-side guard is
//!     nbt_compare::check_valid_oidvector, unit-tested in-crate.
//!
//! SKIPPED rows (out-of-scope carve, see phase1-claims.tsv adt/scalar):
//!   currtid_byrelname (catalog/snapshot/ACL seams, ledger 1294 excluded),
//!   xid_age / mxid_age (xact-seam state reads), *recv wrappers (recv ABI
//!   pointer-datum class, ledger 2440; the pq_getmsgint cores are
//!   wire-tested by wire_pqformat).
//!
//! NOTE on the hash arms (44-48): the mixing KERNELS live in common/hashfn
//! (p1-laneh's claim); these arms exist to execute and value-check the
//! adt/scalar fc_hash* WRAPPER lines (lohalf-fold for xid8, 6-byte tid
//! image, oidvector check+bytes framing) against verbatim
//! hashfn.c/hashfunc.c — an extra rail over laneh's kernel work, and the
//! only oracle of the wrapper framing itself.
//!
//! NOTE on macro-body line ranges flagged red by the round-1 merge
//! (builtins.rs 16-21, 34-35, 240-245, 381-386; lib.rs 20, 28-30): those
//! macro groups (fc_oid2/fc_oid2_oid/fc_xid8_cmp/fc_tid_cmp/oid_cmp_ops)
//! WERE dispatched in round 1 (arms 2-7, 15-20, 23-30) — every generated op
//! individually. If they stay red after this round they are the known
//! macro-expansion instrument-unmapped class (fc*! false-UNCOVERED,
//! coverage-measurement memory 2026-07-30), not a dispatch gap.

use datum::{Datum, NullableDatum};
use types_error::PgError;
use types_fmgr::{LocalFcinfo, PGFunction};

extern "C" {
    // Shared TLS errcode accessor (defined in csrc/pg_float_io.c).
    fn pg_diff_errcode_get() -> i32;
    fn pg_diff_tidin(str_: *const u8, blk: *mut u32, off: *mut u16) -> i32;
    fn pg_diff_tidout(blk: u32, off: u16, buf32: *mut u8);
    fn pg_diff_bttidcmp(blk1: u32, off1: u16, blk2: u32, off2: u16) -> i32;
    fn pg_diff_tidlarger(blk1: u32, off1: u16, blk2: u32, off2: u16, blk: *mut u32, off: *mut u16);
    fn pg_diff_tidsmaller(blk1: u32, off1: u16, blk2: u32, off2: u16, blk: *mut u32, off: *mut u16);
    fn pg_diff_xidout(xid: u32, buf16: *mut u8);
    fn pg_diff_xideq(x1: u32, x2: u32) -> i32;
    fn pg_diff_xidneq(x1: u32, x2: u32) -> i32;
    fn pg_diff_xid8cmp(a: u64, b: u64) -> i32;
    fn pg_diff_xid8rel(which: i32, a: u64, b: u64) -> i32;
    fn pg_diff_xid8_larger(a: u64, b: u64) -> u64;
    fn pg_diff_xid8_smaller(a: u64, b: u64) -> u64;
    fn pg_diff_xid8out(v: u64, buf21: *mut u8);
    fn pg_diff_oidrel(which: i32, a: u32, b: u32) -> i32;
    fn pg_diff_oidlarger(a: u32, b: u32) -> u32;
    fn pg_diff_oidsmaller(a: u32, b: u32) -> u32;
    fn pg_diff_uint32in(s: *const u8, out: *mut u32) -> i32;
    fn pg_diff_uint64in(s: *const u8, out: *mut u64) -> i32;
    fn pg_diff_oidout(o: u32, buf12: *mut u8);
    fn pg_diff_oidvectorin(s: *const u8, values: *mut u32, cap: i32, n: *mut i32) -> i32;
    fn pg_diff_oidvectorout(values: *const u32, n: i32, buf: *mut u8, bufcap: i32) -> i32;
    // Round-2 coverage-extension entries (csrc SECTION 7).
    fn pg_diff_cidout(c: u32, buf16: *mut u8);
    fn pg_diff_cideq(a: u32, b: u32) -> i32;
    fn pg_diff_xid8toxid(v: u64) -> u32;
    fn pg_diff_send32(v: u32, out4: *mut u8);
    fn pg_diff_send64(v: u64, out8: *mut u8);
    fn pg_diff_tidsend(blk: u32, off: u16, out6: *mut u8);
    fn pg_diff_hash_uint32(k: u32) -> u32;
    fn pg_diff_hash_uint32_extended(k: u32, seed: u64) -> u64;
    fn pg_diff_hashint8(val: i64) -> u32;
    fn pg_diff_hashint8extended(val: i64, seed: u64) -> u64;
    fn pg_diff_hashtid(blk: u32, off: u16) -> u32;
    fn pg_diff_hashtidextended(blk: u32, off: u16, seed: u64) -> u64;
    fn pg_diff_btoidvectorcmp(va: *const u32, na: i32, vb: *const u32, nb: i32) -> i32;
    fn pg_diff_hashoidvector(values: *const u32, n: i32) -> u32;
    fn pg_diff_hashoidvectorextended(values: *const u32, n: i32, seed: u64) -> u64;
    // Round-3 datum_ops entries (csrc/pg_scalarxid_datum.c; the scalar-datum
    // Kani family walled -> differential-fuzz route).
    fn pg_dx_eoh_reached() -> i32;
    fn pg_dx_get_size(value: usize, byval: i32, typlen: i32, out: *mut usize) -> i32;
    fn pg_dx_copy(
        value: usize,
        byval: i32,
        typlen: i32,
        outval: *mut usize,
        out: *mut u8,
        cap: usize,
        outlen: *mut usize,
    ) -> i32;
    fn pg_dx_transfer(
        value: usize,
        byval: i32,
        typlen: i32,
        outval: *mut usize,
        out: *mut u8,
        cap: usize,
        outlen: *mut usize,
    ) -> i32;
    fn pg_dx_is_equal(v1: usize, v2: usize, byval: i32, typlen: i32, res: *mut i32) -> i32;
    fn pg_dx_estimate_space(value: usize, isnull: i32, byval: i32, typlen: i32, out: *mut usize)
        -> i32;
    fn pg_dx_serialize(
        value: usize,
        isnull: i32,
        byval: i32,
        typlen: i32,
        out: *mut u8,
        cap: usize,
        outlen: *mut usize,
    ) -> i32;
    fn pg_dx_restore(
        input: *const u8,
        isnull: *mut i32,
        outval: *mut usize,
        out: *mut u8,
        cap: usize,
        outlen: *mut usize,
    ) -> usize;
}

fn c_errcode() -> i32 {
    // SAFETY: plain TLS int read.
    unsafe { pg_diff_errcode_get() }
}

/// Map a shipped-Rust PgError to the oracle's errcode class constants.
fn rust_err_class(e: &PgError) -> i32 {
    if e.sqlstate == types_error::ERRCODE_INVALID_TEXT_REPRESENTATION {
        1
    } else if e.sqlstate == types_error::ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE {
        2
    } else if e.sqlstate == types_error::ERRCODE_DATATYPE_MISMATCH {
        3
    } else {
        99
    }
}

// ---------------------------------------------------------------------------
// fc-wrapper plane plumbing (native LocalFcinfo, real mcx — the proofs
// wrapper-level pattern run without kani).
// ---------------------------------------------------------------------------

/// Invoke an fc_* wrapper over non-null args; returns (result, isnull flag).
fn fc_call<const N: usize>(
    f: PGFunction,
    m: mcx::Mcx<'_>,
    args: [Datum; N],
) -> (types_error::PgResult<Datum>, bool) {
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

/// Bytes of a NUL-terminated cstring result Datum (out-scratch convention).
fn datum_cstr<'a>(d: Datum) -> &'a [u8] {
    // SAFETY: wrappers return NUL-terminated out-scratch/mcx cstrings.
    unsafe { core::ffi::CStr::from_ptr(d.as_usize() as *const core::ffi::c_char) }.to_bytes()
}

use adt_scalar::Tid;

/// 6-byte on-tuple tid image (BlockIdData hi/lo u16 + OffsetNumber), the
/// builtins.rs arg_tid layout.
fn tid_image(t: Tid) -> [u8; 6] {
    let hi = ((t.block >> 16) as u16).to_ne_bytes();
    let lo = (t.block as u16).to_ne_bytes();
    let off = t.offset.to_ne_bytes();
    [hi[0], hi[1], lo[0], lo[1], off[0], off[1]]
}

fn tid_from_image(b: &[u8]) -> Tid {
    let hi = u16::from_ne_bytes([b[0], b[1]]);
    let lo = u16::from_ne_bytes([b[2], b[3]]);
    Tid {
        block: ((hi as u32) << 16) | lo as u32,
        offset: u16::from_ne_bytes([b[4], b[5]]),
    }
}

fn le_u32(b: &[u8]) -> u32 {
    u32::from_le_bytes([b[0], b[1], b[2], b[3]])
}

fn le_u64(b: &[u8]) -> u64 {
    u64::from_le_bytes([b[0], b[1], b[2], b[3], b[4], b[5], b[6], b[7]])
}

fn tid_pair(payload: &[u8]) -> Option<(Tid, Tid)> {
    if payload.len() < 12 {
        return None;
    }
    Some((
        Tid { block: le_u32(&payload[0..4]), offset: u16::from_le_bytes([payload[4], payload[5]]) },
        Tid {
            block: le_u32(&payload[6..10]),
            offset: u16::from_le_bytes([payload[10], payload[11]]),
        },
    ))
}

/// Text payload guard for the parser arms (SCOPE NOTES in the header):
/// NUL-free valid UTF-8, <=512 bytes. Returns a NUL-terminated copy for the
/// C side plus the &str view for Rust.
fn text_payload(payload: &[u8]) -> Option<(Vec<u8>, &str)> {
    if payload.len() > 512 || payload.contains(&0) {
        return None;
    }
    let s = core::str::from_utf8(payload).ok()?;
    let mut c = Vec::with_capacity(payload.len() + 1);
    c.extend_from_slice(payload);
    c.push(0);
    Some((c, s))
}

/// The glibc-accepts empty-field class banded out of the tidin arm (module
/// header carve; ledger oid 48). Mirrors the verbatim C coord scan.
fn tidin_platform_banded(s: &[u8]) -> bool {
    const NTIDARGS: usize = 2;
    let mut coord = [0usize; NTIDARGS];
    let mut n = 0;
    for (p, &c) in s.iter().enumerate() {
        if n >= NTIDARGS || c == b')' {
            break;
        }
        if c == b',' || (c == b'(' && n == 0) {
            coord[n] = p + 1;
            n += 1;
        }
    }
    if n < NTIDARGS {
        return false; // both sides reject before any field parses
    }
    s.get(coord[0]) == Some(&b',') || s.get(coord[1]) == Some(&b')')
}

// ---------------------------------------------------------------------------
// Dispatch
// ---------------------------------------------------------------------------

pub fn scalarxid_diff(data: &[u8]) {
    // one-thread-at-a-time through the C oracles (process-global statics) —
    // the fuzz TARGET's own frame stack needs the lock, same driver-entry
    // idiom as every other pub *_diff (task #144 addendum, trgm precedent).
    let _oracle = crate::oracle_serial();

    let Some((&sel, payload)) = data.split_first() else {
        return;
    };
    match sel % 52 {
        0 => tidin_diff(payload),
        1 => tidout_diff(payload),
        s @ 2..=7 => tidrel_diff(s, payload),
        8 => bttidcmp_diff(payload),
        9 => tidsel_diff(payload, true),
        10 => tidsel_diff(payload, false),
        11 => xidout_diff(payload),
        12 => xideq_diff(payload, true),
        13 => xideq_diff(payload, false),
        14 => xid8cmp_diff(payload),
        s @ 15..=20 => xid8rel_diff(s, payload),
        21 => xid8sel_diff(payload, true),
        22 => xid8sel_diff(payload, false),
        s @ 23..=28 => oidrel_diff(s, payload),
        29 => oidsel_diff(payload, true),
        30 => oidsel_diff(payload, false),
        31 => oidin_diff(payload),
        32 => oidout_diff(payload),
        33 => oidvectorin_diff(payload),
        34 => oidvectorout_diff(payload),
        35 => xidcidin_diff(payload),
        36 => cidout_diff(payload),
        37 => cideq_diff(payload),
        38 => xid8in_diff(payload),
        39 => send32_diff(payload, adt_scalar::builtins::fc_oidsend, "oidsend"),
        40 => send32_diff(payload, adt_scalar::builtins::fc_xidsend, "xidsend/cidsend"),
        41 => xid8send_diff(payload),
        42 => tidsend_diff(payload),
        43 => xid8toxid_diff(payload),
        44 => hashu32_diff(payload),
        45 => hashxid8_diff(payload),
        46 => hashtid_diff(payload),
        47 => oidvectorcmp_diff(payload),
        48 => hashoidvector_diff(payload),
        49 => datum_pair_diff(payload),
        50 => datum_serde_diff(payload),
        _ => datum_err_diff(payload),
    }
}

// ---------------------------------------------------------------------------
// Arm: tidin (catalog oid 48; C source: tid.c).
// ---------------------------------------------------------------------------

fn tidin_diff(payload: &[u8]) {
    let Some((cbuf, s)) = text_payload(payload) else {
        return;
    };
    if tidin_platform_banded(payload) {
        return; // module-header carve, ledger oid 48
    }
    let (mut blk, mut off) = (0u32, 0u16);
    // SAFETY: cbuf is NUL-terminated; out params are plain scalars.
    let cst = unsafe { pg_diff_tidin(cbuf.as_ptr(), &mut blk, &mut off) };
    let cerr = c_errcode();
    if cst == 2 {
        return; // driver refusal (unreachable under the 512-byte cap)
    }
    let rust = adt_scalar::tidin(payload);
    match rust {
        Some(t) => assert!(
            cst == 0 && t.block == blk && t.offset == off,
            "tidin DIVERGENCE input={s:?}: C=(st {cst}, err {cerr}, ({blk},{off})) Rust=Some(({},{}))",
            t.block,
            t.offset
        ),
        None => assert!(
            cst == 1 && cerr == 1,
            "tidin DIVERGENCE input={s:?}: C=(st {cst}, err {cerr}, ({blk},{off})) Rust=None"
        ),
    }
    // fc-wrapper plane: wrapper == core (hard-error shape).
    let ctx = mcx::MemoryContext::new("scalarxid_diff.tidin");
    let (r, _isnull) =
        fc_call(adt_scalar::builtins::fc_tidin, ctx.mcx(), [Datum::from_usize(cbuf.as_ptr() as usize)]);
    match (r, rust) {
        (Ok(d), Some(t)) => {
            let got = tid_from_image(datum_bytes(d, 6));
            assert!(got == t, "fc_tidin wrapper!=core input={s:?}: {got:?} vs {t:?}");
        }
        (Err(e), None) => assert!(
            rust_err_class(&e) == 1,
            "fc_tidin wrapper sqlstate class {} != 22P02 input={s:?}",
            rust_err_class(&e)
        ),
        (r, c) => panic!(
            "fc_tidin wrapper/core verdict mismatch input={s:?}: wrapper Ok={} core Some={}",
            r.is_ok(),
            c.is_some()
        ),
    }
}

// ---------------------------------------------------------------------------
// Arm: tidout (C source: tid.c). Exact byte-image parity.
// ---------------------------------------------------------------------------

fn tidout_diff(payload: &[u8]) {
    if payload.len() < 6 {
        return;
    }
    let t =
        Tid { block: le_u32(&payload[0..4]), offset: u16::from_le_bytes([payload[4], payload[5]]) };
    let mut cbuf = [0u8; 32];
    // SAFETY: 32-byte out buffer per the driver contract.
    unsafe { pg_diff_tidout(t.block, t.offset, cbuf.as_mut_ptr()) };
    let clen = cbuf.iter().position(|&b| b == 0).unwrap();
    let mut rbuf = [0u8; 32];
    let rlen = adt_scalar::tidout(t, &mut rbuf);
    assert!(
        cbuf[..clen] == rbuf[..rlen],
        "tidout DIVERGENCE tid=({},{}): C={:?} Rust={:?}",
        t.block,
        t.offset,
        core::str::from_utf8(&cbuf[..clen]),
        core::str::from_utf8(&rbuf[..rlen])
    );
    // fc plane: cstring result == core bytes.
    let img = tid_image(t);
    let ctx = mcx::MemoryContext::new("scalarxid_diff.tidout");
    let (r, _) =
        fc_call(adt_scalar::builtins::fc_tidout, ctx.mcx(), [Datum::from_usize(img.as_ptr() as usize)]);
    let d = r.expect("fc_tidout never errors");
    assert!(datum_cstr(d) == &rbuf[..rlen], "fc_tidout wrapper!=core tid=({},{})", t.block, t.offset);
}

// ---------------------------------------------------------------------------
// Arms: tid bool comparisons + bttidcmp + tidlarger/tidsmaller (tid.c).
// ---------------------------------------------------------------------------

fn tidrel_diff(sel: u8, payload: &[u8]) {
    let Some((a, b)) = tid_pair(payload) else {
        return;
    };
    // SAFETY: plain scalar args.
    let ccmp = unsafe { pg_diff_bttidcmp(a.block, a.offset, b.block, b.offset) };
    let rcmp = adt_scalar::tid_cmp(a, b);
    // Per the vendored bodies: each op applied to ItemPointerCompare.
    let (name, cval, rval, fc): (&str, bool, bool, PGFunction) = match sel {
        2 => ("tideq", ccmp == 0, rcmp == 0, adt_scalar::builtins::fc_tideq),
        3 => ("tidne", ccmp != 0, rcmp != 0, adt_scalar::builtins::fc_tidne),
        4 => ("tidlt", ccmp < 0, rcmp < 0, adt_scalar::builtins::fc_tidlt),
        5 => ("tidgt", ccmp > 0, rcmp > 0, adt_scalar::builtins::fc_tidgt),
        6 => ("tidle", ccmp <= 0, rcmp <= 0, adt_scalar::builtins::fc_tidle),
        _ => ("tidge", ccmp >= 0, rcmp >= 0, adt_scalar::builtins::fc_tidge),
    };
    assert!(
        cval == rval,
        "{name} DIVERGENCE a=({},{}) b=({},{}): C {cval} Rust {rval}",
        a.block,
        a.offset,
        b.block,
        b.offset
    );
    let (ia, ib) = (tid_image(a), tid_image(b));
    let ctx = mcx::MemoryContext::new("scalarxid_diff.tidrel");
    let (r, _) = fc_call(fc, ctx.mcx(), [
        Datum::from_usize(ia.as_ptr() as usize),
        Datum::from_usize(ib.as_ptr() as usize),
    ]);
    assert!(r.expect("tid cmp wrappers never error").as_bool() == rval, "fc_{name} wrapper!=core");
}

fn bttidcmp_diff(payload: &[u8]) {
    let Some((a, b)) = tid_pair(payload) else {
        return;
    };
    // SAFETY: plain scalar args.
    let ccmp = unsafe { pg_diff_bttidcmp(a.block, a.offset, b.block, b.offset) };
    let rcmp = adt_scalar::tid_cmp(a, b);
    assert!(
        ccmp == rcmp,
        "bttidcmp DIVERGENCE a=({},{}) b=({},{}): C {ccmp} Rust {rcmp}",
        a.block,
        a.offset,
        b.block,
        b.offset
    );
    let (ia, ib) = (tid_image(a), tid_image(b));
    let ctx = mcx::MemoryContext::new("scalarxid_diff.bttidcmp");
    let (r, _) = fc_call(adt_scalar::builtins::fc_bttidcmp, ctx.mcx(), [
        Datum::from_usize(ia.as_ptr() as usize),
        Datum::from_usize(ib.as_ptr() as usize),
    ]);
    assert!(r.expect("bttidcmp never errors") == Datum::from_i32(rcmp), "fc_bttidcmp wrapper!=core");
}

fn tidsel_diff(payload: &[u8], larger: bool) {
    let Some((a, b)) = tid_pair(payload) else {
        return;
    };
    let (mut blk, mut off) = (0u32, 0u16);
    // SAFETY: plain scalar args/outs.
    unsafe {
        if larger {
            pg_diff_tidlarger(a.block, a.offset, b.block, b.offset, &mut blk, &mut off);
        } else {
            pg_diff_tidsmaller(a.block, a.offset, b.block, b.offset, &mut blk, &mut off);
        }
    }
    let r = if larger { adt_scalar::tid_larger(a, b) } else { adt_scalar::tid_smaller(a, b) };
    let name = if larger { "tidlarger" } else { "tidsmaller" };
    assert!(
        r.block == blk && r.offset == off,
        "{name} DIVERGENCE a=({},{}) b=({},{}): C ({blk},{off}) Rust ({},{})",
        a.block,
        a.offset,
        b.block,
        b.offset,
        r.block,
        r.offset
    );
    let (ia, ib) = (tid_image(a), tid_image(b));
    let ctx = mcx::MemoryContext::new("scalarxid_diff.tidsel");
    let fc =
        if larger { adt_scalar::builtins::fc_tidlarger } else { adt_scalar::builtins::fc_tidsmaller };
    let (d, _) = fc_call(fc, ctx.mcx(), [
        Datum::from_usize(ia.as_ptr() as usize),
        Datum::from_usize(ib.as_ptr() as usize),
    ]);
    let got = tid_from_image(datum_bytes(d.expect("tid selections never error"), 6));
    assert!(got == r, "fc_{name} wrapper!=core: {got:?} vs {r:?}");
}

// ---------------------------------------------------------------------------
// Arms: xid (xid.c) — xidout, xideq/xidneq.
// ---------------------------------------------------------------------------

fn xidout_diff(payload: &[u8]) {
    if payload.len() < 4 {
        return;
    }
    let x = le_u32(payload);
    let mut cbuf = [0u8; 16];
    // SAFETY: 16-byte out buffer per the driver contract.
    unsafe { pg_diff_xidout(x, cbuf.as_mut_ptr()) };
    let clen = cbuf.iter().position(|&b| b == 0).unwrap();
    let mut rbuf = [0u8; 16];
    let rlen = adt_scalar::xidout(x, &mut rbuf);
    assert!(
        cbuf[..clen] == rbuf[..rlen],
        "xidout DIVERGENCE xid={x}: C={:?} Rust={:?}",
        core::str::from_utf8(&cbuf[..clen]),
        core::str::from_utf8(&rbuf[..rlen])
    );
    let ctx = mcx::MemoryContext::new("scalarxid_diff.xidout");
    let (r, _) = fc_call(adt_scalar::builtins::fc_xidout, ctx.mcx(), [Datum::from_u32(x)]);
    assert!(
        datum_cstr(r.expect("fc_xidout never errors")) == &rbuf[..rlen],
        "fc_xidout wrapper!=core xid={x}"
    );
}

fn xideq_diff(payload: &[u8], eq: bool) {
    if payload.len() < 8 {
        return;
    }
    let (x1, x2) = (le_u32(&payload[0..4]), le_u32(&payload[4..8]));
    // SAFETY: plain scalar args.
    let cval = unsafe { if eq { pg_diff_xideq(x1, x2) } else { pg_diff_xidneq(x1, x2) } } != 0;
    let rval = if eq { adt_scalar::xideq(x1, x2) } else { adt_scalar::xidneq(x1, x2) };
    let name = if eq { "xideq" } else { "xidneq" };
    assert!(cval == rval, "{name} DIVERGENCE x1={x1} x2={x2}: C {cval} Rust {rval}");
    let ctx = mcx::MemoryContext::new("scalarxid_diff.xideq");
    let fc = if eq { adt_scalar::builtins::fc_xideq } else { adt_scalar::builtins::fc_xidneq };
    let (r, _) = fc_call(fc, ctx.mcx(), [Datum::from_u32(x1), Datum::from_u32(x2)]);
    assert!(r.expect("xid cmps never error").as_bool() == rval, "fc_{name} wrapper!=core");
}

// ---------------------------------------------------------------------------
// Arms: xid8 (xid.c) — cmp, six bools, larger/smaller (+ xid8out plane).
// ---------------------------------------------------------------------------

fn xid8cmp_diff(payload: &[u8]) {
    if payload.len() < 16 {
        return;
    }
    let (a, b) = (le_u64(&payload[0..8]), le_u64(&payload[8..16]));
    // SAFETY: plain scalar args.
    let ccmp = unsafe { pg_diff_xid8cmp(a, b) };
    let rcmp = adt_scalar::xid8cmp(a, b);
    assert!(ccmp == rcmp, "xid8cmp DIVERGENCE a={a} b={b}: C {ccmp} Rust {rcmp}");
    let ctx = mcx::MemoryContext::new("scalarxid_diff.xid8cmp");
    let (r, _) =
        fc_call(adt_scalar::builtins::fc_xid8cmp, ctx.mcx(), [Datum::from_u64(a), Datum::from_u64(b)]);
    assert!(r.expect("xid8cmp never errors") == Datum::from_i32(rcmp), "fc_xid8cmp wrapper!=core");
}

fn xid8rel_diff(sel: u8, payload: &[u8]) {
    if payload.len() < 16 {
        return;
    }
    let (a, b) = (le_u64(&payload[0..8]), le_u64(&payload[8..16]));
    let (name, which, rval, fc): (&str, i32, bool, PGFunction) = match sel {
        15 => ("xid8eq", 0, a == b, adt_scalar::builtins::fc_xid8eq),
        16 => ("xid8ne", 1, a != b, adt_scalar::builtins::fc_xid8ne),
        17 => ("xid8lt", 2, a < b, adt_scalar::builtins::fc_xid8lt),
        18 => ("xid8gt", 3, a > b, adt_scalar::builtins::fc_xid8gt),
        19 => ("xid8le", 4, a <= b, adt_scalar::builtins::fc_xid8le),
        _ => ("xid8ge", 5, a >= b, adt_scalar::builtins::fc_xid8ge),
    };
    // SAFETY: plain scalar args.
    let cval = unsafe { pg_diff_xid8rel(which, a, b) } != 0;
    assert!(cval == rval, "{name} DIVERGENCE a={a} b={b}: C {cval} Rust {rval}");
    let ctx = mcx::MemoryContext::new("scalarxid_diff.xid8rel");
    let (r, _) = fc_call(fc, ctx.mcx(), [Datum::from_u64(a), Datum::from_u64(b)]);
    assert!(r.expect("xid8 cmps never error").as_bool() == rval, "fc_{name} wrapper!=core");
}

fn xid8sel_diff(payload: &[u8], larger: bool) {
    if payload.len() < 16 {
        return;
    }
    let (a, b) = (le_u64(&payload[0..8]), le_u64(&payload[8..16]));
    // SAFETY: plain scalar args.
    let cval =
        unsafe { if larger { pg_diff_xid8_larger(a, b) } else { pg_diff_xid8_smaller(a, b) } };
    let rval = if larger { a.max(b) } else { a.min(b) };
    let name = if larger { "xid8_larger" } else { "xid8_smaller" };
    assert!(cval == rval, "{name} DIVERGENCE a={a} b={b}: C {cval} Rust {rval}");
    let ctx = mcx::MemoryContext::new("scalarxid_diff.xid8sel");
    let fc = if larger {
        adt_scalar::builtins::fc_xid8_larger
    } else {
        adt_scalar::builtins::fc_xid8_smaller
    };
    let (r, _) = fc_call(fc, ctx.mcx(), [Datum::from_u64(a), Datum::from_u64(b)]);
    assert!(
        r.expect("xid8 selections never error") == Datum::from_u64(rval),
        "fc_{name} wrapper!=core"
    );

    // xid8out value plane rides this arm on `a`: fc_xid8out's pg_ulltoa_n
    // digit image vs the verbatim snprintf %llu.
    let mut cbuf = [0u8; 21];
    // SAFETY: 21-byte out buffer per the driver contract.
    unsafe { pg_diff_xid8out(a, cbuf.as_mut_ptr()) };
    let clen = cbuf.iter().position(|&x| x == 0).unwrap();
    let (r, _) = fc_call(adt_scalar::builtins::fc_xid8out, ctx.mcx(), [Datum::from_u64(a)]);
    assert!(
        datum_cstr(r.expect("fc_xid8out never errors")) == &cbuf[..clen],
        "xid8out DIVERGENCE v={a}: C={:?}",
        core::str::from_utf8(&cbuf[..clen])
    );
}

// ---------------------------------------------------------------------------
// Arms: oid (oid.c) — six bools, larger/smaller, in/out.
// ---------------------------------------------------------------------------

fn oidrel_diff(sel: u8, payload: &[u8]) {
    if payload.len() < 8 {
        return;
    }
    let (a, b) = (le_u32(&payload[0..4]), le_u32(&payload[4..8]));
    let (name, which, rval, fc): (&str, i32, bool, PGFunction) = match sel {
        23 => ("oideq", 0, adt_scalar::oideq(a, b), adt_scalar::builtins::fc_oideq),
        24 => ("oidne", 1, adt_scalar::oidne(a, b), adt_scalar::builtins::fc_oidne),
        25 => ("oidlt", 2, adt_scalar::oidlt(a, b), adt_scalar::builtins::fc_oidlt),
        26 => ("oidle", 3, adt_scalar::oidle(a, b), adt_scalar::builtins::fc_oidle),
        27 => ("oidge", 4, adt_scalar::oidge(a, b), adt_scalar::builtins::fc_oidge),
        _ => ("oidgt", 5, adt_scalar::oidgt(a, b), adt_scalar::builtins::fc_oidgt),
    };
    // SAFETY: plain scalar args.
    let cval = unsafe { pg_diff_oidrel(which, a, b) } != 0;
    assert!(cval == rval, "{name} DIVERGENCE a={a} b={b}: C {cval} Rust {rval}");
    let ctx = mcx::MemoryContext::new("scalarxid_diff.oidrel");
    let (r, _) = fc_call(fc, ctx.mcx(), [Datum::from_oid(a), Datum::from_oid(b)]);
    assert!(r.expect("oid cmps never error").as_bool() == rval, "fc_{name} wrapper!=core");
}

fn oidsel_diff(payload: &[u8], larger: bool) {
    if payload.len() < 8 {
        return;
    }
    let (a, b) = (le_u32(&payload[0..4]), le_u32(&payload[4..8]));
    // SAFETY: plain scalar args.
    let cval = unsafe { if larger { pg_diff_oidlarger(a, b) } else { pg_diff_oidsmaller(a, b) } };
    let rval = if larger { adt_scalar::oidlarger(a, b) } else { adt_scalar::oidsmaller(a, b) };
    let name = if larger { "oidlarger" } else { "oidsmaller" };
    assert!(cval == rval, "{name} DIVERGENCE a={a} b={b}: C {cval} Rust {rval}");
    let ctx = mcx::MemoryContext::new("scalarxid_diff.oidsel");
    let fc =
        if larger { adt_scalar::builtins::fc_oidlarger } else { adt_scalar::builtins::fc_oidsmaller };
    let (r, _) = fc_call(fc, ctx.mcx(), [Datum::from_oid(a), Datum::from_oid(b)]);
    assert!(
        r.expect("oid selections never error") == Datum::from_oid(rval),
        "fc_{name} wrapper!=core"
    );
}

fn oidin_diff(payload: &[u8]) {
    let Some((cbuf, s)) = text_payload(payload) else {
        return;
    };
    let mut cval = 0u32;
    // SAFETY: cbuf NUL-terminated.
    let cst = unsafe { pg_diff_uint32in(cbuf.as_ptr(), &mut cval) };
    let cerr = c_errcode();
    let rust = numutils::uint32in_subr(s, false, "oid", None);
    match rust {
        Ok((v, _)) => assert!(
            cst == 0 && v == cval,
            "oidin DIVERGENCE input={s:?}: C=(st {cst}, err {cerr}, {cval}) Rust=Ok({v})"
        ),
        Err(ref e) => assert!(
            cst == 1 && cerr == rust_err_class(e),
            "oidin DIVERGENCE input={s:?}: C=(st {cst}, err {cerr}) Rust=Err(class {})",
            rust_err_class(e)
        ),
    }
    // fc plane.
    let ctx = mcx::MemoryContext::new("scalarxid_diff.oidin");
    let (r, _) =
        fc_call(adt_scalar::builtins::fc_oidin, ctx.mcx(), [Datum::from_usize(cbuf.as_ptr() as usize)]);
    match (&r, &rust) {
        (Ok(d), Ok((v, _))) => {
            assert!(*d == Datum::from_oid(*v), "fc_oidin wrapper!=core input={s:?}")
        }
        (Err(e), Err(e2)) => assert!(
            rust_err_class(e) == rust_err_class(e2),
            "fc_oidin wrapper sqlstate class mismatch input={s:?}"
        ),
        _ => panic!("fc_oidin wrapper/core verdict mismatch input={s:?}"),
    }
    // uint64in_subr plane (xid8in core) rides the same text.
    let mut c64 = 0u64;
    // SAFETY: cbuf NUL-terminated.
    let cst64 = unsafe { pg_diff_uint64in(cbuf.as_ptr(), &mut c64) };
    let cerr64 = c_errcode();
    match numutils::uint64in_subr(s, false, "xid8", None) {
        Ok((v, _)) => assert!(
            cst64 == 0 && v == c64,
            "xid8in(uint64in_subr) DIVERGENCE input={s:?}: C=(st {cst64}, err {cerr64}, {c64}) Rust=Ok({v})"
        ),
        Err(ref e) => assert!(
            cst64 == 1 && cerr64 == rust_err_class(e),
            "xid8in(uint64in_subr) DIVERGENCE input={s:?}: C=(st {cst64}, err {cerr64}) Rust=Err(class {})",
            rust_err_class(e)
        ),
    }
}

fn oidout_diff(payload: &[u8]) {
    if payload.len() < 4 {
        return;
    }
    let o = le_u32(payload);
    let mut cbuf = [0u8; 12];
    // SAFETY: 12-byte out buffer per the driver contract.
    unsafe { pg_diff_oidout(o, cbuf.as_mut_ptr()) };
    let clen = cbuf.iter().position(|&b| b == 0).unwrap();
    let mut rbuf = [0u8; 12];
    let rlen = numutils::pg_ultoa_n(o, &mut rbuf[..11]);
    assert!(
        cbuf[..clen] == rbuf[..rlen],
        "oidout DIVERGENCE oid={o}: C={:?} Rust={:?}",
        core::str::from_utf8(&cbuf[..clen]),
        core::str::from_utf8(&rbuf[..rlen])
    );
    let ctx = mcx::MemoryContext::new("scalarxid_diff.oidout");
    let (r, _) = fc_call(adt_scalar::builtins::fc_oidout, ctx.mcx(), [Datum::from_oid(o)]);
    assert!(
        datum_cstr(r.expect("fc_oidout never errors")) == &rbuf[..rlen],
        "fc_oidout wrapper!=core oid={o}"
    );
}

// ---------------------------------------------------------------------------
// Arms: oidvectorin / oidvectorout (oid.c). The Rust side is wrapper-only
// (fc_oidvectorin builds the varlena image in-line), so the fc plane IS the
// value plane here.
// ---------------------------------------------------------------------------

/// oidvector image header size (4B varsize + ndim + dataoffset + elemtype +
/// dim1 + lbound1, all 4B) — the fc_oidvectorin layout.
const OIDVEC_HDR: usize = 24;

fn oidvectorin_diff(payload: &[u8]) {
    let Some((cbuf, s)) = text_payload(payload) else {
        return;
    };
    let mut cvals = [0u32; 256];
    let mut cn: i32 = 0;
    // SAFETY: cbuf NUL-terminated; cvals cap passed alongside.
    let cst = unsafe { pg_diff_oidvectorin(cbuf.as_ptr(), cvals.as_mut_ptr(), 256, &mut cn) };
    let cerr = c_errcode();
    if cst == 2 || cn as usize > 256 {
        return; // driver refusal (512-byte text cannot produce >256 oids)
    }
    let ctx = mcx::MemoryContext::new("scalarxid_diff.oidvectorin");
    let (r, _) = fc_call(adt_scalar::builtins::fc_oidvectorin, ctx.mcx(), [Datum::from_usize(
        cbuf.as_ptr() as usize,
    )]);
    match r {
        Ok(d) => {
            assert!(cst == 0, "oidvectorin DIVERGENCE input={s:?}: C err {cerr}, Rust Ok");
            let hdr = datum_bytes(d, OIDVEC_HDR);
            let ndim = i32::from_ne_bytes(hdr[4..8].try_into().unwrap());
            let dataoffset = i32::from_ne_bytes(hdr[8..12].try_into().unwrap());
            let elemtype = u32::from_ne_bytes(hdr[12..16].try_into().unwrap());
            let dim1 = i32::from_ne_bytes(hdr[16..20].try_into().unwrap());
            let lbound1 = i32::from_ne_bytes(hdr[20..24].try_into().unwrap());
            assert!(
                ndim == 1 && dataoffset == 0 && elemtype == 26 && lbound1 == 0,
                "oidvectorin header shape input={s:?}: ndim {ndim} dataoffset {dataoffset} elemtype {elemtype} lbound1 {lbound1}"
            );
            assert!(dim1 == cn, "oidvectorin DIVERGENCE input={s:?}: C n={cn} Rust dim1={dim1}");
            let all = datum_bytes(d, OIDVEC_HDR + dim1 as usize * 4);
            for i in 0..dim1 as usize {
                let rv = u32::from_ne_bytes(
                    all[OIDVEC_HDR + i * 4..OIDVEC_HDR + i * 4 + 4].try_into().unwrap(),
                );
                assert!(
                    rv == cvals[i],
                    "oidvectorin DIVERGENCE input={s:?} elem {i}: C {} Rust {rv}",
                    cvals[i]
                );
            }
        }
        Err(ref e) => assert!(
            cst == 1 && cerr == rust_err_class(e),
            "oidvectorin DIVERGENCE input={s:?}: C=(st {cst}, err {cerr}) Rust=Err(class {})",
            rust_err_class(e)
        ),
    }
}

fn oidvectorout_diff(payload: &[u8]) {
    let Some((&nb, rest)) = payload.split_first() else {
        return;
    };
    let n = (nb % 17) as usize;
    if rest.len() < n * 4 {
        return;
    }
    let mut vals = [0u32; 16];
    for (i, v) in vals.iter_mut().enumerate().take(n) {
        *v = le_u32(&rest[i * 4..i * 4 + 4]);
    }
    let mut cbuf = [0u8; 256];
    // SAFETY: value slice + out buffer per the driver contract.
    let cst = unsafe { pg_diff_oidvectorout(vals.as_ptr(), n as i32, cbuf.as_mut_ptr(), 256) };
    assert!(cst == 0, "oidvectorout C driver refused (n={n})");
    let clen = cbuf.iter().position(|&b| b == 0).unwrap();
    // Rust side: the SQL-boundary image fc_oidvectorout expects.
    let mut img = Vec::with_capacity(OIDVEC_HDR + n * 4);
    img.extend_from_slice(&datum::varlena::set_varsize_4b(OIDVEC_HDR + n * 4));
    img.extend_from_slice(&1i32.to_ne_bytes());
    img.extend_from_slice(&0i32.to_ne_bytes());
    img.extend_from_slice(&26u32.to_ne_bytes());
    img.extend_from_slice(&(n as i32).to_ne_bytes());
    img.extend_from_slice(&0i32.to_ne_bytes());
    for v in &vals[..n] {
        img.extend_from_slice(&v.to_ne_bytes());
    }
    let ctx = mcx::MemoryContext::new("scalarxid_diff.oidvectorout");
    let (r, _) = fc_call(adt_scalar::builtins::fc_oidvectorout, ctx.mcx(), [Datum::from_usize(
        img.as_ptr() as usize,
    )]);
    let d = r.expect("oidvectorout over a valid header never errors");
    assert!(
        datum_cstr(d) == &cbuf[..clen],
        "oidvectorout DIVERGENCE n={n} vals={:?}: C={:?} Rust={:?}",
        &vals[..n],
        core::str::from_utf8(&cbuf[..clen]),
        core::str::from_utf8(datum_cstr(d))
    );
}

// ---------------------------------------------------------------------------
// Round-2 coverage-extension arms.
// ---------------------------------------------------------------------------

/// Payload bytes of a bytea result Datum (send wrappers return
/// varlena_result(pq_endtypsend(..)): 4-byte varsize header + payload).
fn datum_bytea<'a>(d: Datum) -> &'a [u8] {
    let hdr = datum_bytes(d, 4);
    let total = (u32::from_ne_bytes([hdr[0], hdr[1], hdr[2], hdr[3]]) >> 2) as usize;
    &datum_bytes(d, total)[4..]
}

/// Header-valid oidvector varlena image (the fc_oidvectorin layout).
fn oidvector_image(vals: &[u32]) -> Vec<u8> {
    let mut img = Vec::with_capacity(OIDVEC_HDR + vals.len() * 4);
    img.extend_from_slice(&datum::varlena::set_varsize_4b(OIDVEC_HDR + vals.len() * 4));
    img.extend_from_slice(&1i32.to_ne_bytes());
    img.extend_from_slice(&0i32.to_ne_bytes());
    img.extend_from_slice(&26u32.to_ne_bytes());
    img.extend_from_slice(&(vals.len() as i32).to_ne_bytes());
    img.extend_from_slice(&0i32.to_ne_bytes());
    for v in vals {
        img.extend_from_slice(&v.to_ne_bytes());
    }
    img
}

/// Arm 35: fc_xidin + fc_cidin (uint32in_subr wrappers; the C side is the
/// same verbatim subr under both typnames — message text out of scope).
fn xidcidin_diff(payload: &[u8]) {
    let Some((cbuf, s)) = text_payload(payload) else {
        return;
    };
    let mut cval = 0u32;
    // SAFETY: cbuf NUL-terminated.
    let cst = unsafe { pg_diff_uint32in(cbuf.as_ptr(), &mut cval) };
    let cerr = c_errcode();
    let ctx = mcx::MemoryContext::new("scalarxid_diff.xidcidin");
    for (name, fc) in [
        ("xidin", adt_scalar::builtins::fc_xidin as PGFunction),
        ("cidin", adt_scalar::builtins::fc_cidin as PGFunction),
    ] {
        let (r, _) = fc_call(fc, ctx.mcx(), [Datum::from_usize(cbuf.as_ptr() as usize)]);
        match r {
            Ok(d) => assert!(
                cst == 0 && d == Datum::from_u32(cval),
                "{name} DIVERGENCE input={s:?}: C=(st {cst}, err {cerr}, {cval}) Rust=Ok"
            ),
            Err(ref e) => assert!(
                cst == 1 && cerr == rust_err_class(e),
                "{name} DIVERGENCE input={s:?}: C=(st {cst}, err {cerr}) Rust=Err(class {})",
                rust_err_class(e)
            ),
        }
    }
}

/// Arm 36: fc_cidout vs verbatim cidout (%lu image).
fn cidout_diff(payload: &[u8]) {
    if payload.len() < 4 {
        return;
    }
    let c = le_u32(payload);
    let mut cbuf = [0u8; 16];
    // SAFETY: 16-byte out buffer per the driver contract.
    unsafe { pg_diff_cidout(c, cbuf.as_mut_ptr()) };
    let clen = cbuf.iter().position(|&b| b == 0).unwrap();
    let ctx = mcx::MemoryContext::new("scalarxid_diff.cidout");
    let (r, _) = fc_call(adt_scalar::builtins::fc_cidout, ctx.mcx(), [Datum::from_u32(c)]);
    assert!(
        datum_cstr(r.expect("fc_cidout never errors")) == &cbuf[..clen],
        "cidout DIVERGENCE cid={c}: C={:?}",
        core::str::from_utf8(&cbuf[..clen])
    );
}

/// Arm 37: fc_cideq vs verbatim cideq.
fn cideq_diff(payload: &[u8]) {
    if payload.len() < 8 {
        return;
    }
    let (a, b) = (le_u32(&payload[0..4]), le_u32(&payload[4..8]));
    // SAFETY: plain scalar args.
    let cval = unsafe { pg_diff_cideq(a, b) } != 0;
    let ctx = mcx::MemoryContext::new("scalarxid_diff.cideq");
    let (r, _) = fc_call(adt_scalar::builtins::fc_cideq, ctx.mcx(), [
        Datum::from_u32(a),
        Datum::from_u32(b),
    ]);
    let rval = r.expect("cideq never errors").as_bool();
    assert!(cval == rval, "cideq DIVERGENCE a={a} b={b}: C {cval} Rust {rval}");
}

/// Arm 38: fc_xid8in wrapper plane (uint64in_subr + FullTransactionIdFromU64,
/// which is the identity on the u64 datum).
fn xid8in_diff(payload: &[u8]) {
    let Some((cbuf, s)) = text_payload(payload) else {
        return;
    };
    let mut c64 = 0u64;
    // SAFETY: cbuf NUL-terminated.
    let cst = unsafe { pg_diff_uint64in(cbuf.as_ptr(), &mut c64) };
    let cerr = c_errcode();
    let ctx = mcx::MemoryContext::new("scalarxid_diff.xid8in");
    let (r, _) =
        fc_call(adt_scalar::builtins::fc_xid8in, ctx.mcx(), [Datum::from_usize(cbuf.as_ptr() as usize)]);
    match r {
        Ok(d) => assert!(
            cst == 0 && d == Datum::from_u64(c64),
            "xid8in DIVERGENCE input={s:?}: C=(st {cst}, err {cerr}, {c64}) Rust=Ok({})",
            d.as_u64()
        ),
        Err(ref e) => assert!(
            cst == 1 && cerr == rust_err_class(e),
            "xid8in DIVERGENCE input={s:?}: C=(st {cst}, err {cerr}) Rust=Err(class {})",
            rust_err_class(e)
        ),
    }
}

/// Arms 39/40: 4-byte int sends (oidsend; xidsend, which cidsend shares).
fn send32_diff(payload: &[u8], fc: PGFunction, name: &str) {
    if payload.len() < 4 {
        return;
    }
    let v = le_u32(payload);
    let mut cimg = [0u8; 4];
    // SAFETY: 4-byte out buffer.
    unsafe { pg_diff_send32(v, cimg.as_mut_ptr()) };
    let ctx = mcx::MemoryContext::new("scalarxid_diff.send32");
    let (r, _) = fc_call(fc, ctx.mcx(), [Datum::from_u32(v)]);
    let d = r.expect("int sends never error");
    assert!(
        datum_bytea(d) == cimg,
        "{name} DIVERGENCE v={v}: C={cimg:?} Rust={:?}",
        datum_bytea(d)
    );
}

/// Arm 41: xid8send (8-byte BE image).
fn xid8send_diff(payload: &[u8]) {
    if payload.len() < 8 {
        return;
    }
    let v = le_u64(payload);
    let mut cimg = [0u8; 8];
    // SAFETY: 8-byte out buffer.
    unsafe { pg_diff_send64(v, cimg.as_mut_ptr()) };
    let ctx = mcx::MemoryContext::new("scalarxid_diff.xid8send");
    let (r, _) = fc_call(adt_scalar::builtins::fc_xid8send, ctx.mcx(), [Datum::from_u64(v)]);
    let d = r.expect("xid8send never errors");
    assert!(
        datum_bytea(d) == cimg,
        "xid8send DIVERGENCE v={v}: C={cimg:?} Rust={:?}",
        datum_bytea(d)
    );
}

/// Arm 42: tidsend (int32 block + int16 offset wire image).
fn tidsend_diff(payload: &[u8]) {
    if payload.len() < 6 {
        return;
    }
    let t =
        Tid { block: le_u32(&payload[0..4]), offset: u16::from_le_bytes([payload[4], payload[5]]) };
    let mut cimg = [0u8; 6];
    // SAFETY: 6-byte out buffer.
    unsafe { pg_diff_tidsend(t.block, t.offset, cimg.as_mut_ptr()) };
    let img = tid_image(t);
    let ctx = mcx::MemoryContext::new("scalarxid_diff.tidsend");
    let (r, _) =
        fc_call(adt_scalar::builtins::fc_tidsend, ctx.mcx(), [Datum::from_usize(img.as_ptr() as usize)]);
    let d = r.expect("tidsend never errors");
    assert!(
        datum_bytea(d) == cimg,
        "tidsend DIVERGENCE tid=({},{}): C={cimg:?} Rust={:?}",
        t.block,
        t.offset,
        datum_bytea(d)
    );
}

/// Arm 43: fc_xid8toxid (epoch truncation, XidFromFullTransactionId).
fn xid8toxid_diff(payload: &[u8]) {
    if payload.len() < 8 {
        return;
    }
    let v = le_u64(payload);
    // SAFETY: plain scalar arg.
    let cval = unsafe { pg_diff_xid8toxid(v) };
    let ctx = mcx::MemoryContext::new("scalarxid_diff.xid8toxid");
    let (r, _) = fc_call(adt_scalar::builtins::fc_xid8toxid, ctx.mcx(), [Datum::from_u64(v)]);
    let d = r.expect("xid8toxid never errors");
    assert!(
        d == Datum::from_u32(cval),
        "xid8toxid DIVERGENCE v={v:#x}: C {cval} Rust {}",
        d.as_u64()
    );
}

/// Arm 44: fc_hash_uint32 / fc_hash_uint32_extended (hashoid/hashxid/hashcid
/// share these fcs) vs verbatim hash_bytes_uint32(+extended).
fn hashu32_diff(payload: &[u8]) {
    if payload.len() < 12 {
        return;
    }
    let k = le_u32(&payload[0..4]);
    let seed = le_u64(&payload[4..12]);
    // SAFETY: plain scalar args.
    let ch = unsafe { pg_diff_hash_uint32(k) };
    let che = unsafe { pg_diff_hash_uint32_extended(k, seed) };
    let ctx = mcx::MemoryContext::new("scalarxid_diff.hashu32");
    let (r, _) = fc_call(adt_scalar::builtins::fc_hash_uint32, ctx.mcx(), [Datum::from_u32(k)]);
    let d = r.expect("hash_uint32 never errors");
    assert!(d == Datum::from_u32(ch), "hash_uint32 DIVERGENCE k={k}: C {ch:#x} Rust {:#x}", d.as_u64());
    let (r, _) = fc_call(adt_scalar::builtins::fc_hash_uint32_extended, ctx.mcx(), [
        Datum::from_u32(k),
        Datum::from_i64(seed as i64),
    ]);
    let d = r.expect("hash_uint32_extended never errors");
    assert!(
        d == Datum::from_u64(che),
        "hash_uint32_extended DIVERGENCE k={k} seed={seed:#x}: C {che:#x} Rust {:#x}",
        d.as_u64()
    );
}

/// Arm 45: fc_hashxid8 / fc_hashxid8extended (lohalf sign-fold framing) vs
/// verbatim hashint8(+extended).
fn hashxid8_diff(payload: &[u8]) {
    if payload.len() < 16 {
        return;
    }
    let v = le_u64(&payload[0..8]);
    let seed = le_u64(&payload[8..16]);
    // SAFETY: plain scalar args.
    let ch = unsafe { pg_diff_hashint8(v as i64) };
    let che = unsafe { pg_diff_hashint8extended(v as i64, seed) };
    let ctx = mcx::MemoryContext::new("scalarxid_diff.hashxid8");
    let (r, _) = fc_call(adt_scalar::builtins::fc_hashxid8, ctx.mcx(), [Datum::from_u64(v)]);
    let d = r.expect("hashxid8 never errors");
    assert!(d == Datum::from_u32(ch), "hashxid8 DIVERGENCE v={v:#x}: C {ch:#x} Rust {:#x}", d.as_u64());
    let (r, _) = fc_call(adt_scalar::builtins::fc_hashxid8extended, ctx.mcx(), [
        Datum::from_u64(v),
        Datum::from_i64(seed as i64),
    ]);
    let d = r.expect("hashxid8extended never errors");
    assert!(
        d == Datum::from_u64(che),
        "hashxid8extended DIVERGENCE v={v:#x} seed={seed:#x}: C {che:#x} Rust {:#x}",
        d.as_u64()
    );
}

/// Arm 46: fc_hashtid / fc_hashtidextended (6-byte raw-field image framing).
fn hashtid_diff(payload: &[u8]) {
    if payload.len() < 14 {
        return;
    }
    let t =
        Tid { block: le_u32(&payload[0..4]), offset: u16::from_le_bytes([payload[4], payload[5]]) };
    let seed = le_u64(&payload[6..14]);
    // SAFETY: plain scalar args.
    let ch = unsafe { pg_diff_hashtid(t.block, t.offset) };
    let che = unsafe { pg_diff_hashtidextended(t.block, t.offset, seed) };
    let img = tid_image(t);
    let ctx = mcx::MemoryContext::new("scalarxid_diff.hashtid");
    let (r, _) =
        fc_call(adt_scalar::builtins::fc_hashtid, ctx.mcx(), [Datum::from_usize(img.as_ptr() as usize)]);
    let d = r.expect("hashtid never errors");
    assert!(
        d == Datum::from_u32(ch),
        "hashtid DIVERGENCE tid=({},{}): C {ch:#x} Rust {:#x}",
        t.block,
        t.offset,
        d.as_u64()
    );
    let (r, _) = fc_call(adt_scalar::builtins::fc_hashtidextended, ctx.mcx(), [
        Datum::from_usize(img.as_ptr() as usize),
        Datum::from_i64(seed as i64),
    ]);
    let d = r.expect("hashtidextended never errors");
    assert!(
        d == Datum::from_u64(che),
        "hashtidextended DIVERGENCE tid=({},{}) seed={seed:#x}: C {che:#x} Rust {:#x}",
        t.block,
        t.offset,
        d.as_u64()
    );
}

/// Payload decode for the two-vector arms: 1 byte na (%9) + na x u32 +
/// 1 byte nb (%9) + nb x u32.
fn two_oidvectors(payload: &[u8]) -> Option<(Vec<u32>, Vec<u32>)> {
    let (&na, rest) = payload.split_first()?;
    let na = (na % 9) as usize;
    if rest.len() < na * 4 + 1 {
        return None;
    }
    let a: Vec<u32> = (0..na).map(|i| le_u32(&rest[i * 4..i * 4 + 4])).collect();
    let rest = &rest[na * 4..];
    let (&nb, rest) = rest.split_first()?;
    let nb = (nb % 9) as usize;
    if rest.len() < nb * 4 {
        return None;
    }
    let b: Vec<u32> = (0..nb).map(|i| le_u32(&rest[i * 4..i * 4 + 4])).collect();
    Some((a, b))
}

/// Arm 47: oidvectoreq/ne/lt/le/ge/gt — all six ops every exec, vs verbatim
/// btoidvectorcmp (nbtcompare.c).
fn oidvectorcmp_diff(payload: &[u8]) {
    let Some((a, b)) = two_oidvectors(payload) else {
        return;
    };
    // SAFETY: slices live for the call.
    let ccmp =
        unsafe { pg_diff_btoidvectorcmp(a.as_ptr(), a.len() as i32, b.as_ptr(), b.len() as i32) };
    let (ia, ib) = (oidvector_image(&a), oidvector_image(&b));
    let ctx = mcx::MemoryContext::new("scalarxid_diff.oidvectorcmp");
    let ops: [(&str, PGFunction, bool); 6] = [
        ("oidvectoreq", adt_scalar::builtins::fc_oidvectoreq, ccmp == 0),
        ("oidvectorne", adt_scalar::builtins::fc_oidvectorne, ccmp != 0),
        ("oidvectorlt", adt_scalar::builtins::fc_oidvectorlt, ccmp < 0),
        ("oidvectorle", adt_scalar::builtins::fc_oidvectorle, ccmp <= 0),
        ("oidvectorge", adt_scalar::builtins::fc_oidvectorge, ccmp >= 0),
        ("oidvectorgt", adt_scalar::builtins::fc_oidvectorgt, ccmp > 0),
    ];
    for (name, fc, cval) in ops {
        let (r, _) = fc_call(fc, ctx.mcx(), [
            Datum::from_usize(ia.as_ptr() as usize),
            Datum::from_usize(ib.as_ptr() as usize),
        ]);
        let rval = r.expect("oidvector cmps over valid headers never error").as_bool();
        assert!(
            cval == rval,
            "{name} DIVERGENCE a={a:?} b={b:?} (C cmp {ccmp}): C {cval} Rust {rval}"
        );
    }
}

/// Arm 48: fc_hashoidvector / fc_hashoidvectorextended (check+bytes framing).
fn hashoidvector_diff(payload: &[u8]) {
    let Some((&nb, rest)) = payload.split_first() else {
        return;
    };
    let n = (nb % 17) as usize;
    if rest.len() < n * 4 + 8 {
        return;
    }
    let vals: Vec<u32> = (0..n).map(|i| le_u32(&rest[i * 4..i * 4 + 4])).collect();
    let seed = le_u64(&rest[n * 4..n * 4 + 8]);
    // SAFETY: slice lives for the call.
    let ch = unsafe { pg_diff_hashoidvector(vals.as_ptr(), n as i32) };
    let che = unsafe { pg_diff_hashoidvectorextended(vals.as_ptr(), n as i32, seed) };
    let img = oidvector_image(&vals);
    let ctx = mcx::MemoryContext::new("scalarxid_diff.hashoidvector");
    let (r, _) = fc_call(adt_scalar::builtins::fc_hashoidvector, ctx.mcx(), [Datum::from_usize(
        img.as_ptr() as usize,
    )]);
    let d = r.expect("hashoidvector over valid headers never errors");
    assert!(
        d == Datum::from_u32(ch),
        "hashoidvector DIVERGENCE vals={vals:?}: C {ch:#x} Rust {:#x}",
        d.as_u64()
    );
    let (r, _) = fc_call(adt_scalar::builtins::fc_hashoidvectorextended, ctx.mcx(), [
        Datum::from_usize(img.as_ptr() as usize),
        Datum::from_i64(seed as i64),
    ]);
    let d = r.expect("hashoidvectorextended over valid headers never errors");
    assert!(
        d == Datum::from_u64(che),
        "hashoidvectorextended DIVERGENCE vals={vals:?} seed={seed:#x}: C {che:#x} Rust {:#x}",
        d.as_u64()
    );
}

// ---------------------------------------------------------------------------
// Round-3 datum_ops arms (see module header).
// ---------------------------------------------------------------------------

/// A constructed test datum: the Datum word, its type shape, and the backing
/// image the by-ref forms point into (kept alive by the caller).
struct DxDatum {
    value: Datum,
    byval: bool,
    typlen: i16,
    backing: Vec<u8>,
}

/// Decode one datum from (kind, aux, bytes). Headers are constructed
/// well-formed (module-header fence); returns None only on empty byval input
/// shortage (zero-padded instead, so effectively always Some).
fn dx_build(kind: u8, aux: u8, bytes: &[u8]) -> DxDatum {
    match kind % 7 {
        k @ 0..=3 => {
            let typlen = [1i16, 2, 4, 8][k as usize];
            let mut w = [0u8; 8];
            for (i, b) in bytes.iter().take(8).enumerate() {
                w[i] = *b;
            }
            DxDatum { value: Datum::from_u64(u64::from_le_bytes(w)), byval: true, typlen, backing: Vec::new() }
        }
        4 => {
            let typlen = 1 + (aux % 32) as i16;
            let mut backing = vec![0u8; typlen as usize];
            for (i, b) in bytes.iter().take(typlen as usize).enumerate() {
                backing[i] = *b;
            }
            let value = Datum::from_usize(backing.as_ptr() as usize);
            DxDatum { value, byval: false, typlen, backing }
        }
        5 => {
            let backing = if aux & 1 == 1 {
                // 1-byte header varlena: total = 1 + k <= 127
                let k = bytes.len().min(126);
                let total = 1 + k;
                let mut v = Vec::with_capacity(total);
                v.push(((total as u8) << 1) | 1);
                v.extend_from_slice(&bytes[..k]);
                v
            } else {
                // 4-byte header varlena (uncompressed): total = 4 + k
                let k = bytes.len().min(500);
                let total = 4 + k;
                let mut v = Vec::with_capacity(total);
                v.extend_from_slice(&(((total as u32) << 2).to_ne_bytes()));
                v.extend_from_slice(&bytes[..k]);
                v
            };
            let value = Datum::from_usize(backing.as_ptr() as usize);
            DxDatum { value, byval: false, typlen: -1, backing }
        }
        _ => {
            // cstring: interior NULs stripped, NUL-terminated
            let mut backing: Vec<u8> =
                bytes.iter().copied().filter(|&b| b != 0).take(500).collect();
            backing.push(0);
            let value = Datum::from_usize(backing.as_ptr() as usize);
            DxDatum { value, byval: false, typlen: -2, backing }
        }
    }
}

/// Datum-family error classifier matching the C shim's classes:
/// 1 = data exception (invalid Datum pointer), 2 = invalid-typLen internal.
fn dx_err_class(e: &PgError) -> i32 {
    if e.sqlstate == types_error::ERRCODE_DATA_EXCEPTION {
        1
    } else {
        2
    }
}

fn dx_assert_no_eoh(arm: &str) {
    // SAFETY: plain TLS int read.
    assert!(unsafe { pg_dx_eoh_reached() } == 0, "{arm}: C EOH trap fired on a fenced input");
}

/// Value+verdict compare of Rust datum_get_size vs C; returns the agreed
/// size (None = agreed error).
fn dx_get_size_diff(d: &DxDatum, arm: &str) -> Option<usize> {
    let mut csz = 0usize;
    // SAFETY: value backing lives in d; out is a plain scalar.
    let cst =
        unsafe { pg_dx_get_size(d.value.as_usize(), d.byval as i32, d.typlen as i32, &mut csz) };
    dx_assert_no_eoh(arm);
    match adt_scalar::datum_ops::datum_get_size(d.value, d.byval, d.typlen) {
        Ok(sz) => {
            assert!(
                cst == 0 && sz == csz,
                "{arm} datum_get_size DIVERGENCE (byval {} typlen {}): C=(st {cst}, {csz}) Rust=Ok({sz})",
                d.byval,
                d.typlen
            );
            Some(sz)
        }
        Err(ref e) => {
            assert!(
                cst == dx_err_class(e),
                "{arm} datum_get_size DIVERGENCE (byval {} typlen {}): C st {cst} vs Rust class {}",
                d.byval,
                d.typlen,
                dx_err_class(e)
            );
            None
        }
    }
}

/// Arm 49: get_size + is_equal + copy + transfer over a decoded datum pair.
fn datum_pair_diff(payload: &[u8]) {
    if payload.len() < 2 {
        return;
    }
    let (kind, aux, rest) = (payload[0], payload[1], &payload[2..]);
    if rest.len() > 1000 {
        return; // keep C bump-heap headroom (cap 4096)
    }
    let half = rest.len() / 2;
    let d1 = dx_build(kind, aux, &rest[..half]);
    let d2 = dx_build(kind, aux, &rest[half..]);

    let sz1 = dx_get_size_diff(&d1, "arm49");
    let _ = dx_get_size_diff(&d2, "arm49");

    // is_equal
    let mut cres = 0i32;
    // SAFETY: both backings live; out plain scalar.
    let cst = unsafe {
        pg_dx_is_equal(d1.value.as_usize(), d2.value.as_usize(), d1.byval as i32, d1.typlen as i32, &mut cres)
    };
    dx_assert_no_eoh("arm49.is_equal");
    match adt_scalar::datum_ops::datum_is_equal(d1.value, d2.value, d1.byval, d1.typlen) {
        Ok(b) => assert!(
            cst == 0 && b == (cres != 0),
            "datum_is_equal DIVERGENCE (byval {} typlen {}): C=(st {cst}, {cres}) Rust={b}",
            d1.byval,
            d1.typlen
        ),
        Err(ref e) => assert!(
            cst == dx_err_class(e),
            "datum_is_equal verdict DIVERGENCE: C st {cst} vs Rust class {}",
            dx_err_class(e)
        ),
    }

    // copy + transfer (fenced domain: transfer == copy semantics)
    for (name, transfer) in [("datum_copy", false), ("datum_transfer", true)] {
        let mut coutval = 0usize;
        let mut cbytes = [0u8; 1024];
        let mut clen = 0usize;
        // SAFETY: backing lives; out buffer sized above the 1000-byte cap.
        let cst = unsafe {
            if transfer {
                pg_dx_transfer(d1.value.as_usize(), d1.byval as i32, d1.typlen as i32, &mut coutval, cbytes.as_mut_ptr(), 1024, &mut clen)
            } else {
                pg_dx_copy(d1.value.as_usize(), d1.byval as i32, d1.typlen as i32, &mut coutval, cbytes.as_mut_ptr(), 1024, &mut clen)
            }
        };
        dx_assert_no_eoh(name);
        assert!(cst != 100, "{name}: C driver refused under the input cap");
        let ctx = mcx::MemoryContext::new("scalarxid_diff.datum_copy");
        let r = if transfer {
            adt_scalar::datum_ops::datum_transfer(ctx.mcx(), d1.value, d1.byval, d1.typlen)
        } else {
            adt_scalar::datum_ops::datum_copy(ctx.mcx(), d1.value, d1.byval, d1.typlen)
        };
        match r {
            Ok(d) => {
                assert!(cst == 0, "{name} DIVERGENCE: C st {cst}, Rust Ok");
                if d1.byval {
                    assert!(
                        d == d1.value && d.as_usize() == coutval,
                        "{name} byval DIVERGENCE (typlen {})",
                        d1.typlen
                    );
                } else {
                    let sz = sz1.expect("copy succeeded so get_size agreed Ok");
                    let got = datum_bytes(d, sz);
                    assert!(
                        clen == sz && got == &cbytes[..clen] && got == &d1.backing[..sz.min(d1.backing.len())],
                        "{name} byref DIVERGENCE (typlen {}): C len {clen} vs Rust len {sz}",
                        d1.typlen
                    );
                }
            }
            Err(ref e) => assert!(
                cst == dx_err_class(e),
                "{name} verdict DIVERGENCE: C st {cst} vs Rust class {}",
                dx_err_class(e)
            ),
        }
    }
}

/// Arm 50: estimate_space + serialize image parity + cross restore.
fn datum_serde_diff(payload: &[u8]) {
    if payload.len() < 3 {
        return;
    }
    let (kind, aux, flags, rest) = (payload[0], payload[1], payload[2], &payload[3..]);
    if rest.len() > 1000 {
        return;
    }
    let isnull = flags & 1 == 1;
    let d = dx_build(kind, aux, rest);

    // estimate_space
    let mut cest = 0usize;
    // SAFETY: backing lives; out plain scalar.
    let cst = unsafe {
        pg_dx_estimate_space(d.value.as_usize(), isnull as i32, d.byval as i32, d.typlen as i32, &mut cest)
    };
    dx_assert_no_eoh("arm50.estimate");
    let rest_est =
        adt_scalar::datum_ops::datum_estimate_space(d.value, isnull, d.byval, d.typlen);
    match rest_est {
        Ok(sz) => assert!(
            cst == 0 && sz == cest,
            "datum_estimate_space DIVERGENCE (isnull {isnull} byval {} typlen {}): C=(st {cst}, {cest}) Rust=Ok({sz})",
            d.byval,
            d.typlen
        ),
        Err(ref e) => {
            assert!(
                cst == dx_err_class(e),
                "datum_estimate_space verdict DIVERGENCE: C st {cst} vs Rust class {}",
                dx_err_class(e)
            );
            return; // both error: serialize would error identically via get_size
        }
    }

    // serialize: byte-exact image parity
    let mut cimg = [0u8; 1600];
    let mut clen = 0usize;
    // SAFETY: backing lives; image buffer sized above estimate cap.
    let cst = unsafe {
        pg_dx_serialize(d.value.as_usize(), isnull as i32, d.byval as i32, d.typlen as i32, cimg.as_mut_ptr(), 1600, &mut clen)
    };
    dx_assert_no_eoh("arm50.serialize");
    assert!(cst != 100, "serialize: C driver refused under the input cap");
    let ctx = mcx::MemoryContext::new("scalarxid_diff.datum_serde");
    let mut out: mcx::PgVec<u8> = match mcx::vec_with_capacity_in(ctx.mcx(), 1600) {
        Ok(v) => v,
        Err(_) => return,
    };
    match adt_scalar::datum_ops::datum_serialize(d.value, isnull, d.byval, d.typlen, &mut out) {
        Ok(()) => {
            assert!(
                cst == 0 && out.as_slice() == &cimg[..clen],
                "datum_serialize IMAGE DIVERGENCE (isnull {isnull} byval {} typlen {}): C {} bytes vs Rust {} bytes",
                d.byval,
                d.typlen,
                clen,
                out.len()
            );
            assert!(
                out.len() == cest,
                "datum_serialize wrote {} bytes but estimate said {cest}",
                out.len()
            );
        }
        Err(ref e) => {
            assert!(
                cst == dx_err_class(e),
                "datum_serialize verdict DIVERGENCE: C st {cst} vs Rust class {}",
                dx_err_class(e)
            );
            return;
        }
    }

    // cross restore: Rust restores the C image...
    let mut cursor: &[u8] = &cimg[..clen];
    let (rv, rnull) = adt_scalar::datum_ops::datum_restore(ctx.mcx(), &mut cursor)
        .expect("datum_restore of a well-formed image never errors");
    assert!(cursor.is_empty(), "datum_restore left {} unconsumed bytes", cursor.len());
    assert!(rnull == isnull, "datum_restore isnull DIVERGENCE: {rnull} vs {isnull}");
    if !isnull {
        if d.byval {
            assert!(rv == d.value, "datum_restore byval value DIVERGENCE");
        } else {
            let sz = adt_scalar::datum_ops::datum_get_size(d.value, d.byval, d.typlen).unwrap();
            assert!(
                datum_bytes(rv, sz) == &d.backing[..sz.min(d.backing.len())],
                "datum_restore byref payload DIVERGENCE (typlen {})",
                d.typlen
            );
        }
    }
    // ...and C restores the Rust image.
    let mut cnull = 0i32;
    let mut coutval = 0usize;
    let mut cbytes = [0u8; 1024];
    let mut cblen = 0usize;
    // SAFETY: out buffers sized above the caps.
    let consumed = unsafe {
        pg_dx_restore(out.as_ptr(), &mut cnull, &mut coutval, cbytes.as_mut_ptr(), 1024, &mut cblen)
    };
    assert!(consumed == out.len(), "C datumRestore consumed {consumed} of {}", out.len());
    assert!((cnull != 0) == isnull, "C datumRestore isnull DIVERGENCE");
    if !isnull {
        if d.byval {
            assert!(coutval == d.value.as_usize(), "C datumRestore byval DIVERGENCE");
        } else {
            let sz = adt_scalar::datum_ops::datum_get_size(d.value, d.byval, d.typlen).unwrap();
            assert!(
                cblen == sz && &cbytes[..cblen] == &d.backing[..sz.min(d.backing.len())],
                "C datumRestore byref DIVERGENCE"
            );
        }
    }
}

/// Arm 51: error arms — NULL pointer / invalid typlen, verdict+class plane.
fn datum_err_diff(payload: &[u8]) {
    let Some((&which, _rest)) = payload.split_first() else {
        return;
    };
    // (value, byval, typlen): NULL varlena, NULL cstring, invalid typlens
    let (value, typlen): (Datum, i16) = match which % 6 {
        0 => (Datum::null(), -1),
        1 => (Datum::null(), -2),
        2 => (Datum::null(), 0),
        3 => (Datum::null(), -3),
        4 => (Datum::null(), -4),
        _ => (Datum::null(), i16::MIN),
    };
    let d = DxDatum { value, byval: false, typlen, backing: Vec::new() };
    let _ = dx_get_size_diff(&d, "arm51");
    let mut cres = 0i32;
    // SAFETY: NULL value never dereferenced on datumIsEqual's error path
    // (the size check precedes the byte compare on both sides).
    let cst = unsafe { pg_dx_is_equal(value.as_usize(), value.as_usize(), 0, typlen as i32, &mut cres) };
    match adt_scalar::datum_ops::datum_is_equal(value, value, false, typlen) {
        Ok(_) => panic!("datum_is_equal(NULL/invalid typlen {typlen}) unexpectedly Ok"),
        Err(ref e) => assert!(cst == dx_err_class(e), "datum_is_equal error-class DIVERGENCE"),
    }
    // FENCE: with typlen == -1 and a NULL pointer, datumCopy /
    // datumEstimateSpace / datumSerialize dereference the pointer in the
    // expanded-header probe BEFORE any NULL check — in C and in the port
    // alike (a real-PG "can't happen" precondition, callers never pass
    // NULL varlenas there). Only get_size/is_equal guard first.
    if typlen == -1 {
        return;
    }
    // copy / estimate / serialize error paths (cstring NULL + invalid
    // typlens route the error through datumGetSize).
    let ctx = mcx::MemoryContext::new("scalarxid_diff.datum_err");
    let mut coutval = 0usize;
    let mut cbytes = [0u8; 16];
    let mut clen = 0usize;
    // SAFETY: plain scalars; NULL value never dereferenced on the error paths.
    let cst = unsafe {
        pg_dx_copy(value.as_usize(), 0, typlen as i32, &mut coutval, cbytes.as_mut_ptr(), 16, &mut clen)
    };
    match adt_scalar::datum_ops::datum_copy(ctx.mcx(), value, false, typlen) {
        Ok(_) => panic!("datum_copy(NULL/invalid typlen {typlen}) unexpectedly Ok (C st {cst})"),
        Err(ref e) => assert!(
            cst == dx_err_class(e),
            "datum_copy error-class DIVERGENCE (typlen {typlen}): C {cst} vs Rust {}",
            dx_err_class(e)
        ),
    }
    let mut cest = 0usize;
    // SAFETY: as above.
    let cst = unsafe { pg_dx_estimate_space(value.as_usize(), 0, 0, typlen as i32, &mut cest) };
    match adt_scalar::datum_ops::datum_estimate_space(value, false, false, typlen) {
        Ok(_) => panic!("datum_estimate_space(NULL/invalid typlen {typlen}) unexpectedly Ok"),
        Err(ref e) => {
            assert!(cst == dx_err_class(e), "datum_estimate_space error-class DIVERGENCE")
        }
    }
    let mut cimg = [0u8; 16];
    // SAFETY: as above.
    let cst = unsafe { pg_dx_serialize(value.as_usize(), 0, 0, typlen as i32, cimg.as_mut_ptr(), 16, &mut clen) };
    let mut out: mcx::PgVec<u8> = match mcx::vec_with_capacity_in(ctx.mcx(), 16) {
        Ok(v) => v,
        Err(_) => return,
    };
    match adt_scalar::datum_ops::datum_serialize(value, false, false, typlen, &mut out) {
        Ok(()) => panic!("datum_serialize(NULL/invalid typlen {typlen}) unexpectedly Ok"),
        Err(ref e) => assert!(cst == dx_err_class(e), "datum_serialize error-class DIVERGENCE"),
    }
    // NULL + isnull=true serializes fine on both sides (value never read).
    let mut clen2 = 0usize;
    // SAFETY: as above.
    let cst = unsafe { pg_dx_serialize(value.as_usize(), 1, 0, typlen as i32, cimg.as_mut_ptr(), 16, &mut clen2) };
    let mut out2: mcx::PgVec<u8> = match mcx::vec_with_capacity_in(ctx.mcx(), 16) {
        Ok(v) => v,
        Err(_) => return,
    };
    adt_scalar::datum_ops::datum_serialize(value, true, false, typlen, &mut out2)
        .expect("isnull serialize never reads the value");
    assert!(
        cst == 0 && out2.as_slice() == &cimg[..clen2],
        "isnull datum_serialize IMAGE DIVERGENCE (typlen {typlen})"
    );
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
        let dir = concat!(env!("CARGO_MANIFEST_DIR"), "/../corpus/scalarxid_diff");
        let mut n = 0;
        for e in std::fs::read_dir(dir).expect("corpus/scalarxid_diff missing") {
            let p = e.unwrap().path();
            if p.is_file() && p.file_name().is_some_and(|f| f != ".gitkeep") {
                scalarxid_diff(&std::fs::read(&p).unwrap());
                n += 1;
            }
        }
        assert!(n >= 30, "expected >=30 seeds, found {n}");
    }

    fn run(sel: u8, payload: &[u8]) {
        let mut v = vec![sel];
        v.extend_from_slice(payload);
        scalarxid_diff(&v);
    }

    #[test]
    fn tidin_ok_and_error() {
        let _g = crate::c_oracle_serial();
        run(0, b"(12,34)");
        run(0, b"(4294967295,65535)");
        run(0, b"(-1,5)"); // block sign-extension acceptance arm
        run(0, b"(12,34"); // missing RDELIM
        run(0, b"nope");
        run(0, b"(999999999999999999999,1)"); // ERANGE arm
        run(0, b"(12,999999)"); // offset > USHRT_MAX
        run(0, b"(12,34)trailing");
        run(0, b"(,5)"); // banded (platform carve) — must not panic
        run(0, b"(5,)"); // banded
    }

    #[test]
    fn tidout_images() {
        let _g = crate::c_oracle_serial();
        run(1, &[0xff, 0xff, 0xff, 0xff, 0xff, 0xff]);
        run(1, &[0, 0, 0, 0, 0, 0]);
        run(1, &[57, 5, 0, 0, 7, 0]);
    }

    #[test]
    fn tid_cmp_family() {
        let _g = crate::c_oracle_serial();
        let p: Vec<u8> = [
            1u32.to_le_bytes().as_slice(),
            &2u16.to_le_bytes(),
            &1u32.to_le_bytes(),
            &3u16.to_le_bytes(),
        ]
        .concat();
        for sel in 2..=10 {
            run(sel, &p);
        }
        // equal pair exercises the tie arms of larger/smaller
        let q: Vec<u8> = [
            7u32.to_le_bytes().as_slice(),
            &7u16.to_le_bytes(),
            &7u32.to_le_bytes(),
            &7u16.to_le_bytes(),
        ]
        .concat();
        for sel in 2..=10 {
            run(sel, &q);
        }
    }

    #[test]
    fn xid_arms() {
        let _g = crate::c_oracle_serial();
        run(11, &u32::MAX.to_le_bytes());
        run(11, &0u32.to_le_bytes());
        run(12, &[1, 0, 0, 0, 1, 0, 0, 0]);
        run(13, &[1, 0, 0, 0, 2, 0, 0, 0]);
    }

    #[test]
    fn xid8_arms() {
        let _g = crate::c_oracle_serial();
        let p: Vec<u8> = [u64::MAX.to_le_bytes(), 1u64.to_le_bytes()].concat();
        let q: Vec<u8> = [5u64.to_le_bytes(), 5u64.to_le_bytes()].concat();
        for sel in 14..=22 {
            run(sel, &p);
            run(sel, &q);
        }
    }

    #[test]
    fn oid_arms() {
        let _g = crate::c_oracle_serial();
        let p = [5u32.to_le_bytes(), 6u32.to_le_bytes()].concat();
        for sel in 23..=30 {
            run(sel, &p);
        }
        run(31, b"4294967295");
        run(31, b"  42  ");
        run(31, b"0x1f"); // strtoul base-0 hex arm
        run(31, b"0x"); // bare 0x backtrack: parses 0, trailing 'x' errors
        run(31, b"010"); // octal arm
        run(31, b"-1"); // sign wraparound acceptance
        run(31, b"99999999999999999999"); // ERANGE
        run(31, b"4294967296"); // uint32 range-check arm (u64 plane accepts)
        run(31, b"junk");
        run(31, b"");
        run(32, &u32::MAX.to_le_bytes());
        run(32, &0u32.to_le_bytes());
    }

    #[test]
    fn round2_arms() {
        let _g = crate::c_oracle_serial();
        // xidin/cidin + xid8in text shapes
        for t in [&b"42"[..], b"0x1f", b"-1", b"4294967296", b"99999999999999999999", b"junk", b""] {
            run(35, t);
            run(38, t);
        }
        run(36, &u32::MAX.to_le_bytes()); // cidout
        run(36, &0u32.to_le_bytes());
        run(37, &[7, 0, 0, 0, 7, 0, 0, 0]); // cideq equal
        run(37, &[7, 0, 0, 0, 8, 0, 0, 0]);
        for s in [39, 40] {
            run(s, &u32::MAX.to_le_bytes());
            run(s, &1u32.to_le_bytes());
        }
        run(41, &u64::MAX.to_le_bytes());
        run(42, &[0xff, 0xff, 0xff, 0xff, 0xff, 0xff]); // tidsend
        run(42, &[57, 5, 0, 0, 7, 0]);
        // xid8toxid epoch-truncation witness pair: same lo32, different epoch
        run(43, &0x0000_0001_0000_0007u64.to_le_bytes());
        run(43, &0x0000_0002_0000_0007u64.to_le_bytes());
        run(43, &u64::MAX.to_le_bytes());
        // hash arms: value + extended with zero and nonzero seeds
        let mut p = 42u32.to_le_bytes().to_vec();
        p.extend_from_slice(&0u64.to_le_bytes());
        run(44, &p);
        let mut p = 42u32.to_le_bytes().to_vec();
        p.extend_from_slice(&0xdeadbeefu64.to_le_bytes());
        run(44, &p);
        // hashxid8 sign-fold arms: positive and negative i64 views
        let mut p = 5u64.to_le_bytes().to_vec();
        p.extend_from_slice(&1u64.to_le_bytes());
        run(45, &p);
        let mut p = u64::MAX.to_le_bytes().to_vec();
        p.extend_from_slice(&1u64.to_le_bytes());
        run(45, &p);
        let mut p = vec![57, 5, 0, 0, 7, 0];
        p.extend_from_slice(&3u64.to_le_bytes());
        run(46, &p);
        // oidvector cmp: equal, len-differ, elem-differ
        let mut p = vec![2u8];
        p.extend_from_slice(&1u32.to_le_bytes());
        p.extend_from_slice(&2u32.to_le_bytes());
        p.push(2u8);
        p.extend_from_slice(&1u32.to_le_bytes());
        p.extend_from_slice(&2u32.to_le_bytes());
        run(47, &p);
        let mut p = vec![2u8];
        p.extend_from_slice(&1u32.to_le_bytes());
        p.extend_from_slice(&2u32.to_le_bytes());
        p.push(1u8);
        p.extend_from_slice(&1u32.to_le_bytes());
        run(47, &p);
        let mut p = vec![2u8];
        p.extend_from_slice(&1u32.to_le_bytes());
        p.extend_from_slice(&3u32.to_le_bytes());
        p.push(2u8);
        p.extend_from_slice(&1u32.to_le_bytes());
        p.extend_from_slice(&2u32.to_le_bytes());
        run(47, &p);
        // hashoidvector: empty, 1, and word-unaligned-length vectors + seed
        for n in [0u8, 1, 3, 5] {
            let mut p = vec![n];
            for i in 0..n {
                p.extend_from_slice(&(i as u32 + 1).to_le_bytes());
            }
            p.extend_from_slice(&7u64.to_le_bytes());
            run(48, &p);
        }
    }

    #[test]
    fn datum_ops_arms() {
        let _g = crate::c_oracle_serial();
        // arm 49: every kind, pair halves equal and differing
        for kind in 0..7u8 {
            let mut p = vec![kind, 0];
            p.extend_from_slice(&[1, 2, 3, 4, 5, 6, 7, 8, 1, 2, 3, 4, 5, 6, 7, 8]); // equal halves
            run(49, &p);
            let mut p = vec![kind, 1];
            p.extend_from_slice(&[1, 2, 3, 4, 5, 6, 7, 8, 9, 2, 3, 4, 5, 6, 7, 8]); // differ byte 0
            run(49, &p);
        }
        // size-mismatch is_equal pair (varlena halves of unequal length)
        let mut p = vec![5u8, 0];
        p.extend_from_slice(&[1, 2, 3, 4, 5]); // halves: 2 and 3 bytes
        run(49, &p);
        // arm 50: every kind x isnull
        for kind in 0..7u8 {
            for flags in [0u8, 1] {
                let mut p = vec![kind, 0, flags];
                p.extend_from_slice(b"payload-bytes");
                run(50, &p);
                let mut p = vec![kind, 1, flags]; // 1B varlena variant / odd aux
                p.extend_from_slice(b"x");
                run(50, &p);
            }
        }
        // empty-payload varlena (total == header only) + empty cstring
        run(50, &[5, 0, 0]);
        run(50, &[5, 1, 0]);
        run(50, &[6, 0, 0]);
        // arm 51: all six error shapes
        for which in 0..6u8 {
            run(51, &[which]);
        }
    }

    #[test]
    fn oidvector_arms() {
        let _g = crate::c_oracle_serial();
        run(33, b"1 2 3");
        run(33, b"");
        run(33, b"  42  ");
        run(33, b"1 junk");
        run(33, b"1 99999999999999999999");
        run(33, b"0x10 010 -1");
        let mut p = vec![3u8];
        p.extend_from_slice(&1u32.to_le_bytes());
        p.extend_from_slice(&u32::MAX.to_le_bytes());
        p.extend_from_slice(&0u32.to_le_bytes());
        run(34, &p);
        run(34, &[0]); // n = 0: empty vector image
    }
}
