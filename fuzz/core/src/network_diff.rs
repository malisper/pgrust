//! network_diff: differential fuzz driver — shipped Rust `adt_network` vs
//! vendored PostgreSQL 18.3 (Stamp-18.3, upstream sha 62d6c7d3df) C
//! (csrc/pg_network_io.c). Crate under test: crates/backend/utils/adt/network.
//!
//! Comparison planes (float_in_diff conventions): value bytes/struct
//! (family, bits, all 16 address bytes), error-verdict, and
//! errcode/sqlstate class. Message text is out of scope.
//!
//! Input layout: [selector][payload]; selector % 32 picks the arm.
//! TEXT arms (payload = raw bytes, truncated at the first NUL — C input is
//! a cstring — then read through String::from_utf8_lossy, exactly what the
//! shipped fc_inet_in wrapper does with wire bytes; C parses the raw bytes.
//! For valid UTF-8 the two views are identical; for invalid UTF-8 the
//! replacement char can never turn a C-rejected input into a Rust-accepted
//! one, and the comparator catches it if it somehow does):
//!   0 inet_in  (oid 910)    1 cidr_in (oid 1267)
//! BINARY arms (payload = one or two FENCED inet values; an inet value is
//! decoded from 18 bytes as [family-select][bits][addr[16]] with
//! family = AF_INET/AF_INET6 by bit 0 and bits fenced to 0..=maxbits by
//! modulo — C has undefined behavior on malformed inet structs, and real
//! Datums always satisfy this invariant, so the comparison domain is
//! exactly the valid-value plane; same producer-invariant fence the
//! proofs/network harnesses use):
//!   2 inet_out (911)        3 cidr_out (1427)      4 inet_abbrev (598)
//!   5 cidr_abbrev (599)     6 network_host (699)   7 network_show (730)
//!   8 network_cmp (926)     9 inet_set_masklen (605, + i32 LE)
//!   10 cidr_set_masklen (635, + i32 LE)            11 network_network (683)
//!   12 network_netmask (696)                       13 network_broadcast (698)
//!   14 network_hostmask (1362)                     15 inet_to_cidr (1715)
//!   16 inet_merge (4063)    17 inet_same_family (4071)
//!   18 inetand (2628)       19 inetor (2629)       20 inetnot (2627)
//!   21 inetpl (2630, + i64 LE)   22 inetmi_int8 (2632, + i64 LE)
//!   23 inetmi (2633)
//!   24 network_abbrev_convert (sortsupport key kernel, abbrev.rs — no oid;
//!      compared against the verbatim network.c key computation)
//! ROUND-2 arms (SECTION 6 of the oracle):
//!   25 inet_recv (2496, payload = raw wire bytes)  26 cidr_recv (2498)
//!   27 inet_send/cidr_send (2497/2499; flag = payload bit 1 of byte 0)
//!   28 network_lt/le/eq/ge/gt/ne + smaller/larger (921/922/920/924/923/925,
//!      3563/3562)
//!   29 network_sub/subeq/sup/supeq/overlap (927/928/929/930/3551)
//!   30 network_family/masklen (711/697) + convert_network_to_scalar (no
//!      oid) + hashinet/hashinetextended WRAPPER-FOLDS (422/779 — kernel
//!      differential owned by proofs/hash; lane-0a pg_lsn precedent)
//!   31 network_scan_first/last (selfuncs support, no oids)
//!
//! FC-WRAPPER PLANE: each arm additionally routes its (already core-vs-C
//! checked) input through the crate's builtins.rs fc_* wrapper via a native
//! types_fmgr::LocalFcinfo frame over a real SET_INET_VARSIZE varlena image
//! and asserts wrapper == core (Datum value / returned bytes incl. the
//! varlena/text headers / error verdict + sqlstate). C-parity keeps being
//! carried by the core comparison; the plane makes the wrapper lines
//! execute every iteration with an in-harness oracle.
//!
//! SKIPPED rows (crate functions NOT in this target, with reasons):
//!   - (round 2 moved the comparison family, recv/send, masklen/family and
//!     the hash wrapper-folds IN — arms 25-31 above. All of these are also
//!     proved at the shipped entry points per the ledger; the fuzz arms add
//!     the measured-coverage axis.)
//!   - network_subset_support (1173): blocked — planner support node, the
//!     IndexCondition arm is unported (ledger row).
//!   - inet_client/server_addr/port (2196-2199): blocked — libc
//!     getnameinfo FFI over MyProcPort session state (ledger rows).
//!   - abbrev.rs NetworkAbbrevState::abort: hyperLogLog cardinality
//!     heuristic (stateful sortsupport estimation, not a value function);
//!     the key kernel `convert` IS covered (arm 24).
//!
//! DIVERGENCE POLICY (row-436 macaddr precedent): if C accepts what Rust
//! rejects or vice versa, the panic below IS the finding — capture the
//! repro, replay against docker postgres:18.3, and record for
//! ratification; never silently patch either side to agree.

use std::ffi::c_char;

use adt_network::{InetRef, InetValue, INET_OUT_BUFLEN, PGSQL_AF_INET, PGSQL_AF_INET6};
use datum::{Datum, NullableDatum};
use types_error::{
    PgError, PgResult, ERRCODE_INTERNAL_ERROR, ERRCODE_INVALID_BINARY_REPRESENTATION,
    ERRCODE_INVALID_PARAMETER_VALUE, ERRCODE_INVALID_TEXT_REPRESENTATION,
    ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE,
};
use types_fmgr::{LocalFcinfo, PGFunction};

extern "C" {
    // csrc/pg_network_io.c driver entries. Inet values travel flat as
    // (family, bits, addr[16]); status 0 = ok, >0 = errcode class; text
    // entries return strlen >= 0 or -(errcode class).
    fn pg_diff_inet_in(src: *const c_char, ofam: *mut u8, obits: *mut u8, oaddr: *mut u8) -> i32;
    fn pg_diff_cidr_in(src: *const c_char, ofam: *mut u8, obits: *mut u8, oaddr: *mut u8) -> i32;
    fn pg_diff_inet_out(fam: u8, bits: u8, addr: *const u8, out: *mut c_char) -> i32;
    fn pg_diff_cidr_out(fam: u8, bits: u8, addr: *const u8, out: *mut c_char) -> i32;
    fn pg_diff_inet_abbrev(fam: u8, bits: u8, addr: *const u8, out: *mut c_char) -> i32;
    fn pg_diff_cidr_abbrev(fam: u8, bits: u8, addr: *const u8, out: *mut c_char) -> i32;
    fn pg_diff_network_host(fam: u8, bits: u8, addr: *const u8, out: *mut c_char) -> i32;
    fn pg_diff_network_show(fam: u8, bits: u8, addr: *const u8, out: *mut c_char) -> i32;
    fn pg_diff_network_cmp(f1: u8, b1: u8, a1: *const u8, f2: u8, b2: u8, a2: *const u8) -> i32;
    fn pg_diff_inet_set_masklen(
        fam: u8,
        abits: u8,
        addr: *const u8,
        bits: i32,
        ofam: *mut u8,
        obits: *mut u8,
        oaddr: *mut u8,
    ) -> i32;
    fn pg_diff_cidr_set_masklen(
        fam: u8,
        abits: u8,
        addr: *const u8,
        bits: i32,
        ofam: *mut u8,
        obits: *mut u8,
        oaddr: *mut u8,
    ) -> i32;
    fn pg_diff_inet_to_cidr(
        fam: u8,
        abits: u8,
        addr: *const u8,
        ofam: *mut u8,
        obits: *mut u8,
        oaddr: *mut u8,
    ) -> i32;
    fn pg_diff_network_network(
        fam: u8,
        abits: u8,
        addr: *const u8,
        ofam: *mut u8,
        obits: *mut u8,
        oaddr: *mut u8,
    ) -> i32;
    fn pg_diff_network_netmask(
        fam: u8,
        abits: u8,
        addr: *const u8,
        ofam: *mut u8,
        obits: *mut u8,
        oaddr: *mut u8,
    ) -> i32;
    fn pg_diff_network_broadcast(
        fam: u8,
        abits: u8,
        addr: *const u8,
        ofam: *mut u8,
        obits: *mut u8,
        oaddr: *mut u8,
    ) -> i32;
    fn pg_diff_network_hostmask(
        fam: u8,
        abits: u8,
        addr: *const u8,
        ofam: *mut u8,
        obits: *mut u8,
        oaddr: *mut u8,
    ) -> i32;
    fn pg_diff_inet_same_family(
        f1: u8,
        b1: u8,
        a1: *const u8,
        f2: u8,
        b2: u8,
        a2: *const u8,
    ) -> i32;
    fn pg_diff_inet_merge(
        f1: u8,
        b1: u8,
        a1: *const u8,
        f2: u8,
        b2: u8,
        a2: *const u8,
        ofam: *mut u8,
        obits: *mut u8,
        oaddr: *mut u8,
    ) -> i32;
    fn pg_diff_inetnot(
        fam: u8,
        abits: u8,
        addr: *const u8,
        ofam: *mut u8,
        obits: *mut u8,
        oaddr: *mut u8,
    ) -> i32;
    fn pg_diff_inetand(
        f1: u8,
        b1: u8,
        a1: *const u8,
        f2: u8,
        b2: u8,
        a2: *const u8,
        ofam: *mut u8,
        obits: *mut u8,
        oaddr: *mut u8,
    ) -> i32;
    fn pg_diff_inetor(
        f1: u8,
        b1: u8,
        a1: *const u8,
        f2: u8,
        b2: u8,
        a2: *const u8,
        ofam: *mut u8,
        obits: *mut u8,
        oaddr: *mut u8,
    ) -> i32;
    fn pg_diff_inetpl(
        fam: u8,
        abits: u8,
        addr: *const u8,
        addend: i64,
        ofam: *mut u8,
        obits: *mut u8,
        oaddr: *mut u8,
    ) -> i32;
    fn pg_diff_inetmi_int8(
        fam: u8,
        abits: u8,
        addr: *const u8,
        addend: i64,
        ofam: *mut u8,
        obits: *mut u8,
        oaddr: *mut u8,
    ) -> i32;
    fn pg_diff_inetmi(
        f1: u8,
        b1: u8,
        a1: *const u8,
        f2: u8,
        b2: u8,
        a2: *const u8,
        ores: *mut i64,
    ) -> i32;
    fn pg_diff_network_abbrev_convert(fam: u8, abits: u8, addr: *const u8) -> u64;
    // SECTION 6 entries (round 2): recv/send + comparison family + selfuncs.
    fn pg_diff_network_recv(
        msg: *const u8,
        msglen: i32,
        is_cidr: i32,
        consumed: *mut i32,
        ofam: *mut u8,
        obits: *mut u8,
        oaddr: *mut u8,
    ) -> i32;
    fn pg_diff_network_send(fam: u8, bits: u8, addr: *const u8, is_cidr: i32, out: *mut u8)
        -> i32;
    fn pg_diff_network_relop(
        f1: u8,
        b1: u8,
        a1: *const u8,
        f2: u8,
        b2: u8,
        a2: *const u8,
        op: i32,
    ) -> i32;
    fn pg_diff_network_smaller(f1: u8, b1: u8, a1: *const u8, f2: u8, b2: u8, a2: *const u8)
        -> i32;
    fn pg_diff_network_larger(f1: u8, b1: u8, a1: *const u8, f2: u8, b2: u8, a2: *const u8)
        -> i32;
    fn pg_diff_network_sub(f1: u8, b1: u8, a1: *const u8, f2: u8, b2: u8, a2: *const u8) -> i32;
    fn pg_diff_network_subeq(f1: u8, b1: u8, a1: *const u8, f2: u8, b2: u8, a2: *const u8)
        -> i32;
    fn pg_diff_network_sup(f1: u8, b1: u8, a1: *const u8, f2: u8, b2: u8, a2: *const u8) -> i32;
    fn pg_diff_network_supeq(f1: u8, b1: u8, a1: *const u8, f2: u8, b2: u8, a2: *const u8)
        -> i32;
    fn pg_diff_network_overlap(f1: u8, b1: u8, a1: *const u8, f2: u8, b2: u8, a2: *const u8)
        -> i32;
    fn pg_diff_network_family(fam: u8, bits: u8, addr: *const u8) -> i32;
    fn pg_diff_network_masklen(fam: u8, bits: u8, addr: *const u8) -> i32;
    fn pg_diff_convert_network_to_scalar(fam: u8, bits: u8, addr: *const u8) -> f64;
    fn pg_diff_network_scan_first(
        fam: u8,
        bits: u8,
        addr: *const u8,
        ofam: *mut u8,
        obits: *mut u8,
        oaddr: *mut u8,
    ) -> i32;
    fn pg_diff_network_scan_last(
        fam: u8,
        bits: u8,
        addr: *const u8,
        ofam: *mut u8,
        obits: *mut u8,
        oaddr: *mut u8,
    ) -> i32;

    /// SHIM-CONTRACT PROBES (read-only, never on a comparator path); see
    /// csrc/pg_network_io.c. `pg_network_pstrdup_len_probe` =
    /// `strlen(pstrdup(s))`, which mcxt.c:1711-1728's contract makes
    /// `== strlen(s)` for every `s`. `pg_network_msgbuf_slack` = bytes the
    /// allocation carries beyond strlen+1+GUARD (0 under exact sizing, -1
    /// before the first call).
    fn pg_network_pstrdup_len_probe(s: *const c_char) -> usize;
    fn pg_network_msgbuf_slack() -> i32;
    fn pg_network_msgbuf_check() -> i32;
}

// ---------------------------------------------------------------------------
// Errcode classes (must match csrc/pg_network_io.c header).
// ---------------------------------------------------------------------------

const CERR_INVALID_TEXT: i32 = 1; /* 22P02 */
const CERR_INVALID_PARAM: i32 = 2; /* 22023 */
const CERR_OUT_OF_RANGE: i32 = 3; /* 22003 */
const CERR_INVALID_BINARY: i32 = 4; /* 22P03 */
const CERR_INTERNAL: i32 = 5; /* XX000 (elog) */
const CERR_PROTOCOL: i32 = 6; /* 08P01 (pq_getmsgbyte past end) */

fn err_class(e: &PgError) -> i32 {
    if e.sqlstate == types_error::ERRCODE_PROTOCOL_VIOLATION {
        return CERR_PROTOCOL;
    }
    if e.sqlstate == ERRCODE_INVALID_TEXT_REPRESENTATION {
        CERR_INVALID_TEXT
    } else if e.sqlstate == ERRCODE_INVALID_PARAMETER_VALUE {
        CERR_INVALID_PARAM
    } else if e.sqlstate == ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE {
        CERR_OUT_OF_RANGE
    } else if e.sqlstate == ERRCODE_INVALID_BINARY_REPRESENTATION {
        CERR_INVALID_BINARY
    } else if e.sqlstate == ERRCODE_INTERNAL_ERROR {
        CERR_INTERNAL
    } else {
        99
    }
}

// ---------------------------------------------------------------------------
// Payload decoding: the valid-inet fence (see module header).
// ---------------------------------------------------------------------------

const INET_WIRE: usize = 18; /* [family-select][bits][addr; 16] */

fn fenced_inet(p: &[u8]) -> InetValue {
    let family = if p[0] & 1 == 0 { PGSQL_AF_INET } else { PGSQL_AF_INET6 };
    let maxbits: u16 = if family == PGSQL_AF_INET { 32 } else { 128 };
    let bits = (p[1] as u16 % (maxbits + 1)) as u8;
    let mut ipaddr = [0u8; 16];
    ipaddr.copy_from_slice(&p[2..18]);
    if family == PGSQL_AF_INET {
        // A real v4 inet datum carries exactly 4 address bytes — bytes past
        // addrsize do not exist in the varlena image, so they are zero in
        // every InetValue both sides can produce. Zeroing them here keeps
        // the fuzz domain on the value plane (fed unfenced, C's
        // whole-struct memcpy in inet_set_masklen would carry the junk
        // tail while Rust's to_value drops it — a harness artifact, not a
        // behavior difference).
        ipaddr[4..].fill(0);
    }
    InetValue { family, bits, ipaddr }
}

/// C oracle triple for an inet value.
fn flat(v: &InetValue) -> (u8, u8, *const u8) {
    (v.family, v.bits, v.ipaddr.as_ptr())
}

// ---------------------------------------------------------------------------
// fc-wrapper plane plumbing (native LocalFcinfo, real mcx — the proofs
// wrapper-level pattern run without kani; same helpers as mac_diff.rs).
// ---------------------------------------------------------------------------

/// Invoke an fc_* wrapper over non-null args; returns its PgResult.
fn fc_call<const N: usize>(f: PGFunction, m: mcx::Mcx<'_>, args: [Datum; N]) -> PgResult<Datum> {
    let mut fcinfo = LocalFcinfo::<N>::new(0);
    // SAFETY: the context owning `m` outlives this single call (caller scope).
    unsafe { fcinfo.set_result_mcx(m) };
    for (i, a) in args.into_iter().enumerate() {
        fcinfo.args[i] = NullableDatum::value(a);
    }
    f(None, &mut fcinfo)
}

/// First `n` bytes behind a by-ref result Datum. Caller contract: `d` came
/// from a wrapper that returned an `n`-byte-or-longer allocation still live
/// in the arming context (or thread-local out scratch).
fn datum_bytes<'a>(d: Datum, n: usize) -> &'a [u8] {
    // SAFETY: caller contract above.
    unsafe { core::slice::from_raw_parts(d.as_usize() as *const u8, n) }
}

fn dptr(bytes: &[u8]) -> Datum {
    Datum::from_usize(bytes.as_ptr() as usize)
}

/// fc plane for an inet-returning wrapper: result must be the exact varlena
/// image of `expect` (header included).
fn fc_inet<const N: usize>(name: &str, f: PGFunction, args: [Datum; N], expect: &InetValue) {
    let cx = mcx::MemoryContext::new("network_fc");
    let d = fc_call::<N>(f, cx.mcx(), args).expect("inet wrapper cannot fail here");
    let (img, len) = expect.image();
    assert_eq!(datum_bytes(d, len), &img[..len], "{name} fc-plane DIVERGENCE");
}

/// fc plane for a wrapper whose core errored: wrapper must error with the
/// same sqlstate class.
fn fc_err<const N: usize>(name: &str, f: PGFunction, args: [Datum; N], class: i32) {
    let cx = mcx::MemoryContext::new("network_fc");
    match fc_call::<N>(f, cx.mcx(), args) {
        Ok(_) => panic!("{name} fc-plane DIVERGENCE: wrapper Ok where core erred (class {class})"),
        Err(e) => assert_eq!(err_class(&e), class, "{name} fc-plane error-class DIVERGENCE"),
    }
}

/// fc plane for a text-returning wrapper (host/show/abbrev family): result
/// is a 4B-header text varlena whose payload must equal `expect`.
fn fc_text<const N: usize>(name: &str, f: PGFunction, args: [Datum; N], expect: &[u8]) {
    let cx = mcx::MemoryContext::new("network_fc");
    let d = fc_call::<N>(f, cx.mcx(), args).expect("text wrapper cannot fail on fenced input");
    let total = datum::VARHDRSZ + expect.len();
    let img = datum_bytes(d, total);
    let hdr = datum::varlena::set_varsize_4b(total);
    assert!(
        img[..datum::VARHDRSZ] == hdr && &img[datum::VARHDRSZ..] == expect,
        "{name} fc-plane DIVERGENCE: image={img:02x?} expect payload={:?}",
        std::str::from_utf8(expect)
    );
}

// ---------------------------------------------------------------------------
// Comparators.
// ---------------------------------------------------------------------------

/// Is `input`'s trailing /masklen digit string in the C int-wraparound zone
/// (true numeric value > i32::MAX)? See the DIVERGENCE(candidate) carve in
/// `check_inet`. All-digit suffix required — anything else never reaches C's
/// accumulation loop as a pure masklen.
fn masklen_wrap_zone(input: &str) -> bool {
    // The text arms pass the input debug-quoted ({s:?}) for printability;
    // strip the surrounding quotes before inspecting the masklen tail.
    let input = input.strip_suffix('"').unwrap_or(input);
    let Some((_, tail)) = input.rsplit_once('/') else {
        return false;
    };
    if tail.is_empty() || !tail.bytes().all(|b| b.is_ascii_digit()) {
        return false;
    }
    match tail.parse::<u64>() {
        Ok(v) => v > i32::MAX as u64,
        Err(_) => true, // > u64::MAX worth of digits: deep in the wrap zone
    }
}

/// Full-struct comparison of a C (status, triple) against a Rust PgResult.
fn check_inet(
    name: &str,
    input: &str,
    cst: i32,
    cval: (u8, u8, [u8; 16]),
    r: &PgResult<InetValue>,
) {
    match r {
        Ok(v) => assert!(
            cst == 0 && cval == (v.family, v.bits, v.ipaddr),
            "{name} DIVERGENCE input={input}: C=(st {cst}, fam {} bits {} addr {:02x?}) \
             Rust=Ok(fam {} bits {} addr {:02x?})",
            cval.0,
            cval.1,
            cval.2,
            v.family,
            v.bits,
            v.ipaddr
        ),
        Err(e) => {
            // DIVERGENCE(candidate) 2026-07-31, p1-lanen — masklen WRAP-ZONE
            // CARVE pending ratification (row-436 macaddr precedent): C's
            // inet_net_pton_ipv4 accumulates the /masklen digit string into a
            // signed int with NO overflow check (UB wraparound), so any digit
            // string whose true value exceeds i32::MAX can wrap back into
            // [0,32] and be silently ACCEPTED — ground-truthed on docker
            // postgres:18.3 (Debian glibc): '0.0.0.1/<50 zeros>2^64-1'::cidr
            // -> 0.0.0.1/32; '1.2.3.4/...4294967328'::inet -> 1.2.3.4.
            // pgrust saturates and rejects 22P02 (pton.rs:157). Ledger rows
            // 910/1267 annotated; repro banked seed-cidrin-wrap-e128bffa.
            // Carve = the wrap zone ONLY (masklen value > i32::MAX); inside
            // it Rust must still reject with 22P02-class.
            if cst == 0 && masklen_wrap_zone(input) {
                assert!(
                    err_class(e) != 0,
                    "{name}: wrap-zone carve expected a Rust rejection, got err class 0"
                );
                return;
            }
            assert!(
                cst == err_class(e),
                "{name} DIVERGENCE input={input}: C st {cst} vs Rust err class {} ({})",
                err_class(e),
                e.message
            )
        }
    }
}

fn hex(b: &[u8]) -> String {
    b.iter().map(|x| format!("{x:02x}")).collect()
}

// ---------------------------------------------------------------------------
// Arms.
// ---------------------------------------------------------------------------

fn in_diff(payload: &[u8], is_cidr: bool) {
    let name = if is_cidr { "cidr_in" } else { "inet_in" };
    // C input is a cstring: truncate at the first NUL; bound the length so
    // the C-side fixed buffer and per-exec copy cost stay small.
    let raw = match payload.iter().position(|&b| b == 0) {
        Some(n) => &payload[..n],
        None => payload,
    };
    if raw.len() > 200 {
        return;
    }
    let mut cbuf = [0u8; 201];
    cbuf[..raw.len()].copy_from_slice(raw);
    // The shipped wrapper reads the cstring through from_utf8_lossy; mirror it.
    let s = String::from_utf8_lossy(raw);

    let (mut f, mut b, mut a) = (0u8, 0u8, [0u8; 16]);
    let cst = unsafe {
        if is_cidr {
            pg_diff_cidr_in(cbuf.as_ptr().cast(), &mut f, &mut b, a.as_mut_ptr())
        } else {
            pg_diff_inet_in(cbuf.as_ptr().cast(), &mut f, &mut b, a.as_mut_ptr())
        }
    };

    // Core: hard-error shape (escontext = None), exactly what fc_*_in runs.
    let r: PgResult<InetValue> = adt_network::network_in(&s, is_cidr, None)
        .map(|o| o.expect("hard shape returns Some on Ok"));
    check_inet(name, &format!("{s:?}"), cst, (f, b, a), &r);

    // fc-wrapper plane over the same cstring bytes.
    let fcw = if is_cidr {
        adt_network::builtins::fc_cidr_in
    } else {
        adt_network::builtins::fc_inet_in
    };
    let cx = mcx::MemoryContext::new("network_fc");
    match fc_call::<1>(fcw, cx.mcx(), [dptr(&cbuf)]) {
        Ok(d) => {
            let v = r.as_ref().expect("wrapper Ok implies core Ok (just checked vs C)");
            let (img, len) = v.image();
            assert_eq!(
                datum_bytes(d, len),
                &img[..len],
                "fc_{name} vs core DIVERGENCE input={s:?}"
            );
        }
        Err(e) => {
            let ec = r.as_ref().expect_err("wrapper Err implies core Err");
            assert_eq!(
                err_class(&e),
                err_class(ec),
                "fc_{name} error-class DIVERGENCE input={s:?}"
            );
        }
    }
}

/// Text-producing unary arms: C entry + Rust core into-buffer + fc wrapper.
enum TextArm {
    InetOut,
    CidrOut,
    InetAbbrev,
    CidrAbbrev,
    Host,
    Show,
}

fn text_diff(p: &[u8], arm: TextArm) {
    let v = fenced_inet(p);
    let (f, b, a) = flat(&v);
    let mut cbuf = [0u8; 64];
    let cst = unsafe {
        match arm {
            TextArm::InetOut => pg_diff_inet_out(f, b, a, cbuf.as_mut_ptr().cast()),
            TextArm::CidrOut => pg_diff_cidr_out(f, b, a, cbuf.as_mut_ptr().cast()),
            TextArm::InetAbbrev => pg_diff_inet_abbrev(f, b, a, cbuf.as_mut_ptr().cast()),
            TextArm::CidrAbbrev => pg_diff_cidr_abbrev(f, b, a, cbuf.as_mut_ptr().cast()),
            TextArm::Host => pg_diff_network_host(f, b, a, cbuf.as_mut_ptr().cast()),
            TextArm::Show => pg_diff_network_show(f, b, a, cbuf.as_mut_ptr().cast()),
        }
    };
    let name = match arm {
        TextArm::InetOut => "inet_out",
        TextArm::CidrOut => "cidr_out",
        TextArm::InetAbbrev => "inet_abbrev",
        TextArm::CidrAbbrev => "cidr_abbrev",
        TextArm::Host => "network_host",
        TextArm::Show => "network_show",
    };

    let ir = v.iref();
    let mut rbuf = [0u8; INET_OUT_BUFLEN];
    let rr: PgResult<usize> = match arm {
        TextArm::InetOut => adt_network::network_out_into(ir, false, &mut rbuf),
        TextArm::CidrOut => adt_network::network_out_into(ir, true, &mut rbuf),
        TextArm::InetAbbrev => adt_network::inet_abbrev_into(ir, &mut rbuf),
        TextArm::CidrAbbrev => adt_network::cidr_abbrev_into(ir, &mut rbuf),
        TextArm::Host => adt_network::network_host_into(ir, &mut rbuf),
        TextArm::Show => adt_network::network_show_into(ir, &mut rbuf),
    };
    let input = format!("fam {} bits {} addr {}", v.family, v.bits, hex(&v.ipaddr));
    let rtext = match rr {
        Ok(rlen) => {
            assert!(
                cst >= 0 && cbuf[..cst as usize] == rbuf[..rlen],
                "{name} DIVERGENCE input=({input}): C=(st {cst}, {:?}) Rust={:?}",
                std::str::from_utf8(&cbuf[..cst.max(0) as usize]),
                std::str::from_utf8(&rbuf[..rlen])
            );
            &rbuf[..rlen]
        }
        Err(e) => {
            assert!(
                cst < 0 && -cst == err_class(&e),
                "{name} DIVERGENCE input=({input}): C st {cst} vs Rust err class {} ({})",
                err_class(&e),
                e.message
            );
            return; /* both errored, same class; no fc value plane */
        }
    };

    // fc-wrapper plane over the real varlena image.
    let (img, len) = v.image();
    let arg = dptr(&img[..len]);
    match arm {
        TextArm::InetOut | TextArm::CidrOut => {
            // cstring result: payload + NUL from the thread-local scratch.
            let fcw = if matches!(arm, TextArm::CidrOut) {
                adt_network::builtins::fc_cidr_out
            } else {
                adt_network::builtins::fc_inet_out
            };
            let cx = mcx::MemoryContext::new("network_fc");
            let d = fc_call::<1>(fcw, cx.mcx(), [arg])
                .expect("out wrapper cannot fail on fenced input");
            let got = datum_bytes(d, rtext.len() + 1);
            assert!(
                &got[..rtext.len()] == rtext && got[rtext.len()] == 0,
                "fc_{name} vs core DIVERGENCE input=({input})"
            );
        }
        TextArm::InetAbbrev => {
            fc_text::<1>("fc_inet_abbrev", adt_network::builtins::fc_inet_abbrev, [arg], rtext)
        }
        TextArm::CidrAbbrev => {
            fc_text::<1>("fc_cidr_abbrev", adt_network::builtins::fc_cidr_abbrev, [arg], rtext)
        }
        TextArm::Host => {
            fc_text::<1>("fc_network_host", adt_network::builtins::fc_network_host, [arg], rtext)
        }
        TextArm::Show => {
            fc_text::<1>("fc_network_show", adt_network::builtins::fc_network_show, [arg], rtext)
        }
    }
}

/// Unary inet->inet arms with no error path on the fenced domain.
fn unary_diff(
    p: &[u8],
    name: &str,
    centry: unsafe extern "C" fn(u8, u8, *const u8, *mut u8, *mut u8, *mut u8) -> i32,
    core: fn(InetRef<'_>) -> InetValue,
    fcw: PGFunction,
) {
    let v = fenced_inet(p);
    let (f, b, a) = flat(&v);
    let (mut of, mut ob, mut oa) = (0u8, 0u8, [0u8; 16]);
    let cst = unsafe { centry(f, b, a, &mut of, &mut ob, oa.as_mut_ptr()) };
    let r: PgResult<InetValue> = Ok(core(v.iref()));
    let input = format!("fam {} bits {} addr {}", v.family, v.bits, hex(&v.ipaddr));
    check_inet(name, &input, cst, (of, ob, oa), &r);

    let (img, len) = v.image();
    fc_inet::<1>(name, fcw, [dptr(&img[..len])], r.as_ref().unwrap());
}

/// Binary inet-pair arms returning inet (with the family-mismatch 22023 arm).
fn binary_inet_diff(
    p: &[u8],
    name: &str,
    centry: unsafe extern "C" fn(
        u8,
        u8,
        *const u8,
        u8,
        u8,
        *const u8,
        *mut u8,
        *mut u8,
        *mut u8,
    ) -> i32,
    core: fn(InetRef<'_>, InetRef<'_>) -> PgResult<InetValue>,
    fcw: PGFunction,
) {
    let v1 = fenced_inet(p);
    let v2 = fenced_inet(&p[INET_WIRE..]);
    let (f1, b1, a1) = flat(&v1);
    let (f2, b2, a2) = flat(&v2);
    let (mut of, mut ob, mut oa) = (0u8, 0u8, [0u8; 16]);
    let cst = unsafe { centry(f1, b1, a1, f2, b2, a2, &mut of, &mut ob, oa.as_mut_ptr()) };
    let r = core(v1.iref(), v2.iref());
    let input = format!(
        "a=(fam {} bits {} {}) b=(fam {} bits {} {})",
        v1.family,
        v1.bits,
        hex(&v1.ipaddr),
        v2.family,
        v2.bits,
        hex(&v2.ipaddr)
    );
    check_inet(name, &input, cst, (of, ob, oa), &r);

    let (img1, len1) = v1.image();
    let (img2, len2) = v2.image();
    let args = [dptr(&img1[..len1]), dptr(&img2[..len2])];
    match &r {
        Ok(v) => fc_inet::<2>(name, fcw, args, v),
        Err(e) => fc_err::<2>(name, fcw, args, err_class(e)),
    }
}

/// set_masklen arms (inet arg + i32 bits, 22023 error arm).
fn set_masklen_diff(p: &[u8], is_cidr: bool) {
    let name = if is_cidr { "cidr_set_masklen" } else { "inet_set_masklen" };
    let v = fenced_inet(p);
    let bits = i32::from_le_bytes(p[INET_WIRE..INET_WIRE + 4].try_into().unwrap());
    let (f, b, a) = flat(&v);
    let (mut of, mut ob, mut oa) = (0u8, 0u8, [0u8; 16]);
    let cst = unsafe {
        if is_cidr {
            pg_diff_cidr_set_masklen(f, b, a, bits, &mut of, &mut ob, oa.as_mut_ptr())
        } else {
            pg_diff_inet_set_masklen(f, b, a, bits, &mut of, &mut ob, oa.as_mut_ptr())
        }
    };
    let r = if is_cidr {
        adt_network::cidr_set_masklen(v.iref(), bits)
    } else {
        adt_network::inet_set_masklen(v.iref(), bits)
    };
    let input =
        format!("fam {} bits {} addr {} arg {}", v.family, v.bits, hex(&v.ipaddr), bits);
    check_inet(name, &input, cst, (of, ob, oa), &r);

    let (img, len) = v.image();
    let fcw = if is_cidr {
        adt_network::builtins::fc_cidr_set_masklen
    } else {
        adt_network::builtins::fc_inet_set_masklen
    };
    let args = [dptr(&img[..len]), Datum::from_i32(bits)];
    match &r {
        Ok(vv) => fc_inet::<2>(name, fcw, args, vv),
        Err(e) => fc_err::<2>(name, fcw, args, err_class(e)),
    }
}

/// inetpl / inetmi_int8 (inet arg + i64 addend, 22003 error arm).
fn pl_diff(p: &[u8], is_mi: bool) {
    let name = if is_mi { "inetmi_int8" } else { "inetpl" };
    let v = fenced_inet(p);
    let addend = i64::from_le_bytes(p[INET_WIRE..INET_WIRE + 8].try_into().unwrap());
    let (f, b, a) = flat(&v);
    let (mut of, mut ob, mut oa) = (0u8, 0u8, [0u8; 16]);
    let cst = unsafe {
        if is_mi {
            pg_diff_inetmi_int8(f, b, a, addend, &mut of, &mut ob, oa.as_mut_ptr())
        } else {
            pg_diff_inetpl(f, b, a, addend, &mut of, &mut ob, oa.as_mut_ptr())
        }
    };
    let r = adt_network::internal_inetpl(
        v.iref(),
        if is_mi { addend.wrapping_neg() } else { addend },
    );
    let input = format!(
        "fam {} bits {} addr {} addend {}",
        v.family,
        v.bits,
        hex(&v.ipaddr),
        addend
    );
    check_inet(name, &input, cst, (of, ob, oa), &r);

    let (img, len) = v.image();
    let fcw = if is_mi {
        adt_network::builtins::fc_inetmi_int8
    } else {
        adt_network::builtins::fc_inetpl
    };
    let args = [dptr(&img[..len]), Datum::from_i64(addend)];
    match &r {
        Ok(vv) => fc_inet::<2>(name, fcw, args, vv),
        Err(e) => fc_err::<2>(name, fcw, args, err_class(e)),
    }
}

fn cmp_diff(p: &[u8]) {
    let v1 = fenced_inet(p);
    let v2 = fenced_inet(&p[INET_WIRE..]);
    let (f1, b1, a1) = flat(&v1);
    let (f2, b2, a2) = flat(&v2);
    let c = unsafe { pg_diff_network_cmp(f1, b1, a1, f2, b2, a2) };
    let r = adt_network::network_cmp_internal(v1.iref(), v2.iref());
    // DIVERGENCE(candidate) 2026-07-31, p1-lanen — MAGNITUDE CARVE, sign-only
    // plane pending ratification: C network_cmp returns raw memcmp() output,
    // whose MAGNITUDE is platform-defined (glibc aarch64 sign-normalizes to
    // -1/0/1; macOS returns the first differing byte difference). pgrust
    // hard-codes the byte-difference convention (lib.rs memcmp), so it
    // matches macOS PG but NOT Linux PG: SELECT network_cmp over inets first
    // differing at 0x04 vs 0x84 -> PG 18.3 docker (Debian glibc) = -1,
    // pgrust = -128. Repro banked: fleet crash-b1064764f0b1 (job
    // pgrust-fuzz-campaign-1785480211-3e0c-59729), corpus seed
    // seed-cmp-div-b1064764. Ledger row 926 annotated divergence(candidate).
    // Only the SIGN is compared below: comparator MAGNITUDE is the
    // established non-surface class (multirange tie-representative ruling
    // 2026-07-31; pg_qsort/nbtcompare cmp conventions; btree_gin sign-only
    // plane) — only signum() is SQL-observable, and here C's own value is
    // libc-dependent. Flagged for explicit confirmation on the row-926
    // ledger entry. (fc plane further down still checks wrapper == core
    // EXACTLY — the carve is only C-vs-Rust magnitude.)
    let (cs, rs) = (c.signum(), r.signum());
    assert!(
        cs == rs,
        "network_cmp DIVERGENCE a=(fam {} bits {} {}) b=(fam {} bits {} {}): C={c} Rust={r}",
        v1.family,
        v1.bits,
        hex(&v1.ipaddr),
        v2.family,
        v2.bits,
        hex(&v2.ipaddr)
    );

    let (img1, len1) = v1.image();
    let (img2, len2) = v2.image();
    let cx = mcx::MemoryContext::new("network_fc");
    let d = fc_call::<2>(
        adt_network::builtins::fc_network_cmp,
        cx.mcx(),
        [dptr(&img1[..len1]), dptr(&img2[..len2])],
    )
    .expect("cmp wrapper cannot fail");
    assert_eq!(d.as_i32(), r, "fc_network_cmp vs core DIVERGENCE");
}

fn same_family_diff(p: &[u8]) {
    let v1 = fenced_inet(p);
    let v2 = fenced_inet(&p[INET_WIRE..]);
    let (f1, b1, a1) = flat(&v1);
    let (f2, b2, a2) = flat(&v2);
    let c = unsafe { pg_diff_inet_same_family(f1, b1, a1, f2, b2, a2) };
    let r = adt_network::inet_same_family(v1.iref(), v2.iref());
    assert!(
        (c != 0) == r,
        "inet_same_family DIVERGENCE fams=({}, {}): C={c} Rust={r}",
        v1.family,
        v2.family
    );

    let (img1, len1) = v1.image();
    let (img2, len2) = v2.image();
    let cx = mcx::MemoryContext::new("network_fc");
    let d = fc_call::<2>(
        adt_network::builtins::fc_inet_same_family,
        cx.mcx(),
        [dptr(&img1[..len1]), dptr(&img2[..len2])],
    )
    .expect("same_family wrapper cannot fail");
    assert_eq!(d.as_bool(), r, "fc_inet_same_family vs core DIVERGENCE");
}

fn inetmi_diff(p: &[u8]) {
    let v1 = fenced_inet(p);
    let v2 = fenced_inet(&p[INET_WIRE..]);
    let (f1, b1, a1) = flat(&v1);
    let (f2, b2, a2) = flat(&v2);
    let mut cres: i64 = 0;
    let cst = unsafe { pg_diff_inetmi(f1, b1, a1, f2, b2, a2, &mut cres) };
    let r = adt_network::inetmi(v1.iref(), v2.iref());
    let input = format!(
        "a=(fam {} {}) b=(fam {} {})",
        v1.family,
        hex(&v1.ipaddr),
        v2.family,
        hex(&v2.ipaddr)
    );
    match &r {
        Ok(rv) => assert!(
            cst == 0 && cres == *rv,
            "inetmi DIVERGENCE {input}: C=(st {cst}, {cres}) Rust=Ok({rv})"
        ),
        Err(e) => assert!(
            cst == err_class(e),
            "inetmi DIVERGENCE {input}: C st {cst} vs Rust err class {} ({})",
            err_class(e),
            e.message
        ),
    }

    let (img1, len1) = v1.image();
    let (img2, len2) = v2.image();
    let args = [dptr(&img1[..len1]), dptr(&img2[..len2])];
    let cx = mcx::MemoryContext::new("network_fc");
    match fc_call::<2>(adt_network::builtins::fc_inetmi, cx.mcx(), args) {
        Ok(d) => assert!(
            matches!(&r, Ok(rv) if d.as_i64() == *rv),
            "fc_inetmi vs core DIVERGENCE {input}"
        ),
        Err(e) => {
            let ec = r.as_ref().expect_err("fc_inetmi Err implies core Err");
            assert_eq!(
                err_class(&e),
                err_class(ec),
                "fc_inetmi error-class DIVERGENCE {input}"
            );
        }
    }
}

fn abbrev_convert_diff(p: &[u8]) {
    let v = fenced_inet(p);
    let (f, b, a) = flat(&v);
    let c = unsafe { pg_diff_network_abbrev_convert(f, b, a) };
    // Alternate the two constructors (Default is a distinct shipped line).
    let mut st = if p[1] & 1 == 0 {
        adt_network::abbrev::NetworkAbbrevState::new()
    } else {
        adt_network::abbrev::NetworkAbbrevState::default()
    };
    let r = st.convert(v.iref());
    assert!(
        c == r,
        "network_abbrev_convert DIVERGENCE fam {} bits {} addr {}: C={c:#018x} Rust={r:#018x}",
        v.family,
        v.bits,
        hex(&v.ipaddr)
    );
}

/// inet_to_cidr: unary but PgResult-returning core (its error arm is
/// unreachable on the fenced domain — both sides asserted equal anyway).
fn to_cidr_diff(p: &[u8]) {
    let v = fenced_inet(p);
    let (f, b, a) = flat(&v);
    let (mut of, mut ob, mut oa) = (0u8, 0u8, [0u8; 16]);
    let cst = unsafe { pg_diff_inet_to_cidr(f, b, a, &mut of, &mut ob, oa.as_mut_ptr()) };
    let r = adt_network::inet_to_cidr(v.iref());
    let input = format!("fam {} bits {} addr {}", v.family, v.bits, hex(&v.ipaddr));
    check_inet("inet_to_cidr", &input, cst, (of, ob, oa), &r);

    let (img, len) = v.image();
    let args = [dptr(&img[..len])];
    match &r {
        Ok(vv) => fc_inet::<1>("inet_to_cidr", fcb::fc_inet_to_cidr, args, vv),
        Err(e) => fc_err::<1>("inet_to_cidr", fcb::fc_inet_to_cidr, args, err_class(e)),
    }
}

// ---------------------------------------------------------------------------
// Round-2 arms: recv/send, comparison family, selfuncs helpers.
// ---------------------------------------------------------------------------

/// inet_recv (2496) / cidr_recv (2498): raw message bytes through both recv
/// paths. Planes: Ok struct + consumed byte count, err verdict + class
/// (22P03 binary, 08P01 protocol-violation past-end), fc-wrapper image.
fn recv_diff(p: &[u8], is_cidr: bool) {
    let name = if is_cidr { "cidr_recv" } else { "inet_recv" };
    if p.len() > 64 {
        return; /* C shim buffer bound; wire messages are <= 22 bytes */
    }
    let (mut of, mut ob, mut oa) = (0u8, 0u8, [0u8; 16]);
    let mut consumed: i32 = 0;
    let cst = unsafe {
        pg_diff_network_recv(
            p.as_ptr(),
            p.len() as i32,
            is_cidr as i32,
            &mut consumed,
            &mut of,
            &mut ob,
            oa.as_mut_ptr(),
        )
    };

    let cx = mcx::MemoryContext::new("network_recv");
    let mcx = cx.mcx();
    let Ok(mut vec) = mcx::vec_with_capacity_in::<u8>(mcx, p.len().max(1)) else {
        return;
    };
    if mcx::vec_append_bytes(&mut vec, p).is_err() {
        return;
    }
    let Ok(mut si) = stringinfo::StringInfo::from_vec(vec) else {
        return;
    };
    let r = adt_network::network_recv(&mut si, is_cidr);
    let input = format!("msg {} is_cidr {is_cidr}", hex(p));
    match &r {
        Ok(v) => {
            assert!(
                cst == 0 && (of, ob, oa) == (v.family, v.bits, v.ipaddr),
                "{name} DIVERGENCE {input}: C=(st {cst}, fam {of} bits {ob} {}) \
                 Rust=Ok(fam {} bits {} {})",
                hex(&oa),
                v.family,
                v.bits,
                hex(&v.ipaddr)
            );
            assert!(
                consumed as usize == si.cursor,
                "{name} consumed-length DIVERGENCE {input}: C={consumed} Rust={}",
                si.cursor
            );
        }
        Err(e) => assert!(
            cst == err_class(e),
            "{name} DIVERGENCE {input}: C st {cst} vs Rust err class {} ({})",
            err_class(e),
            e.message
        ),
    }

    // fc plane over a fresh StringInfo (the core call above consumed cursor).
    let Ok(mut vec2) = mcx::vec_with_capacity_in::<u8>(mcx, p.len().max(1)) else {
        return;
    };
    if mcx::vec_append_bytes(&mut vec2, p).is_err() {
        return;
    }
    let Ok(mut si2) = stringinfo::StringInfo::from_vec(vec2) else {
        return;
    };
    let fcw = if is_cidr { fcb::fc_cidr_recv } else { fcb::fc_inet_recv };
    let arg = Datum::from_usize(&mut si2 as *mut stringinfo::StringInfo as usize);
    let cx2 = mcx::MemoryContext::new("network_fc");
    match fc_call::<1>(fcw, cx2.mcx(), [arg]) {
        Ok(d) => {
            let v = r.as_ref().expect("fc {name} Ok implies core Ok");
            let (img, len) = v.image();
            assert_eq!(datum_bytes(d, len), &img[..len], "fc_{name} vs core DIVERGENCE {input}");
        }
        Err(e) => {
            let ec = r.as_ref().expect_err("fc {name} Err implies core Err");
            assert_eq!(
                err_class(&e),
                err_class(ec),
                "fc_{name} error-class DIVERGENCE {input}"
            );
        }
    }
}

/// inet_send (2497) / cidr_send (2499): fenced inet -> wire image, byte for
/// byte, both flag values; fc wrapper checks the full 4B-header varlena.
fn send_diff(p: &[u8]) {
    let v = fenced_inet(p);
    let is_cidr = p[0] & 2 != 0;
    let (f, b, a) = flat(&v);
    let mut cout = [0u8; 64];
    let clen = unsafe { pg_diff_network_send(f, b, a, is_cidr as i32, cout.as_mut_ptr()) };
    assert!(clen > 0, "network_send C oracle cannot fail");
    let input = format!("fam {} bits {} addr {} is_cidr {is_cidr}", f, b, hex(&v.ipaddr));

    let cx = mcx::MemoryContext::new("network_send");
    let by = adt_network::network_send(cx.mcx(), v.iref(), is_cidr)
        .expect("network_send infallible on fenced input");
    assert!(
        by.data() == &cout[..clen as usize],
        "network_send DIVERGENCE {input}: C={:02x?} Rust={:02x?}",
        &cout[..clen as usize],
        by.data()
    );

    // fc plane: full varlena image (bytea 4B header + payload).
    let (img, len) = v.image();
    let fcw = if is_cidr { fcb::fc_cidr_send } else { fcb::fc_inet_send };
    let total = datum::VARHDRSZ + clen as usize;
    let cx2 = mcx::MemoryContext::new("network_fc");
    let d = fc_call::<1>(fcw, cx2.mcx(), [dptr(&img[..len])])
        .expect("send wrapper cannot fail on fenced input");
    let got = datum_bytes(d, total);
    let hdr = datum::varlena::set_varsize_4b(total);
    assert!(
        got[..datum::VARHDRSZ] == hdr && &got[datum::VARHDRSZ..] == &cout[..clen as usize],
        "fc_{} vs C DIVERGENCE {input}: image={got:02x?}",
        if is_cidr { "cidr_send" } else { "inet_send" }
    );
}

/// network_lt/le/eq/ge/gt/ne (921/922/920/924/923/925) + smaller/larger
/// (3563/3562): verbatim C relational bodies vs the shipped cmp kernel, and
/// the fc bool wrappers + winning-input-datum identity. Sign-only semantics
/// throughout — the cmp MAGNITUDE carve (row 926) does not reach here.
fn relops_diff(p: &[u8]) {
    let v1 = fenced_inet(p);
    let v2 = fenced_inet(&p[INET_WIRE..]);
    let (f1, b1, a1) = flat(&v1);
    let (f2, b2, a2) = flat(&v2);
    let rc = adt_network::network_cmp_internal(v1.iref(), v2.iref());
    let input = format!(
        "a=(fam {} bits {} {}) b=(fam {} bits {} {})",
        v1.family,
        v1.bits,
        hex(&v1.ipaddr),
        v2.family,
        v2.bits,
        hex(&v2.ipaddr)
    );

    let (img1, len1) = v1.image();
    let (img2, len2) = v2.image();
    let args = [dptr(&img1[..len1]), dptr(&img2[..len2])];

    type RelOp = (i32, &'static str, fn(i32) -> bool, PGFunction);
    let ops: [RelOp; 6] = [
        (0, "network_lt", |c| c < 0, fcb::fc_network_lt as PGFunction),
        (1, "network_le", |c| c <= 0, fcb::fc_network_le),
        (2, "network_eq", |c| c == 0, fcb::fc_network_eq),
        (3, "network_ge", |c| c >= 0, fcb::fc_network_ge),
        (4, "network_gt", |c| c > 0, fcb::fc_network_gt),
        (5, "network_ne", |c| c != 0, fcb::fc_network_ne),
    ];
    for (op, name, rel, fcw) in ops {
        let c = unsafe { pg_diff_network_relop(f1, b1, a1, f2, b2, a2, op) };
        let r = rel(rc);
        assert!((c != 0) == r, "{name} DIVERGENCE {input}: C={c} Rust={r}");
        let cx = mcx::MemoryContext::new("network_fc");
        let d = fc_call::<2>(fcw, cx.mcx(), args).expect("relop wrapper cannot fail");
        assert_eq!(d.as_bool(), r, "fc_{name} vs core DIVERGENCE {input}");
    }

    // smaller/larger: C returns the winning input INDEX; the wrapper must
    // return the corresponding input datum ITSELF (pointer identity).
    let cs = unsafe { pg_diff_network_smaller(f1, b1, a1, f2, b2, a2) };
    let cl = unsafe { pg_diff_network_larger(f1, b1, a1, f2, b2, a2) };
    for (name, fcw, cidx) in [
        ("network_smaller", fcb::fc_network_smaller as PGFunction, cs),
        ("network_larger", fcb::fc_network_larger, cl),
    ] {
        let cx = mcx::MemoryContext::new("network_fc");
        let d = fc_call::<2>(fcw, cx.mcx(), args).expect("min/max wrapper cannot fail");
        assert_eq!(
            d.as_usize(),
            args[cidx as usize].as_usize(),
            "fc_{name} winning-datum DIVERGENCE {input}: C picked arg{cidx}"
        );
    }
}

/// network_sub/subeq/sup/supeq/overlap (927/928/929/930/3551): verbatim C
/// bodies vs shipped cores + fc bool wrappers.
fn subsup_diff(p: &[u8]) {
    let v1 = fenced_inet(p);
    let v2 = fenced_inet(&p[INET_WIRE..]);
    let (f1, b1, a1) = flat(&v1);
    let (f2, b2, a2) = flat(&v2);
    let input = format!(
        "a=(fam {} bits {} {}) b=(fam {} bits {} {})",
        v1.family,
        v1.bits,
        hex(&v1.ipaddr),
        v2.family,
        v2.bits,
        hex(&v2.ipaddr)
    );
    let (img1, len1) = v1.image();
    let (img2, len2) = v2.image();
    let args = [dptr(&img1[..len1]), dptr(&img2[..len2])];

    type CEntry = unsafe extern "C" fn(u8, u8, *const u8, u8, u8, *const u8) -> i32;
    type BoolCore = fn(InetRef<'_>, InetRef<'_>) -> bool;
    let fam: [(&str, CEntry, BoolCore, PGFunction); 5] = [
        (
            "network_sub",
            pg_diff_network_sub,
            adt_network::network_sub,
            fcb::fc_network_sub as PGFunction,
        ),
        ("network_subeq", pg_diff_network_subeq, adt_network::network_subeq, fcb::fc_network_subeq),
        ("network_sup", pg_diff_network_sup, adt_network::network_sup, fcb::fc_network_sup),
        ("network_supeq", pg_diff_network_supeq, adt_network::network_supeq, fcb::fc_network_supeq),
        (
            "network_overlap",
            pg_diff_network_overlap,
            adt_network::network_overlap,
            fcb::fc_network_overlap,
        ),
    ];
    for (name, centry, core, fcw) in fam {
        let c = unsafe { centry(f1, b1, a1, f2, b2, a2) };
        let r = core(v1.iref(), v2.iref());
        assert!((c != 0) == r, "{name} DIVERGENCE {input}: C={c} Rust={r}");
        let cx = mcx::MemoryContext::new("network_fc");
        let d = fc_call::<2>(fcw, cx.mcx(), args).expect("subsup wrapper cannot fail");
        assert_eq!(d.as_bool(), r, "fc_{name} vs core DIVERGENCE {input}");
    }
}

/// Scalar helpers over one fenced inet: network_family (711) /
/// network_masklen (697) vs verbatim C + fc planes;
/// convert_network_to_scalar (selfuncs kernel, no oid) bit-exact f64;
/// hashinet (422) / hashinetextended (779) WRAPPER-FOLD parity only — the
/// hash kernel differential is owned by proofs/hash and the composed proofs
/// on the ledger rows (the lane-0a pg_lsn hash wrapper-fold precedent).
fn scalar_diff(p: &[u8]) {
    let v = fenced_inet(p);
    let (f, b, a) = flat(&v);
    let input = format!("fam {} bits {} addr {}", f, b, hex(&v.ipaddr));
    let (img, len) = v.image();
    let arg = dptr(&img[..len]);

    let cfam = unsafe { pg_diff_network_family(f, b, a) };
    let rfam = adt_network::network_family(v.iref());
    assert_eq!(cfam, rfam, "network_family DIVERGENCE {input}");
    let cx = mcx::MemoryContext::new("network_fc");
    let d = fc_call::<1>(fcb::fc_network_family, cx.mcx(), [arg])
        .expect("family wrapper cannot fail");
    assert_eq!(d.as_i32(), rfam, "fc_network_family vs core DIVERGENCE {input}");

    let cml = unsafe { pg_diff_network_masklen(f, b, a) };
    assert_eq!(cml, v.bits as i32, "network_masklen DIVERGENCE {input}");
    let d = fc_call::<1>(fcb::fc_network_masklen, cx.mcx(), [arg])
        .expect("masklen wrapper cannot fail");
    assert_eq!(d.as_i32(), cml, "fc_network_masklen vs C DIVERGENCE {input}");

    let csc = unsafe { pg_diff_convert_network_to_scalar(f, b, a) };
    let rsc = adt_network::convert_network_to_scalar(v.iref());
    assert_eq!(
        csc.to_bits(),
        rsc.to_bits(),
        "convert_network_to_scalar DIVERGENCE {input}: C={csc} Rust={rsc}"
    );

    // Hash wrapper folds (see doc comment).
    let seed = u64::from_le_bytes(p[INET_WIRE..INET_WIRE + 8].try_into().unwrap());
    let d = fc_call::<1>(fcb::fc_hashinet, cx.mcx(), [arg]).expect("hashinet cannot fail");
    assert_eq!(
        d.as_u32(),
        adt_network::hashinet_bytes(v.iref()),
        "fc_hashinet wrapper-fold DIVERGENCE {input}"
    );
    let d = fc_call::<2>(fcb::fc_hashinetextended, cx.mcx(), [arg, Datum::from_u64(seed)])
        .expect("hashinetextended cannot fail");
    assert_eq!(
        d.as_u64(),
        adt_network::hashinet_bytes_extended(v.iref(), seed),
        "fc_hashinetextended wrapper-fold DIVERGENCE {input} seed={seed}"
    );
}

/// network_scan_first/network_scan_last (selfuncs support, no oids): the C
/// DirectFunctionCall compositions vs the shipped cores.
fn scan_diff(p: &[u8]) {
    let v = fenced_inet(p);
    let (f, b, a) = flat(&v);
    let input = format!("fam {} bits {} addr {}", f, b, hex(&v.ipaddr));

    let (mut of, mut ob, mut oa) = (0u8, 0u8, [0u8; 16]);
    let cst = unsafe { pg_diff_network_scan_first(f, b, a, &mut of, &mut ob, oa.as_mut_ptr()) };
    let r = adt_network::network_scan_first(v.iref());
    assert!(
        cst == 0 && (of, ob, oa) == (r.family, r.bits, r.ipaddr),
        "network_scan_first DIVERGENCE {input}"
    );

    let (mut of, mut ob, mut oa) = (0u8, 0u8, [0u8; 16]);
    let cst = unsafe { pg_diff_network_scan_last(f, b, a, &mut of, &mut ob, oa.as_mut_ptr()) };
    let r = adt_network::network_scan_last(v.iref())
        .expect("network_scan_last infallible on fenced input (set_masklen -1)");
    assert!(
        cst == 0 && (of, ob, oa) == (r.family, r.bits, r.ipaddr),
        "network_scan_last DIVERGENCE {input}"
    );
}

// ---------------------------------------------------------------------------
// Dispatch
// ---------------------------------------------------------------------------

use adt_network::builtins as fcb;

pub fn network_diff(data: &[u8]) {
    let _oracle = crate::oracle_serial(); // one-thread-at-a-time through the C oracles (process-global statics)
    let Some((&sel, p)) = data.split_first() else {
        return;
    };
    let one = p.len() >= INET_WIRE;
    let two = p.len() >= 2 * INET_WIRE;
    match sel % 32 {
        0 => in_diff(p, false),
        1 => in_diff(p, true),
        2 if one => text_diff(p, TextArm::InetOut),
        3 if one => text_diff(p, TextArm::CidrOut),
        4 if one => text_diff(p, TextArm::InetAbbrev),
        5 if one => text_diff(p, TextArm::CidrAbbrev),
        6 if one => text_diff(p, TextArm::Host),
        7 if one => text_diff(p, TextArm::Show),
        8 if two => cmp_diff(p),
        9 if p.len() >= INET_WIRE + 4 => set_masklen_diff(p, false),
        10 if p.len() >= INET_WIRE + 4 => set_masklen_diff(p, true),
        11 if one => unary_diff(
            p,
            "network_network",
            pg_diff_network_network,
            adt_network::network_network,
            fcb::fc_network_network,
        ),
        12 if one => unary_diff(
            p,
            "network_netmask",
            pg_diff_network_netmask,
            adt_network::network_netmask,
            fcb::fc_network_netmask,
        ),
        13 if one => unary_diff(
            p,
            "network_broadcast",
            pg_diff_network_broadcast,
            adt_network::network_broadcast,
            fcb::fc_network_broadcast,
        ),
        14 if one => unary_diff(
            p,
            "network_hostmask",
            pg_diff_network_hostmask,
            adt_network::network_hostmask,
            fcb::fc_network_hostmask,
        ),
        15 if one => to_cidr_diff(p),
        16 if two => binary_inet_diff(
            p,
            "inet_merge",
            pg_diff_inet_merge,
            adt_network::inet_merge,
            fcb::fc_inet_merge,
        ),
        17 if two => same_family_diff(p),
        18 if two => {
            binary_inet_diff(p, "inetand", pg_diff_inetand, adt_network::inetand, fcb::fc_inetand)
        }
        19 if two => {
            binary_inet_diff(p, "inetor", pg_diff_inetor, adt_network::inetor, fcb::fc_inetor)
        }
        20 if one => {
            unary_diff(p, "inetnot", pg_diff_inetnot, adt_network::inetnot, fcb::fc_inetnot)
        }
        21 if p.len() >= INET_WIRE + 8 => pl_diff(p, false),
        22 if p.len() >= INET_WIRE + 8 => pl_diff(p, true),
        23 if two => inetmi_diff(p),
        24 if one => abbrev_convert_diff(p),
        25 => recv_diff(p, false),
        26 => recv_diff(p, true),
        27 if one => send_diff(p),
        28 if two => relops_diff(p),
        29 if two => subsup_diff(p),
        30 if p.len() >= INET_WIRE + 8 => scalar_diff(p),
        31 if one => scan_diff(p),
        _ => {}
    }
}

// ---------------------------------------------------------------------------
// Stable-toolchain smoke tests: replay corpus shapes through every arm so
// `cargo test` exercises the C link + all comparators without cargo-fuzz.
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    /// SHIM-CONTRACT PIN (task #131, rework of the refuted 515fffe6d6a) —
    /// THE SCRIBBLER CLASS, second instance.
    ///
    /// The contract is mcxt.c's (vendor/postgres-src, 18.3): `pstrdup(in)` =
    /// `MemoryContextStrdup(CurrentMemoryContext, in)` (mcxt.c:1724-1728),
    /// and MemoryContextStrdup allocates EXACTLY `strlen(string)+1` bytes and
    /// copies the whole string (mcxt.c:1711-1722). Both halves are pinned:
    ///
    ///  * LENGTH: `strlen(pstrdup(s)) == strlen(s)` at every driven length —
    ///    catches any truncating buffer up to 1 MiB.
    ///  * EXACT SIZE: `pg_network_msgbuf_slack() == 0` after EVERY call.
    ///    This is what makes the pin fail for ANY wrong-size buffer, not
    ///    just 64: a fixed buffer of ANY size (the refuted pin was measured
    ///    blind past 4096) and a grow-never-shrink realloc both report
    ///    nonzero slack at some driven length; the long->short tail is the
    ///    dedicated grow-never-shrink control (pg_float_io.c fbb8c572ec3
    ///    lesson: memory-SAFE but detection-BLIND, the overrun class this
    ///    shim exists to expose lands in slack and goes unseen).
    ///
    /// A rewrite that deletes the probes fails to link; a rewrite that lies
    /// through them has to reimplement exact sizing to do so.
    ///
    /// MUST-FAIL CONTROLS (run for task #131, outputs in the lane report):
    ///  * pristine fixed-64 truncating shim -> the len assertion fails at 64;
    ///  * fixed 2 MiB non-truncating buffer -> the slack assertion fails at
    ///    len 0;
    ///  * grow-never-shrink (`>` for `!=`) -> the long->short tail fails.
    #[test]
    fn shim_pstrdup_sizes_exactly_the_scribbler_class() {
        let _serial = crate::c_oracle_serial();

        let probe = |len: usize| -> (usize, i32) {
            let s = std::ffi::CString::new(vec![b'x'; len]).unwrap();
            let got = unsafe { pg_network_pstrdup_len_probe(s.as_ptr()) };
            let slack = unsafe { pg_network_msgbuf_slack() };
            (got, slack)
        };

        for len in [0usize, 1, 49, 63, 64, 65, 300, 4096, 8192, 65536, 1 << 20] {
            let (got, slack) = probe(len);
            assert_eq!(
                got, len,
                "pstrdup truncated a {len}-byte string to {got}: mcxt.c \
                 allocates strlen+1 and never truncates (THE SCRIBBLER, #112)"
            );
            assert_eq!(
                slack, 0,
                "pstrdup allocation carries {slack} bytes of slack at len \
                 {len}: the mcxt.c contract is EXACTLY strlen+1, and slack is \
                 where an input-derived overrun hides from the guard band"
            );
            assert_eq!(
                unsafe { pg_network_msgbuf_check() },
                0,
                "guard band clobbered at len {len}"
            );
        }

        // Grow-never-shrink control: a long call then a short one. Sizing up
        // only would leave ~1 MiB of slack here.
        probe(1 << 20);
        let (_, slack) = probe(10);
        assert_eq!(
            slack, 0,
            "pstrdup kept {slack} bytes of slack after a short call following \
             a 1 MiB one: grow-never-shrink is not mcxt.c's contract \
             (fbb8c572ec3)"
        );
    }

    pub const TEXT_CORPUS: &[&str] = &[
        "0.0.0.0/0",
        "192.168.1.5",
        "192.168.1.5/24",
        "192.168.1.0/24",
        "255.255.255.255",
        "255.255.255.255/32",
        "10",
        "10.1",
        "10.1.2",
        "10/8",
        "10.1.2.3/8",
        "128.1",
        "224.1",
        "240.1.2.3",
        "0x0a000001",
        "0xff",
        "0Xff/8",
        "1.2.3.4/33",
        "1.2.3.4/-1",
        "1.2.3.4/",
        "1.2.3.4/+0",
        "1.2.3.4/032",
        "256.1.1.1",
        "1..2.3",
        "1.2.3.4.5",
        ".1.2.3",
        "1.2.3.4 ",
        " 1.2.3.4",
        "",
        "/",
        "/32",
        "::",
        "::/0",
        "::1",
        "::1/128",
        "fe80::1",
        "ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff",
        "1:2:3:4:5:6:7:8",
        "1:2:3:4:5:6:7:8:9",
        "1:2:3:4:5:6:7",
        "::ffff:1.2.3.4",
        "::ffff:1.2.3.4/100",
        "1:2:3:4:5:6:1.2.3.4",
        "::10.2.3.4",
        "fe80::1%eth0",
        "abcd:ef01::",
        "ABCD:EF01::",
        "12345::",
        "1::2::3",
        ":::",
        ":1:2:3:4:5:6:7",
        "1:2:3:4:5:6:7:",
        "fe80::/10",
        "fe80::/129",
        "fe80::/1a",
        "10.0.0.0/8",
        "10.1.0.0/16",
        "10.1.2.0/255",
        "0/0",
        "0",
        "0.0.0.0/32",
        "223.255.255.255",
        "224.0.0.0",
        "192.5.5.240/28",
    ];

    fn drive(sel: u8, payload: &[u8]) {
        let mut d = vec![sel];
        d.extend_from_slice(payload);
        network_diff(&d);
    }

    #[test]
    fn text_arms_corpus() {
        for s in TEXT_CORPUS {
            drive(0, s.as_bytes());
            drive(1, s.as_bytes());
        }
        // interior NUL: cstring truncation on both sides
        drive(0, b"1.2.3.4\0junk");
        drive(1, b"10.0.0.0/8\0junk");
        // non-UTF-8 bytes: lossy view vs raw C bytes (both must reject)
        drive(0, &[0x31, 0x2e, 0x32, 0xff, 0x80]);
        drive(1, &[0xc3, 0x28, 0x2f]);
    }

    /// One 18-byte wire value.
    pub fn wire(fam_sel: u8, bits: u8, addr: [u8; 16]) -> Vec<u8> {
        let mut w = vec![fam_sel, bits];
        w.extend_from_slice(&addr);
        w
    }

    pub fn wire_pairs() -> Vec<Vec<u8>> {
        let mut out = Vec::new();
        let a = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16];
        let z = [0u8; 16];
        let ff = [0xffu8; 16];
        for &(f1, b1, a1) in
            &[(0u8, 24u8, a), (0, 0, z), (0, 32, ff), (1, 64, a), (1, 0, z), (1, 128, ff)]
        {
            for &(f2, b2, a2) in &[(0u8, 24u8, a), (0, 16, a), (1, 64, a), (1, 128, ff)] {
                let mut p = wire(f1, b1, a1);
                p.extend_from_slice(&wire(f2, b2, a2));
                out.push(p);
            }
        }
        out
    }

    #[test]
    fn binary_arms_corpus() {
        let singles: Vec<Vec<u8>> = vec![
            wire(0, 0, [0; 16]),
            wire(0, 8, [10, 1, 2, 3, 0xde, 0xad, 0xbe, 0xef, 0, 0, 0, 0, 0, 0, 0, 0]),
            wire(0, 24, [192, 168, 1, 226, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]),
            wire(0, 32, [255; 16]),
            wire(0, 31, [255; 16]),
            wire(1, 0, [0; 16]),
            wire(1, 10, [0xfe, 0x80, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1]),
            wire(1, 128, [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0xff, 0xff, 1, 2, 3, 4]),
            wire(1, 120, [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0xff, 0xff, 1, 2, 3, 4]),
            wire(1, 128, [0x20, 1, 0xd, 0xb8, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1]),
            wire(1, 127, [0; 16]),
        ];
        for s in &singles {
            for sel in [2u8, 3, 4, 5, 6, 7, 11, 12, 13, 14, 15, 20, 24] {
                drive(sel, s);
            }
            for bits in [-1i32, 0, 8, 24, 32, 33, 64, 128, 129, i32::MIN, i32::MAX] {
                let mut p = s.clone();
                p.extend_from_slice(&bits.to_le_bytes());
                drive(9, &p);
                drive(10, &p);
            }
            for add in [0i64, 1, -1, 255, 256, -256, i64::MAX, i64::MIN, 1 << 32] {
                let mut p = s.clone();
                p.extend_from_slice(&add.to_le_bytes());
                drive(21, &p);
                drive(22, &p);
            }
        }
        for pair in wire_pairs() {
            for sel in [8u8, 16, 17, 18, 19, 23, 28, 29] {
                drive(sel, &pair);
            }
        }
        // round-2 single-inet arms (send/scalar/scan; scalar wants +8B seed)
        for s in &singles {
            for sel in [27u8, 31] {
                drive(sel, s);
            }
            let mut p = s.clone();
            p.extend_from_slice(&0x9e3779b97f4a7c15u64.to_le_bytes());
            drive(30, &p);
        }
    }

    /// recv arms: valid wire messages both families + every error arm
    /// (bad family, bad bits, bad length, truncation at each byte,
    /// cidr bits-right-of-mask).
    #[test]
    fn recv_arms_corpus() {
        let v4 = [2u8, 24, 1, 192, 4, 10, 1, 2, 3];
        let v6: Vec<u8> = {
            let mut m = vec![3u8, 64, 0, 16];
            m.extend_from_slice(&[0x20, 1, 0xd, 0xb8, 0, 0, 0, 0, 1, 2, 3, 4, 5, 6, 7, 8]);
            m
        };
        for sel in [25u8, 26] {
            drive(sel, &v4);
            drive(sel, &v6);
            drive(sel, &[]); /* empty: protocol violation on byte 0 */
            drive(sel, &[9, 24, 1, 4, 10, 1, 2, 3]); /* bad family */
            drive(sel, &[2, 33, 1, 4, 10, 1, 2, 3]); /* bits > maxbits */
            drive(sel, &[2, 24, 1, 16, 10, 1, 2, 3]); /* nb != addrsize */
            for cut in 1..v4.len() {
                drive(sel, &v4[..cut]); /* truncations: 08P01 parity */
            }
        }
        // cidr bits-right-of-mask: 10.1.2.3/24 valid as inet, invalid as cidr
        drive(25, &v4);
        drive(26, &v4);
    }

    /// Replay every checked-in seed (catches shim/link errors before the
    /// nightly fuzz campaign).
    #[test]
    fn seed_corpus_replays_clean() {
        let dir = concat!(env!("CARGO_MANIFEST_DIR"), "/../corpus/network_diff");
        let mut n = 0;
        for e in std::fs::read_dir(dir).expect("corpus/network_diff missing") {
            let p = e.unwrap().path();
            if p.is_file() {
                network_diff(&std::fs::read(&p).unwrap());
                n += 1;
            }
        }
        assert!(n >= 40, "expected >=40 seeds, found {n}");
    }

    /// Error-arm smoke: each fallible arm's error class fires and agrees.
    #[test]
    fn error_arms_smoke() {
        // 22P02 both types
        drive(0, b"not an ip");
        drive(1, b"not an ip");
        // cidr bits-right-of-mask (22P02 invalid cidr value)
        drive(1, b"192.168.1.5/24");
        // 22023: set_masklen out of range
        let mut p = wire(0, 24, [192, 168, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]);
        p.extend_from_slice(&33i32.to_le_bytes());
        drive(9, &p);
        drive(10, &p);
        // 22023: family mismatch on merge/and/or/mi
        let mut q = wire(0, 24, [1; 16]);
        q.extend_from_slice(&wire(1, 64, [2; 16]));
        for sel in [16u8, 18, 19, 23] {
            drive(sel, &q);
        }
        // 22003: inetmi_int8 overflow (v4 + huge negated addend)
        let mut r = wire(0, 32, [255; 16]);
        r.extend_from_slice(&i64::MAX.to_le_bytes());
        drive(21, &r);
        drive(22, &r);
    }
}
