#![allow(non_snake_case)]
// `PgResult<T>` carries the workspace-wide `PgError`, which is large; every
// sibling adt crate allows this lint rather than boxing the soft-error path away
// from the C-identical control flow.
#![allow(clippy::result_large_err)]

//! Port of `src/backend/utils/adt/network.c`: the `inet` and `cidr` datatypes.
//!
//! Every function in `network.c` is ported here against postgres-18.3 (branch
//! order, message text, and SQLSTATE preserved). The IP-address text<->binary
//! codec lives in the sibling C translation units `inet_net_pton.c`,
//! `src/port/inet_net_ntop.c`, and `inet_cidr_ntop.c`; they are folded in here as
//! the [`inet_net_pton`] / [`inet_net_ntop`] / [`inet_cidr_ntop`] modules (they
//! are self-contained string codecs called only by `network.c`).
//!
//! `inet`/`cidr` values are modeled as the owned [`inet_struct`] payload
//! (`family` / `bits` / `ipaddr`). The fmgr/Datum/varlena envelope (the `inet`
//! varlena header, `PG_GETARG_INET_PP`, the `bytea`/`StringInfo` wrappers) is the
//! project-wide systemic deferral: input functions take `&[u8]`, output functions
//! return `Vec<u8>`, and `inet`-producing functions return [`inet_struct`].
//!
//! The genuinely-external substrate of `inet_client_addr` (MyProcPort),
//! `network_sortsupport` (tuplesort + HyperLogLog), and `network_subset_support`
//! (planner node construction) crosses
//! [`backend_utils_adt_network_seams`] (called here, installed by the unported
//! owner subsystems — a loud panic until they land).

pub mod fmgr_builtins;
pub mod inet_cidr_ntop;
pub mod inet_net_ntop;
pub mod inet_net_pton;
pub mod planner;
pub mod sortsupport;

use backend_utils_adt_network_seams::session;
use mcx::{Mcx, PgVec};
use types_error::{
    ereturn, PgError, PgResult, SoftErrorContext, ERRCODE_INTERNAL_ERROR,
    ERRCODE_INVALID_BINARY_REPRESENTATION, ERRCODE_INVALID_PARAMETER_VALUE,
    ERRCODE_INVALID_TEXT_REPRESENTATION, ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE,
};
use types_network::{inet_struct, macaddr, macaddr8, ResolvedName, SessionEndpoint, PGSQL_AF_INET, PGSQL_AF_INET6};

#[cfg(test)]
mod tests;

// ---------------------------------------------------------------------------
// inet helpers (mirror the ip_* access macros in utils/inet.h)
// ---------------------------------------------------------------------------

/// `sizeof("xxxx:xxxx:xxxx:xxxx:xxxx:xxxx:255.255.255.255/128")` — the text
/// scratch size used throughout `network.c`.
const NTOP_BUFSZ: usize = 50;

/// `AF_INET6` — the *system* address family for IPv6, used only by
/// [`clean_ipv6_addr`] to decide whether to strip a `%zone` suffix from a
/// `getnameinfo` result. The numeric value differs by platform; we mirror the
/// libc values rather than depend on `libc` (PostgreSQL only runs on POSIX-like
/// systems). The session seam reports the same `ss_family` value.
mod system_af {
    #[cfg(target_os = "linux")]
    pub const AF_INET6: i32 = 10;
    #[cfg(any(target_os = "macos", target_os = "ios"))]
    pub const AF_INET6: i32 = 30;
    #[cfg(not(any(target_os = "linux", target_os = "macos", target_os = "ios")))]
    pub const AF_INET6: i32 = 10;
}

/// C: `ip_addrsize(inetptr)` — 4 for IPv4, 16 for IPv6 (utils/inet.h).
#[inline]
fn ip_addrsize(family: u8) -> usize {
    if family == PGSQL_AF_INET {
        4
    } else {
        16
    }
}

/// C: `ip_maxbits(inetptr)` — 32 for IPv4, 128 for IPv6 (utils/inet.h).
#[inline]
fn ip_maxbits(family: u8) -> i32 {
    if family == PGSQL_AF_INET {
        32
    } else {
        128
    }
}

/// A freshly-zeroed `inet` working value, mirroring `palloc0(sizeof(inet))`.
fn new_inet() -> inet_struct {
    inet_struct {
        family: 0,
        bits: 0,
        ipaddr: [0u8; 16],
    }
}

// ---------------------------------------------------------------------------
// codec helpers (thin marshalling to/from the in-crate codec modules)
// ---------------------------------------------------------------------------

/// `pg_inet_net_pton(af, src, &mut ipaddr, size)`. Returns the parsed bits or
/// `-1` on error (C convention). `size` is the C signed `size_t` selector.
fn net_pton(af: i32, src: &[u8], dst: &mut [u8; 16], size: Option<usize>) -> i32 {
    let size = match size {
        Some(n) => n as isize,
        None => -1,
    };
    let s = match core::str::from_utf8(src) {
        Ok(s) => s,
        // `pg_inet_net_pton` walks raw `unsigned char`; a non-UTF-8 byte cannot
        // begin any accepted IP grammar token, so the ISC parser rejects it
        // (ENOENT == -1). Reproduce that without losing bytes by failing here.
        Err(_) => return -1,
    };
    match inet_net_pton::pg_inet_net_pton(af, s, dst, size) {
        Ok(bits) => bits,
        Err(_) => -1,
    }
}

/// `pg_inet_net_ntop(af, &ipaddr, bits, dst)`. Returns `Some(len)` on success or
/// `None` (C `NULL`) on error.
fn net_ntop(af: i32, src: &[u8; 16], bits: i32, dst: &mut [u8]) -> Option<usize> {
    inet_net_ntop::pg_inet_net_ntop(af, src, bits, dst)
}

/// `pg_inet_cidr_ntop(af, &ipaddr, bits, dst)`. Returns `Some(len)` or `None`.
fn cidr_ntop(af: i32, src: &[u8; 16], bits: i32, dst: &mut [u8]) -> Option<usize> {
    inet_cidr_ntop::pg_inet_cidr_ntop(af, src, bits, dst).ok()
}

// ---------------------------------------------------------------------------
// Common INET/CIDR input routine (network.c:74 `network_in`)
// ---------------------------------------------------------------------------

/// `network_in` (network.c:74). Common INET/CIDR input routine. On a soft error
/// returns `Ok(None)` (saving into `escontext`); otherwise `Err`.
fn network_in(
    src: &[u8],
    is_cidr: bool,
    escontext: Option<&mut SoftErrorContext>,
) -> PgResult<Option<inet_struct>> {
    let mut dst = new_inet();

    // First, check whether this is an IPv6 or IPv4 address. IPv6 addresses have
    // a ':' somewhere; if present assume V6, otherwise V4.
    if src.contains(&b':') {
        dst.family = PGSQL_AF_INET6;
    } else {
        dst.family = PGSQL_AF_INET;
    }

    let af = dst.family as i32;
    let addrsize = ip_addrsize(dst.family);
    let size = if is_cidr { Some(addrsize) } else { None };
    let bits = net_pton(af, src, &mut dst.ipaddr, size);

    if bits < 0 || bits > ip_maxbits(dst.family) {
        // translator: first %s is inet or cidr
        return ereturn(
            escontext,
            None,
            PgError::error(format!(
                "invalid input syntax for type {}: \"{}\"",
                if is_cidr { "cidr" } else { "inet" },
                String::from_utf8_lossy(src)
            ))
            .with_sqlstate(ERRCODE_INVALID_TEXT_REPRESENTATION),
        );
    }

    // CIDR values must not have any bits set beyond the masklen.
    if is_cidr && !addressOK(&dst.ipaddr, bits, dst.family) {
        return ereturn(
            escontext,
            None,
            PgError::error(format!("invalid cidr value: \"{}\"", String::from_utf8_lossy(src)))
                .with_sqlstate(ERRCODE_INVALID_TEXT_REPRESENTATION)
                .with_detail("Value has bits set to right of mask."),
        );
    }

    dst.bits = bits as u8;
    Ok(Some(dst))
}

/// `inet_in` (network.c:121).
pub fn inet_in(src: &[u8], escontext: Option<&mut SoftErrorContext>) -> PgResult<Option<inet_struct>> {
    network_in(src, false, escontext)
}

/// `cidr_in` (network.c:129).
pub fn cidr_in(src: &[u8], escontext: Option<&mut SoftErrorContext>) -> PgResult<Option<inet_struct>> {
    network_in(src, true, escontext)
}

// ---------------------------------------------------------------------------
// Common INET/CIDR output routine (network.c:140 `network_out`)
// ---------------------------------------------------------------------------

/// `network_out` (network.c:140). Returns the rendered text (the C `pstrdup`).
fn network_out(src: &inet_struct, is_cidr: bool) -> PgResult<Vec<u8>> {
    let mut tmp = [0u8; NTOP_BUFSZ];

    let mut len = match net_ntop(src.family as i32, &src.ipaddr, src.bits as i32, &mut tmp) {
        Some(n) => n,
        None => {
            return Err(PgError::error("could not format inet value: %m")
                .with_sqlstate(ERRCODE_INVALID_BINARY_REPRESENTATION));
        }
    };

    // For CIDR, add /n if not present.
    if is_cidr && !tmp[..len].contains(&b'/') {
        // snprintf(tmp + len, ..., "/%u", ip_bits(src))
        let suffix = format!("/{}", src.bits);
        let sb = suffix.as_bytes();
        let copy = sb.len().min(NTOP_BUFSZ - 1 - len);
        tmp[len..len + copy].copy_from_slice(&sb[..copy]);
        len += copy;
    }

    Ok(tmp[..len].to_vec())
}

/// `inet_out` (network.c:165). Returns the cstring text (the fmgr layer adds the
/// terminator).
pub fn inet_out(src: &inet_struct) -> PgResult<Vec<u8>> {
    network_out(src, false)
}

/// `cidr_out` (network.c:173).
pub fn cidr_out(src: &inet_struct) -> PgResult<Vec<u8>> {
    network_out(src, true)
}

// ---------------------------------------------------------------------------
// network_recv (network.c:191) / network_send (network.c:269)
// ---------------------------------------------------------------------------

/// A minimal forward cursor over a binary message body, modeling the part of
/// `pq_getmsgbyte` that `network_recv` exercises (advancing one byte at a time
/// through the `StringInfo` buffer). The `StringInfo`/`pqformat` envelope is the
/// deferred fmgr boundary; recv reads the raw message bytes directly.
struct MsgCursor<'a> {
    data: &'a [u8],
    cursor: usize,
}

impl<'a> MsgCursor<'a> {
    fn new(data: &'a [u8]) -> Self {
        Self { data, cursor: 0 }
    }

    /// `pq_getmsgbyte(buf)` — read one byte, advancing the cursor. Past the end
    /// of the buffer the C routine raises "insufficient data left in message".
    fn get_byte(&mut self) -> PgResult<i32> {
        if self.cursor >= self.data.len() {
            return Err(PgError::error("insufficient data left in message")
                .with_sqlstate(ERRCODE_INVALID_BINARY_REPRESENTATION));
        }
        let b = self.data[self.cursor];
        self.cursor += 1;
        Ok(b as i32)
    }
}

/// `network_recv` (network.c:191). Converts external binary format to inet.
fn network_recv(buf: &mut MsgCursor<'_>, is_cidr: bool) -> PgResult<inet_struct> {
    let mut addr = new_inet();

    addr.family = buf.get_byte()? as u8;
    if addr.family != PGSQL_AF_INET && addr.family != PGSQL_AF_INET6 {
        // translator: %s is inet or cidr
        return Err(PgError::error(format!(
            "invalid address family in external \"{}\" value",
            if is_cidr { "cidr" } else { "inet" }
        ))
        .with_sqlstate(ERRCODE_INVALID_BINARY_REPRESENTATION));
    }
    let bits = buf.get_byte()?;
    if bits < 0 || bits > ip_maxbits(addr.family) {
        return Err(PgError::error(format!(
            "invalid bits in external \"{}\" value",
            if is_cidr { "cidr" } else { "inet" }
        ))
        .with_sqlstate(ERRCODE_INVALID_BINARY_REPRESENTATION));
    }
    addr.bits = bits as u8;
    let _ = buf.get_byte()?; // ignore is_cidr
    let nb = buf.get_byte()?;
    if nb != ip_addrsize(addr.family) as i32 {
        return Err(PgError::error(format!(
            "invalid length in external \"{}\" value",
            if is_cidr { "cidr" } else { "inet" }
        ))
        .with_sqlstate(ERRCODE_INVALID_BINARY_REPRESENTATION));
    }

    for i in 0..nb as usize {
        addr.ipaddr[i] = buf.get_byte()? as u8;
    }

    // CIDR values must not have any bits set beyond the masklen.
    if is_cidr && !addressOK(&addr.ipaddr, bits, addr.family) {
        return Err(PgError::error("invalid external \"cidr\" value")
            .with_sqlstate(ERRCODE_INVALID_BINARY_REPRESENTATION)
            .with_detail("Value has bits set to right of mask."));
    }

    Ok(addr)
}

/// `inet_recv` (network.c:250). `buf` is the raw external binary message body.
pub fn inet_recv(buf: &[u8]) -> PgResult<inet_struct> {
    network_recv(&mut MsgCursor::new(buf), false)
}

/// `cidr_recv` (network.c:258).
pub fn cidr_recv(buf: &[u8]) -> PgResult<inet_struct> {
    network_recv(&mut MsgCursor::new(buf), true)
}

/// `network_send` (network.c:269). Converts inet to binary format, returning the
/// `bytea` payload bytes (the fmgr layer wraps them with the varlena header).
///
/// C builds the bytes in a `StringInfo` charged to `CurrentMemoryContext`; this
/// mirror builds the working buffer in a context-charged [`PgVec<u8>`] sized
/// exactly `4 + addrsize` up front, then materializes the owned `Vec<u8>` (the
/// `pq_endtypsend` copy-out analog).
fn network_send(mcx: Mcx<'_>, addr: &inet_struct, is_cidr: bool) -> PgResult<Vec<u8>> {
    let nb = ip_addrsize(addr.family);
    let mut out: PgVec<u8> = mcx::vec_with_capacity_in(mcx, 4 + nb)?;
    out.push(addr.family);
    out.push(addr.bits);
    out.push(is_cidr as u8);
    out.push(nb as u8);
    out.extend_from_slice(&addr.ipaddr[..nb]);
    Ok(out.as_slice().to_vec())
}

/// `inet_send` (network.c:290).
pub fn inet_send(mcx: Mcx<'_>, addr: &inet_struct) -> PgResult<Vec<u8>> {
    network_send(mcx, addr, false)
}

/// `cidr_send` (network.c:298).
pub fn cidr_send(mcx: Mcx<'_>, addr: &inet_struct) -> PgResult<Vec<u8>> {
    network_send(mcx, addr, true)
}

// ---------------------------------------------------------------------------
// masklen / conversion
// ---------------------------------------------------------------------------

/// `inet_to_cidr` (network.c:307).
pub fn inet_to_cidr(src: &inet_struct) -> PgResult<inet_struct> {
    let bits = src.bits as i32;

    // safety check
    if bits < 0 || bits > ip_maxbits(src.family) {
        // C: elog(ERROR, "invalid inet bit length: %d", bits) — an internal
        // (untranslated) error, ERRCODE_INTERNAL_ERROR.
        return Err(PgError::error(format!("invalid inet bit length: {}", bits))
            .with_sqlstate(ERRCODE_INTERNAL_ERROR));
    }

    Ok(cidr_set_masklen_internal(src, bits))
}

/// `inet_set_masklen` (network.c:322).
pub fn inet_set_masklen(src: &inet_struct, bits: i32) -> PgResult<inet_struct> {
    let mut bits = bits;
    let maxbits = ip_maxbits(src.family);

    if bits == -1 {
        bits = maxbits;
    }

    if bits < 0 || bits > maxbits {
        return Err(PgError::error(format!("invalid mask length: {}", bits))
            .with_sqlstate(ERRCODE_INVALID_PARAMETER_VALUE));
    }

    // clone the original data, then ip_bits(dst) = bits;
    let mut dst = *src;
    dst.bits = bits as u8;
    Ok(dst)
}

/// `cidr_set_masklen` (network.c:346).
pub fn cidr_set_masklen(src: &inet_struct, bits: i32) -> PgResult<inet_struct> {
    let mut bits = bits;
    let maxbits = ip_maxbits(src.family);

    if bits == -1 {
        bits = maxbits;
    }

    if bits < 0 || bits > maxbits {
        return Err(PgError::error(format!("invalid mask length: {}", bits))
            .with_sqlstate(ERRCODE_INVALID_PARAMETER_VALUE));
    }

    Ok(cidr_set_masklen_internal(src, bits))
}

/// `cidr_set_masklen_internal` (network.c:366). Copy `src` and set mask length to
/// `bits` (which must be valid for the family).
pub fn cidr_set_masklen_internal(src: &inet_struct, bits: i32) -> inet_struct {
    let mut dst = new_inet();

    dst.family = src.family;
    dst.bits = bits as u8;

    if bits > 0 {
        debug_assert!(bits <= ip_maxbits(dst.family));

        // Clone appropriate bytes of the address, leaving the rest 0.
        let nbytes = ((bits + 7) / 8) as usize;
        dst.ipaddr[..nbytes].copy_from_slice(&src.ipaddr[..nbytes]);

        // Clear any unwanted bits in the last partial byte.
        if bits % 8 != 0 {
            dst.ipaddr[(bits / 8) as usize] &= !(0xFFu8 >> (bits % 8));
        }
    }

    dst
}

// ---------------------------------------------------------------------------
// comparison
// ---------------------------------------------------------------------------

/// `network_cmp_internal` (network.c:403). The raw comparator.
pub fn network_cmp_internal(a1: &inet_struct, a2: &inet_struct) -> i32 {
    if a1.family == a2.family {
        let order = bitncmp(&a1.ipaddr, &a2.ipaddr, (a1.bits as i32).min(a2.bits as i32));
        if order != 0 {
            return order;
        }
        let order = (a1.bits as i32) - (a2.bits as i32);
        if order != 0 {
            return order;
        }
        return bitncmp(&a1.ipaddr, &a2.ipaddr, ip_maxbits(a1.family));
    }

    (a1.family as i32) - (a2.family as i32)
}

/// `network_cmp` (network.c:423).
pub fn network_cmp(a1: &inet_struct, a2: &inet_struct) -> i32 {
    network_cmp_internal(a1, a2)
}

/// `network_lt` (network.c:788).
pub fn network_lt(a1: &inet_struct, a2: &inet_struct) -> bool {
    network_cmp_internal(a1, a2) < 0
}

/// `network_le` (network.c:797).
pub fn network_le(a1: &inet_struct, a2: &inet_struct) -> bool {
    network_cmp_internal(a1, a2) <= 0
}

/// `network_eq` (network.c:806).
pub fn network_eq(a1: &inet_struct, a2: &inet_struct) -> bool {
    network_cmp_internal(a1, a2) == 0
}

/// `network_ge` (network.c:815).
pub fn network_ge(a1: &inet_struct, a2: &inet_struct) -> bool {
    network_cmp_internal(a1, a2) >= 0
}

/// `network_gt` (network.c:824).
pub fn network_gt(a1: &inet_struct, a2: &inet_struct) -> bool {
    network_cmp_internal(a1, a2) > 0
}

/// `network_ne` (network.c:833).
pub fn network_ne(a1: &inet_struct, a2: &inet_struct) -> bool {
    network_cmp_internal(a1, a2) != 0
}

/// `network_smaller` (network.c:845).
pub fn network_smaller(a1: &inet_struct, a2: &inet_struct) -> inet_struct {
    if network_cmp_internal(a1, a2) < 0 {
        *a1
    } else {
        *a2
    }
}

/// `network_larger` (network.c:857).
pub fn network_larger(a1: &inet_struct, a2: &inet_struct) -> inet_struct {
    if network_cmp_internal(a1, a2) > 0 {
        *a1
    } else {
        *a2
    }
}

// ---------------------------------------------------------------------------
// hashing
// ---------------------------------------------------------------------------

/// The bytes hashed by `hashinet` / `hashinetextended`: `hash_any` over
/// `VARDATA_ANY` for `addrsize + 2` bytes (family, bits, then the address).
/// XXX this assumes there are no pad bytes in the data structure.
///
/// C hashes the inet's own `VARDATA_ANY` bytes in place; this mirror gathers the
/// same `addrsize + 2`-byte view into a context-charged buffer (the ambient
/// `CurrentMemoryContext`), then materializes the owned `Vec<u8>` the fmgr layer
/// feeds to `hash_any`.
fn hash_bytes(mcx: Mcx<'_>, addr: &inet_struct) -> PgResult<Vec<u8>> {
    let addrsize = ip_addrsize(addr.family);
    let mut bytes: PgVec<u8> = mcx::vec_with_capacity_in(mcx, addrsize + 2)?;
    bytes.push(addr.family);
    bytes.push(addr.bits);
    bytes.extend_from_slice(&addr.ipaddr[..addrsize]);
    Ok(bytes.as_slice().to_vec())
}

/// `hashinet` (network.c:872). Hash index support — returns the bytes that the
/// fmgr layer feeds to `hash_any`. (The `hash_any` call itself is the deferred
/// fmgr/`common_hashfn` boundary.)
pub fn hashinet(mcx: Mcx<'_>, addr: &inet_struct) -> PgResult<Vec<u8>> {
    hash_bytes(mcx, addr)
}

/// `hashinetextended` (network.c:882). As [`hashinet`], for `hash_any_extended`.
pub fn hashinetextended(mcx: Mcx<'_>, addr: &inet_struct) -> PgResult<Vec<u8>> {
    hash_bytes(mcx, addr)
}

// ---------------------------------------------------------------------------
// containment operators
// ---------------------------------------------------------------------------

/// `network_sub` (network.c:895).
pub fn network_sub(a1: &inet_struct, a2: &inet_struct) -> bool {
    if a1.family == a2.family {
        return a1.bits > a2.bits && bitncmp(&a1.ipaddr, &a2.ipaddr, a2.bits as i32) == 0;
    }
    false
}

/// `network_subeq` (network.c:910).
pub fn network_subeq(a1: &inet_struct, a2: &inet_struct) -> bool {
    if a1.family == a2.family {
        return a1.bits >= a2.bits && bitncmp(&a1.ipaddr, &a2.ipaddr, a2.bits as i32) == 0;
    }
    false
}

/// `network_sup` (network.c:925).
pub fn network_sup(a1: &inet_struct, a2: &inet_struct) -> bool {
    if a1.family == a2.family {
        return a1.bits < a2.bits && bitncmp(&a1.ipaddr, &a2.ipaddr, a1.bits as i32) == 0;
    }
    false
}

/// `network_supeq` (network.c:940).
pub fn network_supeq(a1: &inet_struct, a2: &inet_struct) -> bool {
    if a1.family == a2.family {
        return a1.bits <= a2.bits && bitncmp(&a1.ipaddr, &a2.ipaddr, a1.bits as i32) == 0;
    }
    false
}

/// `network_overlap` (network.c:955).
pub fn network_overlap(a1: &inet_struct, a2: &inet_struct) -> bool {
    if a1.family == a2.family {
        return bitncmp(&a1.ipaddr, &a2.ipaddr, (a1.bits as i32).min(a2.bits as i32)) == 0;
    }
    false
}

// ---------------------------------------------------------------------------
// accessors / formatting
// ---------------------------------------------------------------------------

/// The "could not format inet value: %m" error used by several accessors.
fn format_inet_error() -> PgError {
    PgError::error("could not format inet value: %m")
        .with_sqlstate(ERRCODE_INVALID_BINARY_REPRESENTATION)
}

/// `network_host` (network.c:1138). Returns the `text` payload bytes.
pub fn network_host(ip: &inet_struct) -> PgResult<Vec<u8>> {
    let mut tmp = [0u8; NTOP_BUFSZ];

    // force display of max bits, regardless of masklen...
    let mut len = match net_ntop(ip.family as i32, &ip.ipaddr, ip_maxbits(ip.family), &mut tmp) {
        Some(n) => n,
        None => return Err(format_inet_error()),
    };

    // Suppress /n if present (shouldn't happen now).
    if let Some(pos) = tmp[..len].iter().position(|&b| b == b'/') {
        len = pos;
    }

    Ok(tmp[..len].to_vec())
}

/// `network_show` (network.c:1164). Implements inet/cidr casts to text.
pub fn network_show(ip: &inet_struct) -> PgResult<Vec<u8>> {
    let mut tmp = [0u8; NTOP_BUFSZ];

    let mut len = match net_ntop(ip.family as i32, &ip.ipaddr, ip_maxbits(ip.family), &mut tmp) {
        Some(n) => n,
        None => return Err(format_inet_error()),
    };

    // Add /n if not present (which it won't be).
    if !tmp[..len].contains(&b'/') {
        let suffix = format!("/{}", ip.bits);
        let sb = suffix.as_bytes();
        let copy = sb.len().min(NTOP_BUFSZ - 1 - len);
        tmp[len..len + copy].copy_from_slice(&sb[..copy]);
        len += copy;
    }

    Ok(tmp[..len].to_vec())
}

/// `inet_abbrev` (network.c:1187). Returns the `text` payload bytes.
pub fn inet_abbrev(ip: &inet_struct) -> PgResult<Vec<u8>> {
    let mut tmp = [0u8; NTOP_BUFSZ];

    let len = match net_ntop(ip.family as i32, &ip.ipaddr, ip.bits as i32, &mut tmp) {
        Some(n) => n,
        None => return Err(format_inet_error()),
    };

    Ok(tmp[..len].to_vec())
}

/// `cidr_abbrev` (network.c:1205).
pub fn cidr_abbrev(ip: &inet_struct) -> PgResult<Vec<u8>> {
    let mut tmp = [0u8; NTOP_BUFSZ];

    let len = match cidr_ntop(ip.family as i32, &ip.ipaddr, ip.bits as i32, &mut tmp) {
        Some(n) => n,
        None => {
            return Err(PgError::error("could not format cidr value: %m")
                .with_sqlstate(ERRCODE_INVALID_BINARY_REPRESENTATION));
        }
    };

    Ok(tmp[..len].to_vec())
}

/// `network_masklen` (network.c:1223).
pub fn network_masklen(ip: &inet_struct) -> i32 {
    ip.bits as i32
}

/// `network_family` (network.c:1231).
pub fn network_family(ip: &inet_struct) -> i32 {
    match ip.family {
        f if f == PGSQL_AF_INET => 4,
        f if f == PGSQL_AF_INET6 => 6,
        _ => 0,
    }
}

/// `network_broadcast` (network.c:1250).
pub fn network_broadcast(ip: &inet_struct) -> inet_struct {
    let mut dst = new_inet();

    let maxbytes = ip_addrsize(ip.family);
    let mut bits = ip.bits as i32;

    for byte in 0..maxbytes {
        let mask: u8 = if bits >= 8 {
            bits -= 8;
            0x00
        } else if bits == 0 {
            0xff
        } else {
            let m = 0xffu8 >> bits;
            bits = 0;
            m
        };

        dst.ipaddr[byte] = ip.ipaddr[byte] | mask;
    }

    dst.family = ip.family;
    dst.bits = ip.bits;
    dst
}

/// `network_network` (network.c:1295).
pub fn network_network(ip: &inet_struct) -> inet_struct {
    let mut dst = new_inet();

    let mut bits = ip.bits as i32;
    let mut byte = 0usize;

    while bits != 0 {
        let mask: u8 = if bits >= 8 {
            bits -= 8;
            0xff
        } else {
            let m = 0xffu8 << (8 - bits);
            bits = 0;
            m
        };

        dst.ipaddr[byte] = ip.ipaddr[byte] & mask;
        byte += 1;
    }

    dst.family = ip.family;
    dst.bits = ip.bits;
    dst
}

/// `network_netmask` (network.c:1339).
pub fn network_netmask(ip: &inet_struct) -> inet_struct {
    let mut dst = new_inet();

    let mut bits = ip.bits as i32;
    let mut byte = 0usize;

    while bits != 0 {
        let mask: u8 = if bits >= 8 {
            bits -= 8;
            0xff
        } else {
            let m = 0xffu8 << (8 - bits);
            bits = 0;
            m
        };

        dst.ipaddr[byte] = mask;
        byte += 1;
    }

    dst.family = ip.family;
    dst.bits = ip_maxbits(ip.family) as u8;
    dst
}

/// `network_hostmask` (network.c:1381).
pub fn network_hostmask(ip: &inet_struct) -> inet_struct {
    let mut dst = new_inet();

    let maxbytes = ip_addrsize(ip.family);
    let mut bits = ip_maxbits(ip.family) - ip.bits as i32;
    let mut byte = maxbytes as i32 - 1;

    while bits != 0 {
        let mask: u8 = if bits >= 8 {
            bits -= 8;
            0xff
        } else {
            let m = 0xffu8 >> (8 - bits);
            bits = 0;
            m
        };

        dst.ipaddr[byte as usize] = mask;
        byte -= 1;
    }

    dst.family = ip.family;
    dst.bits = ip_maxbits(ip.family) as u8;
    dst
}

/// `inet_same_family` (network.c:1429).
pub fn inet_same_family(a1: &inet_struct, a2: &inet_struct) -> bool {
    a1.family == a2.family
}

/// `inet_merge` (network.c:1441). Smallest CIDR which contains both inputs.
pub fn inet_merge(a1: &inet_struct, a2: &inet_struct) -> PgResult<inet_struct> {
    if a1.family != a2.family {
        return Err(PgError::error("cannot merge addresses from different families")
            .with_sqlstate(ERRCODE_INVALID_PARAMETER_VALUE));
    }

    let commonbits = bitncommon(&a1.ipaddr, &a2.ipaddr, (a1.bits as i32).min(a2.bits as i32));

    Ok(cidr_set_masklen_internal(a1, commonbits))
}

// ---------------------------------------------------------------------------
// convert_network_to_scalar (network.c:1467)
// ---------------------------------------------------------------------------

/// `convert_network_to_scalar` (network.c:1467), inet/cidr branch (`INETOID` /
/// `CIDROID`). Converts a network value to an approximate scalar for selectivity
/// estimation. The `MACADDROID`/`MACADDR8OID` branches live in
/// [`convert_macaddr_to_scalar`] / [`convert_macaddr8_to_scalar`].
pub fn convert_network_to_scalar(ip: &inet_struct) -> f64 {
    // Note that we don't use the full address for IPv6.
    let len = if ip.family == PGSQL_AF_INET { 4 } else { 5 };

    let mut res = ip.family as f64;
    for i in 0..len {
        res *= 256.0;
        res += ip.ipaddr[i] as f64;
    }
    res
}

/// `convert_network_to_scalar` (network.c:1467), `MACADDROID` branch.
///
/// C computes `(mac->a << 16) | ...` in **signed** `int` (the `unsigned char`
/// fields promote to `int`); we mirror with `i32` so the sign matches.
pub fn convert_macaddr_to_scalar(mac: &macaddr) -> f64 {
    let mut res = ((mac.a as i32) << 16 | (mac.b as i32) << 8 | (mac.c as i32)) as f64;
    res *= 256.0 * 256.0 * 256.0;
    res += ((mac.d as i32) << 16 | (mac.e as i32) << 8 | (mac.f as i32)) as f64;
    res
}

/// `convert_network_to_scalar` (network.c:1467), `MACADDR8OID` branch.
///
/// `(mac->a << 24) | ...` is computed in **signed** `int`: a high bit set in the
/// top byte makes the expression negative. We mirror with `i32` (not `u32`).
pub fn convert_macaddr8_to_scalar(mac: &macaddr8) -> f64 {
    let mut res =
        ((mac.a as i32) << 24 | (mac.b as i32) << 16 | (mac.c as i32) << 8 | (mac.d as i32)) as f64;
    res *= 256.0 * 256.0 * 256.0 * 256.0;
    res +=
        ((mac.e as i32) << 24 | (mac.f as i32) << 16 | (mac.g as i32) << 8 | (mac.h as i32)) as f64;
    res
}

// ---------------------------------------------------------------------------
// low-level bit helpers (exposed via inet.h)
// ---------------------------------------------------------------------------

/// `bitncmp` (network.c:1534). Compare bit masks `l` and `r`, for `n` bits.
/// Returns `<0`, `>0`, or `0` in the libc tradition.
pub fn bitncmp(l: &[u8], r: &[u8], n: i32) -> i32 {
    let b = (n / 8) as usize;

    // x = memcmp(l, r, b)
    let mut x = 0i32;
    for i in 0..b {
        if l[i] != r[i] {
            x = if l[i] < r[i] { -1 } else { 1 };
            break;
        }
    }

    if x != 0 || n % 8 == 0 {
        return x;
    }

    let mut lb = l[b] as u32;
    let mut rb = r[b] as u32;
    let mut bcount = n % 8;
    while bcount > 0 {
        // IS_HIGHBIT_SET(x) == (x & 0x80) != 0
        if (lb & 0x80) != (rb & 0x80) {
            if (lb & 0x80) != 0 {
                return 1;
            }
            return -1;
        }
        lb <<= 1;
        rb <<= 1;
        bcount -= 1;
    }
    0
}

/// `bitncommon` (network.c:1568). Number of leading bits that match (0 to n).
pub fn bitncommon(l: &[u8], r: &[u8], n: i32) -> i32 {
    // number of bits to examine in last byte
    let mut nbits = n % 8;
    let mut byte = 0usize;

    // check whole bytes
    while byte < (n / 8) as usize {
        if l[byte] != r[byte] {
            // at least one bit in the last byte is not common
            nbits = 7;
            break;
        }
        byte += 1;
    }

    // check bits in last partial byte
    if nbits != 0 {
        // calculate diff of first non-matching bytes
        let diff = (l[byte] ^ r[byte]) as u32;

        // compare the bits from the most to the least
        while (diff >> (8 - nbits)) != 0 {
            nbits -= 1;
        }
    }

    (8 * byte as i32) + nbits
}

/// `addressOK` (network.c:1605). Verify a CIDR address has no bits set past the
/// masklen.
fn addressOK(a: &[u8], bits: i32, family: u8) -> bool {
    let (maxbits, maxbytes) = if family == PGSQL_AF_INET {
        (32, 4)
    } else {
        (128, 16)
    };
    debug_assert!(bits <= maxbits);

    if bits == maxbits {
        return true;
    }

    let mut byte = (bits / 8) as usize;

    let nbits = bits % 8;
    let mut mask: u8 = 0xff;
    if bits != 0 {
        mask >>= nbits;
    }

    while byte < maxbytes {
        if a[byte] & mask != 0 {
            return false;
        }
        mask = 0xff;
        byte += 1;
    }

    true
}

// ---------------------------------------------------------------------------
// session-info functions (read MyProcPort via a seam)
// ---------------------------------------------------------------------------

/// `inet_client_addr` (network.c:1681). IP the client is connecting from (`None`
/// -> SQL NULL for Unix socket / no port).
pub fn inet_client_addr() -> PgResult<Option<inet_struct>> {
    addr_for(SessionEndpoint::Client)
}

/// `inet_client_port` (network.c:1718). Port the client is connecting from.
pub fn inet_client_port() -> PgResult<Option<i32>> {
    inet_port(SessionEndpoint::Client)
}

/// `inet_server_addr` (network.c:1753). IP the server accepted the connection on.
pub fn inet_server_addr() -> PgResult<Option<inet_struct>> {
    addr_for(SessionEndpoint::Server)
}

/// `inet_server_port` (network.c:1790). Port the server accepted the connection on.
pub fn inet_server_port() -> PgResult<Option<i32>> {
    inet_port(SessionEndpoint::Server)
}

/// Shared body of `inet_{client,server}_addr`: resolve the endpoint, strip the
/// `%zone` via `clean_ipv6_addr`, and feed the host to `network_in(host, false,
/// NULL)`.
fn addr_for(endpoint: SessionEndpoint) -> PgResult<Option<inet_struct>> {
    let mut resolved: ResolvedName = match session::resolve::call(endpoint) {
        Some(r) => r,
        None => return Ok(None),
    };
    // clean_ipv6_addr(port->{raddr,laddr}.addr.ss_family, host)
    clean_ipv6_addr(resolved.family, &mut resolved.host);
    // PG_RETURN_INET_P(network_in(host, false, NULL))
    network_in(&resolved.host, false, None)
}

/// Shared body of `inet_{client,server}_port`: resolve the endpoint's numeric
/// port string and feed it to `int4in` (mirrors
/// `DirectFunctionCall1(int4in, CStringGetDatum(remote_port))`).
fn inet_port(endpoint: SessionEndpoint) -> PgResult<Option<i32>> {
    let resolved = match session::resolve::call(endpoint) {
        Some(r) => r,
        None => return Ok(None),
    };
    let s = String::from_utf8_lossy(&resolved.port);
    Ok(Some(backend_utils_adt_numutils::pg_strtoint32(&s)?))
}

// ---------------------------------------------------------------------------
// bitwise arithmetic
// ---------------------------------------------------------------------------

/// `inetnot` (network.c:1822).
pub fn inetnot(ip: &inet_struct) -> inet_struct {
    let mut dst = new_inet();

    let mut nb = ip_addrsize(ip.family) as i32;
    while {
        nb -= 1;
        nb >= 0
    } {
        dst.ipaddr[nb as usize] = !ip.ipaddr[nb as usize];
    }
    dst.bits = ip.bits;
    dst.family = ip.family;
    dst
}

/// `inetand` (network.c:1847).
pub fn inetand(ip: &inet_struct, ip2: &inet_struct) -> PgResult<inet_struct> {
    let mut dst = new_inet();

    if ip.family != ip2.family {
        return Err(PgError::error("cannot AND inet values of different sizes")
            .with_sqlstate(ERRCODE_INVALID_PARAMETER_VALUE));
    } else {
        let mut nb = ip_addrsize(ip.family) as i32;
        while {
            nb -= 1;
            nb >= 0
        } {
            dst.ipaddr[nb as usize] = ip.ipaddr[nb as usize] & ip2.ipaddr[nb as usize];
        }
    }
    dst.bits = ip.bits.max(ip2.bits);
    dst.family = ip.family;
    Ok(dst)
}

/// `inetor` (network.c:1879).
pub fn inetor(ip: &inet_struct, ip2: &inet_struct) -> PgResult<inet_struct> {
    let mut dst = new_inet();

    if ip.family != ip2.family {
        return Err(PgError::error("cannot OR inet values of different sizes")
            .with_sqlstate(ERRCODE_INVALID_PARAMETER_VALUE));
    } else {
        let mut nb = ip_addrsize(ip.family) as i32;
        while {
            nb -= 1;
            nb >= 0
        } {
            dst.ipaddr[nb as usize] = ip.ipaddr[nb as usize] | ip2.ipaddr[nb as usize];
        }
    }
    dst.bits = ip.bits.max(ip2.bits);
    dst.family = ip.family;
    Ok(dst)
}

/// `internal_inetpl` (network.c:1911). Shared add/sub core.
fn internal_inetpl(ip: &inet_struct, addend: i64) -> PgResult<inet_struct> {
    let mut dst = new_inet();

    {
        let mut nb = ip_addrsize(ip.family) as i32;
        let mut addend = addend;
        let mut carry: i32 = 0;

        while {
            nb -= 1;
            nb >= 0
        } {
            carry += ip.ipaddr[nb as usize] as i32 + (addend & 0xFF) as i32;
            dst.ipaddr[nb as usize] = (carry & 0xFF) as u8;
            carry >>= 8;

            // Clear low-order byte then divide (see C comment about portability).
            addend &= !0xFFi64;
            addend /= 0x100;
        }

        // addend & carry both 0 if original >= 0, or addend -1 and carry 1 if
        // original < 0.  Anything else means overflow.
        if !((addend == 0 && carry == 0) || (addend == -1 && carry == 1)) {
            return Err(PgError::error("result is out of range")
                .with_sqlstate(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE));
        }
    }

    dst.bits = ip.bits;
    dst.family = ip.family;
    Ok(dst)
}

/// `inetpl` (network.c:1963). Add an int8 offset to an inet.
pub fn inetpl(ip: &inet_struct, addend: i64) -> PgResult<inet_struct> {
    internal_inetpl(ip, addend)
}

/// `inetmi_int8` (network.c:1973). Subtract an int8 offset from an inet.
pub fn inetmi_int8(ip: &inet_struct, addend: i64) -> PgResult<inet_struct> {
    internal_inetpl(ip, addend.wrapping_neg())
}

/// `inetmi` (network.c:1983). Difference of two inets as int8.
pub fn inetmi(ip: &inet_struct, ip2: &inet_struct) -> PgResult<i64> {
    let mut res: i64 = 0;

    if ip.family != ip2.family {
        return Err(PgError::error("cannot subtract inet values of different sizes")
            .with_sqlstate(ERRCODE_INVALID_PARAMETER_VALUE));
    } else {
        // Form the difference using complement, increment, and add, with the
        // increment handled by starting carry at 1.
        let mut nb = ip_addrsize(ip.family) as i32;
        let mut byte = 0usize;
        let mut carry: i32 = 1;

        while {
            nb -= 1;
            nb >= 0
        } {
            // C: carry = pip[nb] + (~pip2[nb] & 0xFF) + carry; the `& 0xFF` keeps
            // the int-promoted complement to one byte (already so on a u8).
            carry += ip.ipaddr[nb as usize] as i32 + (!ip2.ipaddr[nb as usize]) as i32;
            let lobyte = carry & 0xFF;
            if byte < core::mem::size_of::<i64>() {
                res |= (lobyte as i64) << (byte * 8);
            } else {
                // Input wider than int64: check for overflow.
                if if res < 0 { lobyte != 0xFF } else { lobyte != 0 } {
                    return Err(PgError::error("result is out of range")
                        .with_sqlstate(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE));
                }
            }
            carry >>= 8;
            byte += 1;
        }

        // If input is narrower than int64, do proper sign extension.
        if carry == 0 && byte < core::mem::size_of::<i64>() {
            res |= ((-1i64) as u64).wrapping_shl((byte * 8) as u32) as i64;
        }
    }

    Ok(res)
}

/// `clean_ipv6_addr` (network.c:2060). Remove any `%zone` part from an IPv6
/// address string in place. `addr_family` is the *system* socket family
/// (`addr.ss_family`).
pub fn clean_ipv6_addr(addr_family: i32, addr: &mut Vec<u8>) {
    if addr_family == system_af::AF_INET6 {
        if let Some(pct) = addr.iter().position(|&b| b == b'%') {
            addr.truncate(pct);
        }
    }
}

// ---------------------------------------------------------------------------
// DatumGetInetPP — varlena detoast of an inet/cidr value (selfuncs edge)
// ---------------------------------------------------------------------------

/// `VARHDRSZ` (`varatt.h`): the 4-byte length word of a long-header varlena.
const VARHDRSZ: usize = 4;
/// `VARHDRSZ_SHORT` (`varatt.h`): a short (1-byte header) varlena's header.
const VARHDRSZ_SHORT: usize = 1;

/// `VARATT_IS_1B(PTR)` (`varatt.h`): any short (1-byte-header) form — the low
/// bit of `va_header` is set.
#[inline]
fn varatt_is_1b(b: &[u8]) -> bool {
    (b[0] & 0x01) == 0x01
}

/// `VARDATA_ANY(PTR)` (`varatt.h`): the payload bytes of a varlena, after either
/// the 1-byte short header or the 4-byte long header. `pg_detoast_datum_packed`
/// preserves a short header, so the inet accessor must use `VARDATA_ANY` (the
/// short/4-byte branch), exactly as `network.c`'s `ip_family`/`ip_bits`/`ip_addr`
/// macros do.
#[inline]
fn vardata_any(b: &[u8]) -> &[u8] {
    if varatt_is_1b(b) {
        &b[VARHDRSZ_SHORT..]
    } else {
        &b[VARHDRSZ..]
    }
}

/// `DatumGetInetPP(X)` (utils/inet.h): `(inet *) PG_DETOAST_DATUM_PACKED(X)` then
/// `ip_family`/`ip_bits`/`ip_addr` off `VARDATA_ANY`. The canonical [`Datum`]
/// carries the inet/cidr varlena image (`Datum::ByRef`, header included) the
/// selectivity estimators pull from `pg_statistic` / the query `Const`; this
/// detoasts it (possibly short-header / toasted) and decodes the `inet_struct`
/// payload at `VARDATA_ANY` ([`inet_struct::from_datum_bytes`]).
fn datum_get_inet_pp<'mcx>(
    mcx: Mcx<'mcx>,
    value: &types_tuple::backend_access_common_heaptuple::Datum<'mcx>,
) -> PgResult<inet_struct> {
    // PG_DETOAST_DATUM_PACKED(value): the on-disk varlena image rides the
    // by-reference arm; detoast only compressed/external (leaving a short header
    // packed), exactly as the fmgr `PG_GETARG_INET_PP` macro does.
    let bytes = value.as_ref_bytes();
    let detoasted = backend_access_common_detoast::pg_detoast_datum_packed(mcx, bytes)?;
    // (inet_struct *) VARDATA_ANY(detoasted): family, bits, then ipaddr.
    Ok(inet_struct::from_datum_bytes(vardata_any(&detoasted)))
}

// ---------------------------------------------------------------------------
// seam wiring
// ---------------------------------------------------------------------------

/// `network.c` owns no INWARD seams of its own — the `session` / `sortsupport` /
/// `planner` slots in [`backend_utils_adt_network_seams`] are OUTWARD seams it
/// CALLS (installed by the unported owner subsystems). This installer is
/// therefore empty, exactly as the seam-discipline guard expects for a crate
/// that owns no inward contract.
///
/// It does, however, register its SQL-callable `network.c` builtins into the
/// fmgr-core builtin table (C: `fmgr_builtins[]`) so by-OID dispatch resolves
/// them.
pub fn init_seams() {
    fmgr_builtins::register_network_builtins();
    // The `DatumGetInetPP` varlena-detoast edge the inet selectivity estimators
    // reach: `network.c` owns the `inet_struct` decode, and (depending on the
    // detoast owner without a cycle) it owns this slot too.
    backend_utils_adt_network_seams::inet::datum_get_inet_pp::set(datum_get_inet_pp);
}
