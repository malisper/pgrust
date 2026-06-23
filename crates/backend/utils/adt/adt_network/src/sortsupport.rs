//! SortSupport for `inet`/`cidr` (network.c:435-782): `network_sortsupport`,
//! `network_fast_cmp`, `network_abbrev_abort`, and `network_abbrev_convert`.
//!
//! The *pure* parts are ported here 1:1:
//!
//!   * [`network_fast_cmp`] — just [`crate::network_cmp_internal`].
//!   * [`network_abbrev_convert_bits`] — the abbreviated-key bit packing, an
//!     exact port of the `network_abbrev_convert` body (IPv4/IPv6 and
//!     4-byte/8-byte-datum cases) for the target ABI `Datum == usize`.
//!
//! The genuinely-external parts (installing the comparator / abbrev callbacks
//! into the live `SortSupportData` node, and the HyperLogLog cardinality
//! estimator used by `network_abbrev_abort` / the `estimating` branch of
//! `network_abbrev_convert`) cross the
//! [`::network_seams::sortsupport`] seam. With no registrar
//! installed, [`network_sortsupport`] is a faithful no-op (the btree AM falls
//! back to the ordinary `network_cmp` ordering proc).

use ::network_seams::sortsupport;
use types_network::{inet_struct, PGSQL_AF_INET};

use crate::network_cmp_internal;

/// `ip_maxbits` for the abbreviation math (private copy of `lib.rs`'s helper).
#[inline]
fn ip_maxbits(family: u8) -> i32 {
    if family == PGSQL_AF_INET {
        32
    } else {
        128
    }
}

/// `ABBREV_BITS_INET4_NETMASK_SIZE` (network.c:42).
const ABBREV_BITS_INET4_NETMASK_SIZE: u32 = 6;
/// `ABBREV_BITS_INET4_SUBNET` (network.c:43).
const ABBREV_BITS_INET4_SUBNET: u32 = 25;

/// `SIZEOF_DATUM` for the target ABI (`Datum == usize`).
const SIZEOF_DATUM: u32 = core::mem::size_of::<usize>() as u32;
/// `BITS_PER_BYTE` (c.h).
const BITS_PER_BYTE: u32 = 8;

/// `network_fast_cmp` (network.c:471). SortSupport comparison func.
pub fn network_fast_cmp(arg1: &inet_struct, arg2: &inet_struct) -> i32 {
    network_cmp_internal(arg1, arg2)
}

/// `network_abbrev_convert` (network.c:617), the pure key-packing core.
///
/// Produces the abbreviated sort key for `authoritative` — the exact bit math of
/// the C function for the running ABI's `Datum` width, minus the HyperLogLog
/// `addHyperLogLog` side effect (which lives behind the
/// [`::network_seams::sortsupport`] seam together with
/// `uss->input_count` / `estimating`).
pub fn network_abbrev_convert_bits(authoritative: &inet_struct) -> usize {
    let data = authoritative;
    debug_assert!(data.family == PGSQL_AF_INET || data.family == ::types_network::PGSQL_AF_INET6);

    // Unsigned integer representation of the IP address: take the first 4 or 8
    // bytes. The inet's ipaddr is most-significant-byte first, so interpret
    // big-endian (== byteswap on little-endian machines).
    let ipaddr_datum: usize;
    let mut res: usize;
    if data.family == PGSQL_AF_INET {
        let mut b4 = [0u8; 4];
        b4.copy_from_slice(&data.ipaddr[..4]);
        ipaddr_datum = u32::from_be_bytes(b4) as usize;
        // Initialize result without setting ipfamily bit.
        res = 0;
    } else {
        let mut b8 = [0u8; 8];
        b8.copy_from_slice(&data.ipaddr[..8]);
        ipaddr_datum = u64::from_be_bytes(b8) as usize;
        // Initialize result with ipfamily (most significant) bit set.
        res = 1usize << (SIZEOF_DATUM * BITS_PER_BYTE - 1);
    }

    // Split ipaddr_datum into "network" (high bits) and "subnet" (low bits).
    let mut subnet_size = ip_maxbits(data.family) - data.bits as i32;
    debug_assert!(subnet_size >= 0);
    // subnet size must work with prefix ipaddr cases
    subnet_size %= (SIZEOF_DATUM * BITS_PER_BYTE) as i32;

    let subnet_bitmask: usize;
    let network: usize;
    if data.bits == 0 {
        // Fit as many ipaddr bits as possible into subnet.
        subnet_bitmask = 0usize.wrapping_sub(1);
        network = 0;
    } else if (data.bits as u32) < SIZEOF_DATUM * BITS_PER_BYTE {
        // Split ipaddr bits between network and subnet.
        subnet_bitmask = (1usize << subnet_size) - 1;
        network = ipaddr_datum & !subnet_bitmask;
    } else {
        // Fit as many ipaddr bits as possible into network.
        subnet_bitmask = 0;
        network = ipaddr_datum;
    }

    if SIZEOF_DATUM == 8 && data.family == PGSQL_AF_INET {
        // IPv4 with 8 byte datums: keep all 32 netmasked bits, netmask size, and
        // most significant 25 subnet bits.
        let netmask_size: usize = data.bits as usize;
        let mut subnet: usize;

        // Shift left 31 bits: 6 bits netmask size + 25 subnet bits.
        let network = network << (ABBREV_BITS_INET4_NETMASK_SIZE + ABBREV_BITS_INET4_SUBNET);

        // Shift size to make room for subnet bits at the end.
        let netmask_size = netmask_size << ABBREV_BITS_INET4_SUBNET;

        // Extract subnet bits without shifting them.
        subnet = ipaddr_datum & subnet_bitmask;

        // If we have more than 25 subnet bits, shift subnet down.
        if subnet_size > ABBREV_BITS_INET4_SUBNET as i32 {
            subnet >>= subnet_size - ABBREV_BITS_INET4_SUBNET as i32;
        }

        // Assemble final key without clobbering the ipfamily bit.
        res |= network | netmask_size | subnet;
    } else {
        // 4 byte datums, or IPv6 with 8 byte datums: use as many netmasked bits
        // as will fit. Avoid clobbering the ipfamily bit set earlier.
        res |= network >> 1;
    }

    res
}

/// `network_sortsupport` (network.c:435). SortSupport strategy routine.
///
/// Installing `network_fast_cmp` / `network_abbrev_convert` /
/// `network_abbrev_abort` (adapted to the fmgr `SortSupport` ABI, with a
/// HyperLogLog estimator) into the live `SortSupportData` node — together with
/// `network_abbrev_abort`'s HyperLogLog cardinality estimation — belongs to the
/// tuplesort / `lib/hyperloglog` subsystems and is delegated to the
/// [`::network_seams::sortsupport::register`] seam. The pure
/// key-packing it depends on is [`network_abbrev_convert_bits`] and the pure
/// comparator is [`network_fast_cmp`]. Returns whether a registrar was wired
/// (the default is a faithful no-op, as if sortsupport were never registered).
pub fn network_sortsupport() -> bool {
    sortsupport::register::call()
}
