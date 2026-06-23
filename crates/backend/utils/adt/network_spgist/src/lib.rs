#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]
#![allow(clippy::result_large_err)]

//! `src/backend/utils/adt/network_spgist.c` (PostgreSQL 18.3): SP-GiST support
//! for the network (`inet`/`cidr`) types — the `inet_ops` SP-GiST opclass.
//!
//! We split inet index entries first by address family (IPv4 or IPv6). If the
//! entries below a given inner tuple are all of the same family, we identify
//! their common prefix and split by the next bit of the address, and by whether
//! their masklens exceed the length of the common prefix.
//!
//! An inner tuple that has both IPv4 and IPv6 children has a null prefix and
//! exactly two nodes, the first being for IPv4 and the second for IPv6.
//!
//! Otherwise, the prefix is a `cidr` value representing the common prefix, and
//! there are exactly four nodes. Node numbers 0 and 1 are for addresses with
//! the same masklen as the prefix, while node numbers 2 and 3 are for addresses
//! with larger masklen. Node numbers 0 and 1 are distinguished by the next bit
//! of the address after the common prefix, and likewise for node numbers 2 and
//! 3.
//!
//! Every function in `network_spgist.c` is ported 1:1 — identical control flow,
//! branch order, loop bounds, switch arms and `Assert`s:
//!
//!   * [`inet_spg_config`]            (config, `network_spgist.c:54`)
//!   * [`inet_spg_choose`]            (choose, `network_spgist.c:73`)
//!   * [`inet_spg_picksplit`]         (picksplit, `network_spgist.c:172`)
//!   * [`inet_spg_inner_consistent`]  (inner consistent, `network_spgist.c:251`)
//!   * [`inet_spg_leaf_consistent`]   (leaf consistent, `network_spgist.c:339`)
//!   * `inet_spg_node_number`        (static helper, `network_spgist.c:366`)
//!   * `inet_spg_consistent_bitmap`  (static helper, `network_spgist.c:391`)
//!
//! ## Idiomatic carriers / fmgr boundary
//!
//! The C fmgr entry points receive `Pointer`s to the typed `spg*In`/`spg*Out`
//! structs and `palloc` their output arrays. As in the sibling SP-GiST opclasses
//! (`backend-access-spg-quadtree` / `-kdtree` / `backend-access-spg-text`), the
//! bodies here operate directly on the owned [`spgist`] vocabulary structs,
//! with the `inet`/`cidr` payloads carried inside [`::types_tuple::Datum::ByRef`]
//! varlena images. "Allocate an output array" becomes "fill an owned `Vec`".
//!
//! `DatumGetInetPP(d)` becomes [`inet_struct::from_datum_bytes`] over the
//! by-reference image, and `InetPGetDatum(p)` / `cidr_set_masklen_internal`
//! become the [`inet_get_datum`] image writer. Both `inet` and `cidr` share the
//! same on-disk `inet_struct` image, so the `cidr` prefix datums use the same
//! codec. The bit helpers (`bitncmp`, `bitncommon`) and `cidr_set_masklen_internal`
//! reuse the ported implementations in [`adt_network`].
//!
//! The SP-GiST core dispatches its five opclass support procedures by OID
//! through `backend-access-spg-core-seams`; `backend-access-spg-quadtree` is the
//! single installer of that by-OID dispatch and routes the inet support-proc
//! OIDs (config 3795 / choose 3796 / picksplit 3797 / inner_consistent 3798 /
//! leaf_consistent 3799) to the bodies exported here, exactly as it routes the
//! quad-tree / k-d-tree / text opclasses.

use adt_network::{bitncmp, bitncommon, cidr_set_masklen_internal};
use ::mcx::Mcx;
use ::types_core::primitive::Oid;
use ::types_error::PgResult;
use types_network::{inet_struct, PGSQL_AF_INET};
use spgist::{
    spgChooseIn, spgChooseOut, spgChooseOutMatchNode, spgChooseOutResult, spgChooseOutSplitTuple,
    spgConfigIn, spgConfigOut, spgInnerConsistentIn, spgInnerConsistentOut, spgLeafConsistentIn,
    spgLeafConsistentOut, spgPickSplitIn, spgPickSplitOut,
};
use ::types_scan::scankey as types_scan_scankey;
use types_tuple::heaptuple::Datum;

// ---------------------------------------------------------------------------
// pg_proc.dat support-proc OIDs for the inet_ops SP-GiST opclass.
// (pg_amproc.dat: amprocnum 1..5 for inet/inet under the spgist AM.)
// ---------------------------------------------------------------------------

/// `inet_spg_config` (pg_proc.dat oid 3795).
pub const F_INET_SPG_CONFIG: Oid = 3795;
/// `inet_spg_choose` (pg_proc.dat oid 3796).
pub const F_INET_SPG_CHOOSE: Oid = 3796;
/// `inet_spg_picksplit` (pg_proc.dat oid 3797).
pub const F_INET_SPG_PICKSPLIT: Oid = 3797;
/// `inet_spg_inner_consistent` (pg_proc.dat oid 3798).
pub const F_INET_SPG_INNER_CONSISTENT: Oid = 3798;
/// `inet_spg_leaf_consistent` (pg_proc.dat oid 3799).
pub const F_INET_SPG_LEAF_CONSISTENT: Oid = 3799;

// ---------------------------------------------------------------------------
// catalog/pg_type.dat type OIDs used by inet_spg_config.
// ---------------------------------------------------------------------------

/// `CIDROID` (pg_type.dat oid 650).
const CIDROID: Oid = 650;
/// `VOIDOID` (pg_type.dat oid 2278).
const VOIDOID: Oid = 2278;

// ---------------------------------------------------------------------------
// access/stratnum.h — R-tree strategy numbers consumed by the consistency
// checks (the inet_ops opclass uses the RT* spelling throughout).
// ---------------------------------------------------------------------------

/// `RTEqualStrategyNumber` = 18.
const RTEqualStrategyNumber: u16 = 18;
/// `RTNotEqualStrategyNumber` = 19.
const RTNotEqualStrategyNumber: u16 = 19;
/// `RTLessStrategyNumber` = 20.
const RTLessStrategyNumber: u16 = 20;
/// `RTLessEqualStrategyNumber` = 21.
const RTLessEqualStrategyNumber: u16 = 21;
/// `RTGreaterStrategyNumber` = 22.
const RTGreaterStrategyNumber: u16 = 22;
/// `RTGreaterEqualStrategyNumber` = 23.
const RTGreaterEqualStrategyNumber: u16 = 23;
/// `RTSubStrategyNumber` = 24 (for inet `>>`).
const RTSubStrategyNumber: u16 = 24;
/// `RTSubEqualStrategyNumber` = 25 (for inet `>>=`).
const RTSubEqualStrategyNumber: u16 = 25;
/// `RTSuperStrategyNumber` = 26 (for inet `<<`).
const RTSuperStrategyNumber: u16 = 26;
/// `RTSuperEqualStrategyNumber` = 27 (for inet `<<=`).
const RTSuperEqualStrategyNumber: u16 = 27;

// ---------------------------------------------------------------------------
// utils/inet.h accessors over the `inet` value.
// ---------------------------------------------------------------------------

/// `ip_family(inetptr)` (utils/inet.h).
#[inline]
fn ip_family(val: &inet_struct) -> i32 {
    val.family as i32
}
/// `ip_bits(inetptr)` (utils/inet.h).
#[inline]
fn ip_bits(val: &inet_struct) -> i32 {
    val.bits as i32
}
/// `ip_addr(inetptr)` (utils/inet.h).
#[inline]
fn ip_addr(val: &inet_struct) -> &[u8; 16] {
    &val.ipaddr
}
/// `ip_maxbits(inetptr)` (utils/inet.h) — 128 for IPv6, else 32.
#[inline]
fn ip_maxbits(val: &inet_struct) -> i32 {
    if val.family == ::types_network::PGSQL_AF_INET6 {
        128
    } else {
        32
    }
}

/// `Min(a, b)` (c.h).
#[inline]
fn Min(a: i32, b: i32) -> i32 {
    if a < b {
        a
    } else {
        b
    }
}

// ---------------------------------------------------------------------------
// fmgr boundary codecs (DatumGetInetPP / InetPGetDatum).
// ---------------------------------------------------------------------------

/// `VARHDRSZ` — the 4-byte uncompressed varlena length word.
const VARHDRSZ: usize = 4;
/// `VARHDRSZ_SHORT` — a short (1-byte-header) varlena's header.
const VARHDRSZ_SHORT: usize = 1;

/// `VARDATA_ANY(PTR)` — payload bytes after either the 1-byte short header or the
/// 4-byte long header. `inet`/`cidr` arrive on the by-ref lane as the on-disk
/// varlena image (header included), so the `inet_struct` payload must be read at
/// `VARDATA_ANY`, exactly as `network.c`'s `ip_family`/`ip_bits`/`ip_addr` do.
#[inline]
fn vardata_any(b: &[u8]) -> &[u8] {
    if (b[0] & 0x01) == 0x01 {
        &b[VARHDRSZ_SHORT..]
    } else {
        &b[VARHDRSZ..]
    }
}

/// `DatumGetInetPP(datum)` — decode an `inet`/`cidr` by-reference image. The
/// canonical by-ref `Datum` carries the full varlena (4-byte or short header),
/// so strip the header before decoding the `inet_struct` payload.
#[inline]
fn datum_get_inet(datum: &Datum<'_>) -> inet_struct {
    inet_struct::from_datum_bytes(vardata_any(datum.as_ref_bytes()))
}

/// `InetPGetDatum(p)` — encode an `inet`/`cidr` value as a by-reference `Datum`
/// in `mcx` (the C macro yields a `palloc`'d varlena). Emit a 4-byte-header
/// varlena image (`SET_VARSIZE`) so the prefix datums this opclass builds
/// round-trip through `datum_get_inet` identically to the on-disk leaf values.
#[inline]
fn inet_get_datum<'mcx>(mcx: Mcx<'mcx>, p: &inet_struct) -> PgResult<Datum<'mcx>> {
    let payload = p.to_datum_bytes();
    let total = payload.len() + VARHDRSZ;
    let mut img = Vec::with_capacity(total);
    img.extend_from_slice(&((total as u32) << 2).to_le_bytes());
    img.extend_from_slice(&payload);
    Ok(Datum::ByRef(::mcx::slice_in(mcx, &img)?))
}

// ===========================================================================
// The SP-GiST configuration function (network_spgist.c:54).
// ===========================================================================

/// `inet_spg_config` (network_spgist.c:54). Fill the opclass config output.
pub fn inet_spg_config(_cfgin: &spgConfigIn, cfg: &mut spgConfigOut) {
    cfg.prefixType = CIDROID;
    cfg.labelType = VOIDOID;
    cfg.canReturnData = true;
    cfg.longValuesOK = false;
}

// ===========================================================================
// The SP-GiST choose function (network_spgist.c:73).
// ===========================================================================

/// `inet_spg_choose` (network_spgist.c:73). The SP-GiST choose function.
pub fn inet_spg_choose<'mcx>(
    mcx: Mcx<'mcx>,
    in_: &spgChooseIn<'mcx>,
    out: &mut spgChooseOut<'mcx>,
) -> PgResult<()> {
    let val = datum_get_inet(&in_.datum);
    let mut commonbits: i32;

    // If we're looking at a tuple that splits by address family, choose the
    // appropriate subnode.
    if !in_.hasPrefix {
        // allTheSame isn't possible for such a tuple
        debug_assert!(!in_.allTheSame);
        debug_assert!(in_.nNodes == 2);

        out.result = spgChooseOutResult::MatchNode(spgChooseOutMatchNode {
            nodeN: if ip_family(&val) == PGSQL_AF_INET as i32 {
                0
            } else {
                1
            },
            levelAdd: 0,
            restDatum: inet_get_datum(mcx, &val)?,
        });

        return Ok(());
    }

    // Else it must split by prefix
    debug_assert!(in_.nNodes == 4 || in_.allTheSame);

    let prefix = datum_get_inet(&in_.prefixDatum);
    commonbits = ip_bits(&prefix);

    // We cannot put addresses from different families under the same inner
    // node, so we have to split if the new value's family is different.
    if ip_family(&val) != ip_family(&prefix) {
        // Set up 2-node tuple
        out.result = spgChooseOutResult::SplitTuple(spgChooseOutSplitTuple {
            prefixHasPrefix: false,
            prefixPrefixDatum: Datum::ByVal(0),
            prefixNNodes: 2,
            prefixNodeLabels: None,

            // Identify which node the existing data goes into
            childNodeN: if ip_family(&prefix) == PGSQL_AF_INET as i32 {
                0
            } else {
                1
            },

            postfixHasPrefix: true,
            postfixPrefixDatum: inet_get_datum(mcx, &prefix)?,
        });

        return Ok(());
    }

    // If the new value does not match the existing prefix, we have to split.
    if ip_bits(&val) < commonbits
        || bitncmp(ip_addr(&prefix), ip_addr(&val), commonbits) != 0
    {
        // Determine new prefix length for the split tuple
        commonbits = bitncommon(
            ip_addr(&prefix),
            ip_addr(&val),
            Min(ip_bits(&val), commonbits),
        );

        // Set up 4-node tuple
        out.result = spgChooseOutResult::SplitTuple(spgChooseOutSplitTuple {
            prefixHasPrefix: true,
            prefixPrefixDatum: inet_get_datum(
                mcx,
                &cidr_set_masklen_internal(&val, commonbits),
            )?,
            prefixNNodes: 4,
            prefixNodeLabels: None,

            // Identify which node the existing data goes into
            childNodeN: inet_spg_node_number(&prefix, commonbits),

            postfixHasPrefix: true,
            postfixPrefixDatum: inet_get_datum(mcx, &prefix)?,
        });

        return Ok(());
    }

    // All OK, choose the node to descend into. (If this tuple is marked
    // allTheSame, the core code will ignore our choice of nodeN; but we need
    // not account for that case explicitly here.)
    out.result = spgChooseOutResult::MatchNode(spgChooseOutMatchNode {
        nodeN: inet_spg_node_number(&val, commonbits),
        levelAdd: 0,
        restDatum: inet_get_datum(mcx, &val)?,
    });

    Ok(())
}

// ===========================================================================
// The SP-GiST PickSplit method (network_spgist.c:172).
// ===========================================================================

/// `inet_spg_picksplit` (network_spgist.c:172). The SP-GiST PickSplit method.
pub fn inet_spg_picksplit<'mcx>(
    mcx: Mcx<'mcx>,
    in_: &spgPickSplitIn<'mcx>,
    out: &mut spgPickSplitOut<'mcx>,
) -> PgResult<()> {
    let mut differentFamilies = false;

    // Initialize the prefix with the first item
    let prefix = datum_get_inet(&in_.datums[0]);
    let mut commonbits = ip_bits(&prefix);

    // Examine remaining items to discover minimum common prefix length
    let mut i = 1;
    while i < in_.nTuples() {
        let tmp = datum_get_inet(&in_.datums[i as usize]);

        if ip_family(&tmp) != ip_family(&prefix) {
            differentFamilies = true;
            break;
        }

        if ip_bits(&tmp) < commonbits {
            commonbits = ip_bits(&tmp);
        }
        commonbits = bitncommon(ip_addr(&prefix), ip_addr(&tmp), commonbits);
        if commonbits == 0 {
            break;
        }
        i += 1;
    }

    // Don't need labels; allocate output arrays
    out.nodeLabels = None;
    let mut mapTuplesToNodes: Vec<i32> = Vec::with_capacity(in_.nTuples() as usize);
    let mut leafTupleDatums: Vec<Datum<'mcx>> = Vec::with_capacity(in_.nTuples() as usize);

    if differentFamilies {
        // Set up 2-node tuple
        out.hasPrefix = false;
        out.prefixDatum = None;
        out.nNodes = 2;

        for i in 0..in_.nTuples() {
            let tmp = datum_get_inet(&in_.datums[i as usize]);
            mapTuplesToNodes.push(if ip_family(&tmp) == PGSQL_AF_INET as i32 {
                0
            } else {
                1
            });
            leafTupleDatums.push(inet_get_datum(mcx, &tmp)?);
        }
    } else {
        // Set up 4-node tuple
        out.hasPrefix = true;
        out.prefixDatum = Some(inet_get_datum(
            mcx,
            &cidr_set_masklen_internal(&prefix, commonbits),
        )?);
        out.nNodes = 4;

        for i in 0..in_.nTuples() {
            let tmp = datum_get_inet(&in_.datums[i as usize]);
            mapTuplesToNodes.push(inet_spg_node_number(&tmp, commonbits));
            leafTupleDatums.push(inet_get_datum(mcx, &tmp)?);
        }
    }

    out.mapTuplesToNodes = mapTuplesToNodes;
    out.leafTupleDatums = leafTupleDatums;

    Ok(())
}

// ===========================================================================
// The SP-GiST query consistency check for inner tuples (network_spgist.c:251).
// ===========================================================================

/// `inet_spg_inner_consistent` (network_spgist.c:251). The SP-GiST query
/// consistency check for inner tuples.
pub fn inet_spg_inner_consistent<'mcx>(
    in_: &spgInnerConsistentIn<'mcx>,
    out: &mut spgInnerConsistentOut<'mcx>,
) -> PgResult<()> {
    let which: i32;

    if !in_.hasPrefix {
        debug_assert!(!in_.allTheSame);
        debug_assert!(in_.nNodes == 2);

        // Identify which child nodes need to be visited
        let mut w = 1 | (1 << 1);

        for i in 0..in_.nkeys() {
            let strategy = in_.scankeys[i as usize].sk_strategy;
            let argument = datum_get_inet(&in_.scankeys[i as usize].sk_argument);

            match strategy {
                RTLessStrategyNumber | RTLessEqualStrategyNumber => {
                    if ip_family(&argument) == PGSQL_AF_INET as i32 {
                        w &= 1;
                    }
                }
                RTGreaterEqualStrategyNumber | RTGreaterStrategyNumber => {
                    if ip_family(&argument) == ::types_network::PGSQL_AF_INET6 as i32 {
                        w &= 1 << 1;
                    }
                }
                RTNotEqualStrategyNumber => {}
                _ => {
                    // all other ops can only match addrs of same family
                    if ip_family(&argument) == PGSQL_AF_INET as i32 {
                        w &= 1;
                    } else {
                        w &= 1 << 1;
                    }
                }
            }
        }

        which = w;
    } else if !in_.allTheSame {
        debug_assert!(in_.nNodes == 4);

        // Identify which child nodes need to be visited
        which = inet_spg_consistent_bitmap(
            &datum_get_inet(&in_.prefixDatum),
            in_.nkeys(),
            &in_.scankeys,
            false,
        );
    } else {
        // Must visit all nodes; we assume there are less than 32 of 'em
        which = !0;
    }

    out.nNodes = 0;

    if which != 0 {
        let mut nodeNumbers: Vec<i32> = Vec::with_capacity(in_.nNodes as usize);

        for i in 0..in_.nNodes {
            if which & (1 << i) != 0 {
                nodeNumbers.push(i);
                out.nNodes += 1;
            }
        }

        out.nodeNumbers = nodeNumbers;
    }

    Ok(())
}

// ===========================================================================
// The SP-GiST query consistency check for leaf tuples (network_spgist.c:339).
// ===========================================================================

/// `inet_spg_leaf_consistent` (network_spgist.c:339). The SP-GiST query
/// consistency check for leaf tuples. Returns the C `PG_RETURN_BOOL(...)`.
pub fn inet_spg_leaf_consistent<'mcx>(
    mcx: Mcx<'mcx>,
    in_: &spgLeafConsistentIn<'mcx>,
    out: &mut spgLeafConsistentOut<'mcx>,
) -> PgResult<bool> {
    let leaf = datum_get_inet(&in_.leafDatum);

    // All tests are exact.
    out.recheck = false;

    // Leaf is what it is...
    out.leafValue = Some(inet_get_datum(mcx, &leaf)?);

    // Use common code to apply the tests.
    Ok(inet_spg_consistent_bitmap(&leaf, in_.nkeys(), &in_.scankeys, true) != 0)
}

// ===========================================================================
// Static helpers.
// ===========================================================================

/// `inet_spg_node_number(val, commonbits)` (network_spgist.c:366). Calculate
/// node number (within a 4-node, single-family inner index tuple).
fn inet_spg_node_number(val: &inet_struct, commonbits: i32) -> i32 {
    let mut nodeN = 0;

    if commonbits < ip_maxbits(val)
        && (ip_addr(val)[(commonbits / 8) as usize] & (1 << (7 - commonbits % 8))) != 0
    {
        nodeN |= 1;
    }
    if commonbits < ip_bits(val) {
        nodeN |= 2;
    }

    nodeN
}

/// `inet_spg_consistent_bitmap(prefix, nkeys, scankeys, leaf)`
/// (network_spgist.c:391). Calculate bitmap of node numbers that are consistent
/// with the query.
///
/// This can be used either at a 4-way inner tuple, or at a leaf tuple. In the
/// latter case, we should return a boolean result (0 or 1) not a bitmap.
fn inet_spg_consistent_bitmap(
    prefix: &inet_struct,
    nkeys: i32,
    scankeys: &[types_scan_scankey::ScanKeyData<'_>],
    leaf: bool,
) -> i32 {
    // Initialize result to allow visiting all children
    let mut bitmap = if leaf {
        1
    } else {
        1 | (1 << 1) | (1 << 2) | (1 << 3)
    };

    let commonbits = ip_bits(prefix);

    for i in 0..nkeys {
        let argument = datum_get_inet(&scankeys[i as usize].sk_argument);
        let strategy = scankeys[i as usize].sk_strategy;
        let mut order;

        // Check 0: different families
        //
        // Matching families do not help any of the strategies.
        if ip_family(&argument) != ip_family(prefix) {
            match strategy {
                RTLessStrategyNumber | RTLessEqualStrategyNumber => {
                    if ip_family(&argument) < ip_family(prefix) {
                        bitmap = 0;
                    }
                }
                RTGreaterEqualStrategyNumber | RTGreaterStrategyNumber => {
                    if ip_family(&argument) > ip_family(prefix) {
                        bitmap = 0;
                    }
                }
                RTNotEqualStrategyNumber => {}
                _ => {
                    // For all other cases, we can be sure there is no match
                    bitmap = 0;
                }
            }

            if bitmap == 0 {
                break;
            }

            // Other checks make no sense with different families.
            continue;
        }

        // Check 1: network bit count
        //
        // Network bit count (ip_bits) helps to check leaves for sub network and
        // sup network operators. At non-leaf nodes, we know every child value
        // has greater ip_bits, so we can avoid descending in some cases too.
        //
        // This check is less expensive than checking the address bits, so we
        // are doing this before, but it has to be done after for the basic
        // comparison strategies, because ip_bits only affect their results when
        // the common network bits are the same.
        match strategy {
            RTSubStrategyNumber => {
                if commonbits <= ip_bits(&argument) {
                    bitmap &= (1 << 2) | (1 << 3);
                }
            }
            RTSubEqualStrategyNumber => {
                if commonbits < ip_bits(&argument) {
                    bitmap &= (1 << 2) | (1 << 3);
                }
            }
            RTSuperStrategyNumber => {
                if commonbits == ip_bits(&argument) - 1 {
                    bitmap &= 1 | (1 << 1);
                } else if commonbits >= ip_bits(&argument) {
                    bitmap = 0;
                }
            }
            RTSuperEqualStrategyNumber => {
                if commonbits == ip_bits(&argument) {
                    bitmap &= 1 | (1 << 1);
                } else if commonbits > ip_bits(&argument) {
                    bitmap = 0;
                }
            }
            RTEqualStrategyNumber => {
                if commonbits < ip_bits(&argument) {
                    bitmap &= (1 << 2) | (1 << 3);
                } else if commonbits == ip_bits(&argument) {
                    bitmap &= 1 | (1 << 1);
                } else {
                    bitmap = 0;
                }
            }
            _ => {}
        }

        if bitmap == 0 {
            break;
        }

        // Check 2: common network bits
        //
        // Compare available common prefix bits to the query, but not beyond
        // either the query's netmask or the minimum netmask among the
        // represented values. If these bits don't match the query, we can
        // eliminate some cases.
        order = bitncmp(
            ip_addr(prefix),
            ip_addr(&argument),
            Min(commonbits, ip_bits(&argument)),
        );

        if order != 0 {
            match strategy {
                RTLessStrategyNumber | RTLessEqualStrategyNumber => {
                    if order > 0 {
                        bitmap = 0;
                    }
                }
                RTGreaterEqualStrategyNumber | RTGreaterStrategyNumber => {
                    if order < 0 {
                        bitmap = 0;
                    }
                }
                RTNotEqualStrategyNumber => {}
                _ => {
                    // For all other cases, we can be sure there is no match
                    bitmap = 0;
                }
            }

            if bitmap == 0 {
                break;
            }

            // Remaining checks make no sense when common bits don't match.
            continue;
        }

        // Check 3: next network bit
        //
        // We can filter out branch 2 or 3 using the next network bit of the
        // argument, if it is available.
        //
        // This check matters for the performance of the search. The results
        // would be correct without it.
        if (bitmap & ((1 << 2) | (1 << 3))) != 0 && commonbits < ip_bits(&argument) {
            let nextbit =
                ip_addr(&argument)[(commonbits / 8) as usize] & (1 << (7 - commonbits % 8));

            match strategy {
                RTLessStrategyNumber | RTLessEqualStrategyNumber => {
                    if nextbit == 0 {
                        bitmap &= 1 | (1 << 1) | (1 << 2);
                    }
                }
                RTGreaterEqualStrategyNumber | RTGreaterStrategyNumber => {
                    if nextbit != 0 {
                        bitmap &= 1 | (1 << 1) | (1 << 3);
                    }
                }
                RTNotEqualStrategyNumber => {}
                _ => {
                    if nextbit == 0 {
                        bitmap &= 1 | (1 << 1) | (1 << 2);
                    } else {
                        bitmap &= 1 | (1 << 1) | (1 << 3);
                    }
                }
            }

            if bitmap == 0 {
                break;
            }
        }

        // Remaining checks are only for the basic comparison strategies. This
        // test relies on the strategy number ordering defined in stratnum.h.
        if strategy < RTEqualStrategyNumber || strategy > RTGreaterEqualStrategyNumber {
            continue;
        }

        // Check 4: network bit count
        //
        // At this point, we know that the common network bits of the prefix and
        // the argument are the same, so we can go forward and check the ip_bits.
        match strategy {
            RTLessStrategyNumber | RTLessEqualStrategyNumber => {
                if commonbits == ip_bits(&argument) {
                    bitmap &= 1 | (1 << 1);
                } else if commonbits > ip_bits(&argument) {
                    bitmap = 0;
                }
            }
            RTGreaterEqualStrategyNumber | RTGreaterStrategyNumber => {
                if commonbits < ip_bits(&argument) {
                    bitmap &= (1 << 2) | (1 << 3);
                }
            }
            _ => {}
        }

        if bitmap == 0 {
            break;
        }

        // Remaining checks don't make sense with different ip_bits.
        if commonbits != ip_bits(&argument) {
            continue;
        }

        // Check 5: next host bit
        //
        // We can filter out branch 0 or 1 using the next host bit of the
        // argument, if it is available.
        //
        // This check matters for the performance of the search. The results
        // would be correct without it. There is no point in running it for
        // leafs as we have to check the whole address on the next step.
        if !leaf && (bitmap & (1 | (1 << 1))) != 0 && commonbits < ip_maxbits(&argument) {
            let nextbit =
                ip_addr(&argument)[(commonbits / 8) as usize] & (1 << (7 - commonbits % 8));

            match strategy {
                RTLessStrategyNumber | RTLessEqualStrategyNumber => {
                    if nextbit == 0 {
                        bitmap &= 1 | (1 << 2) | (1 << 3);
                    }
                }
                RTGreaterEqualStrategyNumber | RTGreaterStrategyNumber => {
                    if nextbit != 0 {
                        bitmap &= (1 << 1) | (1 << 2) | (1 << 3);
                    }
                }
                RTNotEqualStrategyNumber => {}
                _ => {
                    if nextbit == 0 {
                        bitmap &= 1 | (1 << 2) | (1 << 3);
                    } else {
                        bitmap &= (1 << 1) | (1 << 2) | (1 << 3);
                    }
                }
            }

            if bitmap == 0 {
                break;
            }
        }

        // Check 6: whole address
        //
        // This is the last check for correctness of the basic comparison
        // strategies. It's only appropriate at leaf entries.
        if leaf {
            // Redo ordering comparison using all address bits
            order = bitncmp(ip_addr(prefix), ip_addr(&argument), ip_maxbits(prefix));

            match strategy {
                RTLessStrategyNumber => {
                    if order >= 0 {
                        bitmap = 0;
                    }
                }
                RTLessEqualStrategyNumber => {
                    if order > 0 {
                        bitmap = 0;
                    }
                }
                RTEqualStrategyNumber => {
                    if order != 0 {
                        bitmap = 0;
                    }
                }
                RTGreaterEqualStrategyNumber => {
                    if order < 0 {
                        bitmap = 0;
                    }
                }
                RTGreaterStrategyNumber => {
                    if order <= 0 {
                        bitmap = 0;
                    }
                }
                RTNotEqualStrategyNumber => {
                    if order == 0 {
                        bitmap = 0;
                    }
                }
                _ => {}
            }

            if bitmap == 0 {
                break;
            }
        }
    }

    bitmap
}

#[cfg(test)]
mod tests {
    use super::*;
    use ::types_network::PGSQL_AF_INET6;

    /// Build a v4 `inet_struct` from `a.b.c.d/bits`.
    fn v4(a: u8, b: u8, c: u8, d: u8, bits: u8) -> inet_struct {
        let mut ipaddr = [0u8; 16];
        ipaddr[0] = a;
        ipaddr[1] = b;
        ipaddr[2] = c;
        ipaddr[3] = d;
        inet_struct {
            family: PGSQL_AF_INET,
            bits,
            ipaddr,
        }
    }

    fn v6(prefix: &[u8], bits: u8) -> inet_struct {
        let mut ipaddr = [0u8; 16];
        ipaddr[..prefix.len()].copy_from_slice(prefix);
        inet_struct {
            family: PGSQL_AF_INET6,
            bits,
            ipaddr,
        }
    }

    #[test]
    fn config_sets_cidr_void() {
        let mut cfg = spgConfigOut::default();
        inet_spg_config(&spgConfigIn::default(), &mut cfg);
        assert_eq!(cfg.prefixType, 650); // CIDROID
        assert_eq!(cfg.labelType, 2278); // VOIDOID
        assert!(cfg.canReturnData);
        assert!(!cfg.longValuesOK);
    }

    #[test]
    fn node_number_branches() {
        // commonbits=24, address 10.0.0.1/32: next bit (bit 24) is the MSB of
        // byte 3 = 0 here (value 1 has MSB 0), and 24 < bits(32) so |= 2.
        let val = v4(10, 0, 0, 1, 32);
        assert_eq!(inet_spg_node_number(&val, 24), 2);
        // 10.0.0.128/32: bit 24 (MSB of 0x80) set -> |=1, plus |=2 -> 3.
        let val = v4(10, 0, 0, 128, 32);
        assert_eq!(inet_spg_node_number(&val, 24), 3);
        // commonbits == ip_maxbits (32): no next bit, and 32<bits(32) false -> 0.
        let val = v4(10, 0, 0, 128, 32);
        assert_eq!(inet_spg_node_number(&val, 32), 0);
        // commonbits==ip_bits(masklen) but < maxbits, next bit set -> only |=1.
        let val = v4(10, 0, 0, 128, 24);
        assert_eq!(inet_spg_node_number(&val, 24), 1);
    }

    #[test]
    fn choose_family_split_no_prefix() {
        let owned = ::mcx::MemoryContext::new("t");
        let mcx = owned.mcx();
        let val = v4(10, 0, 0, 1, 32);
        let in_ = spgChooseIn {
            datum: inet_get_datum(mcx, &val).unwrap(),
            leafDatum: Datum::ByVal(0),
            level: 0,
            allTheSame: false,
            hasPrefix: false,
            prefixDatum: Datum::ByVal(0),
            nNodes: 2,
            nodeLabels: None,
        };
        let mut out = spgChooseOut {
            result: spgChooseOutResult::MatchNode(spgChooseOutMatchNode {
                nodeN: -1,
                levelAdd: 0,
                restDatum: Datum::ByVal(0),
            }),
        };
        inet_spg_choose(mcx, &in_, &mut out).unwrap();
        match out.result {
            spgChooseOutResult::MatchNode(m) => {
                assert_eq!(m.nodeN, 0); // IPv4 -> node 0
                assert_eq!(datum_get_inet(&m.restDatum), val);
            }
            _ => panic!("expected MatchNode"),
        }
    }

    #[test]
    fn choose_split_on_family_mismatch() {
        let owned = ::mcx::MemoryContext::new("t");
        let mcx = owned.mcx();
        let val = v6(&[0x20, 0x01], 64);
        let prefix = v4(10, 0, 0, 0, 8);
        let in_ = spgChooseIn {
            datum: inet_get_datum(mcx, &val).unwrap(),
            leafDatum: Datum::ByVal(0),
            level: 0,
            allTheSame: false,
            hasPrefix: true,
            prefixDatum: inet_get_datum(mcx, &prefix).unwrap(),
            nNodes: 4,
            nodeLabels: None,
        };
        let mut out = spgChooseOut {
            result: spgChooseOutResult::MatchNode(spgChooseOutMatchNode {
                nodeN: -1,
                levelAdd: 0,
                restDatum: Datum::ByVal(0),
            }),
        };
        inet_spg_choose(mcx, &in_, &mut out).unwrap();
        match out.result {
            spgChooseOutResult::SplitTuple(s) => {
                assert!(!s.prefixHasPrefix);
                assert_eq!(s.prefixNNodes, 2);
                assert_eq!(s.childNodeN, 0); // existing prefix is IPv4 -> 0
                assert!(s.postfixHasPrefix);
                assert_eq!(datum_get_inet(&s.postfixPrefixDatum), prefix);
            }
            _ => panic!("expected SplitTuple"),
        }
    }

    /// A leaf-consistency EQ test mirroring the C `inet_spg_leaf_consistent`
    /// returning the boolean bitmap result.
    #[test]
    fn leaf_consistent_eq() {
        use ::types_scan::scankey::ScanKeyData;
        let owned = ::mcx::MemoryContext::new("t");
        let mcx = owned.mcx();
        let leaf = v4(127, 0, 0, 1, 32);

        let mut key = ScanKeyData::empty();
        key.sk_strategy = RTEqualStrategyNumber;
        key.sk_argument = inet_get_datum(mcx, &v4(127, 0, 0, 1, 32)).unwrap();

        let in_ = spgLeafConsistentIn {
            scankeys: vec![key.clone()],
            orderbys: Vec::new(),
            reconstructedValue: Datum::ByVal(0),
            traversalValue: None,
            level: 0,
            returnData: true,
            leafDatum: inet_get_datum(mcx, &leaf).unwrap(),
        };
        let mut out = spgLeafConsistentOut::default();
        let res = inet_spg_leaf_consistent(mcx, &in_, &mut out).unwrap();
        assert!(res);
        assert!(!out.recheck);

        // Different address -> no match.
        let mut key2 = ScanKeyData::empty();
        key2.sk_strategy = RTEqualStrategyNumber;
        key2.sk_argument = inet_get_datum(mcx, &v4(127, 0, 0, 2, 32)).unwrap();
        let in2 = spgLeafConsistentIn {
            scankeys: vec![key2],
            orderbys: Vec::new(),
            reconstructedValue: Datum::ByVal(0),
            traversalValue: None,
            level: 0,
            returnData: true,
            leafDatum: inet_get_datum(mcx, &leaf).unwrap(),
        };
        let mut out2 = spgLeafConsistentOut::default();
        assert!(!inet_spg_leaf_consistent(mcx, &in2, &mut out2).unwrap());
    }
}
