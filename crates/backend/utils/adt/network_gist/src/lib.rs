#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]
#![allow(clippy::result_large_err)]

//! `src/backend/utils/adt/network_gist.c` (PostgreSQL 18.3): GiST support for
//! the network (`inet`/`cidr`) types — the `inet_ops` opclass.
//!
//! Every function defined in `network_gist.c` is ported here 1:1 (branch order,
//! loop bounds, indexing, switch arms, and `Assert`s preserved). The
//! `GistInetKey` index-key representation is owned by this file, so it is a real
//! Rust struct; the low-level bit helpers (`bitncmp`, `bitncommon`) reuse the
//! ported implementations in [`adt_network`], and the `inet`
//! payload is [`::types_network::inet_struct`].
//!
//! The fmgr `Datum`/varlena packing of `GistInetKey`/`inet` and the
//! `GIST_SPLITVEC` offset arrays are surfaced as the typed inet GiST
//! support-procedure seams in `backend-utils-adt-network-gist-seams`, installed
//! by [`init_seams`]; the GiST core's by-OID dispatcher
//! (`backend-access-gist-proc`) marshals the `Datum`s and routes the inet
//! support-proc OIDs to these seams.

use ::adt_network::{bitncmp, bitncommon};
use ::types_core::primitive::{uint16, OffsetNumber};
use ::types_error::{PgError, PgResult};
use ::types_network::{inet_struct, GistInetKey, GistInetSplitVec, PGSQL_AF_INET6};

/// `StrategyNumber` (access/stratnum.h) — the comparison strategy a scan key
/// requests of a consistent support procedure.
pub type StrategyNumber = uint16;

// ---------------------------------------------------------------------------
// Operator strategy numbers used in the GiST inet_ops opclass
// (network_gist.c:59-69, mapping the RT* names from <access/stratnum.h>).
// ---------------------------------------------------------------------------

/// `INETSTRAT_OVERLAPS` = `RTOverlapStrategyNumber`.
pub const INETSTRAT_OVERLAPS: StrategyNumber = 3;
/// `INETSTRAT_EQ` = `RTEqualStrategyNumber`.
pub const INETSTRAT_EQ: StrategyNumber = 18;
/// `INETSTRAT_NE` = `RTNotEqualStrategyNumber`.
pub const INETSTRAT_NE: StrategyNumber = 19;
/// `INETSTRAT_LT` = `RTLessStrategyNumber`.
pub const INETSTRAT_LT: StrategyNumber = 20;
/// `INETSTRAT_LE` = `RTLessEqualStrategyNumber`.
pub const INETSTRAT_LE: StrategyNumber = 21;
/// `INETSTRAT_GT` = `RTGreaterStrategyNumber`.
pub const INETSTRAT_GT: StrategyNumber = 22;
/// `INETSTRAT_GE` = `RTGreaterEqualStrategyNumber`.
pub const INETSTRAT_GE: StrategyNumber = 23;
/// `INETSTRAT_SUB` = `RTSubStrategyNumber`.
pub const INETSTRAT_SUB: StrategyNumber = 24;
/// `INETSTRAT_SUBEQ` = `RTSubEqualStrategyNumber`.
pub const INETSTRAT_SUBEQ: StrategyNumber = 25;
/// `INETSTRAT_SUP` = `RTSuperStrategyNumber`.
pub const INETSTRAT_SUP: StrategyNumber = 26;
/// `INETSTRAT_SUPEQ` = `RTSuperEqualStrategyNumber`.
pub const INETSTRAT_SUPEQ: StrategyNumber = 27;

// Access macros (network_gist.c:96-106). The `GistInetKey` /
// `GistInetSplitVec` payload types are owned by `types-network`.

#[inline]
fn gk_ip_family(gk: &GistInetKey) -> i32 {
    gk.family as i32
}
#[inline]
fn gk_ip_minbits(gk: &GistInetKey) -> i32 {
    gk.minbits as i32
}
#[inline]
fn gk_ip_commonbits(gk: &GistInetKey) -> i32 {
    gk.commonbits as i32
}
#[inline]
fn gk_ip_addr(gk: &GistInetKey) -> &[u8; 16] {
    &gk.ipaddr
}
/// `ip_family_maxbits(fam)` — 128 for IPv6, else 32 (network_gist.c:100).
#[inline]
fn ip_family_maxbits(fam: i32) -> i32 {
    if fam == PGSQL_AF_INET6 as i32 {
        128
    } else {
        32
    }
}
/// `gk_ip_addrsize(gkptr)` — 16 for IPv6, else 4 (network_gist.c:103). Requires
/// family to have been set.
#[inline]
fn gk_ip_addrsize(gk: &GistInetKey) -> usize {
    if gk_ip_family(gk) == PGSQL_AF_INET6 as i32 {
        16
    } else {
        4
    }
}
/// `gk_ip_maxbits(gkptr)` (network_gist.c:105). Requires family to be set.
#[inline]
fn gk_ip_maxbits(gk: &GistInetKey) -> i32 {
    ip_family_maxbits(gk_ip_family(gk))
}

// Access to the query `inet` value (utils/inet.h `ip_family`/`ip_bits`/`ip_addr`).

#[inline]
fn ip_family(q: &inet_struct) -> i32 {
    q.family as i32
}
#[inline]
fn ip_bits(q: &inet_struct) -> i32 {
    q.bits as i32
}
#[inline]
fn ip_addr(q: &inet_struct) -> &[u8; 16] {
    &q.ipaddr
}
/// `ip_addrsize(inetptr)` — 4 for IPv4, 16 for IPv6 (utils/inet.h).
#[inline]
fn ip_addrsize(q: &inet_struct) -> usize {
    if q.family == PGSQL_AF_INET6 {
        16
    } else {
        4
    }
}

// ---------------------------------------------------------------------------
// The GiST query consistency check (network_gist.c:114).
// ---------------------------------------------------------------------------

/// `inet_gist_consistent` (network_gist.c:114). The GiST query consistency
/// check.
///
/// `key` is `DatumGetInetKeyP(ent->key)`; `query` is `PG_GETARG_INET_PP(1)`;
/// `is_leaf` is `GIST_LEAF(ent)`. All operators served by this function are
/// exact, so the C `*recheck = false` is reflected by the returned
/// `recheck = false`. Returns `(result, recheck)`.
pub fn inet_gist_consistent(
    key: &GistInetKey,
    query: &inet_struct,
    strategy: StrategyNumber,
    is_leaf: bool,
) -> PgResult<(bool, bool)> {
    // All operators served by this function are exact.
    let recheck = false;

    // Check 0: different families
    //
    // If key represents multiple address families, its children could match
    // anything. This can only happen on an inner index page.
    if gk_ip_family(key) == 0 {
        debug_assert!(!is_leaf);
        return Ok((true, recheck));
    }

    // Check 1: different families
    //
    // Matching families do not help any of the strategies.
    if gk_ip_family(key) != ip_family(query) {
        match strategy {
            INETSTRAT_LT | INETSTRAT_LE => {
                if gk_ip_family(key) < ip_family(query) {
                    return Ok((true, recheck));
                }
            }
            INETSTRAT_GE | INETSTRAT_GT => {
                if gk_ip_family(key) > ip_family(query) {
                    return Ok((true, recheck));
                }
            }
            INETSTRAT_NE => {
                return Ok((true, recheck));
            }
            _ => {}
        }
        // For all other cases, we can be sure there is no match
        return Ok((false, recheck));
    }

    // Check 2: network bit count
    //
    // Network bit count (ip_bits) helps to check leaves for sub network and sup
    // network operators. At non-leaf nodes, we know every child value has
    // ip_bits >= gk_ip_minbits(key), so we can avoid descending in some cases
    // too.
    match strategy {
        INETSTRAT_SUB => {
            if is_leaf && gk_ip_minbits(key) <= ip_bits(query) {
                return Ok((false, recheck));
            }
        }
        INETSTRAT_SUBEQ => {
            if is_leaf && gk_ip_minbits(key) < ip_bits(query) {
                return Ok((false, recheck));
            }
        }
        INETSTRAT_SUPEQ | INETSTRAT_EQ => {
            if gk_ip_minbits(key) > ip_bits(query) {
                return Ok((false, recheck));
            }
        }
        INETSTRAT_SUP => {
            if gk_ip_minbits(key) >= ip_bits(query) {
                return Ok((false, recheck));
            }
        }
        _ => {}
    }

    // Check 3: common network bits
    //
    // Compare available common prefix bits to the query, but not beyond either
    // the query's netmask or the minimum netmask among the represented values.
    // If these bits don't match the query, we have our answer (and may or may
    // not need to descend, depending on the operator). If they do match, and we
    // are not at a leaf, we descend in all cases.
    //
    // Note this is the final check for operators that only consider the network
    // part of the address.
    let mut minbits = gk_ip_commonbits(key).min(gk_ip_minbits(key));
    minbits = minbits.min(ip_bits(query));

    let mut order = bitncmp(gk_ip_addr(key), ip_addr(query), minbits);

    match strategy {
        INETSTRAT_SUB | INETSTRAT_SUBEQ | INETSTRAT_OVERLAPS | INETSTRAT_SUPEQ | INETSTRAT_SUP => {
            return Ok((order == 0, recheck));
        }
        INETSTRAT_LT | INETSTRAT_LE => {
            if order > 0 {
                return Ok((false, recheck));
            }
            if order < 0 || !is_leaf {
                return Ok((true, recheck));
            }
        }
        INETSTRAT_EQ => {
            if order != 0 {
                return Ok((false, recheck));
            }
            if !is_leaf {
                return Ok((true, recheck));
            }
        }
        INETSTRAT_GE | INETSTRAT_GT => {
            if order < 0 {
                return Ok((false, recheck));
            }
            if order > 0 || !is_leaf {
                return Ok((true, recheck));
            }
        }
        INETSTRAT_NE => {
            if order != 0 || !is_leaf {
                return Ok((true, recheck));
            }
        }
        _ => {}
    }

    // Remaining checks are only for leaves and basic comparison strategies. See
    // network_cmp_internal() in network.c for the implementation we need to
    // match. Note that in a leaf key, commonbits should equal the address
    // length, so we compared the whole network parts above.
    debug_assert!(is_leaf);

    // Check 4: network bit count
    //
    // Next step is to compare netmask widths.
    match strategy {
        INETSTRAT_LT | INETSTRAT_LE => {
            if gk_ip_minbits(key) < ip_bits(query) {
                return Ok((true, recheck));
            }
            if gk_ip_minbits(key) > ip_bits(query) {
                return Ok((false, recheck));
            }
        }
        INETSTRAT_EQ => {
            if gk_ip_minbits(key) != ip_bits(query) {
                return Ok((false, recheck));
            }
        }
        INETSTRAT_GE | INETSTRAT_GT => {
            if gk_ip_minbits(key) > ip_bits(query) {
                return Ok((true, recheck));
            }
            if gk_ip_minbits(key) < ip_bits(query) {
                return Ok((false, recheck));
            }
        }
        INETSTRAT_NE => {
            if gk_ip_minbits(key) != ip_bits(query) {
                return Ok((true, recheck));
            }
        }
        _ => {}
    }

    // Check 5: whole address
    //
    // Netmask bit counts are the same, so check all the address bits.
    order = bitncmp(gk_ip_addr(key), ip_addr(query), gk_ip_maxbits(key));

    match strategy {
        INETSTRAT_LT => return Ok((order < 0, recheck)),
        INETSTRAT_LE => return Ok((order <= 0, recheck)),
        INETSTRAT_EQ => return Ok((order == 0, recheck)),
        INETSTRAT_GE => return Ok((order >= 0, recheck)),
        INETSTRAT_GT => return Ok((order > 0, recheck)),
        INETSTRAT_NE => return Ok((order != 0, recheck)),
        _ => {}
    }

    Err(unknown_strategy())
}

/// `elog(ERROR, "unknown strategy for inet GiST")` (network_gist.c:327).
fn unknown_strategy() -> PgError {
    PgError::error("unknown strategy for inet GiST")
}

// ---------------------------------------------------------------------------
// Union parameter calculation (network_gist.c:344, 406).
// ---------------------------------------------------------------------------

/// Output parameters of [`calc_inet_union_params`] /
/// [`calc_inet_union_params_indexed`].
struct UnionParams {
    /// `*minfamily_p` — minimum IP address family number.
    minfamily: i32,
    /// `*maxfamily_p` — maximum IP address family number.
    maxfamily: i32,
    /// `*minbits_p` — minimum netmask width.
    minbits: i32,
    /// `*commonbits_p` — number of leading bits in common among the addresses.
    commonbits: i32,
}

/// `calc_inet_union_params` (network_gist.c:344). Calculate parameters of the
/// union of the keys in elements `m..=n` inclusive of the key array.
///
/// `minbits` and `commonbits` are forced to zero if there's more than one
/// address family.
fn calc_inet_union_params(ent: &[GistInetKey], m: usize, n: usize) -> UnionParams {
    // Must be at least one key.
    debug_assert!(m <= n);

    // Initialize variables using the first key.
    let tmp = &ent[m];
    let mut minfamily = gk_ip_family(tmp);
    let mut maxfamily = gk_ip_family(tmp);
    let mut minbits = gk_ip_minbits(tmp);
    let mut commonbits = gk_ip_commonbits(tmp);
    let addr = gk_ip_addr(tmp);

    // Scan remaining keys.
    for tmp in &ent[m + 1..=n] {
        // Determine range of family numbers
        if minfamily > gk_ip_family(tmp) {
            minfamily = gk_ip_family(tmp);
        }
        if maxfamily < gk_ip_family(tmp) {
            maxfamily = gk_ip_family(tmp);
        }

        // Find minimum minbits
        if minbits > gk_ip_minbits(tmp) {
            minbits = gk_ip_minbits(tmp);
        }

        // Find minimum number of bits in common
        if commonbits > gk_ip_commonbits(tmp) {
            commonbits = gk_ip_commonbits(tmp);
        }
        if commonbits > 0 {
            commonbits = bitncommon(addr, gk_ip_addr(tmp), commonbits);
        }
    }

    // Force minbits/commonbits to zero if more than one family.
    if minfamily != maxfamily {
        minbits = 0;
        commonbits = 0;
    }

    UnionParams {
        minfamily,
        maxfamily,
        minbits,
        commonbits,
    }
}

/// `calc_inet_union_params_indexed` (network_gist.c:406). Same as
/// [`calc_inet_union_params`], but the keys to examine are those at the indices
/// listed in `offsets[0..noffsets]`.
fn calc_inet_union_params_indexed(
    ent: &[GistInetKey],
    offsets: &[OffsetNumber],
    noffsets: usize,
) -> UnionParams {
    // Must be at least one key.
    debug_assert!(noffsets > 0);

    // Initialize variables using the first key.
    let tmp = &ent[offsets[0] as usize];
    let mut minfamily = gk_ip_family(tmp);
    let mut maxfamily = gk_ip_family(tmp);
    let mut minbits = gk_ip_minbits(tmp);
    let mut commonbits = gk_ip_commonbits(tmp);
    let addr = gk_ip_addr(tmp);

    // Scan remaining keys.
    for i in 1..noffsets {
        let tmp = &ent[offsets[i] as usize];

        // Determine range of family numbers
        if minfamily > gk_ip_family(tmp) {
            minfamily = gk_ip_family(tmp);
        }
        if maxfamily < gk_ip_family(tmp) {
            maxfamily = gk_ip_family(tmp);
        }

        // Find minimum minbits
        if minbits > gk_ip_minbits(tmp) {
            minbits = gk_ip_minbits(tmp);
        }

        // Find minimum number of bits in common
        if commonbits > gk_ip_commonbits(tmp) {
            commonbits = gk_ip_commonbits(tmp);
        }
        if commonbits > 0 {
            commonbits = bitncommon(addr, gk_ip_addr(tmp), commonbits);
        }
    }

    // Force minbits/commonbits to zero if more than one family.
    if minfamily != maxfamily {
        minbits = 0;
        commonbits = 0;
    }

    UnionParams {
        minfamily,
        maxfamily,
        minbits,
        commonbits,
    }
}

/// `build_inet_union_key` (network_gist.c:471). Construct a [`GistInetKey`]
/// representing a union value.
///
/// Inputs are the family/minbits/commonbits values to use, plus the address
/// field of one of the union inputs (since we copy just the bits-in-common, it
/// doesn't matter which one). The C version `palloc0`s the result and sets the
/// varlena header; here the result is a zeroed owned struct and the varlena
/// header is the fmgr boundary's concern.
fn build_inet_union_key(
    family: i32,
    minbits: i32,
    commonbits: i32,
    addr: &[u8; 16],
) -> GistInetKey {
    // Make sure any unused bits are zeroed.
    let mut result = GistInetKey {
        family: family as u8,
        minbits: minbits as u8,
        commonbits: commonbits as u8,
        ipaddr: [0u8; 16],
    };

    // Clone appropriate bytes of the address.
    if commonbits > 0 {
        let nbytes = ((commonbits + 7) / 8) as usize;
        result.ipaddr[..nbytes].copy_from_slice(&addr[..nbytes]);
    }

    // Clean any unwanted bits in the last partial byte.
    if commonbits % 8 != 0 {
        result.ipaddr[(commonbits / 8) as usize] &= !(0xFFu8 >> (commonbits % 8));
    }

    result
}

// ---------------------------------------------------------------------------
// The GiST union function (network_gist.c:504).
// ---------------------------------------------------------------------------

/// `inet_gist_union` (network_gist.c:504). The GiST union function.
///
/// See the file header for the definition of the union. `keys` are the entry
/// keys `entryvec->vector[0..entryvec->n]` already unpacked from their Datums.
pub fn inet_gist_union(keys: &[GistInetKey]) -> GistInetKey {
    // Determine parameters of the union.
    let UnionParams {
        minfamily,
        maxfamily,
        minbits,
        commonbits,
    } = calc_inet_union_params(keys, 0, keys.len() - 1);

    // If more than one family, emit family number zero.
    let minfamily = if minfamily != maxfamily { 0 } else { minfamily };

    // Initialize address using the first key.
    let tmp = &keys[0];
    let addr = gk_ip_addr(tmp);

    // Construct the union value.
    build_inet_union_key(minfamily, minbits, commonbits, addr)
}

// ---------------------------------------------------------------------------
// The GiST compress function (network_gist.c:541).
// ---------------------------------------------------------------------------

/// `inet_gist_compress` (network_gist.c:541). Convert an `inet` value to
/// [`GistInetKey`].
///
/// Only leaf keys are converted (for inner entries C returns the entry
/// unchanged); the fmgr boundary handles the `leafkey == false` passthrough and
/// the NULL-key case. `in_` is `DatumGetInetPP(entry->key)`; `None` reflects a
/// NULL key (C builds a `(Datum) 0` entry). Returns the new key, or `None` for
/// a NULL key.
pub fn inet_gist_compress(in_: Option<&inet_struct>) -> Option<GistInetKey> {
    let in_ = in_?;

    let mut r = GistInetKey {
        family: in_.family,
        minbits: ip_bits(in_) as u8,
        commonbits: 0,
        ipaddr: [0u8; 16],
    };
    r.commonbits = gk_ip_maxbits(&r) as u8;
    let addrsize = gk_ip_addrsize(&r);
    r.ipaddr[..addrsize].copy_from_slice(&ip_addr(in_)[..addrsize]);

    Some(r)
}

// ---------------------------------------------------------------------------
// The GiST fetch function (network_gist.c:589).
// ---------------------------------------------------------------------------

/// `inet_gist_fetch` (network_gist.c:589). Reconstruct the original `inet` datum
/// from a [`GistInetKey`].
///
/// `key` is `DatumGetInetKeyP(entry->key)`. Returns the reconstructed
/// [`inet_struct`] payload; the fmgr boundary `palloc0`s the `inet`, writes the
/// payload, and sets the varlena header per `SET_INET_VARSIZE(dst)`.
pub fn inet_gist_fetch(key: &GistInetKey) -> inet_struct {
    let mut dst = inet_struct {
        family: 0,
        bits: 0,
        ipaddr: [0u8; 16],
    };

    dst.family = key.family;
    dst.bits = gk_ip_minbits(key) as u8;
    let addrsize = ip_addrsize(&dst);
    dst.ipaddr[..addrsize].copy_from_slice(&gk_ip_addr(key)[..addrsize]);

    dst
}

// ---------------------------------------------------------------------------
// The GiST page split penalty function (network_gist.c:619).
// ---------------------------------------------------------------------------

/// `inet_gist_penalty` (network_gist.c:619). The GiST page split penalty
/// function.
///
/// Charge a large penalty if address family doesn't match, or a somewhat
/// smaller one if the new value would degrade the union's minbits. Otherwise,
/// penalty is inverse of the new number of common address bits. `orig` / `new_`
/// are `DatumGetInetKeyP(origent->key)` / `DatumGetInetKeyP(newent->key)`. C
/// writes the result through a `float *`; here we return the `f32`.
pub fn inet_gist_penalty(orig: &GistInetKey, new_: &GistInetKey) -> f32 {
    let penalty: f32;

    if gk_ip_family(orig) == gk_ip_family(new_) {
        if gk_ip_minbits(orig) <= gk_ip_minbits(new_) {
            let commonbits = bitncommon(
                gk_ip_addr(orig),
                gk_ip_addr(new_),
                gk_ip_commonbits(orig).min(gk_ip_commonbits(new_)),
            );
            if commonbits > 0 {
                penalty = 1.0f32 / commonbits as f32;
            } else {
                penalty = 2.0;
            }
        } else {
            penalty = 3.0;
        }
    } else {
        penalty = 4.0;
    }

    penalty
}

// ---------------------------------------------------------------------------
// The GiST PickSplit method (network_gist.c:662).
// ---------------------------------------------------------------------------

/// `inet_gist_picksplit` (network_gist.c:662). The GiST PickSplit method.
///
/// `keys` are the entry keys `entryvec->vector[0..entryvec->n]` (so
/// `keys.len() == entryvec->n` and `maxoff == entryvec->n - 1`). As in C, only
/// indices `FirstOffsetNumber..=maxoff` (i.e. `1..=maxoff`) participate; index 0
/// is never read or assigned. The results are written into `splitvec`. Fallible
/// because the C version `palloc`s the offset arrays.
pub fn inet_gist_picksplit(
    keys: &[GistInetKey],
    splitvec: &mut GistInetSplitVec,
) -> PgResult<()> {
    let ent = keys;

    let maxoff = (ent.len() - 1) as OffsetNumber; // entryvec->n - 1
    let nslots = maxoff as usize + 1;

    // left = palloc(nbytes); right = palloc(nbytes). C allocates `maxoff + 1`
    // OffsetNumber slots per side and appends via the spl_nleft/spl_nright
    // counters; we mirror that with fixed-length zero-initialized slot arrays
    // plus explicit counts, so left[0]/right[0] stay addressable on a zero-count
    // side (as in C). The allocation is data-derived, so it goes through the
    // fallible try_reserve per the OOM rule.
    let mut left: Vec<OffsetNumber> = Vec::new();
    let mut right: Vec<OffsetNumber> = Vec::new();
    left.try_reserve(nslots).map_err(|_| PgError::error("out of memory"))?;
    right.try_reserve(nslots).map_err(|_| PgError::error("out of memory"))?;
    left.resize(nslots, 0);
    right.resize(nslots, 0);

    // splitvec->spl_nleft = splitvec->spl_nright = 0;
    let mut spl_nleft: usize = 0;
    let mut spl_nright: usize = 0;

    // Determine parameters of the union of all the inputs.
    let p = calc_inet_union_params(ent, FirstOffsetNumber as usize, maxoff as usize);
    let minfamily = p.minfamily;
    let maxfamily = p.maxfamily;
    let mut commonbits = p.commonbits;

    if minfamily != maxfamily {
        // Multiple families, so split by family.
        let mut i = FirstOffsetNumber;
        while i <= maxoff {
            // If there's more than 2 families, all but maxfamily go into the
            // left union. This could only happen if the inputs include some
            // IPv4, some IPv6, and some already-multiple-family unions.
            let tmp = &ent[i as usize];
            if gk_ip_family(tmp) != maxfamily {
                left[spl_nleft] = i;
                spl_nleft += 1;
            } else {
                right[spl_nright] = i;
                spl_nright += 1;
            }
            i = OffsetNumberNext(i);
        }
    } else {
        // Split on the next bit after the common bits. If that yields a trivial
        // split, try the next bit position to the right. Repeat till success; or
        // if we run out of bits, do an arbitrary 50-50 split.
        let maxbits = ip_family_maxbits(minfamily);

        while commonbits < maxbits {
            // Split using the commonbits'th bit position.
            let bitbyte = (commonbits / 8) as usize;
            let bitmask = 0x80u8 >> (commonbits % 8);

            spl_nleft = 0;
            spl_nright = 0;

            let mut i = FirstOffsetNumber;
            while i <= maxoff {
                let tmp = &ent[i as usize];
                let addr = gk_ip_addr(tmp);
                if (addr[bitbyte] & bitmask) == 0 {
                    left[spl_nleft] = i;
                    spl_nleft += 1;
                } else {
                    right[spl_nright] = i;
                    spl_nright += 1;
                }
                i = OffsetNumberNext(i);
            }

            if spl_nleft > 0 && spl_nright > 0 {
                break; // success
            }
            commonbits += 1;
        }

        if commonbits >= maxbits {
            // Failed ... do a 50-50 split.
            spl_nleft = 0;
            spl_nright = 0;

            let mut i = FirstOffsetNumber;
            while i <= maxoff / 2 {
                left[spl_nleft] = i;
                spl_nleft += 1;
                i = OffsetNumberNext(i);
            }
            while i <= maxoff {
                right[spl_nright] = i;
                spl_nright += 1;
                i = OffsetNumberNext(i);
            }
        }
    }

    // Compute the union value for each side from scratch. In most cases we could
    // approximate the union values with what we already know, but this ensures
    // that each side has minbits and commonbits set as high as possible.
    let p = calc_inet_union_params_indexed(ent, &left, spl_nleft);
    let minfamily = if p.minfamily != p.maxfamily {
        0
    } else {
        p.minfamily
    };
    let tmp = &ent[left[0] as usize];
    let addr = gk_ip_addr(tmp);
    let left_union = build_inet_union_key(minfamily, p.minbits, p.commonbits, addr);

    let p = calc_inet_union_params_indexed(ent, &right, spl_nright);
    let minfamily = if p.minfamily != p.maxfamily {
        0
    } else {
        p.minfamily
    };
    let tmp = &ent[right[0] as usize];
    let addr = gk_ip_addr(tmp);
    let right_union = build_inet_union_key(minfamily, p.minbits, p.commonbits, addr);

    // Export the populated portion of each side (C reads back the first
    // spl_nleft/spl_nright slots of the palloc'd arrays).
    left.truncate(spl_nleft);
    right.truncate(spl_nright);

    splitvec.spl_left = left;
    splitvec.spl_right = right;
    splitvec.spl_ldatum = left_union;
    splitvec.spl_rdatum = right_union;
    Ok(())
}

/// `OffsetNumberNext(offsetNumber)` (storage/off.h:52).
#[inline]
fn OffsetNumberNext(offset_number: OffsetNumber) -> OffsetNumber {
    offset_number + 1
}

/// `FirstOffsetNumber` (storage/off.h:27).
const FirstOffsetNumber: OffsetNumber = 1;

// ---------------------------------------------------------------------------
// The GiST equality function (network_gist.c:796).
// ---------------------------------------------------------------------------

/// `inet_gist_same` (network_gist.c:796). The GiST equality function.
///
/// `left` / `right` are `DatumGetInetKeyP(PG_GETARG_DATUM(0|1))`. C writes the
/// result through a `bool *`; here we return the `bool`.
pub fn inet_gist_same(left: &GistInetKey, right: &GistInetKey) -> bool {
    let addrsize = gk_ip_addrsize(left);
    gk_ip_family(left) == gk_ip_family(right)
        && gk_ip_minbits(left) == gk_ip_minbits(right)
        && gk_ip_commonbits(left) == gk_ip_commonbits(right)
        && gk_ip_addr(left)[..addrsize] == gk_ip_addr(right)[..addrsize]
}

// ---------------------------------------------------------------------------
// Seam installation
// ---------------------------------------------------------------------------

/// Install the inet GiST opclass support-procedure bodies into the typed seams
/// declared in `backend-utils-adt-network-gist-seams`. The GiST core's by-OID
/// dispatcher (`backend-access-gist-proc`) marshals the `Datum`s and routes the
/// inet support-proc OIDs to these seams.
pub fn init_seams() {
    use network_gist_seams as s;
    s::inet_gist_consistent::set(|key, query, strategy, is_leaf| {
        inet_gist_consistent(&key, &query, strategy, is_leaf)
    });
    s::inet_gist_union::set(|keys| inet_gist_union(&keys));
    s::inet_gist_compress::set(|in_| inet_gist_compress(in_.as_ref()));
    s::inet_gist_fetch::set(|key| inet_gist_fetch(&key));
    s::inet_gist_penalty::set(|orig, new_| inet_gist_penalty(&orig, &new_));
    s::inet_gist_picksplit::set(|keys| {
        let mut sv = GistInetSplitVec::default();
        inet_gist_picksplit(&keys, &mut sv)?;
        Ok(sv)
    });
    s::inet_gist_same::set(|left, right| inet_gist_same(&left, &right));

    register_inet_gist_builtins();
}

// ---------------------------------------------------------------------------
// fmgr builtin-table rows (C: network_gist.c's `fmgr_builtins[]` rows).
//
// The inet GiST opclass support procedures are `prolang => internal` procs
// whose `pg_proc.dat` rows carry `internal`-language args (`GISTENTRY *`, the
// `internal` query, the `GIST_SPLITVEC *`). In C they are real `fmgr_builtins[]`
// rows whose `fn_addr` the GiST access method invokes through
// `FunctionCallNColl`. The port replaced that fmgr-frame call with the typed,
// by-OID dispatch installed above (`backend-access-gist-proc` routes each inet
// support-proc OID to the seams `init_seams` installs), reading `FmgrInfo.fn_oid`
// — never `fn_addr`.
//
// `initGISTstate` builds each `GISTSTATE` slot with `index_getprocinfo` →
// `fmgr_info`, which — for an `internal`-language proc — looks the prosrc name
// up in the fmgr builtin table (`fmgr_lookupByName`) and errors (`internal
// function "inet_gist_consistent" is not in internal lookup table`) when it is
// absent. So every inet GiST support proc MUST have its `fmgr_builtins[]` row
// registered for `CREATE INDEX ... USING gist (... inet_ops)` (and opclass
// validation) to resolve it — exactly C's table. The inet GiST dispatch is
// already INSTALLED+active (see [`init_seams`] above and
// `backend-access-gist-proc`'s `dispatch_consistent`), so these rows complete a
// real, end-to-end-working opclass.
//
// Because the faithful invocation IS the by-OID typed dispatch (the `fn_addr` is
// structurally never reached through the fmgr frame), the `func` adapter here is
// the fmgr-frame entry the port never enters; if a future C-faithful
// `FunctionCallNColl` ever reaches it, it raises a clear `ereport(ERROR)` naming
// the dispatch seam to route through. OIDs / nargs from `pg_proc.dat` (every row
// is `proisstrict => 't'` — the default — and not retset).
// ---------------------------------------------------------------------------

/// The shared fmgr-frame entry point for every inet GiST opclass support proc.
/// In the owned model the GiST access method invokes these procs through the
/// typed by-OID dispatch (`backend-access-gist-proc` → the seams installed by
/// [`init_seams`]), reading `FmgrInfo::fn_oid` — never `fn_addr`. This frame
/// entry therefore is never reached on any port path; it exists so the
/// `fmgr_builtins[]` row carries a non-`None` callable (matching C's table). It
/// raises a clear error if a future fmgr-frame call site is added, pointing at
/// the dispatch seam to use instead.
fn fc_inet_gist_support_via_dispatch(
    fcinfo: &mut fmgr::FunctionCallInfoBaseData,
) -> ::types_error::PgResult<datum::Datum> {
    let foid = fcinfo.flinfo.as_ref().map(|fi| fi.fn_oid).unwrap_or(0);
    Err(PgError::error(format!(
        "inet GiST support function (OID {foid}) must be invoked through the \
         typed opclass dispatch (backend-access-gist-proc / \
         backend-utils-adt-network-gist-seams), not the fmgr frame; the owned \
         GiST access method dispatches these by FmgrInfo.fn_oid"
    )))
}

fn inet_gist_builtin(
    foid: u32,
    name: &str,
    nargs: i16,
) -> (fmgr::BuiltinFunction, fmgr::PgFnNative) {
    (
        fmgr::BuiltinFunction {
            foid,
            name: name.to_string(),
            nargs,
            strict: true,
            retset: false,
            func: None,
        },
        fc_inet_gist_support_via_dispatch,
    )
}

/// Register the `fmgr_builtins[]` rows for every inet GiST opclass support
/// procedure ported in this crate (C: `network_gist.c`'s `fmgr_builtins[]`
/// rows). Resolving these rows is what lets `index_getprocinfo` → `fmgr_info`
/// build the `GISTSTATE` `FmgrInfo` slots for the inet GiST opclass (without
/// which `CREATE INDEX ... USING gist (... inet_ops)` errors `internal function
/// "inet_gist_consistent" is not in internal lookup table`). OIDs / nargs from
/// `pg_proc.dat`.
pub fn register_inet_gist_builtins() {
    fmgr_core::register_builtins_native([
        inet_gist_builtin(3553, "inet_gist_consistent", 5),
        inet_gist_builtin(3554, "inet_gist_union", 2),
        inet_gist_builtin(3555, "inet_gist_compress", 1),
        inet_gist_builtin(3573, "inet_gist_fetch", 1),
        inet_gist_builtin(3557, "inet_gist_penalty", 3),
        inet_gist_builtin(3558, "inet_gist_picksplit", 2),
        inet_gist_builtin(3559, "inet_gist_same", 3),
    ]);
}

#[cfg(test)]
mod tests {
    use super::*;
    use ::types_network::PGSQL_AF_INET;

    /// Build a v4 `inet_struct` from `a.b.c.d/bits`.
    fn v4_inet(a: u8, b: u8, c: u8, d: u8, bits: u8) -> inet_struct {
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

    #[test]
    fn compress_then_fetch_roundtrip() {
        let in_ = v4_inet(10, 0, 0, 1, 24);
        let key = inet_gist_compress(Some(&in_)).expect("non-null key");
        assert_eq!(key.family, PGSQL_AF_INET);
        assert_eq!(key.minbits, 24);
        assert_eq!(key.commonbits, 32); // gk_ip_maxbits for v4
        assert_eq!(&key.ipaddr[..4], &[10, 0, 0, 1]);

        let dst = inet_gist_fetch(&key);
        assert_eq!(dst.family, PGSQL_AF_INET);
        assert_eq!(dst.bits, 24);
        assert_eq!(&dst.ipaddr[..4], &[10, 0, 0, 1]);
    }

    #[test]
    fn datum_bytes_roundtrip() {
        let in_ = v4_inet(192, 168, 0, 1, 24);
        let key = inet_gist_compress(Some(&in_)).unwrap();
        let bytes = key.to_datum_bytes();
        assert_eq!(GistInetKey::from_datum_bytes(&bytes), key);

        // The inet_struct codec the by-OID dispatch relies on (query Datum and
        // fetch result) must round-trip too.
        let q = v4_inet(10, 20, 30, 40, 16);
        assert_eq!(inet_struct::from_datum_bytes(&q.to_datum_bytes()), q);
    }

    #[test]
    fn compress_null() {
        assert!(inet_gist_compress(None).is_none());
    }

    #[test]
    fn same_equal_and_differing() {
        let a = inet_gist_compress(Some(&v4_inet(192, 168, 0, 0, 16))).unwrap();
        let b = inet_gist_compress(Some(&v4_inet(192, 168, 0, 0, 16))).unwrap();
        let c = inet_gist_compress(Some(&v4_inet(192, 168, 0, 1, 16))).unwrap();
        assert!(inet_gist_same(&a, &b));
        assert!(!inet_gist_same(&a, &c));
    }

    #[test]
    fn union_common_prefix() {
        let a = inet_gist_compress(Some(&v4_inet(192, 168, 1, 0, 24))).unwrap();
        let b = inet_gist_compress(Some(&v4_inet(192, 168, 2, 0, 20))).unwrap();
        let u = inet_gist_union(&[a, b]);
        assert_eq!(u.family, PGSQL_AF_INET);
        assert_eq!(u.minbits, 20); // min(24, 20)
        assert_eq!(u.commonbits, 22); // 192.168.1 vs 192.168.2 share 22 bits
    }

    #[test]
    fn union_mixed_family() {
        let v4 = inet_gist_compress(Some(&v4_inet(10, 0, 0, 0, 8))).unwrap();
        let mut ip6 = [0u8; 16];
        ip6[0] = 0x20;
        ip6[1] = 0x01;
        let in6 = inet_struct {
            family: PGSQL_AF_INET6,
            bits: 32,
            ipaddr: ip6,
        };
        let v6 = inet_gist_compress(Some(&in6)).unwrap();
        let u = inet_gist_union(&[v4, v6]);
        assert_eq!(u.family, 0);
        assert_eq!(u.minbits, 0);
        assert_eq!(u.commonbits, 0);
    }

    #[test]
    fn penalty_cases() {
        let a = inet_gist_compress(Some(&v4_inet(10, 0, 0, 0, 8))).unwrap();
        assert!((inet_gist_penalty(&a, &a) - 1.0 / 32.0).abs() < 1e-6);

        let mut ip6 = [0u8; 16];
        ip6[0] = 0x20;
        let in6 = inet_struct {
            family: PGSQL_AF_INET6,
            bits: 128,
            ipaddr: ip6,
        };
        let v6 = inet_gist_compress(Some(&in6)).unwrap();
        assert_eq!(inet_gist_penalty(&a, &v6), 4.0);
    }

    #[test]
    fn consistent_eq_leaf() {
        let key = inet_gist_compress(Some(&v4_inet(127, 0, 0, 1, 32))).unwrap();
        let q_match = v4_inet(127, 0, 0, 1, 32);
        let (res, recheck) = inet_gist_consistent(&key, &q_match, INETSTRAT_EQ, true).unwrap();
        assert!(res);
        assert!(!recheck);

        let q_diff = v4_inet(127, 0, 0, 2, 32);
        let (res, _) = inet_gist_consistent(&key, &q_diff, INETSTRAT_EQ, true).unwrap();
        assert!(!res);
    }

    #[test]
    fn consistent_family_zero_inner_descends() {
        let key = GistInetKey::default(); // family 0
        let q = v4_inet(10, 0, 0, 0, 8);
        let (res, _) = inet_gist_consistent(&key, &q, INETSTRAT_SUB, false).unwrap();
        assert!(res);
    }

    #[test]
    fn consistent_unknown_strategy_errors() {
        let key = inet_gist_compress(Some(&v4_inet(10, 0, 0, 1, 32))).unwrap();
        let q = v4_inet(10, 0, 0, 1, 32);
        assert!(inet_gist_consistent(&key, &q, 99, true).is_err());
    }

    #[test]
    fn picksplit_by_bit() {
        let dummy = GistInetKey::default();
        let k1 = inet_gist_compress(Some(&v4_inet(0, 0, 0, 0, 32))).unwrap();
        let k2 = inet_gist_compress(Some(&v4_inet(128, 0, 0, 0, 32))).unwrap();
        let keys = [dummy, k1, k2];
        let mut sv = GistInetSplitVec::default();
        inet_gist_picksplit(&keys, &mut sv).unwrap();
        assert_eq!(sv.spl_nleft(), 1);
        assert_eq!(sv.spl_nright(), 1);
        assert_eq!(sv.spl_left, vec![1]);
        assert_eq!(sv.spl_right, vec![2]);
        assert_eq!(sv.spl_ldatum.family, PGSQL_AF_INET);
        assert_eq!(sv.spl_rdatum.family, PGSQL_AF_INET);
    }
}
