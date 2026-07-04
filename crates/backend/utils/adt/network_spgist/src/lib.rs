//! network_spgist.c: SP-GiST inet_ops — split by family at the root, then by
//! common prefix / next bit / masklen (4-node inner tuples).
#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]

use ::adt_network::{bitncmp, bitncommon, cidr_set_masklen_internal, InetRef, PGSQL_AF_INET};
use ::datum::Datum;
use ::types_core::Oid;
use ::types_error::PgResult;
use ::types_fmgr::{
    byref_result, FmgrBuiltin, FmgrInfo, FunctionCallInfoBaseData as Fcinfo, PGFunction,
};
use ::types_scan::scankey::ScanKeyData;
use ::types_spgist::state::{
    spgChooseIn, spgChooseOut, spgInnerConsistentIn, spgInnerConsistentOut, spgLeafConsistentIn,
    spgLeafConsistentOut, spgPickSplitIn, spgPickSplitOut,
};
use ::types_spgist::spgConfigOut;

const CIDROID: Oid = 650;
const VOIDOID: Oid = 2278;

const RTEqualStrategyNumber: u16 = 18;
const RTNotEqualStrategyNumber: u16 = 19;
const RTLessStrategyNumber: u16 = 20;
const RTLessEqualStrategyNumber: u16 = 21;
const RTGreaterStrategyNumber: u16 = 22;
const RTGreaterEqualStrategyNumber: u16 = 23;
const RTSubStrategyNumber: u16 = 24;
const RTSubEqualStrategyNumber: u16 = 25;
const RTSuperStrategyNumber: u16 = 26;
const RTSuperEqualStrategyNumber: u16 = 27;

// DatumGetInetPP: inet never TOASTs external/compressed (22 bytes max), so
// only the 1B/4B header forms can arrive (leaf storage repacks short).
unsafe fn inet_at<'a>(d: Datum) -> InetRef<'a> {
    let pv = ::types_fmgr::PackedVarlena::from_ptr(d.as_usize() as *const u8);
    let payload: &'a [u8] =
        core::slice::from_raw_parts(pv.data().as_ptr(), pv.data().len());
    InetRef::from_payload(payload)
}

fn inet_datum_result(fcinfo: &Fcinfo, v: &::adt_network::InetValue) -> PgResult<Datum> {
    let (img, len) = v.image();
    byref_result(fcinfo.result_mcx(), &img[..len])
}

fn fc_inet_spg_config(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: spgist opclass fmgr protocol — args are live in/out structs.
    let cfg = unsafe { &mut *(fcinfo.arg(1).as_usize() as *mut spgConfigOut) };
    cfg.prefixType = CIDROID;
    cfg.labelType = VOIDOID;
    cfg.canReturnData = true;
    cfg.longValuesOK = false;
    Ok(Datum::null())
}

/// inet_spg_node_number: even/odd by the next address bit, +2 when masklen
/// exceeds the prefix's.
fn inet_spg_node_number(val: InetRef<'_>, commonbits: i32) -> i32 {
    let mut nodeN = 0;
    if commonbits < val.maxbits() as i32
        && val.addr[commonbits as usize / 8] & (1 << (7 - commonbits % 8)) != 0
    {
        nodeN |= 1;
    }
    if commonbits < val.bits as i32 {
        nodeN |= 2;
    }
    nodeN
}

fn fc_inet_spg_choose(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: spgist opclass fmgr protocol.
    let input = unsafe { &*(fcinfo.arg(0).as_usize() as *const spgChooseIn) };
    let out = unsafe { &mut *(fcinfo.arg(1).as_usize() as *mut spgChooseOut) };
    // SAFETY: untoasted inet datums per protocol.
    let val = unsafe { inet_at(input.datum) };

    if !input.hasPrefix {
        debug_assert!(!input.allTheSame);
        debug_assert!(input.nNodes == 2);
        *out = spgChooseOut::MatchNode {
            nodeN: if val.family == PGSQL_AF_INET { 0 } else { 1 },
            levelAdd: 0,
            restDatum: input.datum,
        };
        return Ok(Datum::null());
    }

    debug_assert!(input.nNodes == 4 || input.allTheSame);

    // SAFETY: as above.
    let prefix = unsafe { inet_at(input.prefixDatum) };
    let commonbits = prefix.bits as i32;

    if val.family != prefix.family {
        *out = spgChooseOut::SplitTuple {
            prefixHasPrefix: false,
            prefixPrefixDatum: Datum::null(),
            prefixNNodes: 2,
            prefixNodeLabels: core::ptr::null(),
            childNodeN: if prefix.family == PGSQL_AF_INET { 0 } else { 1 },
            postfixHasPrefix: true,
            postfixPrefixDatum: input.prefixDatum,
        };
        return Ok(Datum::null());
    }

    if (val.bits as i32) < commonbits
        || bitncmp(prefix.addr, val.addr, commonbits) != 0
    {
        let commonbits = bitncommon(prefix.addr, val.addr, (val.bits as i32).min(commonbits));
        let new_prefix = cidr_set_masklen_internal(val, commonbits);
        *out = spgChooseOut::SplitTuple {
            prefixHasPrefix: true,
            prefixPrefixDatum: inet_datum_result(fcinfo, &new_prefix)?,
            prefixNNodes: 4,
            prefixNodeLabels: core::ptr::null(),
            childNodeN: inet_spg_node_number(prefix, commonbits),
            postfixHasPrefix: true,
            postfixPrefixDatum: input.prefixDatum,
        };
        return Ok(Datum::null());
    }

    *out = spgChooseOut::MatchNode {
        nodeN: inet_spg_node_number(val, commonbits),
        levelAdd: 0,
        restDatum: input.datum,
    };
    Ok(Datum::null())
}

fn fc_inet_spg_picksplit(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: spgist opclass fmgr protocol.
    let input = unsafe { &*(fcinfo.arg(0).as_usize() as *const spgPickSplitIn) };
    let out = unsafe { &mut *(fcinfo.arg(1).as_usize() as *mut spgPickSplitOut) };
    let mcx = fcinfo.result_mcx();
    let n = input.nTuples as usize;
    // SAFETY: nTuples datums per protocol.
    let datums = unsafe { core::slice::from_raw_parts(input.datums, n) };

    // SAFETY: untoasted inet datums.
    let prefix = unsafe { inet_at(datums[0]) };
    let mut commonbits = prefix.bits as i32;
    let mut differentFamilies = false;

    for &d in datums.iter().skip(1) {
        // SAFETY: as above.
        let tmp = unsafe { inet_at(d) };
        if tmp.family != prefix.family {
            differentFamilies = true;
            break;
        }
        if (tmp.bits as i32) < commonbits {
            commonbits = tmp.bits as i32;
        }
        commonbits = bitncommon(prefix.addr, tmp.addr, commonbits);
        if commonbits == 0 {
            break;
        }
    }

    out.nodeLabels = core::ptr::null();
    let mut map: ::mcx::PgVec<'_, i32> = ::mcx::vec_with_capacity_in(mcx, n)?;
    let mut leaf: ::mcx::PgVec<'_, Datum> = ::mcx::vec_with_capacity_in(mcx, n)?;

    if differentFamilies {
        out.hasPrefix = false;
        out.nNodes = 2;
        for &d in datums {
            // SAFETY: as above.
            let tmp = unsafe { inet_at(d) };
            map.push(if tmp.family == PGSQL_AF_INET { 0 } else { 1 });
            leaf.push(d);
        }
    } else {
        out.hasPrefix = true;
        let p = cidr_set_masklen_internal(prefix, commonbits);
        let (img, len) = p.image();
        out.prefixDatum = byref_result(mcx, &img[..len])?;
        out.nNodes = 4;
        for &d in datums {
            // SAFETY: as above.
            let tmp = unsafe { inet_at(d) };
            map.push(inet_spg_node_number(tmp, commonbits));
            leaf.push(d);
        }
    }

    out.mapTuplesToNodes = map.as_mut_ptr();
    core::mem::forget(map);
    out.leafTupleDatums = leaf.as_ptr();
    core::mem::forget(leaf);
    Ok(Datum::null())
}

fn fc_inet_spg_inner_consistent(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: spgist opclass fmgr protocol.
    let input = unsafe { &*(fcinfo.arg(0).as_usize() as *const spgInnerConsistentIn) };
    let out = unsafe { &mut *(fcinfo.arg(1).as_usize() as *mut spgInnerConsistentOut) };
    let mcx = fcinfo.result_mcx();
    // SAFETY: nkeys scankeys per protocol.
    let scankeys =
        unsafe { core::slice::from_raw_parts(input.scankeys, input.nkeys.max(0) as usize) };

    let which: u32 = if !input.hasPrefix {
        debug_assert!(!input.allTheSame);
        debug_assert!(input.nNodes == 2);
        let mut which = 1 | (1 << 1);
        for key in scankeys {
            // SAFETY: untoasted inet scankey argument.
            let argument = unsafe { inet_at(key.sk_argument) };
            match key.sk_strategy {
                RTLessStrategyNumber | RTLessEqualStrategyNumber => {
                    if argument.family == PGSQL_AF_INET {
                        which &= 1;
                    }
                }
                RTGreaterEqualStrategyNumber | RTGreaterStrategyNumber => {
                    if argument.family != PGSQL_AF_INET {
                        which &= 1 << 1;
                    }
                }
                RTNotEqualStrategyNumber => {}
                _ => {
                    if argument.family == PGSQL_AF_INET {
                        which &= 1;
                    } else {
                        which &= 1 << 1;
                    }
                }
            }
        }
        which
    } else if !input.allTheSame {
        debug_assert!(input.nNodes == 4);
        // SAFETY: untoasted cidr prefix.
        let prefix = unsafe { inet_at(input.prefixDatum) };
        inet_spg_consistent_bitmap(prefix, scankeys, false)
    } else {
        !0
    };

    out.nNodes = 0;
    if which != 0 {
        let mut nodes: ::mcx::PgVec<'_, i32> =
            ::mcx::vec_with_capacity_in(mcx, input.nNodes.max(0) as usize)?;
        for i in 0..input.nNodes {
            if which & (1 << i) != 0 {
                nodes.push(i);
            }
        }
        out.nNodes = nodes.len() as i32;
        out.nodeNumbers = nodes.as_ptr();
        core::mem::forget(nodes);
    }
    Ok(Datum::null())
}

fn fc_inet_spg_leaf_consistent(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: spgist opclass fmgr protocol.
    let input = unsafe { &*(fcinfo.arg(0).as_usize() as *const spgLeafConsistentIn) };
    let out = unsafe { &mut *(fcinfo.arg(1).as_usize() as *mut spgLeafConsistentOut) };
    // SAFETY: untoasted leaf inet.
    let leaf = unsafe { inet_at(input.leafDatum) };
    out.recheck = false;
    out.leafValue = input.leafDatum;
    // SAFETY: nkeys scankeys per protocol.
    let scankeys =
        unsafe { core::slice::from_raw_parts(input.scankeys, input.nkeys.max(0) as usize) };
    Ok(Datum::from_bool(
        inet_spg_consistent_bitmap(leaf, scankeys, true) != 0,
    ))
}

/// inet_spg_consistent_bitmap: node bitmap at a 4-way inner tuple, or 0/1 at
/// a leaf.
fn inet_spg_consistent_bitmap(prefix: InetRef<'_>, scankeys: &[ScanKeyData], leaf: bool) -> u32 {
    let mut bitmap: u32 = if leaf {
        1
    } else {
        1 | (1 << 1) | (1 << 2) | (1 << 3)
    };
    let commonbits = prefix.bits as i32;

    for key in scankeys {
        // SAFETY: untoasted inet scankey argument.
        let argument = unsafe { inet_at(key.sk_argument) };
        let strategy = key.sk_strategy;
        let argbits = argument.bits as i32;

        if argument.family != prefix.family {
            match strategy {
                RTLessStrategyNumber | RTLessEqualStrategyNumber => {
                    if argument.family < prefix.family {
                        bitmap = 0;
                    }
                }
                RTGreaterEqualStrategyNumber | RTGreaterStrategyNumber => {
                    if argument.family > prefix.family {
                        bitmap = 0;
                    }
                }
                RTNotEqualStrategyNumber => {}
                _ => bitmap = 0,
            }
            if bitmap == 0 {
                break;
            }
            continue;
        }

        match strategy {
            RTSubStrategyNumber => {
                if commonbits <= argbits {
                    bitmap &= (1 << 2) | (1 << 3);
                }
            }
            RTSubEqualStrategyNumber => {
                if commonbits < argbits {
                    bitmap &= (1 << 2) | (1 << 3);
                }
            }
            RTSuperStrategyNumber => {
                if commonbits == argbits - 1 {
                    bitmap &= 1 | (1 << 1);
                } else if commonbits >= argbits {
                    bitmap = 0;
                }
            }
            RTSuperEqualStrategyNumber => {
                if commonbits == argbits {
                    bitmap &= 1 | (1 << 1);
                } else if commonbits > argbits {
                    bitmap = 0;
                }
            }
            RTEqualStrategyNumber => {
                if commonbits < argbits {
                    bitmap &= (1 << 2) | (1 << 3);
                } else if commonbits == argbits {
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

        let order = bitncmp(prefix.addr, argument.addr, commonbits.min(argbits));
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
                _ => bitmap = 0,
            }
            if bitmap == 0 {
                break;
            }
            continue;
        }

        if bitmap & ((1 << 2) | (1 << 3)) != 0 && commonbits < argbits {
            let nextbit =
                argument.addr[commonbits as usize / 8] & (1 << (7 - commonbits % 8)) != 0;
            match strategy {
                RTLessStrategyNumber | RTLessEqualStrategyNumber => {
                    if !nextbit {
                        bitmap &= 1 | (1 << 1) | (1 << 2);
                    }
                }
                RTGreaterEqualStrategyNumber | RTGreaterStrategyNumber => {
                    if nextbit {
                        bitmap &= 1 | (1 << 1) | (1 << 3);
                    }
                }
                RTNotEqualStrategyNumber => {}
                _ => {
                    if !nextbit {
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

        // Checks 4-6 rely on the RT strategy number ordering (stratnum.h).
        if !(RTEqualStrategyNumber..=RTGreaterEqualStrategyNumber).contains(&strategy) {
            continue;
        }

        match strategy {
            RTLessStrategyNumber | RTLessEqualStrategyNumber => {
                if commonbits == argbits {
                    bitmap &= 1 | (1 << 1);
                } else if commonbits > argbits {
                    bitmap = 0;
                }
            }
            RTGreaterEqualStrategyNumber | RTGreaterStrategyNumber => {
                if commonbits < argbits {
                    bitmap &= (1 << 2) | (1 << 3);
                }
            }
            _ => {}
        }
        if bitmap == 0 {
            break;
        }

        if commonbits != argbits {
            continue;
        }

        if !leaf
            && bitmap & (1 | (1 << 1)) != 0
            && commonbits < argument.maxbits() as i32
        {
            let nextbit =
                argument.addr[commonbits as usize / 8] & (1 << (7 - commonbits % 8)) != 0;
            match strategy {
                RTLessStrategyNumber | RTLessEqualStrategyNumber => {
                    if !nextbit {
                        bitmap &= 1 | (1 << 2) | (1 << 3);
                    }
                }
                RTGreaterEqualStrategyNumber | RTGreaterStrategyNumber => {
                    if nextbit {
                        bitmap &= (1 << 1) | (1 << 2) | (1 << 3);
                    }
                }
                RTNotEqualStrategyNumber => {}
                _ => {
                    if !nextbit {
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

        if leaf {
            let order = bitncmp(prefix.addr, argument.addr, prefix.maxbits() as i32);
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

const fn b(foid: Oid, name: &'static str, nargs: i16, func: PGFunction) -> FmgrBuiltin {
    FmgrBuiltin {
        foid,
        name,
        nargs,
        strict: true,
        retset: false,
        func,
    }
}

pub const NETWORK_SPGIST_BUILTINS: &[FmgrBuiltin] = &[
    b(3795, "inet_spg_config", 2, fc_inet_spg_config),
    b(3796, "inet_spg_choose", 2, fc_inet_spg_choose),
    b(3797, "inet_spg_picksplit", 2, fc_inet_spg_picksplit),
    b(3798, "inet_spg_inner_consistent", 2, fc_inet_spg_inner_consistent),
    b(3799, "inet_spg_leaf_consistent", 2, fc_inet_spg_leaf_consistent),
];

#[cfg(test)]
mod tests;
