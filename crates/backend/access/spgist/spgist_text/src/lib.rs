//! spgtextproc.c: radix tree (compressed trie) over text — the text_ops
//! SP-GiST opclass.
#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]

use ::datum::Datum;
use ::mcx::Mcx;
use ::types_core::{Oid, BLCKSZ};
use ::types_error::PgResult;
use ::types_fmgr::{FmgrBuiltin, FmgrInfo, FunctionCallInfoBaseData as Fcinfo};
use ::types_spgist::state::{
    spgChooseIn, spgChooseOut, spgInnerConsistentIn, spgInnerConsistentOut, spgLeafConsistentIn,
    spgLeafConsistentOut, spgPickSplitIn, spgPickSplitOut,
};
use ::types_spgist::spgConfigOut;

const TEXTOID: Oid = 25;
const INT2OID: Oid = 21;
const VARHDRSZ: usize = 4;
const VARHDRSZ_SHORT: usize = 1;
const VARATT_SHORT_MAX: usize = 0x7F;

const SPGIST_MAX_PREFIX_LENGTH: usize = {
    let v = BLCKSZ as isize - 258 * 16 - 100;
    if v > 32 { v as usize } else { 32 }
};

const SPG_STRATEGY_ADDITION: u16 = 10;
const RTPrefixStrategyNumber: u16 = 28;
const BTLessStrategyNumber: u16 = 1;
const BTLessEqualStrategyNumber: u16 = 2;
const BTEqualStrategyNumber: u16 = 3;
const BTGreaterEqualStrategyNumber: u16 = 4;
const BTGreaterStrategyNumber: u16 = 5;

#[inline]
fn is_collation_aware(strategy: u16) -> bool {
    strategy > SPG_STRATEGY_ADDITION && strategy != RTPrefixStrategyNumber
}

// VARDATA_ANY/VARSIZE_ANY_EXHDR over an untoasted (possibly short) text datum.
// SAFETY: datum points at a live, untoasted varlena (opclass protocol).
unsafe fn text_bytes<'a>(d: Datum) -> &'a [u8] {
    let p = d.as_usize() as *const u8;
    if ::types_tuple::varatt::varatt_is_1b(p) {
        let len = ::types_tuple::varatt::varsize_1b(p);
        core::slice::from_raw_parts(p.add(VARHDRSZ_SHORT), len - VARHDRSZ_SHORT)
    } else {
        let len = ::types_tuple::varatt::varsize_4b(p);
        core::slice::from_raw_parts(p.add(VARHDRSZ), len - VARHDRSZ)
    }
}

// formTextDatum: short header when possible, allocated in `mcx` (arena-owned).
fn form_text_datum(mcx: Mcx<'_>, data: &[u8]) -> PgResult<Datum> {
    let datalen = data.len();
    let (total, hdr) = if datalen + VARHDRSZ_SHORT <= VARATT_SHORT_MAX {
        (datalen + VARHDRSZ_SHORT, VARHDRSZ_SHORT)
    } else {
        (datalen + VARHDRSZ, VARHDRSZ)
    };
    let mut buf: ::mcx::PgVec<'_, u8> = ::mcx::vec_with_capacity_in(mcx, total.max(VARHDRSZ))?;
    if hdr == VARHDRSZ_SHORT {
        buf.push(((total << 1) | 1) as u8);
    } else {
        buf.extend_from_slice(&((total << 2) as u32).to_ne_bytes());
    }
    buf.extend_from_slice(data);
    let p = buf.as_ptr() as usize;
    core::mem::forget(buf);
    Ok(Datum::from_usize(p))
}

// 4B-header text in `mcx` with uninitialized-then-filled body; returns the
// datum and a raw pointer to the data area.
fn form_text_4b(mcx: Mcx<'_>, len: usize) -> PgResult<(Datum, *mut u8)> {
    let total = len + VARHDRSZ;
    let mut buf: ::mcx::PgVec<'_, u8> = ::mcx::vec_with_capacity_in(mcx, total)?;
    buf.extend_from_slice(&((total << 2) as u32).to_ne_bytes());
    buf.resize(total, 0);
    let p = buf.as_mut_ptr();
    core::mem::forget(buf);
    // SAFETY: arena-owned allocation of `total` bytes.
    Ok((Datum::from_usize(p as usize), unsafe { p.add(VARHDRSZ) }))
}

fn common_prefix(a: &[u8], b: &[u8]) -> usize {
    let n = a.len().min(b.len());
    let mut i = 0;
    while i < n && a[i] == b[i] {
        i += 1;
    }
    i
}

// searchChar: binary search over int16 label datums.
// SAFETY: labels points at n live datums (opclass protocol).
unsafe fn search_char(labels: *const Datum, n: i32, c: i16) -> (bool, i32) {
    let mut lo = 0i32;
    let mut hi = n;
    while lo < hi {
        let mid = (lo + hi) >> 1;
        let middle = (*labels.add(mid as usize)).as_i16();
        if c < middle {
            hi = mid;
        } else if c > middle {
            lo = mid + 1;
        } else {
            return (true, mid);
        }
    }
    (false, hi)
}

pub fn spg_text_config(_cfgin: &::types_spgist::spgConfigIn, cfg: &mut spgConfigOut) {
    cfg.prefixType = TEXTOID;
    cfg.labelType = INT2OID;
    cfg.canReturnData = true;
    cfg.longValuesOK = true;
}

fn fc_spg_text_config(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: spgist opclass fmgr protocol — args are live in/out structs.
    let cfgin = unsafe { &*(fcinfo.arg(0).as_usize() as *const ::types_spgist::spgConfigIn) };
    let cfg = unsafe { &mut *(fcinfo.arg(1).as_usize() as *mut spgConfigOut) };
    spg_text_config(cfgin, cfg);
    Ok(Datum::null())
}

fn fc_spg_text_choose(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: spgist opclass fmgr protocol.
    let input = unsafe { &*(fcinfo.arg(0).as_usize() as *const spgChooseIn) };
    let out = unsafe { &mut *(fcinfo.arg(1).as_usize() as *mut spgChooseOut) };
    let mcx = fcinfo.result_mcx();

    // SAFETY: untoasted text datums per protocol.
    let in_str = unsafe { text_bytes(input.datum) };
    let in_size = in_str.len();
    let level = input.level as usize;

    let mut common_len = 0usize;
    let mut node_char: i16 = 0;

    if input.hasPrefix {
        // SAFETY: prefix datum is a live text value.
        let prefix = unsafe { text_bytes(input.prefixDatum) };
        common_len = common_prefix(&in_str[level..], prefix);

        if common_len == prefix.len() {
            node_char = if in_size - level > common_len {
                in_str[level + common_len] as i16
            } else {
                -1
            };
        } else {
            // incoming value doesn't match prefix: split
            let prefix_has_prefix = common_len != 0;
            let prefix_prefix = if prefix_has_prefix {
                form_text_datum(mcx, &prefix[..common_len])?
            } else {
                Datum::null()
            };
            let mut labels: ::mcx::PgVec<'_, Datum> = ::mcx::vec_with_capacity_in(mcx, 1)?;
            labels.push(Datum::from_i16(prefix[common_len] as i16));
            let labels_ptr = labels.as_ptr();
            core::mem::forget(labels);

            let postfix_has_prefix = prefix.len() - common_len != 1;
            let postfix_prefix = if postfix_has_prefix {
                form_text_datum(mcx, &prefix[common_len + 1..])?
            } else {
                Datum::null()
            };

            *out = spgChooseOut::SplitTuple {
                prefixHasPrefix: prefix_has_prefix,
                prefixPrefixDatum: prefix_prefix,
                prefixNNodes: 1,
                prefixNodeLabels: labels_ptr,
                childNodeN: 0,
                postfixHasPrefix: postfix_has_prefix,
                postfixPrefixDatum: postfix_prefix,
            };
            return Ok(Datum::null());
        }
    } else if in_size > level {
        node_char = in_str[level] as i16;
    } else {
        node_char = -1;
    }

    // SAFETY: nodeLabels live per protocol (null only when nNodes == 0,
    // which searchChar handles by returning (false, 0)).
    let (found, i) = unsafe { search_char(input.nodeLabels, input.nNodes, node_char) };
    if found {
        let mut level_add = common_len as i32;
        if node_char >= 0 {
            level_add += 1;
        }
        let rest = if in_size as i32 - input.level - level_add > 0 {
            form_text_datum(mcx, &in_str[level + level_add as usize..])?
        } else {
            form_text_datum(mcx, &[])?
        };
        *out = spgChooseOut::MatchNode {
            nodeN: i,
            levelAdd: level_add,
            restDatum: rest,
        };
    } else if input.allTheSame {
        let mut labels: ::mcx::PgVec<'_, Datum> = ::mcx::vec_with_capacity_in(mcx, 1)?;
        labels.push(Datum::from_i16(-2));
        let labels_ptr = labels.as_ptr();
        core::mem::forget(labels);
        *out = spgChooseOut::SplitTuple {
            prefixHasPrefix: input.hasPrefix,
            prefixPrefixDatum: input.prefixDatum,
            prefixNNodes: 1,
            prefixNodeLabels: labels_ptr,
            childNodeN: 0,
            postfixHasPrefix: false,
            postfixPrefixDatum: Datum::null(),
        };
    } else {
        *out = spgChooseOut::AddNode {
            nodeLabel: Datum::from_i16(node_char),
            nodeN: i,
        };
    }
    Ok(Datum::null())
}

#[derive(Clone, Copy)]
struct SpgNodePtr {
    d: Datum,
    i: i32,
    c: i16,
}

fn fc_spg_text_picksplit(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: spgist opclass fmgr protocol.
    let input = unsafe { &*(fcinfo.arg(0).as_usize() as *const spgPickSplitIn) };
    let out = unsafe { &mut *(fcinfo.arg(1).as_usize() as *mut spgPickSplitOut) };
    let mcx = fcinfo.result_mcx();
    let n = input.nTuples as usize;

    // SAFETY: nTuples datums per protocol.
    let datums = unsafe { core::slice::from_raw_parts(input.datums, n) };

    // SAFETY: untoasted text values.
    let text0 = unsafe { text_bytes(datums[0]) };
    let mut common_len = text0.len();
    for &d in datums.iter().skip(1) {
        if common_len == 0 {
            break;
        }
        // SAFETY: untoasted text values.
        let ti = unsafe { text_bytes(d) };
        let tmp = common_prefix(text0, ti);
        if tmp < common_len {
            common_len = tmp;
        }
    }
    common_len = common_len.min(SPGIST_MAX_PREFIX_LENGTH);

    if common_len == 0 {
        out.hasPrefix = false;
    } else {
        out.hasPrefix = true;
        out.prefixDatum = form_text_datum(mcx, &text0[..common_len])?;
    }

    let mut nodes: ::mcx::PgVec<'_, SpgNodePtr> = ::mcx::vec_with_capacity_in(mcx, n)?;
    for (i, &d) in datums.iter().enumerate() {
        // SAFETY: untoasted text values.
        let ti = unsafe { text_bytes(d) };
        let c = if common_len < ti.len() {
            ti[common_len] as i16
        } else {
            -1
        };
        nodes.push(SpgNodePtr { d, i: i as i32, c });
    }

    // pg_qsort is unstable; keys can tie, but grouping only needs the sort
    // order of c and C's qsort on equal keys is order-unspecified too. Use a
    // stable sort so tuple->node mapping is deterministic (matches C's
    // practical med3 behavior for the regression corpus).
    nodes.sort_by(|a, b| a.c.cmp(&b.c));

    let mut labels: ::mcx::PgVec<'_, Datum> = ::mcx::vec_with_capacity_in(mcx, n)?;
    let mut map: ::mcx::PgVec<'_, i32> = ::mcx::vec_with_capacity_in(mcx, n)?;
    map.resize(n, 0);
    let mut leaf_datums: ::mcx::PgVec<'_, Datum> = ::mcx::vec_with_capacity_in(mcx, n)?;
    leaf_datums.resize(n, Datum::null());

    let mut n_nodes = 0i32;
    for i in 0..n {
        // SAFETY: untoasted text values.
        let ti = unsafe { text_bytes(nodes[i].d) };
        if i == 0 || nodes[i].c != nodes[i - 1].c {
            labels.push(Datum::from_i16(nodes[i].c));
            n_nodes += 1;
        }
        let leaf_d = if common_len < ti.len() {
            form_text_datum(mcx, &ti[common_len + 1..])?
        } else {
            form_text_datum(mcx, &[])?
        };
        leaf_datums[nodes[i].i as usize] = leaf_d;
        map[nodes[i].i as usize] = n_nodes - 1;
    }

    out.nNodes = n_nodes;
    out.nodeLabels = labels.as_ptr();
    core::mem::forget(labels);
    out.mapTuplesToNodes = map.as_mut_ptr();
    core::mem::forget(map);
    out.leafTupleDatums = leaf_datums.as_ptr();
    core::mem::forget(leaf_datums);

    Ok(Datum::null())
}

fn fc_spg_text_inner_consistent(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: spgist opclass fmgr protocol.
    let input = unsafe { &*(fcinfo.arg(0).as_usize() as *const spgInnerConsistentIn) };
    let out = unsafe { &mut *(fcinfo.arg(1).as_usize() as *mut spgInnerConsistentOut) };
    let mcx = fcinfo.result_mcx();
    let collation = fcinfo.fncollation;

    let collate_is_c = pg_locale::pg_newlocale_from_collation(collation)?.collate_is_c;

    let level = input.level as usize;
    debug_assert!(
        (input.reconstructedValue.as_usize() == 0) == (level == 0)
    );

    let mut max_reconstr_len = level + 1;
    let prefix: &[u8] = if input.hasPrefix {
        // SAFETY: untoasted text prefix.
        unsafe { text_bytes(input.prefixDatum) }
    } else {
        &[]
    };
    max_reconstr_len += prefix.len();

    // reconstrText: always long-format
    let (_reconstr_datum, reconstr_data) = form_text_4b(mcx, max_reconstr_len)?;
    // SAFETY: max_reconstr_len writable bytes at reconstr_data.
    let reconstr = unsafe { core::slice::from_raw_parts_mut(reconstr_data, max_reconstr_len) };
    if level > 0 {
        // SAFETY: reconstructedValue is a long-format text of length `level`.
        let prev = unsafe { text_bytes(input.reconstructedValue) };
        reconstr[..level].copy_from_slice(&prev[..level]);
    }
    if !prefix.is_empty() {
        reconstr[level..level + prefix.len()].copy_from_slice(prefix);
    }

    let n_nodes_in = input.nNodes as usize;
    let mut node_numbers: ::mcx::PgVec<'_, i32> = ::mcx::vec_with_capacity_in(mcx, n_nodes_in)?;
    let mut level_adds: ::mcx::PgVec<'_, i32> = ::mcx::vec_with_capacity_in(mcx, n_nodes_in)?;
    let mut recon_values: ::mcx::PgVec<'_, Datum> = ::mcx::vec_with_capacity_in(mcx, n_nodes_in)?;

    // SAFETY: nNodes labels; scankeys nkeys entries (protocol).
    let labels = unsafe { core::slice::from_raw_parts(input.nodeLabels, n_nodes_in) };
    let scankeys =
        unsafe { core::slice::from_raw_parts(input.scankeys, input.nkeys.max(0) as usize) };

    for (i, &label) in labels.iter().enumerate() {
        let node_char = label.as_i16();
        let this_len = if node_char <= 0 {
            max_reconstr_len - 1
        } else {
            reconstr[max_reconstr_len - 1] = node_char as u8;
            max_reconstr_len
        };

        let mut res = true;
        for key in scankeys {
            let mut strategy = key.sk_strategy;
            if is_collation_aware(strategy) {
                if collate_is_c {
                    strategy -= SPG_STRATEGY_ADDITION;
                } else {
                    continue;
                }
            }

            // SAFETY: scankey argument is an untoasted text (planner detoasts).
            let in_text = unsafe { text_bytes(key.sk_argument) };
            let cmp_len = in_text.len().min(this_len);
            let r = cmp_bytes(&reconstr[..cmp_len], &in_text[..cmp_len]);

            res = match strategy {
                BTLessStrategyNumber | BTLessEqualStrategyNumber => r <= 0,
                BTEqualStrategyNumber => r == 0 && in_text.len() >= this_len,
                BTGreaterEqualStrategyNumber | BTGreaterStrategyNumber => r >= 0,
                RTPrefixStrategyNumber => r == 0,
                other => panic!("unrecognized strategy number: {other}"),
            };
            if !res {
                break;
            }
        }

        if res {
            node_numbers.push(i as i32);
            level_adds.push((this_len - level) as i32);
            let copy = form_text_4b(mcx, this_len)?;
            // SAFETY: this_len writable bytes at copy.1.
            unsafe {
                core::ptr::copy_nonoverlapping(reconstr.as_ptr(), copy.1, this_len);
            }
            recon_values.push(copy.0);
        }
    }

    out.nNodes = node_numbers.len() as i32;
    out.nodeNumbers = node_numbers.as_ptr();
    core::mem::forget(node_numbers);
    out.levelAdds = level_adds.as_ptr();
    core::mem::forget(level_adds);
    out.reconstructedValues = recon_values.as_ptr();
    core::mem::forget(recon_values);

    Ok(Datum::null())
}

#[inline]
fn cmp_bytes(a: &[u8], b: &[u8]) -> i32 {
    match a.cmp(b) {
        core::cmp::Ordering::Less => -1,
        core::cmp::Ordering::Equal => 0,
        core::cmp::Ordering::Greater => 1,
    }
}

fn fc_spg_text_leaf_consistent(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: spgist opclass fmgr protocol.
    let input = unsafe { &*(fcinfo.arg(0).as_usize() as *const spgLeafConsistentIn) };
    let out = unsafe { &mut *(fcinfo.arg(1).as_usize() as *mut spgLeafConsistentOut) };
    let mcx = fcinfo.result_mcx();
    let collation = fcinfo.fncollation;
    let level = input.level as usize;

    out.recheck = false;

    // SAFETY: untoasted leaf text.
    let leaf = unsafe { text_bytes(input.leafDatum) };

    let reconstr: &[u8] = if input.reconstructedValue.as_usize() != 0 {
        // SAFETY: long-format reconstructed text.
        unsafe { text_bytes(input.reconstructedValue) }
    } else {
        &[]
    };
    debug_assert!(reconstr.len() == level);

    let full_len = level + leaf.len();
    let full_value: &[u8];
    if leaf.is_empty() && level > 0 {
        full_value = reconstr;
        out.leafValue = input.reconstructedValue;
    } else {
        let (d, p) = form_text_4b(mcx, full_len)?;
        // SAFETY: full_len writable bytes at p.
        unsafe {
            if level > 0 {
                core::ptr::copy_nonoverlapping(reconstr.as_ptr(), p, level);
            }
            if !leaf.is_empty() {
                core::ptr::copy_nonoverlapping(leaf.as_ptr(), p.add(level), leaf.len());
            }
            full_value = core::slice::from_raw_parts(p, full_len);
        }
        out.leafValue = d;
    }

    // SAFETY: nkeys scankeys (protocol).
    let scankeys =
        unsafe { core::slice::from_raw_parts(input.scankeys, input.nkeys.max(0) as usize) };

    let mut res = true;
    for key in scankeys {
        let mut strategy = key.sk_strategy;
        // SAFETY: untoasted query text.
        let query = unsafe { text_bytes(key.sk_argument) };

        if strategy == RTPrefixStrategyNumber {
            res = level >= query.len() || {
                if !pg_locale_seams::collation_is_deterministic::call(collation)? {
                    panic!("nondeterministic collations are not supported for substring searches");
                }
                full_value.len() >= query.len() && &full_value[..query.len()] == query
            };
            if !res {
                break;
            }
            continue;
        }

        let r = if is_collation_aware(strategy) {
            strategy -= SPG_STRATEGY_ADDITION;
            varlena::varstr_cmp(full_value, query, collation)?
        } else {
            let n = query.len().min(full_value.len());
            let mut r = cmp_bytes(&full_value[..n], &query[..n]);
            if r == 0 {
                if query.len() > full_value.len() {
                    r = -1;
                } else if query.len() < full_value.len() {
                    r = 1;
                }
            }
            r
        };

        res = match strategy {
            BTLessStrategyNumber => r < 0,
            BTLessEqualStrategyNumber => r <= 0,
            BTEqualStrategyNumber => r == 0,
            BTGreaterEqualStrategyNumber => r >= 0,
            BTGreaterStrategyNumber => r > 0,
            other => panic!("unrecognized strategy number: {other}"),
        };
        if !res {
            break;
        }
    }

    Ok(Datum::from_bool(res))
}

fn fc_spghandler(_f: Option<&mut FmgrInfo>, _fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    panic!("spghandler: the closed AM set dispatches via IndexAmKind, never through fmgr")
}

const fn b(foid: Oid, name: &'static str, nargs: i16, func: ::types_fmgr::PGFunction) -> FmgrBuiltin {
    FmgrBuiltin {
        foid,
        name,
        nargs,
        strict: true,
        retset: false,
        func,
    }
}

pub const SPGIST_TEXT_BUILTINS: &[FmgrBuiltin] = &[
    b(334, "spghandler", 1, fc_spghandler),
    b(4027, "spg_text_config", 2, fc_spg_text_config),
    b(4028, "spg_text_choose", 2, fc_spg_text_choose),
    b(4029, "spg_text_picksplit", 2, fc_spg_text_picksplit),
    b(4030, "spg_text_inner_consistent", 2, fc_spg_text_inner_consistent),
    b(4031, "spg_text_leaf_consistent", 2, fc_spg_text_leaf_consistent),
];
