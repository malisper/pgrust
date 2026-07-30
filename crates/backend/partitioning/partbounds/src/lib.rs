// partbounds.c, LIST/RANGE/HASH create + search lane, plus the planner-side
// partitionwise-join merge kernel (merge module).
#![allow(non_snake_case)]

pub mod merge;
mod qual;
#[cfg(test)]
mod merge_tests;
#[cfg(test)]
mod tests;

pub use merge::{
    partition_bounds_equal, partition_bounds_merge, MergeRel, PartitionBoundsMergeResult,
};
pub use qual::{
    check_default_partition_contents, get_proposed_default_constraint, get_qual_from_partbound,
    make_ands_explicit, map_partition_varattnos, read_boundspec, read_boundspec_opt,
    PARTBOUNDS_BUILTINS,
};

use datum::Datum;
use mcx::{Mcx, PgVec};
use types_core::Oid;
use types_error::{PgError, PgResult, ERRCODE_INVALID_OBJECT_DEFINITION, ERROR};
use types_fmgr::{FmgrInfo, LocalFcinfo};
use types_nodes::primnodes::Const;
use types_nodes::rawnodes::{PartitionBoundSpec, PartitionRangeDatum, PartitionRangeDatumKind};
use partcache::PartitionKeyData;

pub const PARTITION_STRATEGY_LIST: u8 = b'l';
pub const PARTITION_STRATEGY_RANGE: u8 = b'r';
pub const PARTITION_STRATEGY_HASH: u8 = b'h';

pub const HASH_PARTITION_SEED: u64 = 0x7A5B22367996DCFD;

pub struct PartitionBoundInfoData<'m> {
    pub strategy: i8,
    pub ndatums: usize,
    // width datums per row: 1 for LIST, partnatts for RANGE.
    pub width: usize,
    pub datums: PgVec<'m, Datum>,
    pub kind: PgVec<'m, i8>,
    pub indexes: PgVec<'m, i32>,
    pub null_index: i32,
    pub default_index: i32,
}

impl<'m> PartitionBoundInfoData<'m> {
    #[inline]
    pub fn datum(&self, i: usize, j: usize) -> Datum {
        self.datums[i * self.width + j]
    }
    #[inline]
    pub fn kind_at(&self, i: usize, j: usize) -> i8 {
        self.kind[i * self.width + j]
    }
    #[inline]
    pub fn accepts_nulls(&self) -> bool {
        self.null_index != -1
    }
    #[inline]
    pub fn has_default(&self) -> bool {
        self.default_index != -1
    }
}

pub const KIND_MINVALUE: i8 = -1;
pub const KIND_VALUE: i8 = 0;
pub const KIND_MAXVALUE: i8 = 1;

// datumCopy (datum.c) into `mcx`; cstring datums unreachable for key types.
pub(crate) fn datum_copy<'m>(
    mcx: Mcx<'m>,
    value: Datum,
    typbyval: bool,
    typlen: i16,
) -> PgResult<Datum> {
    if typbyval {
        return Ok(value);
    }
    let p = value.as_usize() as *const u8;
    let len = match typlen {
        l if l > 0 => l as usize,
        -1 => {
            // SAFETY: byref bound datum is a live inline varlena.
            unsafe {
                let b0 = *p;
                if b0 & 0x01 != 0 {
                    (b0 as usize >> 1) & 0x7F
                } else {
                    (u32::from_ne_bytes(core::slice::from_raw_parts(p, 4).try_into().unwrap())
                        as usize)
                        >> 2
                }
            }
        }
        other => panic!("datum_copy: typlen {other} unported"),
    };
    let mut buf: PgVec<'m, u8> = mcx::vec_with_capacity_in(mcx, len)?;
    // SAFETY: len derived from the datum's own image.
    buf.extend_from_slice(unsafe { core::slice::from_raw_parts(p, len) });
    Ok(Datum::from_usize(buf.leak().as_ptr() as usize))
}

fn spec_const<'a>(n: types_nodes::Node<'a>) -> &'a Const {
    n.as_variant::<Const>().expect("partition bound datum is not a Const")
}

struct PartitionRangeBound {
    index: i32,
    datums: Vec<Datum>,
    kind: Vec<i8>,
    lower: bool,
}

fn make_one_partition_rbound(
    key: &PartitionKeyData,
    index: i32,
    datums: &types_nodes::NodeList<'_>,
    lower: bool,
) -> PartitionRangeBound {
    let n = key.partnatts as usize;
    let mut b = PartitionRangeBound {
        index,
        datums: vec![Datum::null(); n],
        kind: vec![KIND_VALUE; n],
        lower,
    };
    for (i, node) in datums.iter().enumerate() {
        let prd = node
            .as_variant::<PartitionRangeDatum>()
            .expect("range bound datum is not a PartitionRangeDatum");
        b.kind[i] = prd.kind as i8;
        if prd.kind == PartitionRangeDatumKind::Value {
            let c = spec_const(prd.value.expect("PartitionRangeDatum value"));
            assert!(!c.constisnull, "invalid range bound datum");
            b.datums[i] = c.constvalue;
        }
    }
    b
}

fn key_cmp(key: &PartitionKeyData, col: usize, a: Datum, b: Datum) -> i32 {
    key.cmp(col, a, b)
        .unwrap_or_else(|e| panic!("partition support function failed: {e:?}"))
}

// partition_rbound_cmp: signed column number encodes the mismatch position.
pub fn partition_rbound_cmp(
    key: &PartitionKeyData,
    datums1: &[Datum],
    kind1: &[i8],
    lower1: bool,
    b2_datums: &[Datum],
    b2_kind: &[i8],
    b2_lower: bool,
) -> i32 {
    let mut colnum = 0i32;
    let mut cmpval = 0i32;
    for i in 0..key.partnatts as usize {
        colnum += 1;
        if kind1[i] < b2_kind[i] {
            return -colnum;
        } else if kind1[i] > b2_kind[i] {
            return colnum;
        } else if kind1[i] != KIND_VALUE {
            break;
        }
        cmpval = key_cmp(key, i, datums1[i], b2_datums[i]);
        if cmpval != 0 {
            break;
        }
    }
    if cmpval == 0 && lower1 != b2_lower {
        cmpval = if lower1 { 1 } else { -1 };
    }
    if cmpval == 0 {
        0
    } else if cmpval < 0 {
        -colnum
    } else {
        colnum
    }
}

pub fn partition_rbound_datum_cmp(
    key: &PartitionKeyData,
    rb_datums: &[Datum],
    rb_kind: &[i8],
    tuple_datums: &[Datum],
) -> i32 {
    let mut cmpval = -1;
    for i in 0..tuple_datums.len() {
        if rb_kind[i] == KIND_MINVALUE {
            return -1;
        } else if rb_kind[i] == KIND_MAXVALUE {
            return 1;
        }
        cmpval = key_cmp(key, i, rb_datums[i], tuple_datums[i]);
        if cmpval != 0 {
            break;
        }
    }
    cmpval
}

pub fn partition_list_bsearch(
    key: &PartitionKeyData,
    boundinfo: &PartitionBoundInfoData<'_>,
    value: Datum,
    is_equal: &mut bool,
) -> i32 {
    let mut lo: i32 = -1;
    let mut hi: i32 = boundinfo.ndatums as i32 - 1;
    while lo < hi {
        let mid = (lo + hi + 1) / 2;
        let cmpval = key_cmp(key, 0, boundinfo.datum(mid as usize, 0), value);
        if cmpval <= 0 {
            lo = mid;
            *is_equal = cmpval == 0;
            if *is_equal {
                break;
            }
        } else {
            hi = mid - 1;
        }
    }
    lo
}

pub fn partition_range_datum_bsearch(
    key: &PartitionKeyData,
    boundinfo: &PartitionBoundInfoData<'_>,
    values: &[Datum],
    is_equal: &mut bool,
) -> i32 {
    let w = boundinfo.width;
    let mut lo: i32 = -1;
    let mut hi: i32 = boundinfo.ndatums as i32 - 1;
    while lo < hi {
        let mid = (lo + hi + 1) / 2;
        let m = mid as usize;
        let cmpval = partition_rbound_datum_cmp(
            key,
            &boundinfo.datums[m * w..(m + 1) * w],
            &boundinfo.kind[m * w..(m + 1) * w],
            values,
        );
        if cmpval <= 0 {
            lo = mid;
            *is_equal = cmpval == 0;
            if *is_equal {
                break;
            }
        } else {
            hi = mid - 1;
        }
    }
    lo
}

fn partition_range_bsearch(
    key: &PartitionKeyData,
    boundinfo: &PartitionBoundInfoData<'_>,
    probe: &PartitionRangeBound,
    cmpval_out: &mut i32,
) -> i32 {
    let w = boundinfo.width;
    let mut lo: i32 = -1;
    let mut hi: i32 = boundinfo.ndatums as i32 - 1;
    while lo < hi {
        let mid = (lo + hi + 1) / 2;
        let m = mid as usize;
        *cmpval_out = partition_rbound_cmp(
            key,
            &boundinfo.datums[m * w..(m + 1) * w],
            &boundinfo.kind[m * w..(m + 1) * w],
            boundinfo.indexes[m] == -1,
            &probe.datums,
            &probe.kind,
            probe.lower,
        );
        if *cmpval_out <= 0 {
            lo = mid;
            if *cmpval_out == 0 {
                break;
            }
        } else {
            hi = mid - 1;
        }
    }
    lo
}

// partition_bounds_create: mapping[i] = canonical index of original slot i.
pub fn partition_bounds_create<'m>(
    mcx: Mcx<'m>,
    boundspecs: &[&PartitionBoundSpec<'_>],
    key: &PartitionKeyData,
) -> PgResult<(PartitionBoundInfoData<'m>, Vec<i32>)> {
    assert!(!boundspecs.is_empty());
    let mut mapping = vec![-1i32; boundspecs.len()];
    let info = match key.strategy as u8 {
        PARTITION_STRATEGY_HASH => create_hash_bounds(mcx, boundspecs, key, &mut mapping)?,
        PARTITION_STRATEGY_LIST => create_list_bounds(mcx, boundspecs, key, &mut mapping)?,
        PARTITION_STRATEGY_RANGE => create_range_bounds(mcx, boundspecs, key, &mut mapping)?,
        other => panic!("unexpected partition strategy: {}", other as char),
    };
    Ok((info, mapping))
}

fn create_hash_bounds<'m>(
    mcx: Mcx<'m>,
    boundspecs: &[&PartitionBoundSpec<'_>],
    key: &PartitionKeyData,
    mapping: &mut [i32],
) -> PgResult<PartitionBoundInfoData<'m>> {
    let nparts = boundspecs.len();
    let mut hbounds: Vec<(i32, i32, i32)> = Vec::with_capacity(nparts);
    for (i, spec) in boundspecs.iter().enumerate() {
        assert!(
            spec.strategy == PARTITION_STRATEGY_HASH,
            "invalid strategy in partition bound spec"
        );
        hbounds.push((spec.modulus, spec.remainder, i as i32));
    }
    hbounds.sort_by(|a, b| partition_hbound_cmp(a.0, a.1, b.0, b.1).cmp(&0));

    let greatest_modulus = hbounds[nparts - 1].0;
    let mut info = PartitionBoundInfoData {
        strategy: key.strategy,
        ndatums: nparts,
        width: 2,
        datums: mcx::vec_with_capacity_in(mcx, nparts * 2)?,
        kind: PgVec::new_in(mcx),
        indexes: mcx::vec_with_capacity_in(mcx, greatest_modulus as usize)?,
        null_index: -1,
        default_index: -1,
    };
    info.indexes.resize(greatest_modulus as usize, -1);
    for (i, &(modulus, remainder, orig_index)) in hbounds.iter().enumerate() {
        info.datums.push(Datum::from_i32(modulus));
        info.datums.push(Datum::from_i32(remainder));
        let mut remainder = remainder;
        while remainder < greatest_modulus {
            assert!(info.indexes[remainder as usize] == -1);
            info.indexes[remainder as usize] = i as i32;
            remainder += modulus;
        }
        mapping[orig_index as usize] = i as i32;
    }
    Ok(info)
}

fn create_list_bounds<'m>(
    mcx: Mcx<'m>,
    boundspecs: &[&PartitionBoundSpec<'_>],
    key: &PartitionKeyData,
    mapping: &mut [i32],
) -> PgResult<PartitionBoundInfoData<'m>> {
    let mut null_index: i32 = -1;
    let mut default_index: i32 = -1;
    let mut next_index: i32 = 0;
    let mut all_values: Vec<(i32, Datum)> = Vec::new();

    for (i, spec) in boundspecs.iter().enumerate() {
        assert!(spec.strategy == PARTITION_STRATEGY_LIST, "invalid strategy in partition bound spec");
        if spec.is_default {
            default_index = i as i32;
            continue;
        }
        for c in spec.listdatums.iter() {
            let val = spec_const(c);
            if !val.constisnull {
                all_values.push((i as i32, val.constvalue));
            } else {
                assert!(null_index == -1, "found null more than once");
                null_index = i as i32;
            }
        }
    }

    all_values.sort_by(|a, b| key_cmp(key, 0, a.1, b.1).cmp(&0));

    let ndatums = all_values.len();
    let mut info = PartitionBoundInfoData {
        strategy: key.strategy,
        ndatums,
        width: 1,
        datums: mcx::vec_with_capacity_in(mcx, ndatums)?,
        kind: PgVec::new_in(mcx),
        indexes: mcx::vec_with_capacity_in(mcx, ndatums)?,
        null_index: -1,
        default_index: -1,
    };
    for &(orig_index, value) in &all_values {
        info.datums.push(datum_copy(mcx, value, key.parttypbyval[0], key.parttyplen[0])?);
        if mapping[orig_index as usize] == -1 {
            mapping[orig_index as usize] = next_index;
            next_index += 1;
        }
        info.indexes.push(mapping[orig_index as usize]);
    }
    if null_index != -1 {
        if mapping[null_index as usize] == -1 {
            mapping[null_index as usize] = next_index;
            next_index += 1;
        }
        info.null_index = mapping[null_index as usize];
    }
    if default_index != -1 {
        assert!(mapping[default_index as usize] == -1);
        mapping[default_index as usize] = next_index;
        next_index += 1;
        info.default_index = mapping[default_index as usize];
    }
    assert_eq!(next_index as usize, boundspecs.len());
    Ok(info)
}

fn create_range_bounds<'m>(
    mcx: Mcx<'m>,
    boundspecs: &[&PartitionBoundSpec<'_>],
    key: &PartitionKeyData,
    mapping: &mut [i32],
) -> PgResult<PartitionBoundInfoData<'m>> {
    let nparts = boundspecs.len();
    let partnatts = key.partnatts as usize;
    let mut next_index: i32 = 0;
    let mut default_index: i32 = -1;
    let mut all_bounds: Vec<PartitionRangeBound> = Vec::with_capacity(2 * nparts);

    for (i, spec) in boundspecs.iter().enumerate() {
        assert!(spec.strategy == PARTITION_STRATEGY_RANGE, "invalid strategy in partition bound spec");
        if spec.is_default {
            default_index = i as i32;
            continue;
        }
        all_bounds.push(make_one_partition_rbound(key, i as i32, &spec.lowerdatums, true));
        all_bounds.push(make_one_partition_rbound(key, i as i32, &spec.upperdatums, false));
    }

    all_bounds.sort_by(|a, b| {
        partition_rbound_cmp(key, &a.datums, &a.kind, a.lower, &b.datums, &b.kind, b.lower)
            .cmp(&0)
    });

    // Distinct bounds only (C's rbounds pass).
    let mut rbounds: Vec<&PartitionRangeBound> = Vec::with_capacity(all_bounds.len());
    for (i, cur) in all_bounds.iter().enumerate() {
        let mut is_distinct = false;
        let prev = if i == 0 { None } else { Some(&all_bounds[i - 1]) };
        for j in 0..partnatts {
            let Some(prev) = prev else {
                is_distinct = true;
                break;
            };
            if cur.kind[j] != prev.kind[j] {
                is_distinct = true;
                break;
            }
            if cur.kind[j] != KIND_VALUE {
                break;
            }
            if key_cmp(key, j, cur.datums[j], prev.datums[j]) != 0 {
                is_distinct = true;
                break;
            }
        }
        if is_distinct {
            rbounds.push(cur);
        }
    }

    let ndatums = rbounds.len();
    let mut info = PartitionBoundInfoData {
        strategy: key.strategy,
        ndatums,
        width: partnatts,
        datums: mcx::vec_with_capacity_in(mcx, ndatums * partnatts)?,
        kind: mcx::vec_with_capacity_in(mcx, ndatums * partnatts)?,
        indexes: mcx::vec_with_capacity_in(mcx, ndatums + 1)?,
        null_index: -1,
        default_index: -1,
    };
    for rb in &rbounds {
        for j in 0..partnatts {
            let d = if rb.kind[j] == KIND_VALUE {
                datum_copy(mcx, rb.datums[j], key.parttypbyval[j], key.parttyplen[j])?
            } else {
                Datum::null()
            };
            info.datums.push(d);
            info.kind.push(rb.kind[j]);
        }
        if rb.lower {
            info.indexes.push(-1);
        } else {
            let orig_index = rb.index as usize;
            if mapping[orig_index] == -1 {
                mapping[orig_index] = next_index;
                next_index += 1;
            }
            info.indexes.push(mapping[orig_index]);
        }
    }
    if default_index != -1 {
        assert!(mapping[default_index as usize] == -1);
        mapping[default_index as usize] = next_index;
        next_index += 1;
        info.default_index = mapping[default_index as usize];
    }
    info.indexes.push(-1);
    assert_eq!(next_index as usize, nparts);
    Ok(info)
}

pub fn partition_hbound_cmp(modulus1: i32, remainder1: i32, modulus2: i32, remainder2: i32) -> i32 {
    if modulus1 < modulus2 {
        return -1;
    }
    if modulus1 > modulus2 {
        return 1;
    }
    if modulus1 == modulus2 && remainder1 != remainder2 {
        return if remainder1 > remainder2 { 1 } else { -1 };
    }
    0
}

pub fn partition_hash_bsearch(
    boundinfo: &PartitionBoundInfoData<'_>,
    modulus: i32,
    remainder: i32,
) -> i32 {
    let mut lo: i32 = -1;
    let mut hi: i32 = boundinfo.ndatums as i32 - 1;
    while lo < hi {
        let mid = (lo + hi + 1) / 2;
        let bound_modulus = boundinfo.datum(mid as usize, 0).as_i32();
        let bound_remainder = boundinfo.datum(mid as usize, 1).as_i32();
        let cmpval = partition_hbound_cmp(bound_modulus, bound_remainder, modulus, remainder);
        if cmpval <= 0 {
            lo = mid;
            if cmpval == 0 {
                break;
            }
        } else {
            hi = mid - 1;
        }
    }
    lo
}

// src/include/common/hashfn.h hash_combine64.
#[inline]
pub fn hash_combine64(a: u64, b: u64) -> u64 {
    a ^ (b
        .wrapping_add(0x49a0f4dd15e5a8e3)
        .wrapping_add(a << 54)
        .wrapping_add(a >> 7))
}

pub fn compute_partition_hash_value(
    mcx: Mcx<'_>,
    partsupfunc: &mut [FmgrInfo],
    partcollation: &[Oid],
    values: &[Datum],
    isnull: &[bool],
) -> PgResult<u64> {
    let seed = Datum::from_u64(HASH_PARTITION_SEED);
    let mut row_hash: u64 = 0;
    for i in 0..partsupfunc.len() {
        if isnull[i] {
            continue;
        }
        let mut fcinfo = LocalFcinfo::<2>::new(partcollation[i]);
        // Custom hash opclasses (SQL-function support procs) allocate by-ref
        // intermediates through the frame's result mcx (C: caller's
        // CurrentMemoryContext).
        // SAFETY: `mcx` outlives this stack frame's single call.
        unsafe { fcinfo.set_result_mcx(mcx) };
        fcinfo.set_arg(0, values[i]);
        fcinfo.set_arg(1, seed);
        let hash = partsupfunc[i].invoke(&mut fcinfo)?;
        assert!(!fcinfo.isnull, "partition hash support function returned NULL");
        row_hash = hash_combine64(row_hash, hash.as_u64());
    }
    Ok(row_hash)
}

pub fn get_hash_partition_greatest_modulus(bound: &PartitionBoundInfoData<'_>) -> i32 {
    debug_assert!(bound.strategy as u8 == PARTITION_STRATEGY_HASH);
    bound.indexes.len() as i32
}

#[track_caller]
#[cold]
fn modulus_factor_error(
    new_modulus: i32,
    other_modulus: i32,
    how: &str,
    with_name: &str,
) -> Box<PgError> {
    Box::new(
        PgError::new(
            ERROR,
            "every hash partition modulus must be a factor of the next larger modulus".to_string(),
        )
        .with_detail(format!(
            "The new modulus {new_modulus} is {how} {other_modulus}, the modulus of existing \
             partition \"{with_name}\"."
        ))
        .with_sqlstate(ERRCODE_INVALID_OBJECT_DEFINITION),
    )
}

#[track_caller]
#[cold]
fn overlap_error(relname: &str, with_name: &str) -> Box<PgError> {
    Box::new(
        PgError::new(
            ERROR,
            format!("partition \"{relname}\" would overlap partition \"{with_name}\""),
        )
        .with_sqlstate(ERRCODE_INVALID_OBJECT_DEFINITION),
    )
}

fn rel_name(mcx: Mcx<'_>, oid: Oid) -> String {
    lsyscache::get_rel_name(mcx, oid)
        .ok()
        .flatten()
        .map(|s| s.as_str().to_string())
        .unwrap_or_default()
}

// check_new_partition_bound.
pub fn check_new_partition_bound<'mcx>(
    mcx: Mcx<'mcx>,
    relname: &str,
    key: &PartitionKeyData,
    boundinfo: Option<&PartitionBoundInfoData<'_>>,
    part_oids: &[Oid],
    spec: &PartitionBoundSpec<'mcx>,
    sourcetext: Option<&[u8]>,
) -> PgResult<()> {
    let errpos = |loc: i32| {
        parser_small1::parser_errposition_source(sourcetext, loc, mbutils::GetDatabaseEncoding())
    };
    if spec.is_default {
        let Some(boundinfo) = boundinfo else { return Ok(()) };
        if !boundinfo.has_default() {
            return Ok(());
        }
        return Err(Box::new(
            PgError::new(
                ERROR,
                format!(
                    "partition \"{relname}\" conflicts with existing default partition \"{}\"",
                    rel_name(mcx, part_oids[boundinfo.default_index as usize])
                ),
            )
            .with_sqlstate(ERRCODE_INVALID_OBJECT_DEFINITION)
            .with_cursor_position(errpos(spec.location)),
        ));
    }
    let mut with: i32 = -1;
    let mut overlap = false;
    let mut overlap_location: i32 = -1;

    match key.strategy as u8 {
        PARTITION_STRATEGY_HASH => {
            assert!(spec.strategy == PARTITION_STRATEGY_HASH);
            debug_assert!(spec.remainder >= 0 && spec.remainder < spec.modulus);
            if let Some(boundinfo) = boundinfo {
                let offset = partition_hash_bsearch(boundinfo, spec.modulus, spec.remainder);
                if offset < 0 {
                    let next_modulus = boundinfo.datum(0, 0).as_i32();
                    if next_modulus % spec.modulus != 0 {
                        return Err(modulus_factor_error(
                            spec.modulus,
                            next_modulus,
                            "not a factor of",
                            &rel_name(mcx, part_oids[0]),
                        ));
                    }
                } else {
                    let prev_modulus = boundinfo.datum(offset as usize, 0).as_i32();
                    if spec.modulus % prev_modulus != 0 {
                        return Err(modulus_factor_error(
                            spec.modulus,
                            prev_modulus,
                            "not divisible by",
                            &rel_name(mcx, part_oids[offset as usize]),
                        ));
                    }
                    if ((offset + 1) as usize) < boundinfo.ndatums {
                        let next_modulus = boundinfo.datum(offset as usize + 1, 0).as_i32();
                        if next_modulus % spec.modulus != 0 {
                            return Err(modulus_factor_error(
                                spec.modulus,
                                next_modulus,
                                "not a factor of",
                                &rel_name(mcx, part_oids[offset as usize + 1]),
                            ));
                        }
                    }
                }
                let greatest_modulus = boundinfo.indexes.len() as i32;
                let mut remainder = spec.remainder;
                if remainder >= greatest_modulus {
                    remainder %= greatest_modulus;
                }
                loop {
                    if boundinfo.indexes[remainder as usize] != -1 {
                        overlap = true;
                        overlap_location = spec.location;
                        with = boundinfo.indexes[remainder as usize];
                        break;
                    }
                    remainder += spec.modulus;
                    if remainder >= greatest_modulus {
                        break;
                    }
                }
            }
        }
        PARTITION_STRATEGY_LIST => {
            if let Some(boundinfo) = boundinfo {
                for cell in spec.listdatums.iter() {
                    let val = spec_const(cell);
                    if !val.constisnull {
                        let mut equal = false;
                        let offset =
                            partition_list_bsearch(key, boundinfo, val.constvalue, &mut equal);
                        if offset >= 0 && equal {
                            overlap = true;
                            overlap_location = val.location;
                            with = boundinfo.indexes[offset as usize];
                            break;
                        }
                    } else if boundinfo.accepts_nulls() {
                        overlap = true;
                        overlap_location = spec_const(cell).location;
                        with = boundinfo.null_index;
                        break;
                    }
                }
            }
        }
        PARTITION_STRATEGY_RANGE => {
            let lower = make_one_partition_rbound(key, -1, &spec.lowerdatums, true);
            let upper = make_one_partition_rbound(key, -1, &spec.upperdatums, false);
            let cmpval = partition_rbound_cmp(
                key,
                &lower.datums,
                &lower.kind,
                true,
                &upper.datums,
                &upper.kind,
                upper.lower,
            );
            debug_assert!(cmpval != 0);
            if cmpval > 0 {
                return Err(Box::new(
                    PgError::new(
                        ERROR,
                        format!("empty range bound specified for partition \"{relname}\""),
                    )
                    .with_detail(format!(
                        "Specified lower bound {} is greater than or equal to upper bound {}.",
                        range_partbound_string(mcx, &spec.lowerdatums)?,
                        range_partbound_string(mcx, &spec.upperdatums)?,
                    ))
                    .with_sqlstate(ERRCODE_INVALID_OBJECT_DEFINITION)
                    // C points at the problematic key in the lower datums
                    // (partbounds.c:3121-3132): list_nth(lowerdatums, cmpval-1).
                    .with_cursor_position(errpos(range_datum_location(
                        &spec.lowerdatums,
                        cmpval as usize - 1,
                    ))),
                ));
            }
            if let Some(boundinfo) = boundinfo {
                let mut cmpval = 0;
                let offset = partition_range_bsearch(key, boundinfo, &lower, &mut cmpval);
                if boundinfo.indexes[(offset + 1) as usize] < 0 {
                    if ((offset + 1) as usize) < boundinfo.ndatums {
                        let m = (offset + 1) as usize;
                        let w = boundinfo.width;
                        let is_lower = boundinfo.indexes[m] == -1;
                        let cmpval2 = partition_rbound_cmp(
                            key,
                            &boundinfo.datums[m * w..(m + 1) * w],
                            &boundinfo.kind[m * w..(m + 1) * w],
                            is_lower,
                            &upper.datums,
                            &upper.kind,
                            upper.lower,
                        );
                        if cmpval2 < 0 {
                            overlap = true;
                            overlap_location = range_datum_location(
                                &spec.upperdatums,
                                if cmpval2 == 0 { 0 } else { cmpval2.unsigned_abs() as usize - 1 },
                            );
                            with = boundinfo.indexes[(offset + 2) as usize];
                        }
                    }
                } else {
                    overlap = true;
                    overlap_location = range_datum_location(
                        &spec.lowerdatums,
                        if cmpval == 0 { 0 } else { cmpval.unsigned_abs() as usize - 1 },
                    );
                    with = boundinfo.indexes[(offset + 1) as usize];
                }
            }
        }
        other => panic!("unexpected partition strategy: {}", other as char),
    }

    if overlap {
        debug_assert!(with >= 0);
        let e = overlap_error(relname, &rel_name(mcx, part_oids[with as usize]));
        return Err(Box::new((*e).with_cursor_position(errpos(overlap_location))));
    }
    Ok(())
}

fn range_datum_location(datums: &types_nodes::NodeList<'_>, idx: usize) -> i32 {
    datums
        .iter()
        .nth(idx)
        .and_then(|n| n.as_variant::<PartitionRangeDatum>())
        .map(|d| d.location)
        .unwrap_or(-1)
}

// get_range_partbound_string (ruleutils.c); the Const deparse rides the
// ruleutils seam (ruleutils depends on this crate).
fn range_partbound_string<'mcx>(
    mcx: Mcx<'mcx>,
    datums: &types_nodes::NodeList<'mcx>,
) -> PgResult<String> {
    let mut s = String::from("(");
    for (i, node) in datums.iter().enumerate() {
        if i > 0 {
            s.push_str(", ");
        }
        let prd = node
            .as_variant::<PartitionRangeDatum>()
            .expect("range bound datum is not a PartitionRangeDatum");
        match prd.kind {
            PartitionRangeDatumKind::Minvalue => s.push_str("MINVALUE"),
            PartitionRangeDatumKind::Maxvalue => s.push_str("MAXVALUE"),
            PartitionRangeDatumKind::Value => {
                let value = prd.value.expect("PartitionRangeDatum value");
                s.push_str(&ruleutils_seams::deparse_partbound_const::call(mcx, value)?);
            }
        }
    }
    s.push(')');
    Ok(s)
}
