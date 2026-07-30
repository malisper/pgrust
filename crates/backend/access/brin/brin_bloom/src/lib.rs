//! brin_bloom.c: Bloom opclass for BRIN.
#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]
#![allow(clippy::too_many_arguments)]

use ::datum::Datum;
use ::fmgr::FmgrInfo;
use ::hashfn::hash_bytes_uint32_extended;
use ::mcx::Mcx;
use ::types_brin::{
    BloomOpaque, BrinColInfo, BrinDesc, BrinOpcKind, BrinValues, MinmaxOpaque,
    PG_BRIN_BLOOM_SUMMARYOID, SizeOfBrinTuple,
};
use ::types_core::Oid;
use ::types_error::{PgError, PgResult, ERRCODE_INVALID_OBJECT_DEFINITION};
use ::types_scan::scankey::{ScanKeyData, SK_ISNULL};
use ::types_storage::bufpage::MaxHeapTuplesPerPage;
use ::types_tuple::varatt::{set_varsize_4b_word, varatt_is_1b, varatt_is_1b_e, varsize_any};

mod builtins;
pub use builtins::BLOOM_BUILTINS;

const BloomEqualStrategyNumber: u16 = 1;
const PROCNUM_HASH: u16 = 11;

const BLOOM_MIN_NDISTINCT_PER_RANGE: f64 = 16.0;
pub(crate) const BLOOM_DEFAULT_NDISTINCT_PER_RANGE: f64 = -0.1;
pub(crate) const BLOOM_MIN_FALSE_POSITIVE_RATE: f64 = 0.0001;
pub(crate) const BLOOM_MAX_FALSE_POSITIVE_RATE: f64 = 0.25;
pub(crate) const BLOOM_DEFAULT_FALSE_POSITIVE_RATE: f64 = 0.01;

const BLOOM_SEED_1: u64 = 0x71d924af;
const BLOOM_SEED_2: u64 = 0xba48b314;

// BloomFilter varlena layout (byte offsets; C pads nhashes..nbits by 1):
// vl_len_ u32 | flags u16 | nhashes u8 | pad | nbits u32 | nbits_set u32 | data.
const OFF_NHASHES: usize = 6;
const OFF_NBITS: usize = 8;
pub(crate) const OFF_NBITS_SET: usize = 12;
const OFF_DATA: usize = 16;

// BloomMaxFilterSize: MAXALIGN_DOWN(BLCKSZ - (MAXALIGN(SizeOfPageHeaderData +
// sizeof(ItemIdData)) + MAXALIGN(sizeof(BrinSpecialSpace)) + SizeOfBrinTuple)).
const BLOOM_MAX_FILTER_SIZE: usize = (8192 - (32 + 8 + SizeOfBrinTuple)) & !7;
const _: () = assert!(BLOOM_MAX_FILTER_SIZE == 8144);

pub fn brin_bloom_opcinfo(_typoid: Oid) -> BrinColInfo {
    BrinColInfo {
        oi_opclass_options: None,
        oi_nstored: 1,
        oi_regular_nulls: true,
        kind: BrinOpcKind::Bloom,
        oi_typids: [PG_BRIN_BLOOM_SUMMARYOID, 0, 0],
        minmax: MinmaxOpaque::default(),
        distance_procinfo: core::cell::RefCell::new(None),
        bloom: Some(Box::new(BloomOpaque::default())),
        inclusion: None,
    }
}

fn bloom_filter_size(ndistinct: i32, false_positive_rate: f64) -> (usize, u32, u8) {
    let mut nbits =
        (-(ndistinct as f64 * false_positive_rate.ln()) / (2.0f64.ln().powi(2))).ceil() as i64;
    let nbytes = ((nbits + 7) / 8) as usize;
    nbits = (nbytes * 8) as i64;
    let k = 2.0f64.ln() * nbits as f64 / ndistinct as f64;
    let k = if k - k.floor() >= 0.5 { k.ceil() } else { k.floor() };
    (nbytes, nbits as u32, k as u8)
}

fn bloom_init<'mcx>(mcx: Mcx<'mcx>, ndistinct: i32, false_positive_rate: f64) -> PgResult<Datum> {
    debug_assert!(ndistinct > 0);
    debug_assert!(false_positive_rate > 0.0 && false_positive_rate < 1.0);
    let (nbytes, nbits, nhashes) = bloom_filter_size(ndistinct, false_positive_rate);

    // Reachable via large pages_per_range reloptions; C elog(ERROR).
    if nbytes > BLOOM_MAX_FILTER_SIZE {
        return Err(Box::new(PgError::error(format!(
            "the bloom filter is too large ({nbytes} > {BLOOM_MAX_FILTER_SIZE})"
        ))));
    }

    let len = OFF_DATA + nbytes;
    let mut v: mcx::PgVec<'mcx, u8> = mcx::vec_with_capacity_in(mcx, len)?;
    v.resize(len, 0);
    let buf = v.leak();
    buf[0..4].copy_from_slice(&set_varsize_4b_word(len as u32).to_ne_bytes());
    buf[OFF_NHASHES] = nhashes;
    buf[OFF_NBITS..OFF_NBITS + 4].copy_from_slice(&nbits.to_ne_bytes());
    Ok(Datum::from_usize(buf.as_ptr() as usize))
}

/// PG_DETOAST_DATUM: the stored filter may carry a short header or be
/// compressed (TOAST_INDEX_HACK); bit ops need the flat 4B image. Returns the
/// (possibly fresh) datum; mutation happens through it in place, as in C.
fn detoast_filter(mcx: Mcx<'_>, value: Datum) -> PgResult<Datum> {
    let p = value.as_usize() as *const u8;
    // SAFETY: stored filter datum is a live varlena image.
    unsafe {
        if varatt_is_1b(p) || varatt_is_1b_e(p) || varatt_is_compressed(p) {
            let image = core::slice::from_raw_parts(p, varsize_any(p));
            let flat = detoast::detoast_attr(mcx, image)?.leak();
            Ok(Datum::from_usize(flat.as_ptr() as usize))
        } else {
            Ok(value)
        }
    }
}

// VARATT_IS_COMPRESSED on a 4B varlena word.
// SAFETY: p live, at least 1 readable byte.
unsafe fn varatt_is_compressed(p: *const u8) -> bool {
    if cfg!(target_endian = "little") {
        (*p & 0x03) == 0x02
    } else {
        (*p & 0xC0) == 0x40
    }
}

// SAFETY: filter datum is a live flat BloomFilter varlena.
unsafe fn filter_bytes_mut<'a>(filter: Datum) -> &'a mut [u8] {
    let p = filter.as_usize() as *mut u8;
    core::slice::from_raw_parts_mut(p, varsize_any(p))
}

fn read_u32(b: &[u8], off: usize) -> u32 {
    u32::from_ne_bytes(b[off..off + 4].try_into().unwrap())
}

fn bloom_add_value(filter: &mut [u8], value: u32) -> bool {
    let nbits = read_u32(filter, OFF_NBITS) as u64;
    let nhashes = filter[OFF_NHASHES] as u32;
    let h1 = hash_bytes_uint32_extended(value, BLOOM_SEED_1) % nbits;
    let h2 = hash_bytes_uint32_extended(value, BLOOM_SEED_2) % nbits;
    let mut updated = false;
    let mut nbits_set = read_u32(filter, OFF_NBITS_SET);
    for i in 0..nhashes {
        let h = ((h1 + i as u64 * h2) % nbits) as u32;
        let (byte, bit) = ((h / 8) as usize, h % 8);
        if filter[OFF_DATA + byte] & (1 << bit) == 0 {
            filter[OFF_DATA + byte] |= 1 << bit;
            nbits_set += 1;
            updated = true;
        }
    }
    filter[OFF_NBITS_SET..OFF_NBITS_SET + 4].copy_from_slice(&nbits_set.to_ne_bytes());
    updated
}

fn bloom_contains_value(filter: &[u8], value: u32) -> bool {
    let nbits = read_u32(filter, OFF_NBITS) as u64;
    let nhashes = filter[OFF_NHASHES] as u32;
    let h1 = hash_bytes_uint32_extended(value, BLOOM_SEED_1) % nbits;
    let h2 = hash_bytes_uint32_extended(value, BLOOM_SEED_2) % nbits;
    for i in 0..nhashes {
        let h = ((h1 + i as u64 * h2) % nbits) as u32;
        let (byte, bit) = ((h / 8) as usize, h % 8);
        if filter[OFF_DATA + byte] & (1 << bit) == 0 {
            return false;
        }
    }
    true
}

// BloomOptions image: vl_len_ u32 | pad | f64 nDistinctPerRange @8 |
// f64 falsePositiveRate @16 (size 24).
pub(crate) const BLOOMOPTIONS_SIZE: usize = 24;
pub(crate) const BLOOMOPTIONS_NDISTINCT_OFF: usize = 8;
pub(crate) const BLOOMOPTIONS_FPR_OFF: usize = 16;

fn opts_f64(opts: Option<&[u8]>, off: usize) -> Option<f64> {
    let img = opts?;
    Some(f64::from_ne_bytes(img[off..off + 8].try_into().unwrap()))
}

// BloomGetNDistinctPerRange: option value 0 falls back to the default.
fn bloom_get_ndistinct_per_range(opts: Option<&[u8]>) -> f64 {
    match opts_f64(opts, BLOOMOPTIONS_NDISTINCT_OFF) {
        Some(v) if v != 0.0 => v,
        _ => BLOOM_DEFAULT_NDISTINCT_PER_RANGE,
    }
}

// BloomGetFalsePositiveRate: option value 0 falls back to the default.
fn bloom_get_false_positive_rate(opts: Option<&[u8]>) -> f64 {
    match opts_f64(opts, BLOOMOPTIONS_FPR_OFF) {
        Some(v) if v != 0.0 => v,
        _ => BLOOM_DEFAULT_FALSE_POSITIVE_RATE,
    }
}

/// brin_bloom_get_ndistinct.
fn brin_bloom_get_ndistinct(bdesc: &BrinDesc<'_>, opts: Option<&[u8]>) -> i32 {
    let pages_per_range = bdesc.bd_pages_per_range;
    let mut ndistinct = bloom_get_ndistinct_per_range(opts);
    let maxtuples = (MaxHeapTuplesPerPage as f64) * pages_per_range as f64;
    if ndistinct < 0.0 {
        ndistinct = -ndistinct * maxtuples;
    }
    ndistinct = ndistinct.max(BLOOM_MIN_NDISTINCT_PER_RANGE);
    ndistinct = ndistinct.min(maxtuples);
    ndistinct as i32
}

pub fn brin_bloom_add_value(
    mcx: Mcx<'_>,
    bdesc: &BrinDesc<'_>,
    column: &mut BrinValues,
    newval: Datum,
    isnull: bool,
    colloid: Oid,
) -> PgResult<bool> {
    debug_assert!(!isnull);
    let attno = column.bv_attno;

    let opts = bdesc.bd_info[attno as usize - 1].oi_opclass_options.as_deref();
    let mut updated = false;
    if column.bv_allnulls {
        column.bv_values[0] = bloom_init(
            mcx,
            brin_bloom_get_ndistinct(bdesc, opts),
            bloom_get_false_positive_rate(opts),
        )?;
        column.bv_allnulls = false;
        updated = true;
    } else {
        column.bv_values[0] = detoast_filter(mcx, column.bv_values[0])?;
    }

    let mut hash_fn = bloom_get_procinfo(bdesc, attno)?;
    let hash_value =
        fmgr_core::function_call1_coll_in(&mut hash_fn, colloid, mcx, newval)?.as_u32();

    // SAFETY: bv_values[0] is the flat filter produced above.
    let filter = unsafe { filter_bytes_mut(column.bv_values[0]) };
    updated |= bloom_add_value(filter, hash_value);

    Ok(updated)
}

pub fn brin_bloom_consistent(
    mcx: Mcx<'_>,
    bdesc: &BrinDesc<'_>,
    column: &BrinValues,
    key: &ScanKeyData,
) -> PgResult<bool> {
    debug_assert!(!column.bv_allnulls);
    debug_assert!(key.sk_flags & SK_ISNULL == 0);
    let filter_datum = detoast_filter(mcx, column.bv_values[0])?;
    // SAFETY: flat filter image per detoast_filter.
    let filter: &[u8] = unsafe { filter_bytes_mut(filter_datum) };

    match key.sk_strategy {
        BloomEqualStrategyNumber => {
            let mut hash_fn = bloom_get_procinfo(bdesc, key.sk_attno as u16)?;
            let hash_value = fmgr_core::function_call1_coll_in(
                &mut hash_fn,
                key.sk_collation,
                mcx,
                key.sk_argument,
            )?
            .as_u32();
            Ok(bloom_contains_value(filter, hash_value))
        }
        other => panic!("invalid strategy number {other}"),
    }
}

pub fn brin_bloom_union(
    mcx: Mcx<'_>,
    _bdesc: &BrinDesc<'_>,
    _colloid: Oid,
    col_a: &mut BrinValues,
    col_b: &BrinValues,
) -> PgResult<()> {
    debug_assert!(col_a.bv_attno == col_b.bv_attno);
    debug_assert!(!col_a.bv_allnulls && !col_b.bv_allnulls);

    col_a.bv_values[0] = detoast_filter(mcx, col_a.bv_values[0])?;
    let filter_b_datum = detoast_filter(mcx, col_b.bv_values[0])?;

    // SAFETY: both are flat filter images per detoast_filter; a != b.
    let filter_a = unsafe { filter_bytes_mut(col_a.bv_values[0]) };
    let filter_b: &[u8] = unsafe { filter_bytes_mut(filter_b_datum) };

    debug_assert!(read_u32(filter_a, OFF_NBITS) == read_u32(filter_b, OFF_NBITS));
    debug_assert!(filter_a[OFF_NHASHES] == filter_b[OFF_NHASHES]);
    let nbits = read_u32(filter_a, OFF_NBITS);
    debug_assert!(nbits > 0 && nbits % 8 == 0);
    let nbytes = (nbits / 8) as usize;

    let mut nbits_set = 0u32;
    for i in 0..nbytes {
        filter_a[OFF_DATA + i] |= filter_b[OFF_DATA + i];
        nbits_set += filter_a[OFF_DATA + i].count_ones();
    }
    filter_a[OFF_NBITS_SET..OFF_NBITS_SET + 4].copy_from_slice(&nbits_set.to_ne_bytes());
    Ok(())
}

fn bloom_get_procinfo(bdesc: &BrinDesc<'_>, attno: u16) -> PgResult<FmgrInfo> {
    let opaque = bdesc.bd_info[attno as usize - 1]
        .bloom
        .as_ref()
        .expect("bloom column");
    if let Some(fi) = opaque.hash_procinfo.borrow().as_ref() {
        return Ok(fi.clone());
    }
    let opfamily = bdesc.bd_opfamily[attno as usize - 1];
    let opcintype = bdesc.bd_opcintype[attno as usize - 1];
    let proc =
        lsyscache::get_opfamily_proc(opfamily, opcintype, opcintype, PROCNUM_HASH as i16)?;
    if proc == 0 {
        return Err(invalid_opclass(PROCNUM_HASH, attno));
    }
    let finfo = fmgr_core::fmgr_info(proc)?;
    *opaque.hash_procinfo.borrow_mut() = Some(finfo.clone());
    Ok(finfo)
}

#[track_caller]
#[cold]
#[inline(never)]
fn invalid_opclass(procnum: u16, attno: u16) -> Box<PgError> {
    Box::new(
        PgError::error("invalid opclass definition")
            .with_sqlstate(ERRCODE_INVALID_OBJECT_DEFINITION)
            .with_detail(format!(
                "The operator class is missing support function {procnum} for column {attno}."
            )),
    )
}

#[cfg(test)]
mod tests;
