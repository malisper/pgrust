//! Idiomatic port of `src/backend/access/brin/brin_bloom.c` (PostgreSQL 18.3).
//!
//! The Bloom BRIN operator class: it summarizes each page range into a bloom
//! filter built on type-specific hashes of the indexed values, using the
//! Kirsch/Mitzenmacher two-function `(h1 + i*h2)` scheme. It supports only
//! equality (like hash indexes) and bitmap scans. Four support procedures plus
//! the per-attribute hash-procinfo cache and the on-disk summary codec:
//!
//!   * [`brin_bloom_opcinfo`]     (brin_bloom.c:447)
//!   * [`brin_bloom_add_value`]   (brin_bloom.c:537)
//!   * [`brin_bloom_consistent`]  (brin_bloom.c:592)
//!   * [`brin_bloom_union`]       (brin_bloom.c:664)
//!   * [`bloom_get_procinfo`]     (brin_bloom.c:715)
//!   * [`brin_bloom_options`]     (brin_bloom.c:745)
//!   * the summary type I/O (`brin_bloom_summary_{in,out,recv,send}`)
//!
//! These are reached by the BRIN AM (`brin.c`, unported) through the
//! `backend-access-brin-entry-seams` opclass-dispatch seams; the single
//! installer of those seams lives in `backend-access-brin-minmax`, which
//! dispatches the built-in `brin_bloom_*` support-procedure OIDs into the public
//! bodies here. This is the BRIN F0-opclass S3-bloom stage; it reuses the
//! [`types_brin::OpaqueOpcInfo`] carrier (its [`types_brin::BloomOpaque`]
//! variant).
//!
//! ## Carrier and fmgr-dispatch
//!
//! C's `BloomOpaque { FmgrInfo extra_procinfos[BLOOM_MAX_PROCNUMS]; }` lives in
//! the `palloc0`'d tail of the `BrinOpcInfo` (`oi_opaque`, a `void *`). The repo
//! models `oi_opaque` as the typed enum [`types_brin::OpaqueOpcInfo`]; bloom's
//! variant is [`types_brin::BloomOpaque`], whose cached `FmgrInfo` is reduced to
//! the resolved function's `Oid` (the repo's fmgr-call seams re-resolve by OID).
//! The AM dispatches the support procs through a `&BrinDesc` (immutable), so the
//! cache slot is a `Cell` and fills lazily through the shared reference, matching
//! C's mutation through `bdesc->bd_info[]->oi_opaque`.
//!
//! ## The summary store
//!
//! C stores the bloom filter as a `bytea` varlena in `column->bv_values[0]`
//! (`PointerGetDatum(filter)`). The repo carries `bv_values` as the canonical
//! [`Datum`] lane; the filter rides the by-reference arm as its verbatim on-disk
//! byte image, produced by [`BloomFilter::serialize`] and parsed by
//! [`BloomFilter::deserialize`] (reproducing the exact `#[repr(C)]` `BloomFilter`
//! layout). `PG_DETOAST_DATUM` is a no-op on the already-detoasted by-reference
//! bytes (BRIN never TOASTs its own tuples; varlena compression is handled by the
//! page codec, not here).
//!
//! ## Trim-gap defaults (BRIN reloptions / opclass options)
//!
//! `brin_bloom_get_ndistinct` reads `BrinGetPagesPerRange(bdesc->bd_index)` and
//! `brin_bloom_add_value` reads `PG_GET_OPCLASS_OPTIONS()` (a `BloomOptions`).
//! Neither is reachable yet: the relcache trims `rd_options` to
//! [`types_rel::StdRdOptions`] (no BRIN `pagesPerRange`) and carries no
//! `rd_opcoptions` accessor, and the `brin_addvalue` seam does not thread the
//! per-attribute opclass options. As the already-landed
//! `brin-insert-vacuum::brin_get_auto_summarize` does for `autosummarize`, the
//! behaviour-preserving value here is the C default
//! ([`BRIN_DEFAULT_PAGES_PER_RANGE`] and `opts = None`, i.e. the bloom defaults).
//! When the BRIN reloptions / opclass-options carrier lands in the relcache trim
//! and the `brin_addvalue` seam threads it, this reads it instead.

#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]

extern crate alloc;

use alloc::format;
use alloc::string::{String, ToString};
use alloc::vec;

use mcx::{vec_with_capacity_in, Mcx, PgBox, PgVec};
use types_brin::{BloomOpaque, BrinDesc, BrinOpcInfo, BrinValues, OpaqueOpcInfo, BLOOM_MAX_PROCNUMS};
use types_core::primitive::{AttrNumber, BlockNumber, InvalidBlockNumber, Oid};
use types_error::error::{
    ERRCODE_FEATURE_NOT_SUPPORTED, ERRCODE_INTERNAL_ERROR, ERRCODE_INVALID_OBJECT_DEFINITION, ERROR,
};
use types_error::PgResult;
use types_scan::scankey::{ScanKeyData, SK_ISNULL};
use types_storage::bufpage::SizeOfPageHeaderData;
use types_tuple::backend_access_common_heaptuple::Datum;

use backend_utils_error::ereport;

use backend_access_index_indexam_seams as indexam;
use backend_utils_cache_typcache_seams as typcache;
use backend_utils_fmgr_fmgr_seams as fmgr;
use common_hashfn_seams as hashfn;

// ---------------------------------------------------------------------------
// brin_bloom.c constants.
// ---------------------------------------------------------------------------

/// `BloomEqualStrategyNumber` (brin_bloom.c:132).
const BLOOM_EQUAL_STRATEGY_NUMBER: u16 = 1;

/// `PROCNUM_HASH` (brin_bloom.c:142): required.
const PROCNUM_HASH: u16 = 11;
/// `PROCNUM_BASE` (brin_bloom.c:148): subtract from procnum to index the
/// `BloomOpaque` arrays (== minimum of the private procnums).
const PROCNUM_BASE: u16 = 11;

/// `BLOOM_MIN_NDISTINCT_PER_RANGE` (brin_bloom.c:168).
const BLOOM_MIN_NDISTINCT_PER_RANGE: f64 = 16.0;
/// `BLOOM_DEFAULT_NDISTINCT_PER_RANGE` (brin_bloom.c:175): 10% of values.
const BLOOM_DEFAULT_NDISTINCT_PER_RANGE: f64 = -0.1;
/// `BLOOM_MIN_FALSE_POSITIVE_RATE` (brin_bloom.c:191): 0.01% fp rate.
const BLOOM_MIN_FALSE_POSITIVE_RATE: f64 = 0.0001;
/// `BLOOM_MAX_FALSE_POSITIVE_RATE` (brin_bloom.c:192): 25% fp rate.
const BLOOM_MAX_FALSE_POSITIVE_RATE: f64 = 0.25;
/// `BLOOM_DEFAULT_FALSE_POSITIVE_RATE` (brin_bloom.c:193): 1% fp rate.
const BLOOM_DEFAULT_FALSE_POSITIVE_RATE: f64 = 0.01;

/// `BLOOM_SEED_1` (brin_bloom.c:221).
const BLOOM_SEED_1: u32 = 0x71d924af;
/// `BLOOM_SEED_2` (brin_bloom.c:222).
const BLOOM_SEED_2: u32 = 0xba48b314;

/// `PG_BRIN_BLOOM_SUMMARYOID` (pg_type.dat): the summary type stored in
/// `oi_typcache[0]` (brin_bloom.c:465).
const PG_BRIN_BLOOM_SUMMARYOID: Oid = 4600;

/// `BRIN_DEFAULT_PAGES_PER_RANGE` (brin.h): the default page range size, used as
/// the behaviour-preserving default while the relcache trim carries no BRIN
/// `pagesPerRange` reloption (see the module-level trim-gap note).
const BRIN_DEFAULT_PAGES_PER_RANGE: BlockNumber = 128;

/// `MaxHeapTuplesPerPage` (htup_details.h) as an `i32` for the sizing math.
const MAX_HEAP_TUPLES_PER_PAGE: i32 = types_storage::bufpage::MaxHeapTuplesPerPage as i32;

/// `InvalidOid` (postgres_ext.h).
const INVALID_OID: Oid = 0;

// ---------------------------------------------------------------------------
// Built-in `brin_bloom_*` support-procedure OIDs (pg_proc.dat).
// ---------------------------------------------------------------------------

/// `brin_bloom_opcinfo` (pg_proc.dat oid 4591).
pub const F_BRIN_BLOOM_OPCINFO: Oid = 4591;
/// `brin_bloom_add_value` (pg_proc.dat oid 4592).
pub const F_BRIN_BLOOM_ADD_VALUE: Oid = 4592;
/// `brin_bloom_consistent` (pg_proc.dat oid 4593).
pub const F_BRIN_BLOOM_CONSISTENT: Oid = 4593;
/// `brin_bloom_union` (pg_proc.dat oid 4594).
pub const F_BRIN_BLOOM_UNION: Oid = 4594;

// ---------------------------------------------------------------------------
// MAXALIGN helpers (c.h) and BloomMaxFilterSize (brin_bloom.c:204).
// ---------------------------------------------------------------------------

/// `MAXIMUM_ALIGNOF` (8 on supported platforms).
const MAXIMUM_ALIGNOF: usize = 8;

/// `MAXALIGN(LEN)`.
#[inline]
const fn maxalign(len: usize) -> usize {
    (len + (MAXIMUM_ALIGNOF - 1)) & !(MAXIMUM_ALIGNOF - 1)
}

/// `MAXALIGN_DOWN(LEN)`.
#[inline]
const fn maxalign_down(len: usize) -> usize {
    len & !(MAXIMUM_ALIGNOF - 1)
}

/// `BloomMaxFilterSize` (brin_bloom.c:204): the largest bloom we can fit onto a
/// page (estimate). `MAXALIGN_DOWN(BLCKSZ - (MAXALIGN(SizeOfPageHeaderData +
/// sizeof(ItemIdData)) + MAXALIGN(sizeof(BrinSpecialSpace)) + SizeOfBrinTuple))`.
const fn bloom_max_filter_size() -> usize {
    // `sizeof(ItemIdData)` on the target ABI: 4 bytes.
    const SIZEOF_ITEM_ID_DATA: usize = 4;
    // `sizeof(BrinSpecialSpace)` == MAXALIGN(1) == 8 (brin_page.h).
    const SIZEOF_BRIN_SPECIAL_SPACE: usize = 8;
    maxalign_down(
        types_core::primitive::BLCKSZ
            - (maxalign(SizeOfPageHeaderData + SIZEOF_ITEM_ID_DATA)
                + maxalign(SIZEOF_BRIN_SPECIAL_SPACE)
                + types_brin::SIZE_OF_BRIN_TUPLE),
    )
}

// ---------------------------------------------------------------------------
// BloomFilter (brin_bloom.c:241) and its on-disk varlena codec.
// ---------------------------------------------------------------------------

/// `offsetof(BloomFilter, data)`: the header is the 4-byte varlena length, then
/// `flags`(u16) + `nhashes`(u8) + 1 pad + `nbits`(u32) + `nbits_set`(u32). The
/// `char data[]` follows at offset 16.
const BLOOM_FILTER_DATA_OFFSET: usize = 16;

/// `BloomFilter` (brin_bloom.c:241): a bloom filter built on `uint32` hashes.
///
/// In C this is a flexible-array varlena: `{ int32 vl_len_; uint16 flags; uint8
/// nhashes; uint32 nbits; uint32 nbits_set; char data[]; }`. Here it is an owned
/// value; the on-disk byte image (with the varlena header) is produced by
/// [`BloomFilter::serialize`] and parsed by [`BloomFilter::deserialize`],
/// reproducing the exact layout above.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BloomFilter {
    /// `flags` (unused for now).
    pub flags: u16,
    /// `nhashes`: number of hash functions.
    pub nhashes: u8,
    /// `nbits`: number of bits in the bitmap.
    pub nbits: u32,
    /// `nbits_set`: number of bits set to 1.
    pub nbits_set: u32,
    /// `data`: the bitmap bytes (`nbits / 8` long).
    pub data: alloc::vec::Vec<u8>,
}

/// `bloom_filter_size` (brin_bloom.c:269): calculate the optimal bloom filter
/// parameters (`nbytes`, `nbits`, `nhashes`) for `ndistinct` distinct values and
/// the desired `false_positive_rate`.
fn bloom_filter_size(ndistinct: i32, false_positive_rate: f64) -> (i32, i32, i32) {
    // sizing bloom filter: -(n * ln(p)) / (ln(2))^2
    let mut nbits =
        (-((ndistinct as f64) * false_positive_rate.ln()) / (2.0f64.ln().powf(2.0))).ceil() as i32;

    // round m to whole bytes
    let nbytes = (nbits + 7) / 8;
    nbits = nbytes * 8;

    // round(log(2.0) * m / ndistinct), round() may not be available on Windows
    let mut k = 2.0f64.ln() * (nbits as f64) / (ndistinct as f64);
    k = if k - k.floor() >= 0.5 {
        k.ceil()
    } else {
        k.floor()
    };

    (nbytes, nbits, k as i32)
}

impl BloomFilter {
    /// `bloom_init` (brin_bloom.c:308): initialize the bloom filter with optimal
    /// size for `ndistinct` expected values at `false_positive_rate`. Raises the
    /// `"the bloom filter is too large"` `elog(ERROR)` when too big for a page.
    fn init(ndistinct: i32, false_positive_rate: f64) -> PgResult<BloomFilter> {
        // Assert(ndistinct > 0);
        debug_assert!(ndistinct > 0);
        // Assert(false_positive_rate > 0 && false_positive_rate < 1);
        debug_assert!(false_positive_rate > 0.0 && false_positive_rate < 1.0);

        // calculate bloom filter size / parameters
        let (nbytes, nbits, nhashes) = bloom_filter_size(ndistinct, false_positive_rate);

        // Reject filters that are obviously too large to store on a page.
        if (nbytes as usize) > bloom_max_filter_size() {
            // elog(ERROR, "the bloom filter is too large (%d > %zu)", nbytes, BloomMaxFilterSize);
            return Err(ereport(ERROR)
                .errcode(ERRCODE_INTERNAL_ERROR)
                .errmsg_internal(format!(
                    "the bloom filter is too large ({} > {})",
                    nbytes,
                    bloom_max_filter_size()
                ))
                .into_error());
        }

        // palloc0(offsetof(BloomFilter, data) + nbytes): a zeroed bitmap.
        Ok(BloomFilter {
            flags: 0,
            nhashes: nhashes as u8,
            nbits: nbits as u32,
            nbits_set: 0,
            data: vec![0u8; nbytes as usize],
        })
    }

    /// `bloom_add_value` (brin_bloom.c:368): add `value` (a `uint32` hash) to the
    /// filter; sets `updated` true if any new bit was set.
    fn add_value(&mut self, value: u32, updated: &mut bool) {
        // compute the two hashes, used for the bloom filter
        let h1 =
            hashfn::hash_bytes_uint32_extended::call(value, BLOOM_SEED_1 as u64) % (self.nbits as u64);
        let h2 =
            hashfn::hash_bytes_uint32_extended::call(value, BLOOM_SEED_2 as u64) % (self.nbits as u64);

        // compute the requested number of hashes
        for i in 0..(self.nhashes as u64) {
            // h1 + i * h2
            let h = (h1.wrapping_add(i.wrapping_mul(h2)) % (self.nbits as u64)) as u32;
            let byte = (h / 8) as usize;
            let bit = h % 8;

            // if the bit is not set, set it and remember we did that
            if (self.data[byte] & (0x01 << bit)) == 0 {
                self.data[byte] |= 0x01 << bit;
                self.nbits_set += 1;
                *updated = true;
            }
        }
    }

    /// `bloom_contains_value` (brin_bloom.c:405): check if the filter contains
    /// `value` (a `uint32` hash).
    fn contains_value(&self, value: u32) -> bool {
        let h1 =
            hashfn::hash_bytes_uint32_extended::call(value, BLOOM_SEED_1 as u64) % (self.nbits as u64);
        let h2 =
            hashfn::hash_bytes_uint32_extended::call(value, BLOOM_SEED_2 as u64) % (self.nbits as u64);

        for i in 0..(self.nhashes as u64) {
            let h = (h1.wrapping_add(i.wrapping_mul(h2)) % (self.nbits as u64)) as u32;
            let byte = (h / 8) as usize;
            let bit = h % 8;

            // if the bit is not set, the value is not there
            if (self.data[byte] & (0x01 << bit)) == 0 {
                return false;
            }
        }

        // all hashes found in bloom filter
        true
    }

    /// Serialize to the on-disk byte image (with the 4-byte varlena header),
    /// matching the C `#[repr(C)]` `BloomFilter` layout. This is the
    /// `PointerGetDatum(filter)` value an opclass stores in `bv_values[0]`.
    fn serialize(&self) -> alloc::vec::Vec<u8> {
        let len = BLOOM_FILTER_DATA_OFFSET + self.data.len();
        let mut buf = vec![0u8; len];
        // SET_VARSIZE: the 4-byte length header (full 4B header form).
        buf[0..4].copy_from_slice(&(len as u32).to_ne_bytes());
        buf[4..6].copy_from_slice(&self.flags.to_ne_bytes());
        buf[6] = self.nhashes;
        // byte 7 is padding before the u32 fields.
        buf[8..12].copy_from_slice(&self.nbits.to_ne_bytes());
        buf[12..16].copy_from_slice(&self.nbits_set.to_ne_bytes());
        buf[BLOOM_FILTER_DATA_OFFSET..].copy_from_slice(&self.data);
        buf
    }

    /// Parse a detoasted on-disk byte image (`PG_DETOAST_DATUM`, *with* the
    /// 4-byte length header) into a [`BloomFilter`].
    fn deserialize(bytes: &[u8]) -> BloomFilter {
        debug_assert!(bytes.len() >= BLOOM_FILTER_DATA_OFFSET);
        let flags = u16::from_ne_bytes([bytes[4], bytes[5]]);
        let nhashes = bytes[6];
        let nbits = u32::from_ne_bytes([bytes[8], bytes[9], bytes[10], bytes[11]]);
        let nbits_set = u32::from_ne_bytes([bytes[12], bytes[13], bytes[14], bytes[15]]);
        let data = bytes[BLOOM_FILTER_DATA_OFFSET..].to_vec();
        BloomFilter {
            flags,
            nhashes,
            nbits,
            nbits_set,
            data,
        }
    }
}

/// `BloomOptions` (brin_bloom.c:153): the BRIN reloptions for the bloom opclass.
///
/// Reached through `PG_GET_OPCLASS_OPTIONS()` in C; `None` is the C NULL `opts`
/// (no opclass options set) — see the module-level trim-gap note.
#[derive(Clone, Copy, Debug, Default)]
pub struct BloomOptions {
    /// `nDistinctPerRange`: number of distinct values per range (0 == unset).
    pub n_distinct_per_range: f64,
    /// `falsePositiveRate`: false positive rate (0.0 == unset).
    pub false_positive_rate: f64,
}

/// `BloomGetNDistinctPerRange(opts)` (brin_bloom.c:195).
fn bloom_get_ndistinct_per_range(opts: Option<&BloomOptions>) -> f64 {
    match opts {
        Some(o) if o.n_distinct_per_range != 0.0 => o.n_distinct_per_range,
        _ => BLOOM_DEFAULT_NDISTINCT_PER_RANGE,
    }
}

/// `BloomGetFalsePositiveRate(opts)` (brin_bloom.c:200).
fn bloom_get_false_positive_rate(opts: Option<&BloomOptions>) -> f64 {
    match opts {
        Some(o) if o.false_positive_rate != 0.0 => o.false_positive_rate,
        _ => BLOOM_DEFAULT_FALSE_POSITIVE_RATE,
    }
}

// ---------------------------------------------------------------------------
// fmgr / carrier helpers.
// ---------------------------------------------------------------------------

/// `DatumGetUInt32(FunctionCall1Coll(hashFn, colloid, value))` reduced to the
/// repo's by-OID call seam. `function_id` is the cached hash-procedure OID.
fn call_hash(function_id: Oid, colloid: Oid, value: &Datum) -> PgResult<u32> {
    let r = fmgr::function_call1_coll::call(
        function_id,
        colloid,
        types_datum::Datum::from_usize(value.as_usize()),
    )?;
    Ok(r.as_u32())
}

/// Borrow the [`BloomOpaque`] cache out of `bdesc.bd_info[attno - 1].oi_opaque`.
/// Created by [`brin_bloom_opcinfo`]; any other shape is a caller/dispatch bug.
fn bloom_opaque<'a, 'mcx>(bdesc: &'a BrinDesc<'mcx>, attno: AttrNumber) -> &'a BloomOpaque {
    match bdesc.bd_info[(attno - 1) as usize].oi_opaque.as_ref() {
        Some(OpaqueOpcInfo::Bloom(o)) => o,
        _ => panic!("brin_bloom: oi_opaque is not a BloomOpaque cache"),
    }
}

// ===========================================================================
// brin_bloom_opcinfo (brin_bloom.c:447)
// ===========================================================================

/// `brin_bloom_opcinfo` (brin_bloom.c:447): build the [`BrinOpcInfo`] for the
/// bloom opclass — one stored `BYTEA` column (the serialized filter), regular
/// NULL handling, and a fresh (`palloc0`-zeroed) [`BloomOpaque`]. The stored
/// column's type-cache slot is `lookup_type_cache(PG_BRIN_BLOOM_SUMMARYOID, 0)`.
///
/// The `typoid` of the indexed column is unused by C here (bloom always stores a
/// single bloom-summary column regardless of the indexed type).
pub fn brin_bloom_opcinfo<'mcx>(
    mcx: Mcx<'mcx>,
    _typoid: Oid,
) -> PgResult<PgBox<'mcx, BrinOpcInfo<'mcx>>> {
    // result = palloc0(MAXALIGN(SizeofBrinOpcInfo(1)) + sizeof(BloomOpaque));
    // result->oi_nstored = 1;
    // result->oi_regular_nulls = true;
    // result->oi_opaque = (BloomOpaque *) MAXALIGN(...);  -- palloc0-zeroed.
    // result->oi_typcache[0] = lookup_type_cache(PG_BRIN_BLOOM_SUMMARYOID, 0);
    let tce = typcache::lookup_type_cache::call(PG_BRIN_BLOOM_SUMMARYOID, 0)?;
    let mut oi_typcache: PgVec<'mcx, _> = vec_with_capacity_in(mcx, 1)?;
    oi_typcache.push(tce);

    mcx::alloc_in(
        mcx,
        BrinOpcInfo {
            oi_nstored: 1,
            oi_regular_nulls: true,
            oi_opaque: Some(OpaqueOpcInfo::Bloom(BloomOpaque::default())),
            oi_typcache,
        },
    )
}

// ===========================================================================
// brin_bloom_get_ndistinct (brin_bloom.c:494)
// ===========================================================================

/// `brin_bloom_get_ndistinct` (brin_bloom.c:494): determine the ndistinct value
/// used to size the bloom filter, given the index's `pagesPerRange` reloption.
///
/// `pages_per_range` is `BrinGetPagesPerRange(bdesc->bd_index)`; until the BRIN
/// reloptions carrier lands in the relcache trim the caller passes
/// [`BRIN_DEFAULT_PAGES_PER_RANGE`] (the behaviour-preserving default — see the
/// module-level trim-gap note).
fn brin_bloom_get_ndistinct(pages_per_range: BlockNumber, opts: Option<&BloomOptions>) -> i32 {
    let mut ndistinct = bloom_get_ndistinct_per_range(opts);

    // Assert(BlockNumberIsValid(pagesPerRange));
    debug_assert!(pages_per_range != InvalidBlockNumber);

    let maxtuples = (MAX_HEAP_TUPLES_PER_PAGE as f64) * (pages_per_range as f64);

    // Negative values are relative to maxtuples.
    if ndistinct < 0.0 {
        ndistinct = (-ndistinct) * maxtuples;
    }

    // Apply safeties: not unreasonably small, not larger than maxtuples.
    ndistinct = ndistinct.max(BLOOM_MIN_NDISTINCT_PER_RANGE);
    ndistinct = ndistinct.min(maxtuples);

    ndistinct as i32
}

// ===========================================================================
// brin_bloom_add_value (brin_bloom.c:537)
// ===========================================================================

/// `brin_bloom_add_value` (brin_bloom.c:537): examine the column summary by
/// hashing `newval` (with the opclass hash function) and adding the hash to the
/// column's bloom filter; if the first non-null value, initialize the filter.
/// Returns whether the summary changed.
///
/// `_isnull` is the C `PG_GETARG_DATUM(3)` (`PG_USED_FOR_ASSERTS_ONLY`). `opts`
/// is `PG_GET_OPCLASS_OPTIONS()` and `pages_per_range` is
/// `BrinGetPagesPerRange(bdesc->bd_index)`; see the trim-gap note for the
/// behaviour-preserving defaults the dispatcher passes.
#[allow(clippy::too_many_arguments)]
pub fn brin_bloom_add_value<'mcx>(
    mcx: Mcx<'mcx>,
    bdesc: &BrinDesc<'mcx>,
    column: &mut BrinValues<'mcx>,
    newval: &Datum<'mcx>,
    _isnull: bool,
    colloid: Oid,
    pages_per_range: BlockNumber,
    opts: Option<&BloomOptions>,
) -> PgResult<bool> {
    // Assert(!isnull);
    debug_assert!(!_isnull);

    let attno = column.bv_attno;
    let mut updated = false;

    // If this is the first non-null value, initialize the bloom filter.
    // Otherwise extract the existing one from BrinValues.
    let mut filter = if column.bv_allnulls {
        let f = BloomFilter::init(
            brin_bloom_get_ndistinct(pages_per_range, opts),
            bloom_get_false_positive_rate(opts),
        )?;
        column.bv_allnulls = false;
        updated = true;
        f
    } else {
        // filter = (BloomFilter *) PG_DETOAST_DATUM(column->bv_values[0]);
        BloomFilter::deserialize(column.bv_values[0].as_ref_bytes())
    };

    // Compute the hash of the new value with the supplied hash function and add
    // the hash to the bloom filter.
    let hash_fn = bloom_get_procinfo(bdesc, attno, PROCNUM_HASH)?;
    let hash_value = call_hash(hash_fn, colloid, newval)?;

    filter.add_value(hash_value, &mut updated);

    // column->bv_values[0] = PointerGetDatum(filter): re-serialize and store.
    column.bv_values[0] = Datum::ByRef(mcx::slice_in(mcx, &filter.serialize())?);

    Ok(updated)
}

// ===========================================================================
// brin_bloom_consistent (brin_bloom.c:592)
// ===========================================================================

/// `brin_bloom_consistent` (brin_bloom.c:592): given a column summary and the
/// scan keys, return whether all keys are consistent with the bloom filter. Uses
/// the multi-key signature (`PG_NARGS() == 4`); stops on the first eliminating
/// key. `colloid` is `PG_GET_COLLATION()`.
pub fn brin_bloom_consistent<'mcx>(
    bdesc: &BrinDesc<'mcx>,
    column: &BrinValues<'mcx>,
    keys: &[ScanKeyData<'mcx>],
    colloid: Oid,
) -> PgResult<bool> {
    // filter = (BloomFilter *) PG_DETOAST_DATUM(column->bv_values[0]);
    let filter = BloomFilter::deserialize(column.bv_values[0].as_ref_bytes());

    // Assume all scan keys match; search for a key eliminating the range.
    let mut matches = true;

    for key in keys.iter() {
        // NULL keys are handled and filtered-out in bringetbitmap.
        debug_assert!((key.sk_flags & SK_ISNULL) == 0);

        let attno = key.sk_attno;
        let value = &key.sk_argument;

        match key.sk_strategy {
            BLOOM_EQUAL_STRATEGY_NUMBER => {
                // Return the range if the bloom filter seems to contain the value.
                let finfo = bloom_get_procinfo(bdesc, attno, PROCNUM_HASH)?;
                let hash_value = call_hash(finfo, colloid, value)?;
                matches &= filter.contains_value(hash_value);
            }
            // shouldn't happen
            other => {
                // elog(ERROR, "invalid strategy number %d", key->sk_strategy);
                return Err(ereport(ERROR)
                    .errcode(ERRCODE_INTERNAL_ERROR)
                    .errmsg_internal(format!("invalid strategy number {}", other as i32))
                    .into_error());
            }
        }

        if !matches {
            break;
        }
    }

    Ok(matches)
}

// ===========================================================================
// brin_bloom_union (brin_bloom.c:664)
// ===========================================================================

/// `brin_bloom_union` (brin_bloom.c:664): update `col_a` so it becomes the union
/// of the bloom filters in `col_a` and `col_b` (OR the bitmaps); `col_b` is
/// untouched. The filters are assumed to use the same parameters.
pub fn brin_bloom_union<'mcx>(
    mcx: Mcx<'mcx>,
    col_a: &mut BrinValues<'mcx>,
    col_b: &BrinValues<'mcx>,
) -> PgResult<()> {
    // Assert(col_a->bv_attno == col_b->bv_attno);
    debug_assert_eq!(col_a.bv_attno, col_b.bv_attno);
    // Assert(!col_a->bv_allnulls && !col_b->bv_allnulls);
    debug_assert!(!col_a.bv_allnulls && !col_b.bv_allnulls);

    let mut filter_a = BloomFilter::deserialize(col_a.bv_values[0].as_ref_bytes());
    let filter_b = BloomFilter::deserialize(col_b.bv_values[0].as_ref_bytes());

    // make sure the filters use the same parameters
    debug_assert_eq!(filter_a.nbits, filter_b.nbits);
    debug_assert_eq!(filter_a.nhashes, filter_b.nhashes);
    debug_assert!(filter_a.nbits > 0 && filter_a.nbits % 8 == 0);

    let nbytes = (filter_a.nbits / 8) as usize;

    // simply OR the bitmaps
    for i in 0..nbytes {
        filter_a.data[i] |= filter_b.data[i];
    }

    // update the number of bits set in the filter (pg_popcount)
    filter_a.nbits_set = filter_a.data[..nbytes].iter().map(|b| b.count_ones()).sum();

    // store the updated summary back into col_a
    col_a.bv_values[0] = Datum::ByRef(mcx::slice_in(mcx, &filter_a.serialize())?);

    Ok(())
}

// ===========================================================================
// bloom_get_procinfo (brin_bloom.c:715)
// ===========================================================================

/// `bloom_get_procinfo(bdesc, attno, procnum)` (brin_bloom.c:715): cache and
/// return the bloom support-procedure OID for the given support-function number.
/// Raises the `"invalid opclass definition"` `ereport(ERROR)` if missing.
///
/// The cached `FmgrInfo` is reduced to the resolved function's `Oid`. The
/// `RegProcedureIsValid(index_getprocid(...))` test + `fmgr_info_copy` /
/// `index_getprocinfo` resolution collapses to the indexam `index_getprocid`
/// (no-error missing test) + `index_getprocinfo` (the OID we cache).
fn bloom_get_procinfo(bdesc: &BrinDesc<'_>, attno: AttrNumber, procnum: u16) -> PgResult<Oid> {
    let opaque = bloom_opaque(bdesc, attno);
    let basenum = (procnum - PROCNUM_BASE) as usize;
    debug_assert!(basenum < BLOOM_MAX_PROCNUMS);

    if opaque.extra_procinfos[basenum].get() == INVALID_OID {
        // if (RegProcedureIsValid(index_getprocid(bdesc->bd_index, attno, procnum)))
        //     fmgr_info_copy(&opaque->extra_procinfos[basenum],
        //                    index_getprocinfo(bdesc->bd_index, attno, procnum), ...);
        let procid = indexam::index_getprocid::call(&bdesc.bd_index, attno, procnum)?;
        if procid != INVALID_OID {
            // index_getprocinfo resolves + caches the FmgrInfo; we keep its OID.
            let finfo = indexam::index_getprocinfo::call(&bdesc.bd_index, attno, procnum)?;
            opaque.extra_procinfos[basenum].set(finfo.fn_oid);
        } else {
            // ereport(ERROR, errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
            //   errmsg_internal("invalid opclass definition"),
            //   errdetail_internal("The operator class is missing support
            //                       function %d for column %d.", procnum, attno));
            return Err(ereport(ERROR)
                .errcode(ERRCODE_INVALID_OBJECT_DEFINITION)
                .errmsg_internal("invalid opclass definition".to_string())
                .errdetail_internal(format!(
                    "The operator class is missing support function {} for column {}.",
                    procnum, attno
                ))
                .into_error());
        }
    }

    Ok(opaque.extra_procinfos[basenum].get())
}

// ===========================================================================
// brin_bloom_options (brin_bloom.c:745) + summary type I/O.
// ===========================================================================

/// `brin_bloom_options` (brin_bloom.c:745): the default-filled [`BloomOptions`].
///
/// In C this registers the two real reloptions (`n_distinct_per_range`,
/// `false_positive_rate`) on the `local_relopts` via
/// `init_local_reloptions`/`add_local_real_reloption`; the bounds reproduced
/// here are `BLOOM_DEFAULT_NDISTINCT_PER_RANGE` (range `-1.0 .. INT_MAX`) and
/// `BLOOM_DEFAULT_FALSE_POSITIVE_RATE` (range `BLOOM_MIN_.. BLOOM_MAX_`).
pub fn brin_bloom_options() -> BloomOptions {
    // add_local_real_reloption(relopts, "n_distinct_per_range", ...,
    //   BLOOM_DEFAULT_NDISTINCT_PER_RANGE, -1.0, INT_MAX, ...);
    // add_local_real_reloption(relopts, "false_positive_rate", ...,
    //   BLOOM_DEFAULT_FALSE_POSITIVE_RATE,
    //   BLOOM_MIN_FALSE_POSITIVE_RATE, BLOOM_MAX_FALSE_POSITIVE_RATE, ...);
    let _ = (BLOOM_MIN_FALSE_POSITIVE_RATE, BLOOM_MAX_FALSE_POSITIVE_RATE);
    BloomOptions {
        n_distinct_per_range: BLOOM_DEFAULT_NDISTINCT_PER_RANGE,
        false_positive_rate: BLOOM_DEFAULT_FALSE_POSITIVE_RATE,
    }
}

/// `brin_bloom_summary_in` (brin_bloom.c:775): input is disallowed.
pub fn brin_bloom_summary_in() -> PgResult<()> {
    Err(cannot_accept_value())
}

/// `brin_bloom_summary_out` (brin_bloom.c:797): a human-readable description of
/// the bloom summary. `summary` is the detoasted on-disk byte image.
pub fn brin_bloom_summary_out(summary: &[u8]) -> String {
    // detoast the data to get value with a full 4B header
    let filter = BloomFilter::deserialize(summary);
    format!(
        "{{mode: hashed  nhashes: {}  nbits: {}  nbits_set: {}}}",
        filter.nhashes, filter.nbits, filter.nbits_set
    )
}

/// `brin_bloom_summary_recv` (brin_bloom.c:821): binary input is disallowed.
pub fn brin_bloom_summary_recv() -> PgResult<()> {
    Err(cannot_accept_value())
}

/// `brin_bloom_summary_send` (brin_bloom.c:838): `byteasend(fcinfo)` — the
/// summary is serialized in a bytea value, so send it directly.
pub fn brin_bloom_summary_send(summary: alloc::vec::Vec<u8>) -> alloc::vec::Vec<u8> {
    summary
}

/// `ereport(ERROR, errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("cannot accept
/// a value of type %s", "pg_brin_bloom_summary"))` (brin_bloom.c:782 / :824).
fn cannot_accept_value() -> types_error::PgError {
    ereport(ERROR)
        .errcode(ERRCODE_FEATURE_NOT_SUPPORTED)
        .errmsg(format!(
            "cannot accept a value of type {}",
            "pg_brin_bloom_summary"
        ))
        .into_error()
}

/// `BRIN_DEFAULT_PAGES_PER_RANGE` re-exported for the dispatcher (the
/// behaviour-preserving default while the trim carries no BRIN reloption).
pub const PAGES_PER_RANGE_DEFAULT: BlockNumber = BRIN_DEFAULT_PAGES_PER_RANGE;

#[cfg(test)]
mod tests;
