//! Port of `backend/statistics/mcv.c` — multivariate MCV (most-common-value)
//! lists (the MCV slice of the combined `backend-statistics-core` unit; sibling
//! of `backend-statistics-dependencies`).
//!
//! SELF-CONTAINED in this crate (100% of the C logic):
//!   * `statext_mcv_serialize` / `statext_mcv_deserialize` — the EXACT C byte
//!     layout (the deduplicated-per-dimension-values format: header fields,
//!     per-dimension `DimensionInfo`, deduplicated value arrays, then the items
//!     with `uint16` indexes / `bool` null flags / two `double` frequencies),
//!     `STATS_MCV_MAGIC` / `STATS_MCV_TYPE_BASIC` and every `elog(ERROR)`
//!     validity check + allocation-safety bound;
//!   * `get_mincount_for_mcv_list` and `mcv_combine_selectivities` (pure
//!     arithmetic);
//!   * the selectivity-summation drivers `mcv_clauselist_selectivity` /
//!     `mcv_clause_selectivity_or` (the match bitmap itself crosses a seam);
//!   * `statext_mcv_load`'s deserialization (the syscache read crosses a seam);
//!   * `pg_mcv_list_in` / `_recv` (text/binary input disallowed) and
//!     `pg_mcv_list_send` (`return byteasend(fcinfo)`; delegates to the ported
//!     `byteasend`).
//!
//! SEAMED to the not-yet-ported owner (`backend-statistics-core`, covering
//! `extended_stats.c` + the multi-sort support + vacuum's `VacAttrStats`), via
//! `backend_statistics_core_seams` (panics until the owner installs them):
//!   * `statext_mcv_build` — needs `build_mss` / `build_sorted_items` /
//!     `build_distinct_groups` / `build_column_frequencies` over the opaque
//!     `StatsBuildData` (mirrors how `dependencies.c`'s `dependency_degree` was
//!     seamed in the dependencies sibling);
//!   * the syscache MCV-bytea read (`mcv_load_bytea`);
//!   * the per-dimension `Datum`<->bytes value codec
//!     (`mcv_value_to_serialized_bytes` / `mcv_serialized_bytes_to_value`), the
//!     type-cache ordering-operator lookup (`mcv_lookup_lt_opr`) and the scalar
//!     comparison (`mcv_compare_scalars_simple`) — the project-wide-deferred
//!     `Datum`-value surface (`store_att_byval` / `fetch_att` / detoast / fmgr);
//!   * the planner-arena clause introspection + per-clause fmgr operator
//!     dispatch (`mcv_get_match_bitmap`) and the `rte->inh` read
//!     (`mcv_rte_inh_for_rel`);
//!   * the SRF (`pg_stats_ext_mcvlist_items`) and `pg_mcv_list_out`
//!     (`byteaout`).
//!
//! See audits/backend-statistics-mcv.md.

// The clause/loop ladders mirror the C `for (i = 0; ...; i++)` control flow 1:1
// (parallel-vector indexing, fallible bodies); collapsing them would obscure the
// correspondence.
#![allow(clippy::needless_range_loop)]
#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]

use core::mem::size_of;

use mcx::{Mcx, PgVec};
use types_core::Oid;
use types_datum::Datum;
use types_error::{PgError, PgResult, ERRCODE_FEATURE_NOT_SUPPORTED};
use types_statistics::{
    DimensionInfo, MCVItem, MCVList, StatsBuildDataHandle, STATS_MAX_DIMENSIONS,
    STATS_MCVLIST_MAX_ITEMS, STATS_MCV_MAGIC, STATS_MCV_TYPE_BASIC,
};

use backend_statistics_core_seams as core_seam;

/* ---------------------------------------------------------------------------
 * byte-layout / size helpers (in-crate; pure, mirroring the C macros).
 * ------------------------------------------------------------------------- */

const MAXIMUM_ALIGNOF: usize = 8;
/// `VARHDRSZ` — varlena 4-byte length header.
const VARHDRSZ: usize = size_of::<u32>();

/// `MAXALIGN(len)` (`c.h`).
#[inline]
const fn MAXALIGN(len: usize) -> usize {
    (len + (MAXIMUM_ALIGNOF - 1)) & !(MAXIMUM_ALIGNOF - 1)
}

/// `sizeof(uint16)`.
const SIZE_U16: usize = 2;
/// `sizeof(uint32)`.
const SIZE_U32: usize = 4;
/// `sizeof(double)`.
const SIZE_F64: usize = 8;
/// `sizeof(bool)`.
const SIZE_BOOL: usize = 1;
/// `sizeof(AttrNumber)` — `int16`.
const SIZE_ATTRNUMBER: usize = 2;
/// `sizeof(Oid)` — `uint32`.
const SIZE_OID: usize = 4;

/// `sizeof(DimensionInfo)` — the C struct `{int nvalues; int nbytes; int
/// nbytes_aligned; int typlen; bool typbyval;}`: four `int`s (16 bytes) + one
/// `bool`, padded to a 4-byte boundary, i.e. 20 bytes. The serializer copies
/// these raw struct bytes (including padding), so the layout must match PG
/// exactly for the serialized bytea to interop with the real catalog.
const SIZEOF_DIMENSION_INFO: usize = 20;

/// `ITEM_SIZE(ndims)` (mcv.c:53) — bytes per serialized MCV item:
/// `ndims * (sizeof(uint16) + sizeof(bool)) + 2 * sizeof(double)`.
#[inline]
const fn item_size(ndims: usize) -> usize {
    ndims * (SIZE_U16 + SIZE_BOOL) + 2 * SIZE_F64
}

/// `MinSizeOfMCVList` (mcv.c:59) — `VARHDRSZ + sizeof(uint32) * 3 +
/// sizeof(AttrNumber)`.
#[inline]
const fn min_size_of_mcvlist() -> usize {
    VARHDRSZ + SIZE_U32 * 3 + SIZE_ATTRNUMBER
}

/// `SizeOfMCVList(ndims, nitems)` (mcv.c:68) — size excluding the deduplicated
/// per-dimension values.
#[inline]
const fn size_of_mcvlist(ndims: usize, nitems: usize) -> usize {
    (min_size_of_mcvlist() + SIZE_OID * ndims)
        + (ndims * SIZEOF_DIMENSION_INFO)
        + (nitems * item_size(ndims))
}

/// `RESULT_MERGE(value, is_or, match)` (mcv.c:88).
#[inline]
const fn result_merge(value: bool, is_or: bool, m: bool) -> bool {
    if is_or {
        value || m
    } else {
        value && m
    }
}

/// `RESULT_IS_FINAL(value, is_or)` (mcv.c:100).
#[inline]
const fn result_is_final(value: bool, is_or: bool) -> bool {
    if is_or {
        value
    } else {
        !value
    }
}

/// `CLAMP_PROBABILITY(p)` (`utils/selfuncs.h`): `if (p < 0) p = 0; else if (p >
/// 1) p = 1;`. Mirrors the macro's exact branch structure (NOT `f64::clamp`,
/// which differs on NaN).
#[inline]
fn clamp_probability(p: f64) -> f64 {
    if p < 0.0 {
        0.0
    } else if p > 1.0 {
        1.0
    } else {
        p
    }
}

// Keep the unused-helper lints away from the merge-macros while they have no
// in-crate caller yet (used by the seamed match-bitmap when the owner lands).
const _: fn(bool, bool, bool) -> bool = result_merge;
const _: fn(bool, bool) -> bool = result_is_final;

/* ---------------------------------------------------------------------------
 * get_mincount_for_mcv_list (mcv.c:147)
 * ------------------------------------------------------------------------- */

/// `get_mincount_for_mcv_list(samplerows, totalrows)` (mcv.c:147) — minimum
/// number of times a value must appear in the sample to be kept.
pub fn get_mincount_for_mcv_list(samplerows: i32, totalrows: f64) -> f64 {
    let n: f64 = samplerows as f64;
    let N: f64 = totalrows;

    let numer = n * (N - n);
    let denom = N - n + 0.04 * n * (N - 1.0);

    /* Guard against division by zero (possible if n = N = 1) */
    if denom == 0.0 {
        return 0.0;
    }

    numer / denom
}

/* ---------------------------------------------------------------------------
 * statext_mcv_build (mcv.c:179) — SEAMED build side.
 * ------------------------------------------------------------------------- */

/// `statext_mcv_build(data, totalrows, stattarget)` (mcv.c:179).
///
/// SEAMED, not in-crate: the build kernel needs `build_mss` /
/// `build_sorted_items` / `build_distinct_groups` / `build_column_frequencies`
/// over the opaque `StatsBuildData` (the `VacAttrStats` matrix and `Datum`/`bool`
/// value matrices in the not-yet-ported extended-stats build framework, plus the
/// multi-sort support). Mirrors how the dependencies sibling seamed
/// `dependency_degree`. Returns `None` when nothing was built (C `NULL`).
pub fn statext_mcv_build(
    data: StatsBuildDataHandle,
    totalrows: f64,
    stattarget: i32,
) -> PgResult<Option<MCVList>> {
    core_seam::statext_mcv_build::call(data, totalrows, stattarget)
}

/* ---------------------------------------------------------------------------
 * statext_mcv_load (mcv.c:557)
 * ------------------------------------------------------------------------- */

/// `statext_mcv_load(mvoid, inh)` (mcv.c:557) — load the MCV list for the
/// indicated `pg_statistic_ext_data` tuple. The syscache read crosses the seam
/// (`mcv_load_bytea`, which `elog`s for a missing object or un-built MCV kind);
/// the deserialization stays in-crate.
pub fn statext_mcv_load<'mcx>(mcx: Mcx<'mcx>, mvoid: Oid, inh: bool) -> PgResult<Option<MCVList>> {
    let data = core_seam::mcv_load_bytea::call(mcx, mvoid, inh)?;
    statext_mcv_deserialize(mcx, Some(&data))
}

/* ---------------------------------------------------------------------------
 * statext_mcv_serialize (mcv.c:620) — EXACT byte layout.
 *
 * Produces the serialized payload as an owned `Vec<u8>` whose bytes match the C
 * `bytea` exactly (the 4-byte varlena header is included, written by
 * `set_varsize`). The per-dimension `(typlen, typbyval, lt_opr, collation)` come
 * from `stats[dim]` (a `VacAttrStats *` in C); the caller supplies them as
 * [`McvDimStats`] so the serialize path does not need the full `VacAttrStats`.
 * ------------------------------------------------------------------------- */

/// Per-dimension info the serializer reads from `stats[dim]` (a `VacAttrStats *`
/// in C: `attrtypid`, `attrcollid`, `attrtype->typlen`, `attrtype->typbyval`).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct McvDimStats {
    pub attrtypid: Oid,
    pub attrcollid: Oid,
    pub typlen: i16,
    pub typbyval: bool,
}

/// `statext_mcv_serialize(mcvlist, stats)` (mcv.c:620).
pub fn statext_mcv_serialize(
    mcx: Mcx<'_>,
    mcvlist: &MCVList,
    stats: &[McvDimStats],
) -> PgResult<Vec<u8>> {
    let ndims = mcvlist.ndimensions as usize;
    let nitems = mcvlist.nitems as usize;

    debug_assert_eq!(stats.len(), ndims);

    /* per-dimension DimensionInfo (palloc0) */
    let mut info: Vec<DimensionInfo> = Vec::new();
    info.try_reserve(ndims)
        .map_err(|_| mcx.oom(ndims * SIZEOF_DIMENSION_INFO))?;
    info.resize(ndims, DimensionInfo::default());

    /*
     * Deduplicated value arrays per dimension, the serialized payload bytes per
     * kept value (so we can both compute nbytes and emit them), and each
     * dimension's `(lt_opr, collation)` for the index bsearch.
     */
    let mut values: Vec<Vec<Datum>> = Vec::new();
    values
        .try_reserve(ndims)
        .map_err(|_| mcx.oom(ndims * size_of::<usize>()))?;
    values.resize(ndims, Vec::new());

    let mut payloads: Vec<Vec<Vec<u8>>> = Vec::new();
    payloads
        .try_reserve(ndims)
        .map_err(|_| mcx.oom(ndims * size_of::<usize>()))?;
    payloads.resize(ndims, Vec::new());

    /* per-dimension (lt_opr, collation); `None` if the dim is all-NULL */
    let mut dim_cmp: Vec<Option<(Oid, Oid)>> = Vec::new();
    dim_cmp
        .try_reserve(ndims)
        .map_err(|_| mcx.oom(ndims * size_of::<usize>()))?;
    dim_cmp.resize(ndims, None);

    /* collect and deduplicate values for each dimension (attribute) */
    let mut dim = 0usize;
    while dim < ndims {
        let st = &stats[dim];

        /* Lookup the LT operator. */
        let lt_opr = core_seam::mcv_lookup_lt_opr::call(st.attrtypid)?;

        /* copy important info about the data type (length, by-value) */
        info[dim].typlen = st.typlen as i32;
        info[dim].typbyval = st.typbyval;

        /* allocate space for values in the attribute and collect them */
        let mut coll: Vec<Datum> = Vec::new();
        coll.try_reserve(nitems)
            .map_err(|_| mcx.oom(nitems * size_of::<Datum>()))?;
        let mut i = 0usize;
        while i < nitems {
            let item = &mcvlist.items[i];
            /* skip NULL values - we don't need to deduplicate those */
            if !item.isnull[dim] {
                coll.push(item.values[dim]);
            }
            i += 1;
        }

        /* if there are just NULL values in this dimension, we're done */
        if coll.is_empty() {
            values[dim] = coll;
            dim += 1;
            continue;
        }

        /* sort and deduplicate the data */
        let collation = st.attrcollid;
        coll.sort_by(|a, b| {
            match core_seam::mcv_compare_scalars_simple::call(*a, *b, lt_opr, collation) {
                n if n < 0 => core::cmp::Ordering::Less,
                0 => core::cmp::Ordering::Equal,
                _ => core::cmp::Ordering::Greater,
            }
        });

        /*
         * Walk through the array and eliminate duplicate values, but keep the
         * ordering (so that we can do a binary search later). We know there's at
         * least one item as (counts[dim] != 0), so we can skip the first element.
         */
        let mut ndistinct = 1usize; /* number of distinct values */
        let mut i = 1usize;
        while i < coll.len() {
            /* expect sorted array */
            debug_assert!(
                core_seam::mcv_compare_scalars_simple::call(coll[i - 1], coll[i], lt_opr, collation)
                    <= 0
            );

            /* if the value is the same as the previous one, we can skip it */
            if core_seam::mcv_compare_scalars_simple::call(coll[i - 1], coll[i], lt_opr, collation)
                == 0
            {
                i += 1;
                continue;
            }

            coll[ndistinct] = coll[i];
            ndistinct += 1;
            i += 1;
        }
        coll.truncate(ndistinct);

        /* we must not exceed PG_UINT16_MAX, as we use uint16 indexes */
        debug_assert!(ndistinct <= u16::MAX as usize);

        info[dim].nvalues = ndistinct as i32;

        /*
         * Store additional info about the attribute - number of deduplicated
         * values, and also size of the serialized data. For fixed-length data
         * types this is trivial to compute, for varwidth types we need to walk
         * the array and sum the sizes.
         */
        let mut dim_payloads: Vec<Vec<u8>> = Vec::new();
        dim_payloads
            .try_reserve(ndistinct)
            .map_err(|_| mcx.oom(ndistinct * size_of::<usize>()))?;

        let mut nbytes = 0i32;
        let mut nbytes_aligned = 0i32;
        let typlen = info[dim].typlen;
        let typbyval = info[dim].typbyval;

        let mut i = 0usize;
        while i < ndistinct {
            let payload =
                core_seam::mcv_value_to_serialized_bytes::call(mcx, coll[i], typlen as i16, typbyval)?;

            if typbyval {
                /* by-value data types: typlen significant bytes */
                debug_assert_eq!(payload.len(), typlen as usize);
                nbytes += typlen;
                /*
                 * We copy the data into the MCV item during deserialization, so
                 * we don't need to allocate any extra space.
                 * info[dim].nbytes_aligned stays 0.
                 */
            } else if typlen > 0 {
                /* fixed-length by-ref */
                debug_assert_eq!(payload.len(), typlen as usize);
                nbytes += typlen;
                nbytes_aligned += MAXALIGN(typlen as usize) as i32;
            } else if typlen == -1 {
                /* varlena: uint32 length + data (no header) */
                let len = payload.len();
                nbytes += SIZE_U32 as i32; /* length */
                nbytes += len as i32; /* value (no header) */
                /* full-header aligned varlena during deserialization */
                nbytes_aligned += MAXALIGN(VARHDRSZ + len) as i32;
            } else if typlen == -2 {
                /* cstring: uint32 length + value (incl. terminator) */
                let len = payload.len();
                nbytes += SIZE_U32 as i32; /* length */
                nbytes += len as i32; /* value */
                nbytes_aligned += MAXALIGN(len) as i32;
            }

            dim_payloads.push(payload.to_vec());
            i += 1;
        }

        info[dim].nbytes = nbytes;
        info[dim].nbytes_aligned = nbytes_aligned;

        /* we know (count>0) so there must be some data */
        debug_assert!(info[dim].nbytes > 0);

        values[dim] = coll;
        payloads[dim] = dim_payloads;
        dim_cmp[dim] = Some((lt_opr, collation));
        dim += 1;
    }

    /*
     * Now we can finally compute how much space we'll actually need for the whole
     * serialized MCV list (varlena header, MCV header, dimension info for each
     * attribute, deduplicated values and items).
     */
    let mut total_length: usize = (3 * SIZE_U32) /* magic + type + nitems */
        + SIZE_ATTRNUMBER /* ndimensions */
        + (ndims * SIZE_OID); /* attribute types */

    /* dimension info */
    total_length += ndims * SIZEOF_DIMENSION_INFO;

    /* add space for the arrays of deduplicated values */
    {
        let mut i = 0usize;
        while i < ndims {
            total_length += info[i].nbytes as usize;
            i += 1;
        }
    }

    /*
     * And finally account for the items (those are fixed-length, thanks to
     * replacing values with uint16 indexes into the deduplicated arrays).
     * C uses ITEM_SIZE(dim), where dim == ndims after the loop above.
     */
    total_length += nitems * item_size(ndims);

    /*
     * Allocate space for the whole serialized MCV list (we'll skip bytes, so we
     * set them to zero to make the result more compressible).
     */
    let mut raw: Vec<u8> = Vec::new();
    raw.try_reserve(VARHDRSZ + total_length)
        .map_err(|_| mcx.oom(VARHDRSZ + total_length))?;
    raw.resize(VARHDRSZ + total_length, 0);
    set_varsize(&mut raw, VARHDRSZ + total_length);

    /*
     * Assemble the data part (everything after the varlena header). The C walks
     * a `ptr` through the pre-zeroed buffer; here we append into the buffer
     * region after the header (the bytes are sized exactly `total_length`).
     */
    let body = &mut raw[VARHDRSZ..];
    let mut pos = 0usize;
    let endptr = total_length;

    /* copy the MCV list header fields, one by one */
    pos = put_bytes(body, pos, &mcvlist.magic.to_ne_bytes());
    pos = put_bytes(body, pos, &mcvlist.r#type.to_ne_bytes());
    pos = put_bytes(body, pos, &mcvlist.nitems.to_ne_bytes());
    pos = put_bytes(body, pos, &mcvlist.ndimensions.to_ne_bytes());

    /* attribute type Oids */
    {
        let mut i = 0usize;
        while i < ndims {
            pos = put_bytes(body, pos, &mcvlist.types[i].to_ne_bytes());
            i += 1;
        }
    }

    /* store information about the attributes (data amounts, ...) */
    {
        let mut i = 0usize;
        while i < ndims {
            pos = put_bytes(body, pos, &dimension_info_to_bytes(&info[i]));
            i += 1;
        }
    }

    /* Copy the deduplicated values for all attributes to the output. */
    {
        let mut dim = 0usize;
        while dim < ndims {
            let start = pos; /* for the Asserts below */

            let typlen = info[dim].typlen;
            let typbyval = info[dim].typbyval;

            let mut i = 0usize;
            while i < info[dim].nvalues as usize {
                let payload = &payloads[dim][i];

                if typbyval || typlen > 0 {
                    /* by-value or fixed-length by-ref: raw bytes */
                    pos = put_bytes(body, pos, payload);
                } else if typlen == -1 || typlen == -2 {
                    /* varlena / cstring: uint32 length then data */
                    let len = payload.len() as u32;
                    pos = put_bytes(body, pos, &len.to_ne_bytes());
                    pos = put_bytes(body, pos, payload);
                }

                /* no underflows or overflows */
                debug_assert!((pos > start) && ((pos - start) <= info[dim].nbytes as usize));
                i += 1;
            }

            /* we should get exactly nbytes of data for this dimension */
            debug_assert_eq!(pos - start, info[dim].nbytes as usize);
            dim += 1;
        }
    }

    /* Serialize the items, with uint16 indexes instead of the values. */
    {
        let mut i = 0usize;
        while i < nitems {
            let mcvitem = &mcvlist.items[i];

            /* don't write beyond the allocated space */
            debug_assert!(pos <= endptr - item_size(ndims));

            /* copy NULL and frequency flags into the serialized MCV */
            {
                let mut d = 0usize;
                while d < ndims {
                    pos = put_bytes(body, pos, &[mcvitem.isnull[d] as u8]);
                    d += 1;
                }
            }

            pos = put_bytes(body, pos, &mcvitem.frequency.to_ne_bytes());
            pos = put_bytes(body, pos, &mcvitem.base_frequency.to_ne_bytes());

            /* store the indexes last */
            {
                let mut d = 0usize;
                while d < ndims {
                    let mut index: u16 = 0;

                    /* do the lookup only for non-NULL values */
                    if !mcvitem.isnull[d] {
                        let (lt_opr, collation) = dim_cmp[d]
                            .expect("non-NULL dimension must have a prepared comparator");
                        match bsearch_index(mcvitem.values[d], &values[d], lt_opr, collation) {
                            Some(k) => {
                                /* check the index is within expected bounds */
                                debug_assert!((k as i32) < info[d].nvalues);
                                index = k as u16;
                            }
                            None => {
                                /* serialization or deduplication error */
                                return Err(PgError::error(
                                    "MCV item value not found in deduplicated array",
                                ));
                            }
                        }
                    }

                    /* copy the index into the serialized MCV */
                    pos = put_bytes(body, pos, &index.to_ne_bytes());
                    d += 1;
                }
            }

            /* make sure we don't overflow the allocated value */
            debug_assert!(pos <= endptr);
            i += 1;
        }
    }

    /* at this point we expect to match the total_length exactly */
    debug_assert_eq!(pos, endptr);

    Ok(raw)
}

/// `bsearch_arg(&value, values[dim], nvalues, compare_scalars_simple, &ssup)`
/// returning the element index within the deduplicated array.
fn bsearch_index(value: Datum, base: &[Datum], lt_opr: Oid, collation: Oid) -> Option<usize> {
    let mut lo: isize = 0;
    let mut hi: isize = base.len() as isize - 1;
    while lo <= hi {
        let mid = lo + (hi - lo) / 2;
        let cmp = core_seam::mcv_compare_scalars_simple::call(value, base[mid as usize], lt_opr, collation);
        if cmp == 0 {
            return Some(mid as usize);
        } else if cmp < 0 {
            hi = mid - 1;
        } else {
            lo = mid + 1;
        }
    }
    None
}

/* ---------------------------------------------------------------------------
 * statext_mcv_deserialize (mcv.c:995) — EXACT byte layout.
 * ------------------------------------------------------------------------- */

/// `statext_mcv_deserialize(data)` (mcv.c:995) — read a serialized MCV list (the
/// `bytea`, modeled as a byte slice including the 4-byte varlena header) into an
/// owned [`MCVList`]. `None` data yields `None` (the C `NULL` return).
pub fn statext_mcv_deserialize(mcx: Mcx<'_>, data: Option<&[u8]>) -> PgResult<Option<MCVList>> {
    let data = match data {
        None => return Ok(None),
        Some(d) => d,
    };

    let varsize = varsize_any(data)?;

    /*
     * We can't possibly deserialize a MCV list if there's not even a complete
     * header. We need an explicit formula here, because we serialize the header
     * fields one by one, so we need to ignore struct alignment.
     */
    if varsize < min_size_of_mcvlist() {
        return Err(PgError::error(format!(
            "invalid MCV size {} (expected at least {})",
            varsize,
            min_size_of_mcvlist()
        )));
    }

    /* pointer to the data part (skip the varlena header) */
    let body = vardata_any(data)?;
    let mut pos = 0usize;

    /* get the header and perform further sanity checks */
    let magic = read_u32(body, &mut pos)?;
    let r#type = read_u32(body, &mut pos)?;
    let nitems_u = read_u32(body, &mut pos)?;
    let ndimensions = read_i16(body, &mut pos)?;

    let mut mcvlist = MCVList {
        magic,
        r#type,
        nitems: nitems_u,
        ndimensions,
        types: [0; STATS_MAX_DIMENSIONS],
        items: Vec::new(),
    };

    if mcvlist.magic != STATS_MCV_MAGIC {
        return Err(PgError::error(format!(
            "invalid MCV magic {} (expected {})",
            mcvlist.magic, STATS_MCV_MAGIC
        )));
    }

    if mcvlist.r#type != STATS_MCV_TYPE_BASIC {
        return Err(PgError::error(format!(
            "invalid MCV type {} (expected {})",
            mcvlist.r#type, STATS_MCV_TYPE_BASIC
        )));
    }

    if mcvlist.ndimensions == 0 {
        return Err(PgError::error(
            "invalid zero-length dimension array in MCVList",
        ));
    } else if (mcvlist.ndimensions as i32 > STATS_MAX_DIMENSIONS as i32) || (mcvlist.ndimensions < 0)
    {
        return Err(PgError::error(format!(
            "invalid length ({}) dimension array in MCVList",
            mcvlist.ndimensions
        )));
    }

    if mcvlist.nitems == 0 {
        return Err(PgError::error("invalid zero-length item array in MCVList"));
    } else if mcvlist.nitems > STATS_MCVLIST_MAX_ITEMS as u32 {
        return Err(PgError::error(format!(
            "invalid length ({}) item array in MCVList",
            mcvlist.nitems
        )));
    }

    let nitems = mcvlist.nitems as usize;
    let ndims = mcvlist.ndimensions as usize;

    /*
     * Check amount of data including DimensionInfo for all dimensions and also
     * the serialized items (including uint16 indexes). Also, walk through the
     * dimension information and add it to the sum.
     */
    let mut expected_size = size_of_mcvlist(ndims, nitems);

    /*
     * Check that we have at least the dimension and info records, along with the
     * items. We don't know the size of the serialized values yet.
     */
    if varsize < expected_size {
        return Err(PgError::error(format!(
            "invalid MCV size {varsize} (expected {expected_size})"
        )));
    }

    /* Now copy the array of type Oids. */
    {
        let mut i = 0usize;
        while i < ndims {
            mcvlist.types[i] = read_u32(body, &mut pos)?;
            i += 1;
        }
    }

    /* Now it's safe to access the dimension info. */
    let mut info: Vec<DimensionInfo> = Vec::new();
    info.try_reserve(ndims)
        .map_err(|_| mcx.oom(ndims * SIZEOF_DIMENSION_INFO))?;
    {
        let mut i = 0usize;
        while i < ndims {
            info.push(dimension_info_from_bytes(body, &mut pos)?);
            i += 1;
        }
    }

    /* account for the value arrays */
    {
        let mut dim = 0usize;
        while dim < ndims {
            debug_assert!(info[dim].nvalues >= 0);
            debug_assert!(info[dim].nbytes >= 0);
            if info[dim].nvalues < 0 || info[dim].nbytes < 0 {
                return Err(PgError::error("invalid negative dimension info in MCVList"));
            }
            expected_size += info[dim].nbytes as usize;
            dim += 1;
        }
    }

    /*
     * Now we know the total expected MCV size, including all the pieces. So do
     * the final check on size.
     */
    if varsize != expected_size {
        return Err(PgError::error(format!(
            "invalid MCV size {varsize} (expected {expected_size})"
        )));
    }

    /*
     * Build mapping (index => value) for translating the serialized data into
     * the in-memory representation. The by-ref data is materialized through the
     * value-reconstruction seam (the C makes a copy in the MCV list's single
     * chunk).
     */
    let mut map: Vec<Vec<Datum>> = Vec::new();
    map.try_reserve(ndims)
        .map_err(|_| mcx.oom(ndims * size_of::<usize>()))?;
    {
        let mut dim = 0usize;
        while dim < ndims {
            let nvalues = info[dim].nvalues as usize;
            let typlen = info[dim].typlen;
            let typbyval = info[dim].typbyval;

            let mut mapdim: Vec<Datum> = Vec::new();
            mapdim
                .try_reserve(nvalues)
                .map_err(|_| mcx.oom(nvalues * size_of::<Datum>()))?;

            let start = pos; /* for the consumed-exactly Assert */

            if typbyval {
                /* for by-val types we simply copy data into the mapping */
                let mut i = 0usize;
                while i < nvalues {
                    let bytes = read_slice(body, &mut pos, typlen as usize)?;
                    let v = core_seam::mcv_serialized_bytes_to_value::call(
                        mcx,
                        bytes,
                        typlen as i16,
                        true,
                    )?;
                    mapdim.push(v);
                    /* no under/overflow of input array */
                    debug_assert!(pos <= start + info[dim].nbytes as usize);
                    i += 1;
                }
            } else if typlen > 0 {
                /* passed by reference, but fixed length (name, tid, ...) */
                let mut i = 0usize;
                while i < nvalues {
                    let bytes = read_slice(body, &mut pos, typlen as usize)?;
                    let v = core_seam::mcv_serialized_bytes_to_value::call(
                        mcx,
                        bytes,
                        typlen as i16,
                        false,
                    )?;
                    mapdim.push(v);
                    i += 1;
                }
            } else if typlen == -1 {
                /* varlena */
                let mut i = 0usize;
                while i < nvalues {
                    /* read the uint32 length */
                    let len = read_u32(body, &mut pos)? as usize;
                    let bytes = read_slice(body, &mut pos, len)?;
                    let v = core_seam::mcv_serialized_bytes_to_value::call(mcx, bytes, -1, false)?;
                    mapdim.push(v);
                    i += 1;
                }
            } else if typlen == -2 {
                /* cstring */
                let mut i = 0usize;
                while i < nvalues {
                    let len = read_u32(body, &mut pos)? as usize;
                    let bytes = read_slice(body, &mut pos, len)?;
                    let v = core_seam::mcv_serialized_bytes_to_value::call(mcx, bytes, -2, false)?;
                    mapdim.push(v);
                    i += 1;
                }
            }

            /* check we consumed input data for this dimension exactly */
            debug_assert_eq!(pos - start, info[dim].nbytes as usize);

            map.push(mapdim);
            dim += 1;
        }
    }

    /* deserialize the MCV items and translate the indexes to Datums */
    mcvlist
        .items
        .try_reserve(nitems)
        .map_err(|_| mcx.oom(nitems * size_of::<MCVItem>()))?;
    {
        let mut i = 0usize;
        while i < nitems {
            let mut isnull: Vec<bool> = Vec::new();
            isnull
                .try_reserve(ndims)
                .map_err(|_| mcx.oom(ndims * SIZE_BOOL))?;
            {
                let mut d = 0usize;
                while d < ndims {
                    isnull.push(read_bool(body, &mut pos)?);
                    d += 1;
                }
            }

            let frequency = read_f64(body, &mut pos)?;
            let base_frequency = read_f64(body, &mut pos)?;

            let mut values: Vec<Datum> = Vec::new();
            values
                .try_reserve(ndims)
                .map_err(|_| mcx.oom(ndims * size_of::<Datum>()))?;
            values.resize(ndims, Datum::null());

            /* finally translate the indexes (for non-NULL only) */
            {
                let mut d = 0usize;
                while d < ndims {
                    let index = read_u16(body, &mut pos)? as usize;

                    if isnull[d] {
                        d += 1;
                        continue;
                    }

                    if index >= map[d].len() {
                        return Err(PgError::error("MCV item index out of range in MCVList"));
                    }
                    values[d] = map[d][index];
                    d += 1;
                }
            }

            /* check we're not overflowing the input */
            debug_assert!(pos <= body.len());

            mcvlist.items.push(MCVItem {
                frequency,
                base_frequency,
                isnull,
                values,
            });
            i += 1;
        }
    }

    /* check that we processed all the data */
    debug_assert_eq!(pos + vardata_offset(data)?, varsize);

    Ok(Some(mcvlist))
}

/* ---------------------------------------------------------------------------
 * pg_stats_ext_mcvlist_items (mcv.c:1337) + pg_mcv_list type I/O
 * ------------------------------------------------------------------------- */

/// `pg_stats_ext_mcvlist_items(fcinfo)` (mcv.c:1337) — the SRF returning the
/// per-item details. SEAMED: pure SRF / fmgr / tupdesc / array-builder /
/// type-output dispatch over the project-wide-deferred `Datum` fmgr surface.
pub fn pg_stats_ext_mcvlist_items(fcinfo_id: u64) -> PgResult<Datum> {
    core_seam::pg_stats_ext_mcvlist_items::call(fcinfo_id)
}

/// The `ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg(...)))`
/// shared by `pg_mcv_list_in` and `pg_mcv_list_recv`
/// (mcv.c:1478-1480 / 1509-1511).
fn cannot_accept_value() -> PgError {
    PgError::error(format!("cannot accept a value of type {}", "pg_mcv_list"))
        .with_sqlstate(ERRCODE_FEATURE_NOT_SUPPORTED)
}

/// `pg_mcv_list_in(fcinfo)` (mcv.c:1471) — text input is disallowed.
pub fn pg_mcv_list_in() -> PgResult<()> {
    Err(cannot_accept_value())
}

/// `pg_mcv_list_out(fcinfo)` (mcv.c:1497) — `return byteaout(fcinfo)`. SEAMED:
/// the `byteaout` fmgr dispatch over the opaque `FunctionCallInfo`.
pub fn pg_mcv_list_out(fcinfo_id: u64) -> PgResult<Datum> {
    core_seam::pg_mcv_list_out::call(fcinfo_id)
}

/// `pg_mcv_list_recv(fcinfo)` (mcv.c:1506) — binary input disallowed.
pub fn pg_mcv_list_recv() -> PgResult<()> {
    Err(cannot_accept_value())
}

/// `pg_mcv_list_send(fcinfo)` (mcv.c:1522) — `return byteasend(fcinfo)`.
///
/// MCV lists are serialized in a bytea value (although the type is named
/// differently), so let's just send that. Delegates to the ported `byteasend`.
pub fn pg_mcv_list_send<'mcx>(mcx: Mcx<'mcx>, v: &[u8]) -> PgResult<PgVec<'mcx, u8>> {
    backend_utils_adt_varlena::bytea::byteasend(mcx, v)
}

/* ---------------------------------------------------------------------------
 * mcv_get_match_bitmap (mcv.c:1598) — SEAMED clause evaluation.
 * ------------------------------------------------------------------------- */

/// `mcv_get_match_bitmap(root, clauses, keys, exprs, mcvlist, is_or)`
/// (mcv.c:1598) — evaluate the clause list against the MCV list and return a
/// per-item match bitmap (`Vec<bool>` of length `mcvlist->nitems`).
///
/// SEAMED: the body walks planner `Node` clauses over the planner arena
/// (`is_opclause` / `examine_opclause_args` / `mcv_match_expression` /
/// `bms_member_index` / `deconstruct_array`) and dispatches the per-clause fmgr
/// operator (`FunctionCall2Coll`) and `DatumGetBool`. None of that node/fmgr
/// surface is ported; `clauses` / `keys` / `exprs` are opaque planner-arena ids.
pub fn mcv_get_match_bitmap(
    root_id: u64,
    clauses_id: u64,
    keys_id: u64,
    exprs_id: u64,
    mcvlist: &MCVList,
    is_or: bool,
) -> PgResult<Vec<bool>> {
    core_seam::mcv_get_match_bitmap::call(root_id, clauses_id, keys_id, exprs_id, mcvlist, is_or)
}

/* ---------------------------------------------------------------------------
 * mcv_combine_selectivities (mcv.c:2005)
 * ------------------------------------------------------------------------- */

/// `mcv_combine_selectivities(simple_sel, mcv_sel, mcv_basesel, mcv_totalsel)`
/// (mcv.c:2005) — combine per-column and multi-column MCV selectivity estimates.
pub fn mcv_combine_selectivities(
    simple_sel: f64,
    mcv_sel: f64,
    mcv_basesel: f64,
    mcv_totalsel: f64,
) -> f64 {
    /* estimated selectivity of values not covered by MCV matches */
    let mut other_sel = simple_sel - mcv_basesel;
    other_sel = clamp_probability(other_sel);

    /* this non-MCV selectivity cannot exceed 1 - mcv_totalsel */
    if other_sel > 1.0 - mcv_totalsel {
        other_sel = 1.0 - mcv_totalsel;
    }

    /* overall selectivity is the sum of the MCV and non-MCV parts */
    let sel = mcv_sel + other_sel;
    clamp_probability(sel)
}

/* ---------------------------------------------------------------------------
 * mcv_clauselist_selectivity (mcv.c:2047)
 * ------------------------------------------------------------------------- */

/// The selectivity outputs of [`mcv_clauselist_selectivity`]: the matching MCV
/// frequency sum (the C return value), plus the C out-parameters `*basesel` and
/// `*totalsel`.
#[derive(Clone, Copy, Debug, Default, PartialEq)]
pub struct ClauseListSelectivity {
    /// the function return: sum of frequencies of matching items.
    pub s: f64,
    /// `*basesel`: sum of base frequencies of matching items.
    pub basesel: f64,
    /// `*totalsel`: sum of frequencies of all items.
    pub totalsel: f64,
}

/// `mcv_clauselist_selectivity(root, stat, clauses, varRelid, jointype, sjinfo,
/// rel, &basesel, &totalsel)` (mcv.c:2047) — estimate the selectivity of an
/// implicitly-ANDed list of clauses using MCV statistics.
///
/// The match bitmap and the `rte->inh` / MCV-load reads cross seams; the
/// frequency summation is in-crate. `root_id`/`rel_id`/`clauses_id`/`keys_id`/
/// `exprs_id` are opaque planner-arena ids; `stat_oid` is `stat->statOid`.
/// `varRelid`/`jointype`/`sjinfo` are accepted to mirror the C signature but, as
/// in C, are unused here.
#[allow(clippy::too_many_arguments)]
pub fn mcv_clauselist_selectivity(
    mcx: Mcx<'_>,
    root_id: u64,
    stat_oid: Oid,
    clauses_id: u64,
    keys_id: u64,
    exprs_id: u64,
    rel_id: u64,
) -> PgResult<ClauseListSelectivity> {
    /* RangeTblEntry *rte = root->simple_rte_array[rel->relid]; rte->inh */
    let inh = core_seam::mcv_rte_inh_for_rel::call(root_id, rel_id)?;

    /* load the MCV list stored in the statistics object */
    let mcv = match statext_mcv_load(mcx, stat_oid, inh)? {
        Some(m) => m,
        None => {
            /*
             * The C dereferences `mcv` unconditionally; a NULL load would crash.
             * Surface it as an error rather than panic.
             */
            return Err(PgError::error("MCV list not built for statistics object"));
        }
    };

    /* build a match bitmap for the clauses */
    let matches = mcv_get_match_bitmap(root_id, clauses_id, keys_id, exprs_id, &mcv, false)?;

    /* sum frequencies for all the matching MCV items */
    let mut out = ClauseListSelectivity::default();
    let mut i = 0usize;
    while i < mcv.nitems as usize {
        let item = &mcv.items[i];
        out.totalsel += item.frequency;

        if matches[i] {
            out.basesel += item.base_frequency;
            out.s += item.frequency;
        }
        i += 1;
    }

    Ok(out)
}

/* ---------------------------------------------------------------------------
 * mcv_clause_selectivity_or (mcv.c:2125)
 * ------------------------------------------------------------------------- */

/// The selectivity outputs of [`mcv_clause_selectivity_or`]: the matching MCV
/// frequency sum (the C return value) plus the C out-parameters.
#[derive(Clone, Copy, Debug, Default, PartialEq)]
pub struct ClauseSelectivityOr {
    /// the function return: sum of frequencies of items matching this clause.
    pub s: f64,
    /// `*basesel`: sum of base frequencies of items matching this clause.
    pub basesel: f64,
    /// `*overlap_mcvsel`: frequency overlap with the running OR bitmap.
    pub overlap_mcvsel: f64,
    /// `*overlap_basesel`: base-frequency overlap with the running OR bitmap.
    pub overlap_basesel: f64,
    /// `*totalsel`: sum of frequencies of all items.
    pub totalsel: f64,
}

/// `mcv_clause_selectivity_or(root, stat, mcv, clause, &or_matches, &basesel,
/// &overlap_mcvsel, &overlap_basesel, &totalsel)` (mcv.c:2125) — estimate the
/// selectivity of a clause appearing in an ORed list, updating the running
/// `or_matches` bitmap.
///
/// `or_matches` is the in/out OR-match bitmap: pass an empty `Vec` on the first
/// call (the C `*or_matches == NULL`) and reuse the (now-`mcv.nitems`-long)
/// vector for subsequent clauses. `clause_id` is the clause node id; the match
/// bitmap (`list_make1(clause)`) crosses the seam.
#[allow(clippy::too_many_arguments)]
pub fn mcv_clause_selectivity_or(
    mcx: Mcx<'_>,
    root_id: u64,
    mcv: &MCVList,
    clause_id: u64,
    keys_id: u64,
    exprs_id: u64,
    or_matches: &mut Vec<bool>,
) -> PgResult<ClauseSelectivityOr> {
    /* build the OR-matches bitmap, if not built already */
    if or_matches.is_empty() {
        or_matches
            .try_reserve(mcv.nitems as usize)
            .map_err(|_| mcx.oom(mcv.nitems as usize * SIZE_BOOL))?;
        or_matches.resize(mcv.nitems as usize, false);
    }

    /* build the match bitmap for the new clause: list_make1(clause) */
    let new_matches = core_seam::mcv_get_match_bitmap::call(
        root_id,
        clause_id, /* the owner builds list_make1(clause) from this single id */
        keys_id,
        exprs_id,
        mcv,
        false,
    )?;

    /*
     * Sum the frequencies for all the MCV items matching this clause and also
     * those matching the overlap between this clause and any of the preceding
     * clauses.
     */
    let mut out = ClauseSelectivityOr::default();
    let mut i = 0usize;
    while i < mcv.nitems as usize {
        let item = &mcv.items[i];
        out.totalsel += item.frequency;

        if new_matches[i] {
            out.s += item.frequency;
            out.basesel += item.base_frequency;

            if or_matches[i] {
                out.overlap_mcvsel += item.frequency;
                out.overlap_basesel += item.base_frequency;
            }
        }

        /* update the OR-matches bitmap for the next clause */
        or_matches[i] = or_matches[i] || new_matches[i];
        i += 1;
    }

    Ok(out)
}

/* ---------------------------------------------------------------------------
 * varlena / byte read+write helpers (in-crate; pure byte layout).
 * ------------------------------------------------------------------------- */

/// Append `bytes` at `pos` into the pre-zeroed body buffer, returning the new
/// position (the C `memcpy(ptr, ...); ptr += n` walk).
#[inline]
fn put_bytes(body: &mut [u8], pos: usize, bytes: &[u8]) -> usize {
    body[pos..pos + bytes.len()].copy_from_slice(bytes);
    pos + bytes.len()
}

/// `SET_VARSIZE(ptr, len)` — write a 4-byte (long-format) varlena header
/// (`len << 2`, low tag bits clear), matching `varatt.h` `SET_VARSIZE_4B`.
fn set_varsize(buf: &mut [u8], len: usize) {
    let word = (len as u32) << 2;
    buf[..VARHDRSZ].copy_from_slice(&word.to_ne_bytes());
}

/// `VARATT_IS_1B(PTR)` — true iff the value has a 1-byte short header
/// (little-endian: low bit of the first header byte set).
fn varatt_is_1b(data: &[u8]) -> bool {
    (data[0] & 0x01) == 0x01
}

/// `VARSIZE_ANY(ptr)` — total varlena size incl. header, branching on the tag.
fn varsize_any(data: &[u8]) -> PgResult<usize> {
    if data.is_empty() {
        return Err(PgError::error("empty MCV varlena"));
    }
    if varatt_is_1b(data) {
        Ok(((data[0] >> 1) & 0x7F) as usize)
    } else {
        if data.len() < 4 {
            return Err(PgError::error("truncated MCV varlena header"));
        }
        let raw = u32::from_ne_bytes([data[0], data[1], data[2], data[3]]);
        Ok(((raw >> 2) & 0x3FFF_FFFF) as usize)
    }
}

/// Byte offset of the data part past the varlena header (1 for short, 4 for
/// long), matching `VARDATA_ANY`.
fn vardata_offset(data: &[u8]) -> PgResult<usize> {
    if data.is_empty() {
        return Err(PgError::error("empty MCV varlena"));
    }
    Ok(if varatt_is_1b(data) { 1 } else { VARHDRSZ })
}

/// `VARDATA_ANY(ptr)` — the data part, skipping the (short or long) header.
fn vardata_any(data: &[u8]) -> PgResult<&[u8]> {
    let off = vardata_offset(data)?;
    Ok(&data[off..])
}

fn need(body: &[u8], pos: usize, n: usize) -> PgResult<()> {
    if pos + n > body.len() {
        return Err(PgError::error("MCV deserialization overran the buffer"));
    }
    Ok(())
}

fn read_u32(body: &[u8], pos: &mut usize) -> PgResult<u32> {
    need(body, *pos, 4)?;
    let v = u32::from_ne_bytes([body[*pos], body[*pos + 1], body[*pos + 2], body[*pos + 3]]);
    *pos += 4;
    Ok(v)
}

fn read_u16(body: &[u8], pos: &mut usize) -> PgResult<u16> {
    need(body, *pos, 2)?;
    let v = u16::from_ne_bytes([body[*pos], body[*pos + 1]]);
    *pos += 2;
    Ok(v)
}

fn read_i16(body: &[u8], pos: &mut usize) -> PgResult<i16> {
    need(body, *pos, 2)?;
    let v = i16::from_ne_bytes([body[*pos], body[*pos + 1]]);
    *pos += 2;
    Ok(v)
}

fn read_f64(body: &[u8], pos: &mut usize) -> PgResult<f64> {
    need(body, *pos, 8)?;
    let mut b = [0u8; 8];
    b.copy_from_slice(&body[*pos..*pos + 8]);
    *pos += 8;
    Ok(f64::from_ne_bytes(b))
}

fn read_bool(body: &[u8], pos: &mut usize) -> PgResult<bool> {
    need(body, *pos, 1)?;
    let v = body[*pos] != 0;
    *pos += 1;
    Ok(v)
}

fn read_slice<'a>(body: &'a [u8], pos: &mut usize, n: usize) -> PgResult<&'a [u8]> {
    need(body, *pos, n)?;
    let s = &body[*pos..*pos + n];
    *pos += n;
    Ok(s)
}

/// `DimensionInfo` → its raw 20-byte serialized form (matching the C struct
/// layout: `i32 nvalues, i32 nbytes, i32 nbytes_aligned, i32 typlen, bool
/// typbyval, 3 padding bytes`).
fn dimension_info_to_bytes(info: &DimensionInfo) -> [u8; SIZEOF_DIMENSION_INFO] {
    let mut b = [0u8; SIZEOF_DIMENSION_INFO];
    b[0..4].copy_from_slice(&info.nvalues.to_ne_bytes());
    b[4..8].copy_from_slice(&info.nbytes.to_ne_bytes());
    b[8..12].copy_from_slice(&info.nbytes_aligned.to_ne_bytes());
    b[12..16].copy_from_slice(&info.typlen.to_ne_bytes());
    b[16] = info.typbyval as u8;
    /* bytes 17..20 are padding, left zero */
    b
}

/// Parse a `DimensionInfo` from its raw 20-byte serialized form.
fn dimension_info_from_bytes(body: &[u8], pos: &mut usize) -> PgResult<DimensionInfo> {
    let s = read_slice(body, pos, SIZEOF_DIMENSION_INFO)?;
    Ok(DimensionInfo {
        nvalues: i32::from_ne_bytes([s[0], s[1], s[2], s[3]]),
        nbytes: i32::from_ne_bytes([s[4], s[5], s[6], s[7]]),
        nbytes_aligned: i32::from_ne_bytes([s[8], s[9], s[10], s[11]]),
        typlen: i32::from_ne_bytes([s[12], s[13], s[14], s[15]]),
        typbyval: s[16] != 0,
    })
}

/// This crate installs NO inward seams — its public functions are called only by
/// the (unported) `backend-statistics-core` dispatcher and the fmgr catalog,
/// neither of which is in-repo yet. Present so the aggregator can invoke it
/// uniformly; the recurrence guard only requires wiring for crates that actually
/// install a seam.
pub fn init_seams() {}

#[cfg(test)]
mod tests;
