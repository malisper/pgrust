use datum::Datum;
use mcx::{Mcx, PgVec};
use types_core::Oid;
use types_error::{PgError, PgResult};

use crate::sortitem::{pg_qsort, ItemStore, MultiSort, SortItem};
use crate::{build_mss, build_sorted_items, ColStats, StatsBuildData, STATS_MAX_DIMENSIONS};

pub const STATS_MCV_MAGIC: u32 = 0xE1A651C2;
pub const STATS_MCV_TYPE_BASIC: u32 = 1;
pub const STATS_MCVLIST_MAX_ITEMS: usize = 10000;

pub struct MCVItem<'mcx> {
    pub values: PgVec<'mcx, Datum>,
    pub isnull: PgVec<'mcx, bool>,
    pub frequency: f64,
    pub base_frequency: f64,
}

pub struct MCVList<'mcx> {
    pub ndimensions: usize,
    pub types: [Oid; STATS_MAX_DIMENSIONS],
    pub items: PgVec<'mcx, MCVItem<'mcx>>,
}

fn get_mincount_for_mcv_list(samplerows: usize, totalrows: f64) -> f64 {
    let n = samplerows as f64;
    let nn = totalrows;
    let numer = n * (nn - n);
    let denom = nn - n + 0.04 * n * (nn - 1.0);
    if denom == 0.0 {
        return 0.0;
    }
    numer / denom
}

pub fn statext_mcv_build<'mcx>(
    mcx: Mcx<'mcx>,
    data: &StatsBuildData<'mcx>,
    totalrows: f64,
    stattarget: i32,
) -> PgResult<Option<MCVList<'mcx>>> {
    let numattrs = data.attnums.len();
    let numrows = data.numrows;
    let dims: PgVec<'_, usize> = {
        let mut v = mcx::vec_with_capacity_in(mcx, numattrs)?;
        for i in 0..numattrs {
            v.push(i);
        }
        v
    };
    let mut mss = build_mss(&data.stats, &dims)?;

    let Some((items, store)) = build_sorted_items(mcx, data, &mut mss, &dims)? else {
        return Ok(None);
    };

    let (mut groups, ngroups) = build_distinct_groups(mcx, &items, &store, &mut mss)?;

    let mut nitems = stattarget as usize;
    if nitems > ngroups {
        nitems = ngroups;
    }
    let mincount = get_mincount_for_mcv_list(numrows, totalrows);
    for i in 0..nitems {
        if (groups[i].count as f64) < mincount {
            nitems = i;
            break;
        }
    }
    if nitems == 0 {
        return Ok(None);
    }

    // Per-column frequencies over the distinct groups (for base_frequency).
    let mut freqs: PgVec<'_, PgVec<'_, SortItem>> = PgVec::new_in(mcx);
    for dim in 0..numattrs {
        let mut f: PgVec<'_, SortItem> = mcx::vec_with_capacity_in(mcx, ngroups)?;
        f.extend_from_slice(&groups[..ngroups]);
        {
            let store_ref = &store;
            let mss_ref = &mut mss;
            pg_qsort(&mut f, |a, b| {
                let (av, an) = store_ref.value(*a, dim);
                let (bv, bn) = store_ref.value(*b, dim);
                mss_ref.compare_dim(dim, av, an, bv, bn)
            });
        }
        let mut sorted: PgVec<'_, SortItem> = mcx::vec_with_capacity_in(mcx, ngroups)?;
        sorted.extend_from_slice(&f);
        let mut ndistinct = 1usize;
        for i in 1..ngroups {
            let (av, an) = store.value(sorted[i - 1], dim);
            let (bv, bn) = store.value(sorted[i], dim);
            if mss.compare_dim(dim, av, an, bv, bn) == 0 {
                f[ndistinct - 1].count += sorted[i].count;
                continue;
            }
            f[ndistinct] = sorted[i];
            ndistinct += 1;
        }
        f.truncate(ndistinct);
        freqs.push(f);
    }

    let mut mcv_items: PgVec<'mcx, MCVItem<'mcx>> = PgVec::new_in(mcx);
    for i in 0..nitems {
        let mut values: PgVec<'mcx, Datum> = mcx::vec_with_capacity_in(mcx, numattrs)?;
        let mut isnull: PgVec<'mcx, bool> = mcx::vec_with_capacity_in(mcx, numattrs)?;
        let mut base_frequency = 1.0f64;
        for dim in 0..numattrs {
            let (v, n) = store.value(groups[i], dim);
            values.push(v);
            isnull.push(n);
            let f = &freqs[dim];
            let idx = bsearch_dim(f, &store, &mut mss, dim, v, n);
            base_frequency *= f[idx].count as f64 / numrows as f64;
        }
        mcv_items.push(MCVItem {
            values,
            isnull,
            frequency: groups[i].count as f64 / numrows as f64,
            base_frequency,
        });
    }

    let mut types = [0 as Oid; STATS_MAX_DIMENSIONS];
    for i in 0..numattrs {
        types[i] = data.stats[i].attrtypid;
    }
    groups.clear();
    Ok(Some(MCVList { ndimensions: numattrs, types, items: mcv_items }))
}

fn bsearch_dim(
    f: &[SortItem],
    store: &ItemStore<'_>,
    mss: &mut MultiSort,
    dim: usize,
    v: Datum,
    isnull: bool,
) -> usize {
    let mut lo = 0usize;
    let mut hi = f.len();
    while lo < hi {
        let mid = (lo + hi) / 2;
        let (mv, mn) = store.value(f[mid], dim);
        let c = mss.compare_dim(dim, v, isnull, mv, mn);
        match c.cmp(&0) {
            core::cmp::Ordering::Equal => return mid,
            core::cmp::Ordering::Greater => lo = mid + 1,
            core::cmp::Ordering::Less => hi = mid,
        }
    }
    panic!("statext_mcv_build: group value not found in column frequencies");
}

fn build_distinct_groups<'mcx>(
    mcx: Mcx<'mcx>,
    items: &[SortItem],
    store: &ItemStore<'_>,
    mss: &mut MultiSort,
) -> PgResult<(PgVec<'mcx, SortItem>, usize)> {
    let numrows = items.len();
    let mut groups: PgVec<'mcx, SortItem> = PgVec::new_in(mcx);
    groups.push(SortItem { off: items[0].off, count: 1 });
    let mut j = 0usize;
    for i in 1..numrows {
        if store.compare(mss, items[i], items[i - 1]) != 0 {
            groups.push(SortItem { off: items[i].off, count: 0 });
            j += 1;
        }
        groups[j].count += 1;
    }
    let ngroups = groups.len();
    // compare_sort_item_count: descending by count, C-exact qsort tie order.
    pg_qsort(&mut groups, |a, b| {
        if a.count == b.count {
            0
        } else if a.count > b.count {
            -1
        } else {
            1
        }
    });
    Ok((groups, ngroups))
}

// DimensionInfo (extended_stats_internal.h): 5 fields, C layout, 20 bytes
// with 3 zero padding bytes.
struct DimensionInfo {
    nvalues: i32,
    nbytes: i32,
    nbytes_aligned: i32,
    typlen: i32,
    typbyval: bool,
}

const MAXALIGN: usize = 8;
fn maxalign(x: usize) -> usize {
    (x + MAXALIGN - 1) & !(MAXALIGN - 1)
}

fn varsize_any_exhdr(p: *const u8) -> usize {
    // SAFETY: plain inline varlena (build detoasted external/compressed).
    unsafe {
        let b0 = *p;
        if b0 & 0x01 != 0 {
            (((b0 as usize) >> 1) & 0x7F) - 1
        } else {
            ((u32::from_ne_bytes(*(p as *const [u8; 4])) >> 2) as usize) - 4
        }
    }
}

fn vardata_any(p: *const u8) -> *const u8 {
    // SAFETY: as varsize_any_exhdr.
    unsafe {
        if *p & 0x01 != 0 {
            p.add(1)
        } else {
            p.add(4)
        }
    }
}

pub fn statext_mcv_serialize<'mcx>(
    mcx: Mcx<'mcx>,
    mcvlist: &MCVList<'_>,
    stats: &[ColStats],
) -> PgResult<PgVec<'mcx, u8>> {
    let ndims = mcvlist.ndimensions;
    let nitems = mcvlist.items.len();

    let mut values: PgVec<'_, PgVec<'_, Datum>> = PgVec::new_in(mcx);
    let mut info: PgVec<'_, DimensionInfo> = mcx::vec_with_capacity_in(mcx, ndims)?;
    let mut cmps: Vec<MultiSort> = Vec::with_capacity(ndims);

    for dim in 0..ndims {
        let mut mss = MultiSort::init(1);
        mss.add_dimension(stats[dim].attrtypid, stats[dim].attrcollid)?;

        let typlen = stats[dim].typlen;
        let typbyval = stats[dim].typbyval;

        let mut vals: PgVec<'_, Datum> = mcx::vec_with_capacity_in(mcx, nitems)?;
        for item in mcvlist.items.iter() {
            if item.isnull[dim] {
                continue;
            }
            vals.push(item.values[dim]);
        }

        let mut di = DimensionInfo {
            nvalues: 0,
            nbytes: 0,
            nbytes_aligned: 0,
            typlen: typlen as i32,
            typbyval,
        };

        if !vals.is_empty() {
            pg_qsort(&mut vals, |a, b| mss.compare_dim(0, *a, false, *b, false));
            let mut ndistinct = 1usize;
            for i in 1..vals.len() {
                if mss.compare_dim(0, vals[i - 1], false, vals[i], false) == 0 {
                    continue;
                }
                vals[ndistinct] = vals[i];
                ndistinct += 1;
            }
            vals.truncate(ndistinct);
            di.nvalues = ndistinct as i32;

            if typbyval {
                di.nbytes = di.nvalues * di.typlen;
                di.nbytes_aligned = 0;
            } else if typlen > 0 {
                di.nbytes = di.nvalues * di.typlen;
                di.nbytes_aligned = di.nvalues * maxalign(typlen as usize) as i32;
            } else if typlen == -1 {
                for &v in vals.iter() {
                    let len = varsize_any_exhdr(v.as_usize() as *const u8);
                    di.nbytes += 4 + len as i32;
                    di.nbytes_aligned += maxalign(4 + len) as i32;
                }
            } else if typlen == -2 {
                for &v in vals.iter() {
                    // SAFETY: cstring datum.
                    let len = unsafe {
                        let p = v.as_usize() as *const u8;
                        let mut n = 0usize;
                        while *p.add(n) != 0 {
                            n += 1;
                        }
                        n + 1
                    };
                    di.nbytes += 4 + len as i32;
                    di.nbytes_aligned += maxalign(len) as i32;
                }
            }
        }

        values.push(vals);
        info.push(di);
        cmps.push(mss);
    }

    let item_size = ndims * (2 + 1) + 2 * 8;
    let mut total_length = 3 * 4 + 2 + ndims * 4;
    total_length += ndims * 20;
    for di in info.iter() {
        total_length += di.nbytes as usize;
    }
    total_length += nitems * item_size;

    let mut out: PgVec<'mcx, u8> = mcx::vec_with_capacity_in(mcx, 4 + total_length)?;
    out.extend_from_slice(&(((4 + total_length) as u32) << 2).to_ne_bytes());
    out.extend_from_slice(&STATS_MCV_MAGIC.to_ne_bytes());
    out.extend_from_slice(&STATS_MCV_TYPE_BASIC.to_ne_bytes());
    out.extend_from_slice(&(nitems as u32).to_ne_bytes());
    out.extend_from_slice(&(ndims as u16).to_ne_bytes());
    for dim in 0..ndims {
        out.extend_from_slice(&mcvlist.types[dim].to_ne_bytes());
    }
    for di in info.iter() {
        out.extend_from_slice(&di.nvalues.to_ne_bytes());
        out.extend_from_slice(&di.nbytes.to_ne_bytes());
        out.extend_from_slice(&di.nbytes_aligned.to_ne_bytes());
        out.extend_from_slice(&di.typlen.to_ne_bytes());
        out.push(di.typbyval as u8);
        out.extend_from_slice(&[0u8; 3]);
    }

    for dim in 0..ndims {
        let di = &info[dim];
        for &v in values[dim].iter() {
            if di.typbyval {
                // Full 8-byte Datum word; as_usize() truncates on wasm32.
                let bytes = v.as_u64().to_ne_bytes();
                out.extend_from_slice(&bytes[..di.typlen as usize]);
            } else if di.typlen > 0 {
                // SAFETY: fixed-length byref datum.
                let src = unsafe {
                    core::slice::from_raw_parts(v.as_usize() as *const u8, di.typlen as usize)
                };
                out.extend_from_slice(src);
            } else if di.typlen == -1 {
                let p = v.as_usize() as *const u8;
                let len = varsize_any_exhdr(p);
                out.extend_from_slice(&(len as u32).to_ne_bytes());
                // SAFETY: len bytes of varlena body.
                let src = unsafe { core::slice::from_raw_parts(vardata_any(p), len) };
                out.extend_from_slice(src);
            } else if di.typlen == -2 {
                // SAFETY: cstring datum.
                let src = unsafe {
                    let p = v.as_usize() as *const u8;
                    let mut n = 0usize;
                    while *p.add(n) != 0 {
                        n += 1;
                    }
                    core::slice::from_raw_parts(p, n + 1)
                };
                out.extend_from_slice(&((src.len()) as u32).to_ne_bytes());
                out.extend_from_slice(src);
            }
        }
    }

    for item in mcvlist.items.iter() {
        for dim in 0..ndims {
            out.push(item.isnull[dim] as u8);
        }
        out.extend_from_slice(&item.frequency.to_ne_bytes());
        out.extend_from_slice(&item.base_frequency.to_ne_bytes());
        for dim in 0..ndims {
            let mut index: u16 = 0;
            if !item.isnull[dim] {
                let vals = &values[dim];
                let mss = &mut cmps[dim];
                let mut lo = 0usize;
                let mut hi = vals.len();
                let mut found = None;
                while lo < hi {
                    let mid = (lo + hi) / 2;
                    let c = mss.compare_dim(0, item.values[dim], false, vals[mid], false);
                    match c.cmp(&0) {
                        core::cmp::Ordering::Equal => {
                            found = Some(mid);
                            break;
                        }
                        core::cmp::Ordering::Greater => lo = mid + 1,
                        core::cmp::Ordering::Less => hi = mid,
                    }
                }
                index = found.expect("mcv serialize: deduplicated value not found") as u16;
            }
            out.extend_from_slice(&index.to_ne_bytes());
        }
    }

    debug_assert_eq!(out.len(), 4 + total_length);
    Ok(out)
}

fn mcv_read<'a>(data: &'a [u8], off: &mut usize, n: usize) -> PgResult<&'a [u8]> {
    let s = data.get(*off..*off + n).ok_or_else(|| {
        PgError::error(format!(
            "invalid MCV size {} (expected at least {})",
            data.len() + 4,
            *off + n + 4
        ))
    })?;
    *off += n;
    Ok(s)
}

pub fn statext_mcv_deserialize<'mcx>(mcx: Mcx<'mcx>, data: &[u8]) -> PgResult<MCVList<'mcx>> {
    // `data` is the varlena body; C's size checks and messages use
    // VARSIZE_ANY, i.e. body + 4-byte varlena header.
    const VARHDRSZ: usize = 4;
    const MIN_SIZE_OF_MCVLIST: usize = VARHDRSZ + 4 * 3 + 2;
    let varsize = data.len() + VARHDRSZ;
    if varsize < MIN_SIZE_OF_MCVLIST {
        return Err(PgError::error(format!(
            "invalid MCV size {varsize} (expected at least {MIN_SIZE_OF_MCVLIST})"
        ))
        .into());
    }
    let magic = u32::from_ne_bytes(data[0..4].try_into().unwrap());
    let typ = u32::from_ne_bytes(data[4..8].try_into().unwrap());
    let nitems = u32::from_ne_bytes(data[8..12].try_into().unwrap());
    let ndimensions = i16::from_ne_bytes(data[12..14].try_into().unwrap());
    if magic != STATS_MCV_MAGIC {
        return Err(PgError::error(format!(
            "invalid MCV magic {magic} (expected {STATS_MCV_MAGIC})"
        ))
        .into());
    }
    if typ != STATS_MCV_TYPE_BASIC {
        return Err(PgError::error(format!(
            "invalid MCV type {typ} (expected {STATS_MCV_TYPE_BASIC})"
        ))
        .into());
    }
    if ndimensions == 0 {
        return Err(PgError::error("invalid zero-length dimension array in MCVList").into());
    } else if ndimensions > STATS_MAX_DIMENSIONS as i16 || ndimensions < 0 {
        return Err(PgError::error(format!(
            "invalid length ({ndimensions}) dimension array in MCVList"
        ))
        .into());
    }
    if nitems == 0 {
        return Err(PgError::error("invalid zero-length item array in MCVList").into());
    } else if nitems as usize > STATS_MCVLIST_MAX_ITEMS {
        return Err(
            PgError::error(format!("invalid length ({nitems}) item array in MCVList")).into()
        );
    }
    let nitems = nitems as usize;
    let ndims = ndimensions as usize;

    let item_size = ndims * (2 + 1) + 2 * 8;
    let mut expected_size =
        MIN_SIZE_OF_MCVLIST + 4 * ndims + 20 * ndims + nitems * item_size;
    if varsize < expected_size {
        return Err(PgError::error(format!(
            "invalid MCV size {varsize} (expected {expected_size})"
        ))
        .into());
    }

    let mut off = 14usize;
    let mut types = [0 as Oid; STATS_MAX_DIMENSIONS];
    for t in types.iter_mut().take(ndims) {
        *t = u32::from_ne_bytes(data[off..off + 4].try_into().unwrap());
        off += 4;
    }

    struct DimInfo {
        nvalues: usize,
        nbytes: usize,
        typlen: i32,
        typbyval: bool,
    }
    let mut info: PgVec<'_, DimInfo> = mcx::vec_with_capacity_in(mcx, ndims)?;
    for _ in 0..ndims {
        let nvalues = i32::from_ne_bytes(data[off..off + 4].try_into().unwrap());
        let nbytes = i32::from_ne_bytes(data[off + 4..off + 8].try_into().unwrap());
        let typlen = i32::from_ne_bytes(data[off + 12..off + 16].try_into().unwrap());
        let typbyval = data[off + 16] != 0;
        if nvalues < 0 {
            return Err(
                PgError::error(format!("invalid MCV nvalues ({nvalues}) in MCVList")).into()
            );
        }
        if nbytes < 0 {
            return Err(
                PgError::error(format!("invalid MCV nbytes ({nbytes}) in MCVList")).into()
            );
        }
        if typbyval && !matches!(typlen, 1 | 2 | 4 | 8) {
            return Err(PgError::error(format!("unsupported byval length: {typlen}")).into());
        }
        info.push(DimInfo {
            nvalues: nvalues as usize,
            nbytes: nbytes as usize,
            typlen,
            typbyval,
        });
        off += 20;
        expected_size += nbytes as usize;
    }

    if varsize != expected_size {
        return Err(PgError::error(format!(
            "invalid MCV size {varsize} (expected {expected_size})"
        ))
        .into());
    }

    // Deduplicated value maps; by-ref copies land in u64-backed (8-aligned)
    // arena buffers, as C's single-chunk MAXALIGN layout does.
    let mut map: PgVec<'_, PgVec<'_, Datum>> = PgVec::new_in(mcx);
    for (dim, di) in info.iter().enumerate() {
        let mut m: PgVec<'_, Datum> = mcx::vec_with_capacity_in(mcx, di.nvalues)?;
        let start = off;
        if di.typbyval {
            for _ in 0..di.nvalues {
                let mut raw = [0u8; 8];
                raw[..di.typlen as usize]
                    .copy_from_slice(mcv_read(data, &mut off, di.typlen as usize)?);
                let v = u64::from_ne_bytes(raw);
                // fetch_att sign-extends narrow integers.
                let v = match di.typlen {
                    1 => v as u8 as i8 as i64 as u64,
                    2 => v as u16 as i16 as i64 as u64,
                    4 => v as u32 as i32 as i64 as u64,
                    _ => v,
                };
                m.push(Datum::from_u64(v));
            }
        } else if di.typlen > 0 {
            for _ in 0..di.nvalues {
                let src = mcv_read(data, &mut off, di.typlen as usize)?;
                let buf = alloc_aligned(mcx, di.typlen as usize)?;
                buf.copy_from_slice(src);
                m.push(Datum::from_usize(buf.as_ptr() as usize));
            }
        } else if di.typlen == -1 {
            for _ in 0..di.nvalues {
                let len = u32::from_ne_bytes(
                    mcv_read(data, &mut off, 4)?.try_into().unwrap(),
                ) as usize;
                let src = mcv_read(data, &mut off, len)?;
                let buf = alloc_aligned(mcx, len + 4)?;
                buf[..4].copy_from_slice(&(((len + 4) as u32) << 2).to_ne_bytes());
                buf[4..].copy_from_slice(src);
                m.push(Datum::from_usize(buf.as_ptr() as usize));
            }
        } else if di.typlen == -2 {
            for _ in 0..di.nvalues {
                let len = u32::from_ne_bytes(
                    mcv_read(data, &mut off, 4)?.try_into().unwrap(),
                ) as usize;
                let src = mcv_read(data, &mut off, len)?;
                let buf = alloc_aligned(mcx, len)?;
                buf.copy_from_slice(src);
                m.push(Datum::from_usize(buf.as_ptr() as usize));
            }
        }
        if off != start + di.nbytes {
            return Err(PgError::error(format!(
                "invalid MCV nbytes ({}) in MCVList (dimension {dim})",
                di.nbytes
            ))
            .into());
        }
        map.push(m);
    }

    let mut items: PgVec<'mcx, MCVItem<'mcx>> = PgVec::new_in(mcx);
    for _ in 0..nitems {
        let mut isnull: PgVec<'mcx, bool> = mcx::vec_with_capacity_in(mcx, ndims)?;
        for d in 0..ndims {
            isnull.push(data[off + d] != 0);
        }
        off += ndims;
        let frequency = f64::from_ne_bytes(data[off..off + 8].try_into().unwrap());
        off += 8;
        let base_frequency = f64::from_ne_bytes(data[off..off + 8].try_into().unwrap());
        off += 8;
        let mut values: PgVec<'mcx, Datum> = mcx::vec_with_capacity_in(mcx, ndims)?;
        for d in 0..ndims {
            let index = u16::from_ne_bytes(data[off..off + 2].try_into().unwrap()) as usize;
            off += 2;
            if isnull[d] {
                values.push(Datum::null());
            } else {
                let Some(&v) = map[d].get(index) else {
                    return Err(PgError::error(format!(
                        "invalid MCV item index {index} (dimension {d})"
                    ))
                    .into());
                };
                values.push(v);
            }
        }
        items.push(MCVItem { values, isnull, frequency, base_frequency });
    }
    debug_assert_eq!(off, data.len());

    Ok(MCVList { ndimensions: ndims, types, items })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn blob(nitems: u32, index: u16) -> Vec<u8> {
        let mut b = Vec::new();
        b.extend_from_slice(&STATS_MCV_MAGIC.to_ne_bytes());
        b.extend_from_slice(&STATS_MCV_TYPE_BASIC.to_ne_bytes());
        b.extend_from_slice(&nitems.to_ne_bytes());
        b.extend_from_slice(&1i16.to_ne_bytes());
        b.extend_from_slice(&23u32.to_ne_bytes());
        b.extend_from_slice(&1i32.to_ne_bytes());
        b.extend_from_slice(&4i32.to_ne_bytes());
        b.extend_from_slice(&0i32.to_ne_bytes());
        b.extend_from_slice(&4i32.to_ne_bytes());
        b.push(1);
        b.extend_from_slice(&[0u8; 3]);
        b.extend_from_slice(&42i32.to_ne_bytes());
        for _ in 0..nitems {
            b.push(0);
            b.extend_from_slice(&0.5f64.to_ne_bytes());
            b.extend_from_slice(&0.25f64.to_ne_bytes());
            b.extend_from_slice(&index.to_ne_bytes());
        }
        b
    }

    #[test]
    fn deserialize_valid_roundtrip() {
        let cx = mcx::MemoryContext::new("test");
        let m = statext_mcv_deserialize(cx.mcx(), &blob(1, 0)).unwrap();
        assert_eq!(m.ndimensions, 1);
        assert_eq!(m.items.len(), 1);
        assert_eq!(m.items[0].values[0].as_i32(), 42);
        assert_eq!(m.items[0].frequency, 0.5);
    }

    #[test]
    fn deserialize_truncated_returns_err() {
        let cx = mcx::MemoryContext::new("test");
        let full = blob(1, 0);
        for cut in [0, 8, 13, 14, 20, 34, 40, full.len() - 1] {
            assert!(statext_mcv_deserialize(cx.mcx(), &full[..cut]).is_err());
        }
    }

    #[test]
    fn deserialize_trailing_garbage_returns_err() {
        let cx = mcx::MemoryContext::new("test");
        let mut b = blob(1, 0);
        b.push(0);
        assert!(statext_mcv_deserialize(cx.mcx(), &b).is_err());
    }

    #[test]
    fn deserialize_out_of_range_index_returns_err() {
        let cx = mcx::MemoryContext::new("test");
        assert!(statext_mcv_deserialize(cx.mcx(), &blob(1, 5)).is_err());
    }

    #[test]
    fn deserialize_nitems_too_large_returns_err() {
        let cx = mcx::MemoryContext::new("test");
        let mut b = blob(1, 0);
        b[8..12].copy_from_slice(&20000u32.to_ne_bytes());
        assert!(statext_mcv_deserialize(cx.mcx(), &b).is_err());
        let mut b = blob(1, 0);
        b[8..12].copy_from_slice(&2u32.to_ne_bytes());
        assert!(statext_mcv_deserialize(cx.mcx(), &b).is_err());
    }

    #[test]
    fn deserialize_bad_magic_type_ndims_return_err() {
        let cx = mcx::MemoryContext::new("test");
        let mut b = blob(1, 0);
        b[0] ^= 0xFF;
        assert!(statext_mcv_deserialize(cx.mcx(), &b).is_err());
        let mut b = blob(1, 0);
        b[4] ^= 0xFF;
        assert!(statext_mcv_deserialize(cx.mcx(), &b).is_err());
        let mut b = blob(1, 0);
        b[12..14].copy_from_slice(&9i16.to_ne_bytes());
        assert!(statext_mcv_deserialize(cx.mcx(), &b).is_err());
        let mut b = blob(1, 0);
        b[12..14].copy_from_slice(&0i16.to_ne_bytes());
        assert!(statext_mcv_deserialize(cx.mcx(), &b).is_err());
    }
}

fn alloc_aligned<'mcx>(mcx: Mcx<'mcx>, len: usize) -> PgResult<&'mcx mut [u8]> {
    let words = len.div_ceil(8);
    let mut v: PgVec<'mcx, u64> = mcx::vec_with_capacity_in(mcx, words)?;
    v.resize(words, 0);
    let ptr = v.as_mut_ptr() as *mut u8;
    core::mem::forget(v);
    // SAFETY: freshly zeroed arena words, len <= words*8, lives until reset.
    Ok(unsafe { core::slice::from_raw_parts_mut(ptr, len) })
}
