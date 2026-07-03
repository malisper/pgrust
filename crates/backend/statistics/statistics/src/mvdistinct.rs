use mcx::{Mcx, PgVec};
use types_core::AttrNumber;
use types_error::{PgError, PgResult};

use crate::sortitem::SortItem;
use crate::{build_mss, build_sorted_items, StatsBuildData};

pub const STATS_NDISTINCT_MAGIC: u32 = 0xA352BFA4;
pub const STATS_NDISTINCT_TYPE_BASIC: u32 = 1;

pub struct MVNDistinctItem<'mcx> {
    pub ndistinct: f64,
    pub attributes: PgVec<'mcx, AttrNumber>,
}

pub struct MVNDistinct<'mcx> {
    pub items: PgVec<'mcx, MVNDistinctItem<'mcx>>,
}

fn n_choose_k(n: usize, k: usize) -> usize {
    let k = k.min(n - k);
    let mut r: usize = 1;
    let mut n = n;
    for d in 1..=k {
        r *= n;
        n -= 1;
        r /= d;
    }
    r
}

fn num_combinations(n: usize) -> usize {
    (1usize << n) - (n + 1)
}

struct CombinationGenerator<'mcx> {
    k: usize,
    combinations: PgVec<'mcx, u32>,
    current: usize,
}

impl<'mcx> CombinationGenerator<'mcx> {
    fn init(mcx: Mcx<'mcx>, n: usize, k: usize) -> PgResult<Self> {
        let ncomb = n_choose_k(n, k);
        let mut combinations: PgVec<'mcx, u32> = mcx::vec_with_capacity_in(mcx, ncomb * k)?;
        let mut current: PgVec<'mcx, u32> = mcx::vec_with_capacity_in(mcx, k)?;
        current.resize(k, 0);
        recurse(&mut combinations, &mut current, 0, 0, k, n);
        Ok(CombinationGenerator { k, combinations, current: 0 })
    }

    fn next(&mut self) -> Option<&[u32]> {
        let start = self.k * self.current;
        if start >= self.combinations.len() {
            return None;
        }
        self.current += 1;
        Some(&self.combinations[start..start + self.k])
    }
}

fn recurse(out: &mut PgVec<'_, u32>, current: &mut [u32], index: usize, start: usize, k: usize, n: usize) {
    if index < k {
        for i in start..n {
            current[index] = i as u32;
            recurse(out, current, index + 1, i + 1, k, n);
        }
    } else {
        out.extend_from_slice(current);
    }
}

pub fn statext_ndistinct_build<'mcx>(
    mcx: Mcx<'mcx>,
    totalrows: f64,
    data: &StatsBuildData<'mcx>,
) -> PgResult<MVNDistinct<'mcx>> {
    let numattrs = data.attnums.len();
    let numcombs = num_combinations(numattrs);
    let mut items: PgVec<'mcx, MVNDistinctItem<'mcx>> = PgVec::new_in(mcx);

    for k in 2..=numattrs {
        let mut generator = CombinationGenerator::init(mcx, numattrs, k)?;
        while let Some(combination) = generator.next() {
            let mut attributes: PgVec<'mcx, AttrNumber> = mcx::vec_with_capacity_in(mcx, k)?;
            for &c in combination {
                attributes.push(data.attnums[c as usize]);
            }
            let ndistinct = ndistinct_for_combination(mcx, totalrows, data, combination)?;
            items.push(MVNDistinctItem { ndistinct, attributes });
        }
    }
    debug_assert_eq!(items.len(), numcombs);
    Ok(MVNDistinct { items })
}

fn ndistinct_for_combination<'mcx>(
    mcx: Mcx<'mcx>,
    totalrows: f64,
    data: &StatsBuildData<'mcx>,
    combination: &[u32],
) -> PgResult<f64> {
    let numrows = data.numrows;
    let dims: PgVec<'_, usize> = {
        let mut v = mcx::vec_with_capacity_in(mcx, combination.len())?;
        for &c in combination {
            v.push(c as usize);
        }
        v
    };
    let mut mss = build_mss(&data.stats, &dims)?;

    // C copies raw values without the width/detoast filtering of
    // build_sorted_items; mirror that exactly.
    let k = dims.len();
    let mut values: PgVec<'_, datum::Datum> = mcx::vec_with_capacity_in(mcx, numrows * k)?;
    let mut isnull: PgVec<'_, bool> = mcx::vec_with_capacity_in(mcx, numrows * k)?;
    for i in 0..numrows {
        for &j in dims.iter() {
            values.push(data.values[j][i]);
            isnull.push(data.nulls[j][i]);
        }
    }
    let store = crate::sortitem::ItemStore { values, isnull, width: k };
    let mut items: PgVec<'_, SortItem> = mcx::vec_with_capacity_in(mcx, numrows)?;
    for off in 0..numrows {
        items.push(SortItem { off: off as u32, count: 0 });
    }
    crate::sortitem::pg_qsort(&mut items, |a, b| store.compare(&mut mss, *a, *b));

    let mut f1 = 0i32;
    let mut cnt = 1i32;
    let mut d = 1i32;
    for i in 1..numrows {
        if store.compare(&mut mss, items[i], items[i - 1]) != 0 {
            if cnt == 1 {
                f1 += 1;
            }
            d += 1;
            cnt = 0;
        }
        cnt += 1;
    }
    if cnt == 1 {
        f1 += 1;
    }

    Ok(estimate_ndistinct(totalrows, numrows as i32, d, f1))
}

fn estimate_ndistinct(totalrows: f64, numrows: i32, d: i32, f1: i32) -> f64 {
    let numer = numrows as f64 * d as f64;
    let denom = (numrows - f1) as f64 + f1 as f64 * numrows as f64 / totalrows;
    let mut ndistinct = numer / denom;
    if ndistinct < d as f64 {
        ndistinct = d as f64;
    }
    if ndistinct > totalrows {
        ndistinct = totalrows;
    }
    (ndistinct + 0.5).floor()
}

pub fn statext_ndistinct_serialize<'mcx>(
    mcx: Mcx<'mcx>,
    nd: &MVNDistinct<'_>,
) -> PgResult<PgVec<'mcx, u8>> {
    let mut len = 4 + 3 * 4;
    for item in nd.items.iter() {
        len += 8 + 4 + item.attributes.len() * 2;
    }
    let mut out: PgVec<'mcx, u8> = mcx::vec_with_capacity_in(mcx, len)?;
    out.extend_from_slice(&(((len as u32) << 2)).to_ne_bytes());
    out.extend_from_slice(&STATS_NDISTINCT_MAGIC.to_ne_bytes());
    out.extend_from_slice(&STATS_NDISTINCT_TYPE_BASIC.to_ne_bytes());
    out.extend_from_slice(&(nd.items.len() as u32).to_ne_bytes());
    for item in nd.items.iter() {
        out.extend_from_slice(&item.ndistinct.to_ne_bytes());
        out.extend_from_slice(&(item.attributes.len() as i32).to_ne_bytes());
        for &a in item.attributes.iter() {
            out.extend_from_slice(&a.to_ne_bytes());
        }
    }
    debug_assert_eq!(out.len(), len);
    Ok(out)
}

pub fn statext_ndistinct_deserialize<'mcx>(
    mcx: Mcx<'mcx>,
    data: &[u8],
) -> PgResult<MVNDistinct<'mcx>> {
    // `data` is the varlena body (header already stripped by the caller).
    const SIZE_OF_HEADER: usize = 3 * 4;
    const MIN_SIZE_OF_ITEM: usize = 8 + 4 + 2 * 2;
    if data.len() < SIZE_OF_HEADER {
        return Err(PgError::error(format!(
            "invalid MVNDistinct size {} (expected at least {SIZE_OF_HEADER})",
            data.len()
        ))
        .into());
    }
    let magic = u32::from_ne_bytes(data[0..4].try_into().unwrap());
    let typ = u32::from_ne_bytes(data[4..8].try_into().unwrap());
    let nitems = u32::from_ne_bytes(data[8..12].try_into().unwrap()) as usize;
    if magic != STATS_NDISTINCT_MAGIC {
        return Err(PgError::error(format!(
            "invalid ndistinct magic {magic:08x} (expected {STATS_NDISTINCT_MAGIC:08x})"
        ))
        .into());
    }
    if typ != STATS_NDISTINCT_TYPE_BASIC {
        return Err(PgError::error(format!(
            "invalid ndistinct type {} (expected {STATS_NDISTINCT_TYPE_BASIC})", typ as i32
        ))
        .into());
    }
    if nitems == 0 {
        return Err(PgError::error("invalid zero-length item array in MVNDistinct").into());
    }
    let minimum_size = SIZE_OF_HEADER + nitems * MIN_SIZE_OF_ITEM;
    if data.len() < minimum_size {
        return Err(PgError::error(format!(
            "invalid MVNDistinct size {} (expected at least {minimum_size})",
            data.len()
        ))
        .into());
    }
    let mut items: PgVec<'mcx, MVNDistinctItem<'mcx>> = PgVec::new_in(mcx);
    let mut off = 12usize;
    for _ in 0..nitems {
        if data.len() - off < 12 {
            return Err(PgError::error(format!(
                "invalid MVNDistinct size {} (expected at least {})",
                data.len(),
                off + 12
            ))
            .into());
        }
        let ndistinct = f64::from_ne_bytes(data[off..off + 8].try_into().unwrap());
        off += 8;
        let natts = i32::from_ne_bytes(data[off..off + 4].try_into().unwrap());
        off += 4;
        if natts < 2 || natts > crate::STATS_MAX_DIMENSIONS as i32 {
            return Err(PgError::error(format!(
                "invalid number of attributes ({natts}) in MVNDistinct"
            ))
            .into());
        }
        let natts = natts as usize;
        if data.len() - off < natts * 2 {
            return Err(PgError::error(format!(
                "invalid MVNDistinct size {} (expected at least {})",
                data.len(),
                off + natts * 2
            ))
            .into());
        }
        let mut attributes: PgVec<'mcx, AttrNumber> = mcx::vec_with_capacity_in(mcx, natts)?;
        for _ in 0..natts {
            attributes.push(i16::from_ne_bytes(data[off..off + 2].try_into().unwrap()));
            off += 2;
        }
        items.push(MVNDistinctItem { ndistinct, attributes });
    }
    debug_assert_eq!(off, data.len());
    Ok(MVNDistinct { items })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn blob(nitems: u32, items: &[(f64, &[i16])]) -> Vec<u8> {
        let mut b = Vec::new();
        b.extend_from_slice(&STATS_NDISTINCT_MAGIC.to_ne_bytes());
        b.extend_from_slice(&STATS_NDISTINCT_TYPE_BASIC.to_ne_bytes());
        b.extend_from_slice(&nitems.to_ne_bytes());
        for (nd, atts) in items {
            b.extend_from_slice(&nd.to_ne_bytes());
            b.extend_from_slice(&(atts.len() as i32).to_ne_bytes());
            for a in *atts {
                b.extend_from_slice(&a.to_ne_bytes());
            }
        }
        b
    }

    #[test]
    fn deserialize_truncated_returns_err() {
        let cx = mcx::MemoryContext::new("test");
        let full = blob(1, &[(3.0, &[1, 2, 3])]);
        for cut in [0, 4, 11, 12, 15, 20, full.len() - 1] {
            assert!(statext_ndistinct_deserialize(cx.mcx(), &full[..cut]).is_err());
        }
    }

    #[test]
    fn deserialize_nitems_too_large_returns_err() {
        let cx = mcx::MemoryContext::new("test");
        let b = blob(1000, &[(3.0, &[1, 2])]);
        assert!(statext_ndistinct_deserialize(cx.mcx(), &b).is_err());
    }

    #[test]
    fn deserialize_bad_nattributes_returns_err() {
        let cx = mcx::MemoryContext::new("test");
        for atts in [&[1i16][..], &[1; 9][..]] {
            let b = blob(1, &[(3.0, atts)]);
            assert!(statext_ndistinct_deserialize(cx.mcx(), &b).is_err());
        }
    }

    #[test]
    fn deserialize_bad_magic_and_type_return_err() {
        let cx = mcx::MemoryContext::new("test");
        let mut b = blob(1, &[(3.0, &[1, 2])]);
        b[0] ^= 0xFF;
        assert!(statext_ndistinct_deserialize(cx.mcx(), &b).is_err());
        let mut b = blob(1, &[(3.0, &[1, 2])]);
        b[4] ^= 0xFF;
        assert!(statext_ndistinct_deserialize(cx.mcx(), &b).is_err());
    }

    #[test]
    fn deserialize_valid_roundtrip() {
        let cx = mcx::MemoryContext::new("test");
        let b = blob(2, &[(3.0, &[1, 2]), (7.0, &[1, 2, 4])]);
        let nd = statext_ndistinct_deserialize(cx.mcx(), &b).unwrap();
        assert_eq!(nd.items.len(), 2);
        assert_eq!(nd.items[0].ndistinct, 3.0);
        assert_eq!(&nd.items[1].attributes[..], &[1, 2, 4]);
    }
}
