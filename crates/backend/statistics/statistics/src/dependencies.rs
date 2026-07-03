use mcx::{Mcx, PgVec};
use types_core::AttrNumber;
use types_error::{PgError, PgResult};

use crate::{build_mss, build_sorted_items, StatsBuildData};

pub const STATS_DEPS_MAGIC: u32 = 0xB4549A2C;
pub const STATS_DEPS_TYPE_BASIC: u32 = 1;

pub struct MVDependency<'mcx> {
    pub degree: f64,
    pub attributes: PgVec<'mcx, AttrNumber>,
}

pub struct MVDependencies<'mcx> {
    pub deps: PgVec<'mcx, MVDependency<'mcx>>,
}

struct DependencyGenerator<'mcx> {
    k: usize,
    dependencies: PgVec<'mcx, u32>,
    current: usize,
}

impl<'mcx> DependencyGenerator<'mcx> {
    fn init(mcx: Mcx<'mcx>, n: usize, k: usize) -> PgResult<Self> {
        let mut dependencies: PgVec<'mcx, u32> = PgVec::new_in(mcx);
        let mut current: PgVec<'mcx, u32> = mcx::vec_with_capacity_in(mcx, k)?;
        current.resize(k, 0);
        recurse(&mut dependencies, &mut current, 0, 0, k, n);
        Ok(DependencyGenerator { k, dependencies, current: 0 })
    }

    fn next(&mut self) -> Option<&[u32]> {
        let start = self.k * self.current;
        if start >= self.dependencies.len() {
            return None;
        }
        self.current += 1;
        Some(&self.dependencies[start..start + self.k])
    }
}

fn recurse(out: &mut PgVec<'_, u32>, current: &mut [u32], index: usize, start: usize, k: usize, n: usize) {
    if index < k - 1 {
        for i in start..n {
            current[index] = i as u32;
            recurse(out, current, index + 1, i + 1, k, n);
        }
    } else {
        for i in 0..n {
            if current[..index].contains(&(i as u32)) {
                continue;
            }
            current[index] = i as u32;
            out.extend_from_slice(current);
        }
    }
}

fn dependency_degree(
    mcx: Mcx<'_>,
    data: &StatsBuildData<'_>,
    dependency: &[u32],
) -> PgResult<f64> {
    let k = dependency.len();
    let dims: PgVec<'_, usize> = {
        let mut v = mcx::vec_with_capacity_in(mcx, k)?;
        for &d in dependency {
            v.push(d as usize);
        }
        v
    };
    let mut mss = build_mss(&data.stats, &dims)?;
    let Some((items, store)) = build_sorted_items(mcx, data, &mut mss, &dims)? else {
        return Ok(0.0);
    };
    let nitems = items.len();

    let mut group_size = 1i64;
    let mut n_violations = 0i64;
    let mut n_supporting_rows = 0i64;
    for i in 1..=nitems {
        if i == nitems
            || store.compare_dims(&mut mss, 0, k - 2, items[i - 1], items[i]) != 0
        {
            if n_violations == 0 {
                n_supporting_rows += group_size;
            }
            n_violations = 0;
            group_size = 1;
            continue;
        }
        let (av, an) = store.value(items[i - 1], k - 1);
        let (bv, bn) = store.value(items[i], k - 1);
        if mss.compare_dim(k - 1, av, an, bv, bn) != 0 {
            n_violations += 1;
        }
        group_size += 1;
    }

    Ok(n_supporting_rows as f64 * 1.0 / data.numrows as f64)
}

pub fn statext_dependencies_build<'mcx>(
    mcx: Mcx<'mcx>,
    data: &StatsBuildData<'mcx>,
) -> PgResult<Option<MVDependencies<'mcx>>> {
    let nattnums = data.attnums.len();
    let mut deps: PgVec<'mcx, MVDependency<'mcx>> = PgVec::new_in(mcx);

    for k in 2..=nattnums {
        let mut generator = DependencyGenerator::init(mcx, nattnums, k)?;
        while let Some(dependency) = generator.next() {
            let dcx = mcx::MemoryContext::new("dependency_degree cxt");
            let mut dep_copy: PgVec<'mcx, u32> = mcx::vec_with_capacity_in(mcx, k)?;
            dep_copy.extend_from_slice(dependency);
            let degree = dependency_degree(dcx.mcx(), data, &dep_copy)?;
            drop(dcx);
            if degree == 0.0 {
                continue;
            }
            let mut attributes: PgVec<'mcx, AttrNumber> = mcx::vec_with_capacity_in(mcx, k)?;
            for &d in dep_copy.iter() {
                attributes.push(data.attnums[d as usize]);
            }
            deps.push(MVDependency { degree, attributes });
        }
    }

    if deps.is_empty() {
        return Ok(None);
    }
    Ok(Some(MVDependencies { deps }))
}

pub fn statext_dependencies_serialize<'mcx>(
    mcx: Mcx<'mcx>,
    dependencies: &MVDependencies<'_>,
) -> PgResult<PgVec<'mcx, u8>> {
    let mut len = 4 + 3 * 4;
    for d in dependencies.deps.iter() {
        len += 8 + 2 * (1 + d.attributes.len());
    }
    let mut out: PgVec<'mcx, u8> = mcx::vec_with_capacity_in(mcx, len)?;
    out.extend_from_slice(&((len as u32) << 2).to_ne_bytes());
    out.extend_from_slice(&STATS_DEPS_MAGIC.to_ne_bytes());
    out.extend_from_slice(&STATS_DEPS_TYPE_BASIC.to_ne_bytes());
    out.extend_from_slice(&(dependencies.deps.len() as u32).to_ne_bytes());
    for d in dependencies.deps.iter() {
        out.extend_from_slice(&d.degree.to_ne_bytes());
        out.extend_from_slice(&(d.attributes.len() as i16).to_ne_bytes());
        for &a in d.attributes.iter() {
            out.extend_from_slice(&a.to_ne_bytes());
        }
    }
    debug_assert_eq!(out.len(), len);
    Ok(out)
}

pub fn statext_dependencies_deserialize<'mcx>(
    mcx: Mcx<'mcx>,
    data: &[u8],
) -> PgResult<MVDependencies<'mcx>> {
    // `data` is the varlena body (header already stripped by the caller).
    if data.len() < 12 {
        return Err(PgError::error(format!("invalid MVDependencies size {}", data.len())).into());
    }
    let magic = u32::from_ne_bytes(data[0..4].try_into().unwrap());
    let typ = u32::from_ne_bytes(data[4..8].try_into().unwrap());
    let ndeps = u32::from_ne_bytes(data[8..12].try_into().unwrap()) as usize;
    if magic != STATS_DEPS_MAGIC {
        return Err(PgError::error(format!("invalid dependency magic {magic}")).into());
    }
    if typ != STATS_DEPS_TYPE_BASIC {
        return Err(PgError::error(format!("invalid dependency type {typ}")).into());
    }
    if ndeps == 0 {
        return Err(PgError::error("invalid zero-length item array in MVDependencies").into());
    }
    let mut deps: PgVec<'mcx, MVDependency<'mcx>> = mcx::vec_with_capacity_in(mcx, ndeps)?;
    let mut off = 12usize;
    for _ in 0..ndeps {
        let degree = f64::from_ne_bytes(data[off..off + 8].try_into().unwrap());
        off += 8;
        let natts = i16::from_ne_bytes(data[off..off + 2].try_into().unwrap()) as usize;
        off += 2;
        let mut attributes: PgVec<'mcx, AttrNumber> = mcx::vec_with_capacity_in(mcx, natts)?;
        for _ in 0..natts {
            attributes.push(i16::from_ne_bytes(data[off..off + 2].try_into().unwrap()));
            off += 2;
        }
        deps.push(MVDependency { degree, attributes });
    }
    debug_assert_eq!(off, data.len());
    Ok(MVDependencies { deps })
}
