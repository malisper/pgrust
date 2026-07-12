//! btree_utils_num.c — fixed-size lower/upper key framework, monomorphized
//! per type (C dispatches through gbtree_ninfo fn-pointer tables).

use types_error::{PgError, PgResult};
use types_fmgr::FmgrInfo;

pub const BT_LESS: u16 = 1;
pub const BT_LESS_EQUAL: u16 = 2;
pub const BT_EQUAL: u16 = 3;
pub const BT_GREATER_EQUAL: u16 = 4;
pub const BT_GREATER: u16 = 5;
pub const BT_NOT_EQUAL: u16 = 6;

// 0.49F promoted to double, as in C's penalty_num.
pub const C049: f64 = 0.49f32 as f64;

pub struct Ctx<'a> {
    pub flinfo: Option<&'a mut FmgrInfo>,
    pub collation: types_core::Oid,
}

pub trait NumOps {
    const SIZE: usize;
    const INDEXSIZE: usize;
    type V: Copy;

    fn read(b: &[u8]) -> Self::V;
    fn write(out: &mut [u8], v: Self::V);

    fn gt(a: Self::V, b: Self::V, ctx: &mut Ctx) -> PgResult<bool>;
    fn ge(a: Self::V, b: Self::V, ctx: &mut Ctx) -> PgResult<bool>;
    fn eq(a: Self::V, b: Self::V, ctx: &mut Ctx) -> PgResult<bool>;
    fn le(a: Self::V, b: Self::V, ctx: &mut Ctx) -> PgResult<bool>;
    fn lt(a: Self::V, b: Self::V, ctx: &mut Ctx) -> PgResult<bool>;
    // Nsrt comparator over (lower, upper) pairs.
    fn key_cmp(a: (Self::V, Self::V), b: (Self::V, Self::V), ctx: &mut Ctx) -> PgResult<i32>;

    const HAS_DIST: bool = false;
    fn dist(_a: Self::V, _b: Self::V, _ctx: &mut Ctx) -> PgResult<f64> {
        unreachable!("dist called without HAS_DIST")
    }
}

pub fn read_pair<T: NumOps>(key: &[u8]) -> (T::V, T::V) {
    (T::read(&key[..T::SIZE]), T::read(&key[T::SIZE..2 * T::SIZE]))
}

pub fn make_key<T: NumOps>(lower: T::V, upper: T::V) -> Vec<u8> {
    let mut img = vec![0u8; T::INDEXSIZE];
    T::write(&mut img[..T::SIZE], lower);
    T::write(&mut img[T::SIZE..2 * T::SIZE], upper);
    img
}

// gbt_num_consistent.
pub fn consistent<T: NumOps>(
    key: (T::V, T::V),
    query: T::V,
    strategy: u16,
    is_leaf: bool,
    ctx: &mut Ctx,
) -> PgResult<bool> {
    let (lower, upper) = key;
    Ok(match strategy {
        BT_LESS_EQUAL => T::ge(query, lower, ctx)?,
        BT_LESS => {
            if is_leaf {
                T::gt(query, lower, ctx)?
            } else {
                T::ge(query, lower, ctx)?
            }
        }
        BT_EQUAL => {
            if is_leaf {
                T::eq(query, lower, ctx)?
            } else {
                T::le(lower, query, ctx)? && T::le(query, upper, ctx)?
            }
        }
        BT_GREATER => {
            if is_leaf {
                T::lt(query, upper, ctx)?
            } else {
                T::le(query, upper, ctx)?
            }
        }
        BT_GREATER_EQUAL => T::le(query, upper, ctx)?,
        BT_NOT_EQUAL => !(T::eq(query, lower, ctx)? && T::eq(query, upper, ctx)?),
        _ => false,
    })
}

// gbt_num_distance.
pub fn distance<T: NumOps>(
    key: (T::V, T::V),
    query: T::V,
    _is_leaf: bool,
    ctx: &mut Ctx,
) -> PgResult<f64> {
    if !T::HAS_DIST {
        return Err(PgError::error("KNN search is not supported for this btree_gist type").into());
    }
    let (lower, upper) = key;
    if T::le(query, lower, ctx)? {
        T::dist(query, lower, ctx)
    } else if T::ge(query, upper, ctx)? {
        T::dist(query, upper, ctx)
    } else {
        Ok(0.0)
    }
}

// gbt_num_union over the raw entry key images.
pub fn union<T: NumOps>(keys: &[&[u8]], ctx: &mut Ctx) -> PgResult<Vec<u8>> {
    let (mut lower, mut upper) = read_pair::<T>(keys[0]);
    for k in &keys[1..] {
        let (cl, cu) = read_pair::<T>(k);
        if T::gt(lower, cl, ctx)? {
            lower = cl;
        }
        if T::lt(upper, cu, ctx)? {
            upper = cu;
        }
    }
    Ok(make_key::<T>(lower, upper))
}

// gbt_num_same.
pub fn same<T: NumOps>(a: &[u8], b: &[u8], ctx: &mut Ctx) -> PgResult<bool> {
    let (al, au) = read_pair::<T>(a);
    let (bl, bu) = read_pair::<T>(b);
    Ok(T::eq(al, bl, ctx)? && T::eq(au, bu, ctx)?)
}

// gbt_num_picksplit: sorted halves. Entries are 1-based (index 0 unused).
pub fn picksplit<T: NumOps>(
    keys: &[&[u8]],
    ctx: &mut Ctx,
) -> PgResult<(Vec<u16>, Vec<u16>, Vec<u8>, Vec<u8>)> {
    let maxoff = keys.len() - 1;
    let mut arr: Vec<(u16, (T::V, T::V))> = (1..=maxoff)
        .map(|i| (i as u16, read_pair::<T>(keys[i])))
        .collect();
    // C sorts with qsort_arg; the comparator can't return errors mid-qsort,
    // mirror the shim convention (unwind carries the PgError verbatim).
    {
        let ctx_cell = core::cell::RefCell::new(&mut *ctx);
        gistproc::qsort::pg_qsort(&mut arr, |a, b| {
            match T::key_cmp(a.1, b.1, &mut ctx_cell.borrow_mut()) {
                Ok(r) => r,
                Err(e) => std::panic::panic_any(e),
            }
        });
    }

    let mut spl_left = Vec::new();
    let mut spl_right = Vec::new();
    let mut left: Option<(T::V, T::V)> = None;
    let mut right: Option<(T::V, T::V)> = None;
    for (pos, &(off, pair)) in arr.iter().enumerate() {
        let (side, acc) = if pos + 1 <= maxoff / 2 {
            (&mut spl_left, &mut left)
        } else {
            (&mut spl_right, &mut right)
        };
        *acc = Some(match *acc {
            None => pair,
            Some((mut ul, mut uu)) => {
                if T::gt(ul, pair.0, ctx)? {
                    ul = pair.0;
                }
                if T::lt(uu, pair.1, ctx)? {
                    uu = pair.1;
                }
                (ul, uu)
            }
        });
        side.push(off);
    }
    let (ll, lu) = left.expect("picksplit left group nonempty");
    let (rl, ru) = right.expect("picksplit right group nonempty");
    Ok((spl_left, spl_right, make_key::<T>(ll, lu), make_key::<T>(rl, ru)))
}

// penalty_num: C float arithmetic transcribed exactly (0.49F, FLT_MIN,
// FLT_MAX / (natts + 1)).
pub fn penalty_num(o_lower: f64, o_upper: f64, n_lower: f64, n_upper: f64, natts: u16) -> f32 {
    let mut tmp = 0.0f64;
    if n_upper > o_upper {
        tmp += n_upper * C049 - o_upper * C049;
    }
    if o_lower > n_lower {
        tmp += o_lower * C049 - n_lower * C049;
    }
    let mut result = 0.0f32;
    if tmp > 0.0 {
        result += f32::MIN_POSITIVE;
        result += (tmp / (tmp + (o_upper * C049 - o_lower * C049))) as f32;
        result *= f32::MAX / (natts as f32 + 1.0);
    }
    result
}

pub fn penalty_check_max_float(v: f64) -> f64 {
    v.clamp(-(f32::MAX as f64), f32::MAX as f64)
}

// INTERVAL_TO_SEC.
pub fn interval_to_sec(i: &adt_datetime::consts::Interval) -> f64 {
    (i.time as f64) / 1_000_000.0
        + (i.day as f64) * (24.0 * 3600.0)
        + (i.month as f64) * (30.0 * 86400.0)
}
