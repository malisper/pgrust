//! btree_utils_var.c — variable-length lower/upper key framework. Keys are
//! bytea images: [outer 4B header | lower varlena | pad to INTALIGN | upper
//! varlena]; inner varlenas always carry 4B headers (built from detoasted
//! datums).

use types_error::PgResult;

use crate::num::Ctx;

const VARHDRSZ: usize = 4;

pub trait VarOps {
    const TRNC: bool;
    // 0 = fixed single-byte encoding (bytea/numeric/bit); >1 possible for
    // text/bpchar (pg_database_encoding_max_length, resolved by the caller).
    fn eml() -> i32 {
        1
    }
    // Node-key order (C f_cmp); args are full 4B-header varlena images.
    fn cmp(a: &[u8], b: &[u8], ctx: &mut Ctx) -> PgResult<i32>;
    // Leaf order (C f_gt..f_lt); differs from cmp only for bit (bit_cmp on
    // original varbit values vs byteacmp on xfrm'd node keys).
    fn leaf_cmp(a: &[u8], b: &[u8], ctx: &mut Ctx) -> PgResult<i32> {
        Self::cmp(a, b, ctx)
    }
    // C f_eq, used by the NOT_EQUAL arm at every level; bit overrides with
    // the biteq shape.
    fn eq(a: &[u8], b: &[u8], ctx: &mut Ctx) -> PgResult<bool> {
        Ok(Self::cmp(a, b, ctx)? == 0)
    }
    // gbt_bit_l2n's xfrm; None for every other type.
    fn l2n(_leaf: &[u8]) -> Option<Vec<u8>> {
        None
    }
}

pub fn varsize(image: &[u8]) -> usize {
    let raw = u32::from_ne_bytes(image[..4].try_into().unwrap());
    (raw >> 2) as usize
}

pub fn set_varsize(image: &mut [u8], size: usize) {
    image[..4].copy_from_slice(&((size as u32) << 2).to_ne_bytes());
}

const fn intalign(n: usize) -> usize {
    (n + 3) & !3
}

// gbt_var_key_readable: (lower, upper) full inner varlena slices, plus
// whether lower and upper alias (leaf key shape).
pub struct VarKey<'a> {
    pub lower: &'a [u8],
    pub upper: &'a [u8],
    pub is_leaf_shape: bool,
}

pub fn key_readable(k: &[u8]) -> VarKey<'_> {
    let total = varsize(k);
    let lsize = varsize(&k[VARHDRSZ..]);
    let lower = &k[VARHDRSZ..VARHDRSZ + lsize];
    if total > VARHDRSZ + lsize {
        let uoff = VARHDRSZ + intalign(lsize);
        let usize_ = varsize(&k[uoff..]);
        VarKey { lower, upper: &k[uoff..uoff + usize_], is_leaf_shape: false }
    } else {
        VarKey { lower, upper: lower, is_leaf_shape: true }
    }
}

// gbt_var_key_from_datum: leaf key wrapping one detoasted varlena.
pub fn key_from_datum(u: &[u8]) -> Vec<u8> {
    let lowersize = varsize(u);
    let mut r = vec![0u8; lowersize + VARHDRSZ];
    r[VARHDRSZ..].copy_from_slice(&u[..lowersize]);
    set_varsize(&mut r, lowersize + VARHDRSZ);
    r
}

// gbt_var_key_copy.
pub fn key_copy(lower: &[u8], upper: &[u8]) -> Vec<u8> {
    let lowersize = varsize(lower);
    let uppersize = varsize(upper);
    let mut r = vec![0u8; intalign(lowersize) + uppersize + VARHDRSZ];
    r[VARHDRSZ..VARHDRSZ + lowersize].copy_from_slice(&lower[..lowersize]);
    r[VARHDRSZ + intalign(lowersize)..VARHDRSZ + intalign(lowersize) + uppersize]
        .copy_from_slice(&upper[..uppersize]);
    set_varsize(&mut r, intalign(lowersize) + uppersize + VARHDRSZ);
    r
}

// gbt_var_leaf2node: returns the node image (owned) if the type transforms.
fn leaf2node<T: VarOps>(leaf: &[u8]) -> Option<Vec<u8>> {
    let r = key_readable(leaf);
    T::l2n(r.lower).map(|xfrm| key_copy(&xfrm, &xfrm))
}

// gbt_var_node_cp_len.
fn node_cp_len<T: VarOps>(node: &[u8]) -> PgResult<i32> {
    let r = key_readable(node);
    let t1len = varsize(r.lower) as i32 - VARHDRSZ as i32;
    let t2len = varsize(r.upper) as i32 - VARHDRSZ as i32;
    let ml = t1len.min(t2len);
    if ml == 0 {
        return Ok(0);
    }
    let p1 = &r.lower[VARHDRSZ..VARHDRSZ + t1len as usize];
    let p2 = &r.upper[VARHDRSZ..VARHDRSZ + t2len as usize];
    let eml = T::eml();
    let mut i: i32 = 0;
    let mut l_left_to_match: i32 = 0;
    let mut l_total: i32 = 0;
    while i < ml {
        let ui = i as usize;
        if eml > 1 && l_left_to_match == 0 {
            l_total = mbutils::pg_mblen_range(&p1[ui..])?;
            if l_total != mbutils::pg_mblen_range(&p2[ui..])? {
                return Ok(i);
            }
            l_left_to_match = l_total;
        }
        if p1[ui] != p2[ui] {
            if eml > 1 {
                let l_matched_subset = l_total - l_left_to_match;
                return Ok(i - l_matched_subset);
            }
            return Ok(i);
        }
        i += 1;
        l_left_to_match -= 1;
    }
    Ok(ml)
}

// gbt_bytea_pf_match.
fn bytea_pf_match(pf: &[u8], query: &[u8]) -> bool {
    let qlen = varsize(query) - VARHDRSZ;
    let nlen = varsize(pf) - VARHDRSZ;
    nlen <= qlen && query[VARHDRSZ..VARHDRSZ + nlen] == pf[VARHDRSZ..VARHDRSZ + nlen]
}

fn node_pf_match<T: VarOps>(key: &VarKey<'_>, query: &[u8]) -> bool {
    T::TRNC && (bytea_pf_match(key.lower, query) || bytea_pf_match(key.upper, query))
}

// gbt_var_node_truncate.
fn node_truncate(node: &[u8], cpf_length: i32) -> Vec<u8> {
    let r = key_readable(node);
    let len1 = (varsize(r.lower) as i32 - VARHDRSZ as i32).min(cpf_length + 1) as usize;
    let len2 = (varsize(r.upper) as i32 - VARHDRSZ as i32).min(cpf_length + 1) as usize;

    let si = 2 * VARHDRSZ + intalign(len1 + VARHDRSZ) + len2;
    let mut out = vec![0u8; si];
    set_varsize(&mut out, si);
    out[VARHDRSZ..VARHDRSZ + len1 + VARHDRSZ].copy_from_slice(&r.lower[..len1 + VARHDRSZ]);
    set_varsize(&mut out[VARHDRSZ..], len1 + VARHDRSZ);
    let o2 = VARHDRSZ + intalign(len1 + VARHDRSZ);
    out[o2..o2 + len2 + VARHDRSZ].copy_from_slice(&r.upper[..len2 + VARHDRSZ]);
    set_varsize(&mut out[o2..], len2 + VARHDRSZ);
    out
}

// gbt_var_bin_union; `u` is None before the first union.
pub fn bin_union<T: VarOps>(
    u: &mut Option<Vec<u8>>,
    e: &[u8],
    ctx: &mut Ctx,
) -> PgResult<()> {
    let node_img;
    let eo = {
        let r = key_readable(e);
        if r.is_leaf_shape {
            match leaf2node::<T>(e) {
                Some(n) => {
                    node_img = n;
                    key_readable(&node_img)
                }
                None => r,
            }
        } else {
            r
        }
    };

    let replacement = match u {
        Some(cur) => {
            let ro = key_readable(cur);
            let low = T::cmp(ro.lower, eo.lower, ctx)? > 0;
            let up = T::cmp(ro.upper, eo.upper, ctx)? < 0;
            if low || up {
                Some(key_copy(
                    if low { eo.lower } else { ro.lower },
                    if up { eo.upper } else { ro.upper },
                ))
            } else {
                None
            }
        }
        None => Some(key_copy(eo.lower, eo.upper)),
    };
    if let Some(img) = replacement {
        *u = Some(img);
    }
    Ok(())
}

// gbt_var_union.
pub fn union<T: VarOps>(keys: &[&[u8]], ctx: &mut Ctx) -> PgResult<Vec<u8>> {
    let r0 = key_readable(keys[0]);
    let mut out = Some(key_copy(r0.lower, r0.upper));
    for k in &keys[1..] {
        bin_union::<T>(&mut out, k, ctx)?;
    }
    let mut out = out.expect("union nonempty");
    if T::TRNC {
        let plen = node_cp_len::<T>(&out)?;
        out = node_truncate(&out, plen + 1);
    }
    Ok(out)
}

// gbt_var_same.
pub fn same<T: VarOps>(a: &[u8], b: &[u8], ctx: &mut Ctx) -> PgResult<bool> {
    let r1 = key_readable(a);
    let r2 = key_readable(b);
    Ok(T::cmp(r1.lower, r2.lower, ctx)? == 0 && T::cmp(r1.upper, r2.upper, ctx)? == 0)
}

// gbt_var_penalty.
pub fn penalty<T: VarOps>(
    orig: &[u8],
    new: &[u8],
    natts: u16,
    ctx: &mut Ctx,
) -> PgResult<f32> {
    let nk_img;
    let nk = {
        let r = key_readable(new);
        if r.is_leaf_shape {
            match leaf2node::<T>(new) {
                Some(n) => {
                    nk_img = n;
                    key_readable(&nk_img)
                }
                None => r,
            }
        } else {
            r
        }
    };
    let ok = key_readable(orig);

    if varsize(ok.lower) == VARHDRSZ && varsize(ok.upper) == VARHDRSZ {
        return Ok(0.0);
    }
    let inside = (T::cmp(nk.lower, ok.lower, ctx)? >= 0 || bytea_pf_match(ok.lower, nk.lower))
        && (T::cmp(nk.upper, ok.upper, ctx)? <= 0 || bytea_pf_match(ok.upper, nk.upper));
    if inside {
        return Ok(0.0);
    }

    let mut d: Option<Vec<u8>> = None;
    bin_union::<T>(&mut d, orig, ctx)?;
    let ol = node_cp_len::<T>(d.as_ref().expect("union set"))?;
    bin_union::<T>(&mut d, new, ctx)?;
    let d = d.expect("union set");
    let ul = node_cp_len::<T>(&d)?;

    let dres = if ul < ol {
        (ol - ul) as f64
    } else {
        let uk = key_readable(&d);
        let byte_at = |v: &[u8], at: i32| -> i32 {
            let len = varsize(v) as i32 - VARHDRSZ as i32;
            if len <= at {
                0
            } else {
                v[VARHDRSZ + at as usize] as i32
            }
        };
        let t0 = byte_at(ok.lower, ul);
        let t1 = byte_at(uk.lower, ul);
        let t2 = byte_at(ok.upper, ul);
        let t3 = byte_at(uk.upper, ul);
        ((t0 - t1).abs() + (t3 - t2).abs()) as f64 / 256.0
    };

    let mut res = 0.0f32;
    res += f32::MIN_POSITIVE;
    res += (dres / ((ol + 1) as f64)) as f32;
    res *= f32::MAX / (natts as f32 + 1.0);
    Ok(res)
}

// gbt_var_picksplit.
pub fn picksplit<T: VarOps>(
    keys: &[&[u8]],
    ctx: &mut Ctx,
) -> PgResult<(Vec<u16>, Vec<u16>, Vec<u8>, Vec<u8>)> {
    let maxoff = keys.len() - 1;
    // C leaf2node's temporaries (sv) live for the whole sort.
    let mut owned: Vec<Option<Vec<u8>>> = Vec::with_capacity(maxoff + 1);
    owned.push(None);
    for i in 1..=maxoff {
        let r = key_readable(keys[i]);
        owned.push(if r.is_leaf_shape { leaf2node::<T>(keys[i]) } else { None });
    }
    let mut arr: Vec<(u16, &[u8])> = (1..=maxoff)
        .map(|i| (i as u16, owned[i].as_deref().unwrap_or(keys[i])))
        .collect();

    {
        let ctx_cell = core::cell::RefCell::new(&mut *ctx);
        gistproc::qsort::pg_qsort(&mut arr, |a, b| {
            let ar = key_readable(a.1);
            let br = key_readable(b.1);
            let mut c = ctx_cell.borrow_mut();
            let r = T::cmp(ar.lower, br.lower, &mut c).and_then(|res| {
                if res == 0 {
                    T::cmp(ar.upper, br.upper, &mut c)
                } else {
                    Ok(res)
                }
            });
            match r {
                Ok(r) => r,
                Err(e) => std::panic::panic_any(e),
            }
        });
    }

    let mut spl_left = Vec::new();
    let mut spl_right = Vec::new();
    let mut ldatum: Option<Vec<u8>> = None;
    let mut rdatum: Option<Vec<u8>> = None;
    for (pos, &(off, key)) in arr.iter().enumerate() {
        if pos + 1 <= maxoff / 2 {
            bin_union::<T>(&mut ldatum, key, ctx)?;
            spl_left.push(off);
        } else {
            bin_union::<T>(&mut rdatum, key, ctx)?;
            spl_right.push(off);
        }
    }
    let mut ldatum = ldatum.expect("left group nonempty");
    let mut rdatum = rdatum.expect("right group nonempty");

    if T::TRNC {
        let ll = node_cp_len::<T>(&ldatum)?;
        let lr = node_cp_len::<T>(&rdatum)?;
        let ll = ll.max(lr) + 1;
        ldatum = node_truncate(&ldatum, ll);
        rdatum = node_truncate(&rdatum, ll);
    }
    Ok((spl_left, spl_right, ldatum, rdatum))
}

// gbt_var_consistent; `query` is a full detoasted 4B-header varlena image
// (already l2n-transformed by the caller for non-leaf bit keys).
pub fn consistent<T: VarOps>(
    key: &VarKey<'_>,
    query: &[u8],
    strategy: u16,
    is_leaf: bool,
    ctx: &mut Ctx,
) -> PgResult<bool> {
    use crate::num::{BT_EQUAL, BT_GREATER, BT_GREATER_EQUAL, BT_LESS, BT_LESS_EQUAL, BT_NOT_EQUAL};
    Ok(match strategy {
        BT_LESS_EQUAL | BT_LESS => {
            if is_leaf {
                if strategy == BT_LESS_EQUAL {
                    T::leaf_cmp(query, key.lower, ctx)? >= 0
                } else {
                    T::leaf_cmp(query, key.lower, ctx)? > 0
                }
            } else {
                T::cmp(query, key.lower, ctx)? >= 0 || node_pf_match::<T>(key, query)
            }
        }
        BT_EQUAL => {
            if is_leaf {
                T::leaf_cmp(query, key.lower, ctx)? == 0
            } else {
                (T::cmp(key.lower, query, ctx)? <= 0 && T::cmp(query, key.upper, ctx)? <= 0)
                    || node_pf_match::<T>(key, query)
            }
        }
        BT_GREATER | BT_GREATER_EQUAL => {
            if is_leaf {
                if strategy == BT_GREATER {
                    T::leaf_cmp(query, key.upper, ctx)? < 0
                } else {
                    T::leaf_cmp(query, key.upper, ctx)? <= 0
                }
            } else {
                T::cmp(query, key.upper, ctx)? <= 0 || node_pf_match::<T>(key, query)
            }
        }
        BT_NOT_EQUAL => {
            !(T::eq(query, key.lower, ctx)? && T::eq(query, key.upper, ctx)?)
        }
        _ => false,
    })
}
