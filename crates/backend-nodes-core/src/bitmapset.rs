//! KEYSTONE family — `nodes/bitmapset.c`, the `Bitmapset` set operations.
//!
//! This is the shared ABI/lifetime foundation the rest of the
//! `backend-nodes-core` unit (and a wall of already-merged executor/optimizer
//! units) compiles against: the owned `types_nodes::Bitmapset<'mcx>` and the
//! `bms_*` operations over it. It owns and installs `backend-nodes-core-seams`.
//!
//! ## Owned model
//!
//! C represents an empty set as a NULL `Bitmapset *` and a non-empty set as a
//! palloc'd struct whose `words[]` flexible array never has a trailing zero
//! word (so `nwords >= 1`). The owned model mirrors this exactly:
//!
//! * the C NULL set is Rust `None` (`Option<&Bitmapset>` / `Option<PgBox<…>>`);
//! * a non-empty set is `Bitmapset { words: PgVec<bitmapword> }` with no
//!   trailing zero word.
//!
//! Operations that "recycle" their input (the C reuses/extends `a` in place and
//! returns it) consume the owning `PgBox` and hand it back; read-only operations
//! borrow `&Bitmapset`. Growth allocates in `mcx`, so those ops are fallible
//! (`PgResult`); the C `elog(ERROR, "negative bitmapset member not allowed")`
//! is a caller bug and panics, matching the C surface.

use common_hashfn::hash_bytes;
use mcx::{Mcx, PgBox, PgVec};
use types_error::PgResult;
use types_nodes::bitmapset::{bitmapword, Bitmapset};

/// `BITS_PER_BITMAPWORD` (nodes/bitmapset.h): 64 on LP64.
const BITS_PER_BITMAPWORD: i32 = 64;

/// `WORDNUM(x)` — `x / BITS_PER_BITMAPWORD`.
#[inline]
fn wordnum(x: i32) -> usize {
    (x / BITS_PER_BITMAPWORD) as usize
}

/// `BITNUM(x)` — `x % BITS_PER_BITMAPWORD`.
#[inline]
fn bitnum(x: i32) -> u32 {
    (x % BITS_PER_BITMAPWORD) as u32
}

/// `bmw_popcount(w)` — number of set bits (C: `pg_popcount64`).
#[inline]
fn bmw_popcount(w: bitmapword) -> i32 {
    w.count_ones() as i32
}

/// `bmw_rightmost_one_pos(w)` — 0-based position of the lowest set bit
/// (C: `pg_rightmost_one_pos64`). `w` must be nonzero.
#[inline]
fn bmw_rightmost_one_pos(w: bitmapword) -> i32 {
    w.trailing_zeros() as i32
}

/// `bmw_leftmost_one_pos(w)` — 0-based position of the highest set bit
/// (C: `pg_leftmost_one_pos64`). `w` must be nonzero.
#[inline]
fn bmw_leftmost_one_pos(w: bitmapword) -> i32 {
    (BITS_PER_BITMAPWORD - 1) - w.leading_zeros() as i32
}

/// `HAS_MULTIPLE_ONES(x)` — more than one bit set.
#[inline]
fn has_multiple_ones(w: bitmapword) -> bool {
    w & w.wrapping_neg() != w
}

/// `BMS_Comparison` (nodes/bitmapset.h) — result of [`bms_subset_compare`].
#[allow(non_camel_case_types)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u32)]
pub enum BMS_Comparison {
    /// sets are equal
    BMS_EQUAL = 0,
    /// first set is a subset of the second
    BMS_SUBSET1 = 1,
    /// second set is a subset of the first
    BMS_SUBSET2 = 2,
    /// neither set is a subset of the other
    BMS_DIFFERENT = 3,
}

/// `BMS_Membership` (nodes/bitmapset.h) — result of [`bms_membership`].
#[allow(non_camel_case_types)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u32)]
pub enum BMS_Membership {
    /// 0 members
    BMS_EMPTY_SET = 0,
    /// 1 member
    BMS_SINGLETON = 1,
    /// >1 member
    BMS_MULTIPLE = 2,
}

/// `a->nwords` — number of words, or 0 for the C NULL set.
#[inline]
fn nwords(a: Option<&Bitmapset>) -> usize {
    a.map_or(0, |s| s.words.len())
}

/// Allocate an `nwords`-word zeroed set in `mcx` (C: `palloc0(BITMAPSET_SIZE)`).
fn alloc_zeroed<'mcx>(mcx: Mcx<'mcx>, n: usize) -> PgResult<PgBox<'mcx, Bitmapset<'mcx>>> {
    let mut words = mcx::vec_with_capacity_in::<bitmapword>(mcx, n)?;
    for _ in 0..n {
        words.push(0);
    }
    mcx::alloc_in(mcx, Bitmapset { words })
}

/// `bms_copy(a)` — a palloc'd duplicate of `a` (C NULL copies as `None`).
pub fn bms_copy<'mcx>(
    mcx: Mcx<'mcx>,
    a: Option<&Bitmapset>,
) -> PgResult<Option<PgBox<'mcx, Bitmapset<'mcx>>>> {
    match a {
        None => Ok(None),
        Some(s) => Ok(Some(mcx::alloc_in(
            mcx,
            Bitmapset {
                words: mcx::slice_in(mcx, &s.words)?,
            },
        )?)),
    }
}

/// `bms_equal(a, b)` — are two sets equal (or both NULL)?
pub fn bms_equal(a: Option<&Bitmapset>, b: Option<&Bitmapset>) -> bool {
    match (a, b) {
        (None, None) => true,
        (None, Some(_)) | (Some(_), None) => false,
        (Some(a), Some(b)) => a.words == b.words,
    }
}

/// `bms_compare(a, b)` — qsort-style comparator (equal iff `bms_equal`).
pub fn bms_compare(a: Option<&Bitmapset>, b: Option<&Bitmapset>) -> i32 {
    let (a, b) = match (a, b) {
        (None, None) => return 0,
        (None, Some(_)) => return -1,
        (Some(_), None) => return 1,
        (Some(a), Some(b)) => (a, b),
    };
    if a.words.len() != b.words.len() {
        return if a.words.len() > b.words.len() { 1 } else { -1 };
    }
    for i in (0..a.words.len()).rev() {
        let (aw, bw) = (a.words[i], b.words[i]);
        if aw != bw {
            return if aw > bw { 1 } else { -1 };
        }
    }
    0
}

/// `bms_make_singleton(x)` — a set containing the single member `x`.
pub fn bms_make_singleton<'mcx>(mcx: Mcx<'mcx>, x: i32) -> PgResult<PgBox<'mcx, Bitmapset<'mcx>>> {
    if x < 0 {
        panic!("negative bitmapset member not allowed");
    }
    let wnum = wordnum(x);
    let mut result = alloc_zeroed(mcx, wnum + 1)?;
    result.words[wnum] = (1 as bitmapword) << bitnum(x);
    Ok(result)
}

/// `bms_free(a)` — free the set (the owned model drops it; NULL is a no-op).
pub fn bms_free(_a: Option<PgBox<Bitmapset>>) {}

/// `bms_union(a, b)` — a new set with the union of the inputs.
pub fn bms_union<'mcx>(
    mcx: Mcx<'mcx>,
    a: Option<&Bitmapset>,
    b: Option<&Bitmapset>,
) -> PgResult<Option<PgBox<'mcx, Bitmapset<'mcx>>>> {
    let (a, b) = match (a, b) {
        (None, _) => return bms_copy(mcx, b),
        (_, None) => return bms_copy(mcx, a),
        (Some(a), Some(b)) => (a, b),
    };
    // Copy the longer; union the shorter into it.
    let (mut result, other) = if a.words.len() <= b.words.len() {
        (bms_copy(mcx, Some(b))?.unwrap(), a)
    } else {
        (bms_copy(mcx, Some(a))?.unwrap(), b)
    };
    for i in 0..other.words.len() {
        result.words[i] |= other.words[i];
    }
    Ok(Some(result))
}

/// `bms_intersect(a, b)` — a new set of the common members (NULL if empty).
pub fn bms_intersect<'mcx>(
    mcx: Mcx<'mcx>,
    a: Option<&Bitmapset>,
    b: Option<&Bitmapset>,
) -> PgResult<Option<PgBox<'mcx, Bitmapset<'mcx>>>> {
    let (a, b) = match (a, b) {
        (None, _) | (_, None) => return Ok(None),
        (Some(a), Some(b)) => (a, b),
    };
    // Copy the shorter; intersect the longer into it.
    let (mut result, other) = if a.words.len() <= b.words.len() {
        (bms_copy(mcx, Some(a))?.unwrap(), b)
    } else {
        (bms_copy(mcx, Some(b))?.unwrap(), a)
    };
    let resultlen = result.words.len();
    let mut lastnonzero: i32 = -1;
    for i in 0..resultlen {
        result.words[i] &= other.words[i];
        if result.words[i] != 0 {
            lastnonzero = i as i32;
        }
    }
    if lastnonzero == -1 {
        return Ok(None);
    }
    result.words.truncate((lastnonzero + 1) as usize);
    Ok(Some(result))
}

/// `bms_difference(a, b)` — a new set of `a` without `b`'s members.
pub fn bms_difference<'mcx>(
    mcx: Mcx<'mcx>,
    a: Option<&Bitmapset>,
    b: Option<&Bitmapset>,
) -> PgResult<Option<PgBox<'mcx, Bitmapset<'mcx>>>> {
    let a = match a {
        None => return Ok(None),
        Some(a) => a,
    };
    let b = match b {
        None => return bms_copy(mcx, Some(a)),
        Some(b) => b,
    };
    if !bms_nonempty_difference(Some(a), Some(b)) {
        return Ok(None);
    }
    let mut result = bms_copy(mcx, Some(a))?.unwrap();
    if result.words.len() > b.words.len() {
        for i in 0..b.words.len() {
            result.words[i] &= !b.words[i];
        }
    } else {
        let mut lastnonzero: i32 = -1;
        for i in 0..result.words.len() {
            result.words[i] &= !b.words[i];
            if result.words[i] != 0 {
                lastnonzero = i as i32;
            }
        }
        result.words.truncate((lastnonzero + 1) as usize);
    }
    Ok(Some(result))
}

/// `bms_is_subset(a, b)` — is A a subset of B?
pub fn bms_is_subset(a: Option<&Bitmapset>, b: Option<&Bitmapset>) -> bool {
    let a = match a {
        None => return true,
        Some(a) => a,
    };
    let b = match b {
        None => return false,
        Some(b) => b,
    };
    if a.words.len() > b.words.len() {
        return false;
    }
    for i in 0..a.words.len() {
        if a.words[i] & !b.words[i] != 0 {
            return false;
        }
    }
    true
}

/// `bms_subset_compare(a, b)` — equality/subset relationship in one pass.
pub fn bms_subset_compare(a: Option<&Bitmapset>, b: Option<&Bitmapset>) -> BMS_Comparison {
    use BMS_Comparison::*;
    let (a, b) = match (a, b) {
        (None, None) => return BMS_EQUAL,
        (None, Some(_)) => return BMS_SUBSET1,
        (Some(_), None) => return BMS_SUBSET2,
        (Some(a), Some(b)) => (a, b),
    };
    let mut result = BMS_EQUAL;
    let shortlen = a.words.len().min(b.words.len());
    for i in 0..shortlen {
        let aword = a.words[i];
        let bword = b.words[i];
        if aword & !bword != 0 {
            if result == BMS_SUBSET1 {
                return BMS_DIFFERENT;
            }
            result = BMS_SUBSET2;
        }
        if bword & !aword != 0 {
            if result == BMS_SUBSET2 {
                return BMS_DIFFERENT;
            }
            result = BMS_SUBSET1;
        }
    }
    if a.words.len() > b.words.len() {
        if result == BMS_SUBSET1 {
            return BMS_DIFFERENT;
        }
        return BMS_SUBSET2;
    } else if a.words.len() < b.words.len() {
        if result == BMS_SUBSET2 {
            return BMS_DIFFERENT;
        }
        return BMS_SUBSET1;
    }
    result
}

/// `bms_is_member(x, a)` — is `x` a member of `a`?
pub fn bms_is_member(x: i32, a: Option<&Bitmapset>) -> bool {
    if x < 0 {
        panic!("negative bitmapset member not allowed");
    }
    let a = match a {
        None => return false,
        Some(a) => a,
    };
    let wnum = wordnum(x);
    if wnum >= a.words.len() {
        return false;
    }
    a.words[wnum] & ((1 as bitmapword) << bitnum(x)) != 0
}

/// `bms_member_index(a, x)` — 0-based index of `x`, or -1 if not a member.
pub fn bms_member_index(a: Option<&Bitmapset>, x: i32) -> i32 {
    if !bms_is_member(x, a) {
        return -1;
    }
    let a = a.unwrap();
    let wnum = wordnum(x);
    let bnum = bitnum(x);
    let mut result = 0;
    for i in 0..wnum {
        let w = a.words[i];
        if w != 0 {
            result += bmw_popcount(w);
        }
    }
    let mask = ((1 as bitmapword) << bnum) - 1;
    result += bmw_popcount(a.words[wnum] & mask);
    result
}

/// `bms_overlap(a, b)` — do the sets have a common member?
pub fn bms_overlap(a: Option<&Bitmapset>, b: Option<&Bitmapset>) -> bool {
    let (a, b) = match (a, b) {
        (Some(a), Some(b)) => (a, b),
        _ => return false,
    };
    let shortlen = a.words.len().min(b.words.len());
    for i in 0..shortlen {
        if a.words[i] & b.words[i] != 0 {
            return true;
        }
    }
    false
}

/// `bms_overlap_list(a, b)` — does the set overlap an integer list?
pub fn bms_overlap_list(a: Option<&Bitmapset>, b: &[i32]) -> bool {
    let a = match a {
        None => return false,
        Some(a) => a,
    };
    for &x in b {
        if x < 0 {
            panic!("negative bitmapset member not allowed");
        }
        let wnum = wordnum(x);
        if wnum < a.words.len() && a.words[wnum] & ((1 as bitmapword) << bitnum(x)) != 0 {
            return true;
        }
    }
    false
}

/// `bms_nonempty_difference(a, b)` — does `a` have a member not in `b`?
pub fn bms_nonempty_difference(a: Option<&Bitmapset>, b: Option<&Bitmapset>) -> bool {
    let a = match a {
        None => return false,
        Some(a) => a,
    };
    let b = match b {
        None => return true,
        Some(b) => b,
    };
    if a.words.len() > b.words.len() {
        return true;
    }
    for i in 0..a.words.len() {
        if a.words[i] & !b.words[i] != 0 {
            return true;
        }
    }
    false
}

/// `bms_singleton_member(a)` — the sole member; panics if |a| != 1.
pub fn bms_singleton_member(a: Option<&Bitmapset>) -> i32 {
    let a = match a {
        None => panic!("bitmapset is empty"),
        Some(a) => a,
    };
    let mut result: i32 = -1;
    for wnum in 0..a.words.len() {
        let w = a.words[wnum];
        if w != 0 {
            if result >= 0 || has_multiple_ones(w) {
                panic!("bitmapset has multiple members");
            }
            result = wnum as i32 * BITS_PER_BITMAPWORD + bmw_rightmost_one_pos(w);
        }
    }
    result
}

/// `bms_get_singleton_member(a, member)` — `Some(member)` iff `a` is a singleton.
pub fn bms_get_singleton_member(a: Option<&Bitmapset>) -> Option<i32> {
    let a = a?;
    let mut result: i32 = -1;
    for wnum in 0..a.words.len() {
        let w = a.words[wnum];
        if w != 0 {
            if result >= 0 || has_multiple_ones(w) {
                return None;
            }
            result = wnum as i32 * BITS_PER_BITMAPWORD + bmw_rightmost_one_pos(w);
        }
    }
    Some(result)
}

/// `bms_num_members(a)` — count of members.
pub fn bms_num_members(a: Option<&Bitmapset>) -> i32 {
    let a = match a {
        None => return 0,
        Some(a) => a,
    };
    let mut result = 0;
    for &w in a.words.iter() {
        if w != 0 {
            result += bmw_popcount(w);
        }
    }
    result
}

/// `bms_membership(a)` — zero, one, or multiple members?
pub fn bms_membership(a: Option<&Bitmapset>) -> BMS_Membership {
    use BMS_Membership::*;
    let a = match a {
        None => return BMS_EMPTY_SET,
        Some(a) => a,
    };
    let mut result = BMS_EMPTY_SET;
    for &w in a.words.iter() {
        if w != 0 {
            if result != BMS_EMPTY_SET || has_multiple_ones(w) {
                return BMS_MULTIPLE;
            }
            result = BMS_SINGLETON;
        }
    }
    result
}

/// `bms_add_member(a, x)` — add `x`, recycling `a`.
pub fn bms_add_member<'mcx>(
    mcx: Mcx<'mcx>,
    a: Option<PgBox<'mcx, Bitmapset<'mcx>>>,
    x: i32,
) -> PgResult<PgBox<'mcx, Bitmapset<'mcx>>> {
    if x < 0 {
        panic!("negative bitmapset member not allowed");
    }
    let mut a = match a {
        None => return bms_make_singleton(mcx, x),
        Some(a) => a,
    };
    let wnum = wordnum(x);
    if wnum >= a.words.len() {
        // enlarge, zero-filling the new words (C: repalloc + zero)
        let mut grown = mcx::vec_with_capacity_in::<bitmapword>(mcx, wnum + 1)?;
        for &w in a.words.iter() {
            grown.push(w);
        }
        while grown.len() < wnum + 1 {
            grown.push(0);
        }
        a.words = grown;
    }
    a.words[wnum] |= (1 as bitmapword) << bitnum(x);
    Ok(a)
}

/// `bms_del_member(a, x)` — remove `x`, recycling `a` (trims trailing zeros).
pub fn bms_del_member<'mcx>(
    a: Option<PgBox<'mcx, Bitmapset<'mcx>>>,
    x: i32,
) -> Option<PgBox<'mcx, Bitmapset<'mcx>>> {
    if x < 0 {
        panic!("negative bitmapset member not allowed");
    }
    let mut a = a?;
    let wnum = wordnum(x);
    if wnum >= a.words.len() {
        return Some(a);
    }
    a.words[wnum] &= !((1 as bitmapword) << bitnum(x));
    if a.words[wnum] == 0 && wnum == a.words.len() - 1 {
        for i in (0..wnum).rev() {
            if a.words[i] != 0 {
                a.words.truncate(i + 1);
                return Some(a);
            }
        }
        return None;
    }
    Some(a)
}

/// `bms_add_members(a, b)` — like `bms_union`, recycling `a`.
pub fn bms_add_members<'mcx>(
    mcx: Mcx<'mcx>,
    a: Option<PgBox<'mcx, Bitmapset<'mcx>>>,
    b: Option<&Bitmapset>,
) -> PgResult<Option<PgBox<'mcx, Bitmapset<'mcx>>>> {
    let a = match a {
        None => return bms_copy(mcx, b),
        Some(a) => a,
    };
    let b = match b {
        None => return Ok(Some(a)),
        Some(b) => b,
    };
    if a.words.len() < b.words.len() {
        // copy the longer (b), union a into it
        let mut result = bms_copy(mcx, Some(b))?.unwrap();
        for i in 0..a.words.len() {
            result.words[i] |= a.words[i];
        }
        Ok(Some(result))
    } else {
        let mut a = a;
        for i in 0..b.words.len() {
            a.words[i] |= b.words[i];
        }
        Ok(Some(a))
    }
}

/// `bms_replace_members(a, b)` — drop `a`'s members, repopulate from `b`.
pub fn bms_replace_members<'mcx>(
    mcx: Mcx<'mcx>,
    a: Option<PgBox<'mcx, Bitmapset<'mcx>>>,
    b: Option<&Bitmapset>,
) -> PgResult<Option<PgBox<'mcx, Bitmapset<'mcx>>>> {
    let a = match a {
        None => return bms_copy(mcx, b),
        Some(a) => a,
    };
    let b = match b {
        None => return Ok(None),
        Some(b) => b,
    };
    let mut a = a;
    // Resize a's storage to exactly b->nwords, copying b's words.
    let mut words = mcx::vec_with_capacity_in::<bitmapword>(mcx, b.words.len())?;
    for &w in b.words.iter() {
        words.push(w);
    }
    a.words = words;
    Ok(Some(a))
}

/// `bms_add_range(a, lower, upper)` — add `[lower, upper]`, recycling `a`.
pub fn bms_add_range<'mcx>(
    mcx: Mcx<'mcx>,
    a: Option<PgBox<'mcx, Bitmapset<'mcx>>>,
    lower: i32,
    upper: i32,
) -> PgResult<Option<PgBox<'mcx, Bitmapset<'mcx>>>> {
    if upper < lower {
        return Ok(a);
    }
    if lower < 0 {
        panic!("negative bitmapset member not allowed");
    }
    let uwordnum = wordnum(upper);
    let mut a = match a {
        None => alloc_zeroed(mcx, uwordnum + 1)?,
        Some(mut a) => {
            if uwordnum >= a.words.len() {
                let mut grown = mcx::vec_with_capacity_in::<bitmapword>(mcx, uwordnum + 1)?;
                for &w in a.words.iter() {
                    grown.push(w);
                }
                while grown.len() < uwordnum + 1 {
                    grown.push(0);
                }
                a.words = grown;
            }
            a
        }
    };

    let lwordnum = wordnum(lower);
    let lbitnum = bitnum(lower);
    let ushiftbits = (BITS_PER_BITMAPWORD - (bitnum(upper) as i32 + 1)) as u32;

    if lwordnum == uwordnum {
        a.words[lwordnum] |=
            !(((1 as bitmapword) << lbitnum) - 1) & ((!0 as bitmapword) >> ushiftbits);
    } else {
        let mut w = lwordnum;
        a.words[w] |= !(((1 as bitmapword) << lbitnum) - 1);
        w += 1;
        while w < uwordnum {
            a.words[w] = !0 as bitmapword;
            w += 1;
        }
        a.words[uwordnum] |= (!0 as bitmapword) >> ushiftbits;
    }
    Ok(Some(a))
}

/// `bms_int_members(a, b)` — like `bms_intersect`, recycling `a`.
pub fn bms_int_members<'mcx>(
    a: Option<PgBox<'mcx, Bitmapset<'mcx>>>,
    b: Option<&Bitmapset>,
) -> Option<PgBox<'mcx, Bitmapset<'mcx>>> {
    let mut a = a?;
    let b = match b {
        None => return None,
        Some(b) => b,
    };
    let shortlen = a.words.len().min(b.words.len());
    let mut lastnonzero: i32 = -1;
    for i in 0..shortlen {
        a.words[i] &= b.words[i];
        if a.words[i] != 0 {
            lastnonzero = i as i32;
        }
    }
    if lastnonzero == -1 {
        return None;
    }
    a.words.truncate((lastnonzero + 1) as usize);
    Some(a)
}

/// `bms_del_members(a, b)` — delete `b`'s members from `a`, recycling `a`.
pub fn bms_del_members<'mcx>(
    a: Option<PgBox<'mcx, Bitmapset<'mcx>>>,
    b: Option<&Bitmapset>,
) -> Option<PgBox<'mcx, Bitmapset<'mcx>>> {
    let a = match a {
        None => return None,
        Some(a) => a,
    };
    let b = match b {
        None => return Some(a),
        Some(b) => b,
    };
    let mut a = a;
    if a.words.len() > b.words.len() {
        for i in 0..b.words.len() {
            a.words[i] &= !b.words[i];
        }
    } else {
        let mut lastnonzero: i32 = -1;
        for i in 0..a.words.len() {
            a.words[i] &= !b.words[i];
            if a.words[i] != 0 {
                lastnonzero = i as i32;
            }
        }
        if lastnonzero == -1 {
            return None;
        }
        a.words.truncate((lastnonzero + 1) as usize);
    }
    Some(a)
}

/// `bms_join(a, b)` — union, recycling *either* input (both consumed).
pub fn bms_join<'mcx>(
    a: Option<PgBox<'mcx, Bitmapset<'mcx>>>,
    b: Option<PgBox<'mcx, Bitmapset<'mcx>>>,
) -> Option<PgBox<'mcx, Bitmapset<'mcx>>> {
    let a = match a {
        None => return b,
        Some(a) => a,
    };
    let b = match b {
        None => return Some(a),
        Some(b) => b,
    };
    // Use the longer as result; union the shorter into it.
    let (mut result, other) = if a.words.len() < b.words.len() {
        (b, a)
    } else {
        (a, b)
    };
    for i in 0..other.words.len() {
        result.words[i] |= other.words[i];
    }
    Some(result)
}

/// `bms_next_member(a, prevbit)` — smallest member > `prevbit`, or -2.
pub fn bms_next_member(a: Option<&Bitmapset>, prevbit: i32) -> i32 {
    let a = match a {
        None => return -2,
        Some(a) => a,
    };
    let nwords = a.words.len();
    let prevbit = prevbit + 1;
    let mut mask = (!0 as bitmapword) << bitnum(prevbit);
    let mut wnum = wordnum(prevbit);
    while wnum < nwords {
        let w = a.words[wnum] & mask;
        if w != 0 {
            return wnum as i32 * BITS_PER_BITMAPWORD + bmw_rightmost_one_pos(w);
        }
        mask = !0 as bitmapword;
        wnum += 1;
    }
    -2
}

/// `bms_prev_member(a, prevbit)` — largest member < `prevbit`, or -2.
pub fn bms_prev_member(a: Option<&Bitmapset>, prevbit: i32) -> i32 {
    let a = match a {
        None => return -2,
        Some(a) => a,
    };
    if prevbit == 0 {
        return -2;
    }
    let prevbit = if prevbit == -1 {
        a.words.len() as i32 * BITS_PER_BITMAPWORD - 1
    } else {
        prevbit - 1
    };
    let ushiftbits = (BITS_PER_BITMAPWORD - (bitnum(prevbit) as i32 + 1)) as u32;
    let mut mask = (!0 as bitmapword) >> ushiftbits;
    let mut wnum = wordnum(prevbit) as i32;
    while wnum >= 0 {
        let w = a.words[wnum as usize] & mask;
        if w != 0 {
            return wnum * BITS_PER_BITMAPWORD + bmw_leftmost_one_pos(w);
        }
        mask = !0 as bitmapword;
        wnum -= 1;
    }
    -2
}

/// `bms_hash_value(a)` — a hash key for a Bitmapset (empty sets hash to 0).
pub fn bms_hash_value(a: Option<&Bitmapset>) -> u32 {
    let a = match a {
        None => return 0,
        Some(a) => a,
    };
    // C: hash_any over the words byte range (nwords * sizeof(bitmapword)).
    let mut bytes = Vec::with_capacity(a.words.len() * core::mem::size_of::<bitmapword>());
    for &w in a.words.iter() {
        bytes.extend_from_slice(&w.to_ne_bytes());
    }
    hash_bytes(&bytes)
}

/// `bitmap_match(key1, key2)` — dynahash match fn: 0 iff equal (C: `!bms_equal`).
pub fn bitmap_match(key1: Option<&Bitmapset>, key2: Option<&Bitmapset>) -> i32 {
    !bms_equal(key1, key2) as i32
}

// `bms_is_empty(a)` — convenience used by the executor (a NULL set is empty).
/// `bms_is_empty(a)`.
pub fn bms_is_empty(a: Option<&Bitmapset>) -> bool {
    nwords(a) == 0
}

#[cfg(test)]
mod tests {
    use super::*;

    fn set_of<'mcx>(mcx: Mcx<'mcx>, members: &[i32]) -> Option<PgBox<'mcx, Bitmapset<'mcx>>> {
        let mut a = None;
        for &m in members {
            a = Some(bms_add_member(mcx, a, m).unwrap());
        }
        a
    }

    #[test]
    fn membership_and_iteration() {
        let ctx = mcx::MemoryContext::new("t");
        let mcx = ctx.mcx();
        let a = set_of(mcx, &[1, 64, 130]);
        let r = a.as_deref();
        assert!(bms_is_member(1, r));
        assert!(bms_is_member(64, r));
        assert!(bms_is_member(130, r));
        assert!(!bms_is_member(2, r));
        assert_eq!(bms_num_members(r), 3);
        // forward iteration
        let mut x = -1;
        let mut seen = Vec::new();
        loop {
            x = bms_next_member(r, x);
            if x < 0 {
                break;
            }
            seen.push(x);
        }
        assert_eq!(seen, vec![1, 64, 130]);
        // reverse
        let mut x = -1;
        let mut rev = Vec::new();
        loop {
            x = bms_prev_member(r, x);
            if x < 0 {
                break;
            }
            rev.push(x);
        }
        assert_eq!(rev, vec![130, 64, 1]);
    }

    #[test]
    fn union_intersect_difference_equal() {
        let ctx = mcx::MemoryContext::new("t");
        let mcx = ctx.mcx();
        let a = set_of(mcx, &[1, 2, 100]);
        let b = set_of(mcx, &[2, 3, 100]);
        let u = bms_union(mcx, a.as_deref(), b.as_deref()).unwrap();
        assert_eq!(bms_num_members(u.as_deref()), 4);
        let i = bms_intersect(mcx, a.as_deref(), b.as_deref()).unwrap();
        assert_eq!(bms_num_members(i.as_deref()), 2);
        assert!(bms_is_member(2, i.as_deref()));
        assert!(bms_is_member(100, i.as_deref()));
        let d = bms_difference(mcx, a.as_deref(), b.as_deref()).unwrap();
        assert_eq!(bms_num_members(d.as_deref()), 1);
        assert!(bms_is_member(1, d.as_deref()));
        // an empty difference collapses to None
        let d2 = bms_difference(mcx, i.as_deref(), u.as_deref()).unwrap();
        assert!(d2.is_none());
        let a2 = set_of(mcx, &[100, 2, 1]);
        assert!(bms_equal(a.as_deref(), a2.as_deref()));
        assert!(!bms_equal(a.as_deref(), b.as_deref()));
    }

    #[test]
    fn del_member_trims_trailing_zero_words() {
        let ctx = mcx::MemoryContext::new("t");
        let mcx = ctx.mcx();
        let a = set_of(mcx, &[5, 200]);
        let a = bms_del_member(a, 200);
        assert_eq!(a.as_ref().unwrap().words.len(), 1);
        assert!(bms_is_member(5, a.as_deref()));
        let a = bms_del_member(a, 5);
        assert!(a.is_none()); // emptied -> NULL set
    }

    #[test]
    fn add_range_and_singleton() {
        let ctx = mcx::MemoryContext::new("t");
        let mcx = ctx.mcx();
        let a = bms_add_range(mcx, None, 60, 70).unwrap();
        assert_eq!(bms_num_members(a.as_deref()), 11);
        assert!(bms_is_member(60, a.as_deref()));
        assert!(bms_is_member(70, a.as_deref()));
        assert!(!bms_is_member(59, a.as_deref()));
        assert!(!bms_is_member(71, a.as_deref()));
        let s = set_of(mcx, &[42]);
        assert_eq!(bms_get_singleton_member(s.as_deref()), Some(42));
        assert_eq!(bms_membership(s.as_deref()), BMS_Membership::BMS_SINGLETON);
    }
}
