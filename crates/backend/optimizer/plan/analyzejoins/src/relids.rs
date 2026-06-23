//! Planner [`Relids`] (`Bitmapset *`) set algebra used by analyzejoins.c.
//!
//! The planner `Relids` is `Option<Box<Bitmapset>>` where the planner
//! [`pathnodes::Bitmapset`] is lifetime-free (`{ words: Vec<u64> }`,
//! `Clone`). The canonical [`nodes_core::bitmapset`] family operates on a
//! *different*, lifetime-bound arena `Bitmapset<'mcx>`, so the small algebra is
//! reproduced here over the word storage directly (mirroring the established
//! planner idiom in prepjointree/sublinks.rs and the joininfo unit test).
//!
//! Trailing all-zero words are trimmed so the empty set canonicalises to `None`,
//! matching the C `Bitmapset *` whose NULL pointer is the empty set.

use alloc::boxed::Box;

use pathnodes::{Bitmapset, Relids};

#[inline]
fn words(a: &Relids) -> &[u64] {
    match a {
        Some(b) => &b.words,
        None => &[],
    }
}

/// `bms_copy(a)`.
#[inline]
pub fn copy(a: &Relids) -> Relids {
    a.as_ref().map(|bms| Box::new((**bms).clone()))
}

/// `bms_is_member(x, a)`.
pub fn is_member(x: i32, a: &Relids) -> bool {
    if x < 0 {
        return false;
    }
    let w = words(a);
    let wn = (x / 64) as usize;
    wn < w.len() && (w[wn] >> (x % 64)) & 1 == 1
}

/// `bms_is_subset(a, b)` — every member of `a` is in `b`.
pub fn is_subset(a: &Relids, b: &Relids) -> bool {
    let aw = words(a);
    let bw = words(b);
    aw.iter()
        .enumerate()
        .all(|(i, &w)| w & !bw.get(i).copied().unwrap_or(0) == 0)
}

/// `bms_equal(a, b)`.
pub fn equal(a: &Relids, b: &Relids) -> bool {
    let aw = words(a);
    let bw = words(b);
    let n = aw.len().max(bw.len());
    (0..n).all(|i| aw.get(i).copied().unwrap_or(0) == bw.get(i).copied().unwrap_or(0))
}

/// `bms_overlap(a, b)`.
pub fn overlap(a: &Relids, b: &Relids) -> bool {
    let aw = words(a);
    let bw = words(b);
    let n = aw.len().min(bw.len());
    (0..n).any(|i| aw[i] & bw[i] != 0)
}

/// `bms_is_empty(a)`.
pub fn is_empty(a: &Relids) -> bool {
    words(a).iter().all(|&w| w == 0)
}

/// `bms_num_members(a)`.
pub fn num_members(a: &Relids) -> i32 {
    words(a).iter().map(|w| w.count_ones() as i32).sum()
}

/// `bms_membership(a) == BMS_MULTIPLE`.
#[inline]
pub fn membership_is_multiple(a: &Relids) -> bool {
    num_members(a) > 1
}

/// `bms_membership(a) == BMS_SINGLETON`.
#[inline]
pub fn membership_is_singleton(a: &Relids) -> bool {
    num_members(a) == 1
}

/// `bms_get_singleton_member(a, &out)` — `Some(member)` iff the set has exactly
/// one member.
pub fn get_singleton_member(a: &Relids) -> Option<i32> {
    let mut found: Option<i32> = None;
    for (wi, &word) in words(a).iter().enumerate() {
        let mut w = word;
        while w != 0 {
            let bit = w.trailing_zeros() as i32;
            let member = wi as i32 * 64 + bit;
            if found.is_some() {
                return None; // more than one
            }
            found = Some(member);
            w &= w - 1; // clear lowest set bit
        }
    }
    found
}

/// `bms_add_member(a, x)` — owned in-place set add.
pub fn add_member(a: Relids, x: i32) -> Relids {
    if x < 0 {
        panic!("negative bitmapset member not allowed");
    }
    let wn = (x / 64) as usize;
    let bit = 1u64 << (x % 64);
    let mut bms = a.unwrap_or_else(|| Box::new(Bitmapset::default()));
    if wn >= bms.words.len() {
        bms.words.resize(wn + 1, 0);
    }
    bms.words[wn] |= bit;
    Some(bms)
}

/// `bms_del_member(a, x)`.
pub fn del_member(a: Relids, x: i32) -> Relids {
    if x < 0 {
        panic!("negative bitmapset member not allowed");
    }
    let mut bms = match a {
        None => return None,
        Some(b) => b,
    };
    let wn = (x / 64) as usize;
    if wn < bms.words.len() {
        bms.words[wn] &= !(1u64 << (x % 64));
    }
    normalize(bms)
}

/// `bms_union(a, b)` — fresh union; inputs untouched.
pub fn union(a: &Relids, b: &Relids) -> Relids {
    join(copy(a), copy(b))
}

/// `bms_add_members(a, b)` — add all members of `b` into owned `a`.
pub fn add_members(a: Relids, b: &Relids) -> Relids {
    join(a, copy(b))
}

/// `bms_difference(a, b)` — fresh `a \ b`.
pub fn difference(a: &Relids, b: &Relids) -> Relids {
    let mut bms = match copy(a) {
        None => return None,
        Some(a) => a,
    };
    if let Some(b) = b.as_ref() {
        for i in 0..bms.words.len().min(b.words.len()) {
            bms.words[i] &= !b.words[i];
        }
    }
    normalize(bms)
}

/// `bms_make_singleton(x)`.
pub fn make_singleton(x: i32) -> Relids {
    add_member(None, x)
}

/// In-place owned union helper (consumes both).
fn join(a: Relids, b: Relids) -> Relids {
    let a = match a {
        None => return b,
        Some(a) => a,
    };
    let b = match b {
        None => return Some(a),
        Some(b) => b,
    };
    let (mut long, short) = if a.words.len() >= b.words.len() {
        (a, b)
    } else {
        (b, a)
    };
    for i in 0..short.words.len() {
        long.words[i] |= short.words[i];
    }
    Some(long)
}

/// Trim trailing all-zero words; an all-empty set becomes `None`.
fn normalize(mut bms: Box<Bitmapset>) -> Relids {
    while let Some(&last) = bms.words.last() {
        if last == 0 {
            bms.words.pop();
        } else {
            break;
        }
    }
    if bms.words.is_empty() {
        None
    } else {
        Some(bms)
    }
}

/// `adjust_relid_set(relids, oldrelid, newrelid)` (rewriteManip.c:760) over the
/// planner [`Relids`]: if `oldrelid` (a normal varno, not a special one) is a
/// member, replace it with `newrelid` (added only when `newrelid` is also a
/// normal varno; a negative/special `newrelid` means "just delete").
pub fn adjust_relid_set(set: &Relids, oldrelid: i32, newrelid: i32) -> Relids {
    // IS_SPECIAL_VARNO(varno) == ((int) varno < 0): the special varnos
    // (INNER_VAR/OUTER_VAR/INDEX_VAR/ROWID_VAR == -1..-4) are negative, as is the
    // left-join removal "delete only" sentinel; real RT indices are >= 1.
    let old_is_special = oldrelid < 0;
    let new_is_special = newrelid < 0;
    if !old_is_special && is_member(oldrelid, set) {
        let mut out = copy(set);
        out = del_member(out, oldrelid);
        if !new_is_special {
            out = add_member(out, newrelid);
        }
        out
    } else {
        copy(set)
    }
}
