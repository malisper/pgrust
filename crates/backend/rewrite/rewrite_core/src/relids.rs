//! Inline `ExprRelids` word-vector set algebra (nodes/bitmapset.c semantics),
//! over the lifetime-free [`ExprRelids`] (`{ words: Vec<u64> }`) carried on
//! `Var.varnullingrels` / `PlaceHolderVar.phrels` / `phnullingrels`.
//!
//! rewriteManip.c needs `bms_add_member`, `bms_next_member`, `bms_union`,
//! `bms_difference`, `bms_is_member`, `bms_overlap`, `bms_is_empty`, `bms_copy`,
//! `bms_del_member` over these relid sets. The canonical
//! [`nodes_core::bitmapset`] family operates on the `'mcx`-arena
//! `Bitmapset`; the `Var`/`PHV` relids are the lifetime-free analogue, so the
//! small algebra is reproduced faithfully here over the word storage (trailing
//! all-zero words trimmed so the empty set is `words == []`, matching the C
//! `Bitmapset *` whose NULL/empty pointer is the empty set).

use alloc::vec::Vec;
use nodes::primnodes::ExprRelids;

const BITS_PER_WORD: i32 = 64;

#[inline]
fn wordnum(x: i32) -> usize {
    (x / BITS_PER_WORD) as usize
}
#[inline]
fn bitnum(x: i32) -> u32 {
    (x % BITS_PER_WORD) as u32
}

/// Trim trailing all-zero words so `is_empty()` ⇔ `words.is_empty()` (canonical).
fn normalize(mut words: Vec<u64>) -> ExprRelids {
    while let Some(&last) = words.last() {
        if last == 0 {
            words.pop();
        } else {
            break;
        }
    }
    ExprRelids { words }
}

/// `bms_is_empty(a)`.
pub fn is_empty(a: &ExprRelids) -> bool {
    a.words.iter().all(|&w| w == 0)
}

/// `bms_copy(a)`.
pub fn copy(a: &ExprRelids) -> ExprRelids {
    normalize(a.words.clone())
}

/// `bms_is_member(x, a)`.
pub fn is_member(x: i32, a: &ExprRelids) -> bool {
    if x < 0 {
        panic!("negative bitmapset member not allowed");
    }
    let wnum = wordnum(x);
    if wnum >= a.words.len() {
        return false;
    }
    (a.words[wnum] & (1u64 << bitnum(x))) != 0
}

/// `bms_add_member(a, x)`.
pub fn add_member(mut a: ExprRelids, x: i32) -> ExprRelids {
    if x < 0 {
        panic!("negative bitmapset member not allowed");
    }
    let wnum = wordnum(x);
    if wnum >= a.words.len() {
        a.words.resize(wnum + 1, 0);
    }
    a.words[wnum] |= 1u64 << bitnum(x);
    a
}

/// `bms_del_member(a, x)`.
pub fn del_member(a: ExprRelids, x: i32) -> ExprRelids {
    if x < 0 {
        panic!("negative bitmapset member not allowed");
    }
    let wnum = wordnum(x);
    let mut words = a.words;
    if wnum < words.len() {
        words[wnum] &= !(1u64 << bitnum(x));
    }
    normalize(words)
}

/// `bms_union(a, b)` — a fresh set `a ∪ b` (inputs unchanged).
pub fn union(a: &ExprRelids, b: &ExprRelids) -> ExprRelids {
    let (longer, shorter) = if a.words.len() >= b.words.len() {
        (&a.words, &b.words)
    } else {
        (&b.words, &a.words)
    };
    let mut words = longer.clone();
    for (i, &w) in shorter.iter().enumerate() {
        words[i] |= w;
    }
    normalize(words)
}

/// `bms_difference(a, b)` — a fresh set `a \ b` (inputs unchanged).
pub fn difference(a: &ExprRelids, b: &ExprRelids) -> ExprRelids {
    let mut words = a.words.clone();
    for i in 0..words.len() {
        if i < b.words.len() {
            words[i] &= !b.words[i];
        }
    }
    normalize(words)
}

/// `bms_overlap(a, b)` — do the sets intersect?
pub fn overlap(a: &ExprRelids, b: &ExprRelids) -> bool {
    let n = a.words.len().min(b.words.len());
    for i in 0..n {
        if a.words[i] & b.words[i] != 0 {
            return true;
        }
    }
    false
}

/// `bms_next_member(a, prevbit)` — iterate set members in increasing order;
/// `prevbit` starts at -1, returns -2 when exhausted (mirrors bitmapset.c). Here
/// we return `Option<i32>` and the caller supplies the previous member.
pub fn next_member(a: &ExprRelids, prevbit: i32) -> Option<i32> {
    let mut bit = prevbit + 1;
    while (bit as usize / BITS_PER_WORD as usize) < a.words.len() {
        let wnum = wordnum(bit);
        let word = a.words[wnum] >> bitnum(bit);
        if word != 0 {
            return Some(bit + word.trailing_zeros() as i32);
        }
        // advance to start of next word
        bit = ((wnum + 1) as i32) * BITS_PER_WORD;
    }
    None
}
