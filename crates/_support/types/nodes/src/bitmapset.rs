use core::alloc::Layout;
use core::marker::PhantomData;
use core::ptr::NonNull;

use mcx::{check_alloc_size, Allocator, Mcx};
use types_error::PgResult;

#[allow(non_camel_case_types)]
pub type bitmapword = u64;

pub const BITS_PER_BITMAPWORD: usize = 64;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u32)]
pub enum BmsComparison {
    BmsEqual = 0,
    BmsSubset1 = 1,
    BmsSubset2 = 2,
    BmsDifferent = 3,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u32)]
pub enum BmsMembership {
    BmsEmptySet = 0,
    BmsSingleton = 1,
    BmsMultiple = 2,
}

#[inline(always)]
fn wordnum(x: i32) -> usize {
    x as usize / BITS_PER_BITMAPWORD
}

#[inline(always)]
fn bitnum(x: i32) -> usize {
    x as usize % BITS_PER_BITMAPWORD
}

#[inline(always)]
fn rightmost_one_pos(w: bitmapword) -> i32 {
    debug_assert!(w != 0);
    w.trailing_zeros() as i32
}

#[inline(always)]
fn leftmost_one_pos(w: bitmapword) -> i32 {
    debug_assert!(w != 0);
    63 - w.leading_zeros() as i32
}

#[inline(always)]
fn has_multiple_ones(w: bitmapword) -> bool {
    (w & w.wrapping_neg()) != w
}

#[cold]
#[inline(never)]
fn negative_member() -> ! {
    // C divergence: elog(ERROR, "negative bitmapset member not allowed").
    panic!("negative bitmapset member not allowed");
}

/// C `Bitmapset *`: the NULL pointer (empty set) is `nwords == 0`. Invariant
/// (matching PG 16+): a non-empty set never has a trailing zero word.
/// `awords` tracks the allocation size (C reads it from the chunk header).
pub struct Bitmapset<'mcx> {
    nwords: i32,
    awords: i32,
    words: NonNull<bitmapword>,
    _arena: PhantomData<&'mcx [bitmapword]>,
}

// SAFETY: no Drop; the word buffer is arena memory reclaimed at reset.
unsafe impl mcx::ArenaSafe for Bitmapset<'_> {}
// SAFETY: same — nothing to run, nothing to leak.
unsafe impl mcx::ForgetSafe for Bitmapset<'_> {}

const _: () = assert!(!core::mem::needs_drop::<Bitmapset<'static>>());
// 64-bit layout pin (fat pointer); wasm32 (ILP32) shrinks it.
#[cfg(not(target_family = "wasm"))]
const _: () = assert!(core::mem::size_of::<Bitmapset<'static>>() == 16);

impl<'mcx> Bitmapset<'mcx> {
    #[inline]
    pub const fn empty() -> Self {
        Bitmapset {
            nwords: 0,
            awords: 0,
            words: NonNull::dangling(),
            _arena: PhantomData,
        }
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.nwords == 0
    }

    #[inline]
    pub fn nwords(&self) -> usize {
        self.nwords as usize
    }

    #[inline]
    fn word_slice(&self) -> &[bitmapword] {
        // SAFETY: the first nwords words are initialized; dangling+0 is valid.
        unsafe { core::slice::from_raw_parts(self.words.as_ptr(), self.nwords as usize) }
    }

    #[inline]
    pub fn as_words(&self) -> &[bitmapword] {
        self.word_slice()
    }

    fn alloc_words(mcx: Mcx<'mcx>, n: usize) -> PgResult<NonNull<bitmapword>> {
        let bytes = n * core::mem::size_of::<bitmapword>();
        check_alloc_size(bytes)?;
        let layout = Layout::array::<bitmapword>(n).map_err(|_| crate::oom(mcx, bytes))?;
        Ok(Allocator::allocate(&mcx, layout)
            .map_err(|_| crate::oom(mcx, bytes))?
            .cast())
    }

    /// Grow the allocation to hold `n` words; words past `self.nwords` are
    /// zero-filled and `nwords` is set to `n` (bms_add_member's repalloc arm).
    /// Out of line: inlined, its frame taxes every caller's fast path.
    #[inline(never)]
    fn enlarge(&mut self, mcx: Mcx<'mcx>, n: usize) -> PgResult<()> {
        debug_assert!(n as i32 > self.nwords);
        if n as i32 > self.awords {
            let bytes = n * core::mem::size_of::<bitmapword>();
            check_alloc_size(bytes)?;
            let new_layout = Layout::array::<bitmapword>(n).map_err(|_| crate::oom(mcx, bytes))?;
            let ptr = if self.awords == 0 {
                Allocator::allocate(&mcx, new_layout)
            } else {
                let old_layout = Layout::array::<bitmapword>(self.awords as usize)
                    .expect("valid old layout");
                // SAFETY: words holds awords words from this arena with
                // old_layout; new_layout is strictly larger.
                unsafe { Allocator::grow(&mcx, self.words.cast(), old_layout, new_layout) }
            };
            self.words = ptr.map_err(|_| crate::oom(mcx, bytes))?.cast();
            self.awords = n as i32;
        }
        for i in self.nwords as usize..n {
            // SAFETY: i < awords; zero-fill the enlarged portion as C does.
            unsafe { self.words.as_ptr().add(i).write(0) };
        }
        self.nwords = n as i32;
        Ok(())
    }

    pub fn make_singleton(mcx: Mcx<'mcx>, x: i32) -> PgResult<Self> {
        if x < 0 {
            negative_member();
        }
        let wn = wordnum(x);
        let mut s = Self::empty();
        s.enlarge(mcx, wn + 1)?;
        // SAFETY: wn < nwords.
        unsafe { *s.words.as_ptr().add(wn) = 1 << bitnum(x) };
        Ok(s)
    }

    pub fn clone_in(&self, mcx: Mcx<'mcx>) -> PgResult<Self> {
        if self.is_empty() {
            return Ok(Self::empty());
        }
        let n = self.nwords as usize;
        let words = Self::alloc_words(mcx, n)?;
        // SAFETY: fresh disjoint buffer of n words.
        unsafe { core::ptr::copy_nonoverlapping(self.words.as_ptr(), words.as_ptr(), n) };
        Ok(Bitmapset { nwords: n as i32, awords: n as i32, words, _arena: PhantomData })
    }

    #[inline]
    pub fn equal(&self, other: &Self) -> bool {
        self.word_slice() == other.word_slice()
    }

    pub fn compare(&self, other: &Self) -> core::cmp::Ordering {
        use core::cmp::Ordering;
        if self.nwords != other.nwords {
            return self.nwords.cmp(&other.nwords);
        }
        let (a, b) = (self.word_slice(), other.word_slice());
        for i in (0..a.len()).rev() {
            if a[i] != b[i] {
                return a[i].cmp(&b[i]);
            }
        }
        Ordering::Equal
    }

    #[inline]
    pub fn is_member(&self, x: i32) -> bool {
        if x < 0 {
            negative_member();
        }
        let wn = wordnum(x);
        if wn >= self.nwords as usize {
            return false;
        }
        // SAFETY: wn < nwords.
        (unsafe { *self.words.as_ptr().add(wn) } & (1 << bitnum(x))) != 0
    }

    #[inline]
    pub fn add_member(&mut self, mcx: Mcx<'mcx>, x: i32) -> PgResult<()> {
        if x < 0 {
            negative_member();
        }
        let wn = wordnum(x);
        if wn >= self.nwords as usize {
            self.enlarge(mcx, wn + 1)?;
        }
        // SAFETY: wn < nwords.
        unsafe { *self.words.as_ptr().add(wn) |= 1 << bitnum(x) };
        Ok(())
    }

    pub fn add_range(&mut self, mcx: Mcx<'mcx>, lower: i32, upper: i32) -> PgResult<()> {
        if upper < lower {
            return Ok(());
        }
        if lower < 0 {
            negative_member();
        }
        let uwordnum = wordnum(upper);
        if uwordnum >= self.nwords as usize {
            self.enlarge(mcx, uwordnum + 1)?;
        }
        let lwordnum = wordnum(lower);
        let lbitnum = bitnum(lower);
        let ushiftbits = BITS_PER_BITMAPWORD - (bitnum(upper) + 1);
        let w = self.words.as_ptr();
        // SAFETY: lwordnum..=uwordnum < nwords after the enlarge above.
        unsafe {
            if lwordnum == uwordnum {
                *w.add(lwordnum) |=
                    !(((1 as bitmapword) << lbitnum) - 1) & ((!0 as bitmapword) >> ushiftbits);
            } else {
                *w.add(lwordnum) |= !(((1 as bitmapword) << lbitnum) - 1);
                for i in lwordnum + 1..uwordnum {
                    *w.add(i) = !0;
                }
                *w.add(uwordnum) |= (!0 as bitmapword) >> ushiftbits;
            }
        }
        Ok(())
    }

    pub fn del_member(&mut self, x: i32) {
        if x < 0 {
            negative_member();
        }
        let wn = wordnum(x);
        if wn >= self.nwords as usize {
            return;
        }
        // SAFETY: wn < nwords throughout; only nwords shrinks below.
        unsafe {
            let w = self.words.as_ptr();
            *w.add(wn) &= !(1 << bitnum(x));
            if *w.add(wn) == 0 && wn == self.nwords as usize - 1 {
                for i in (0..wn).rev() {
                    if *w.add(i) != 0 {
                        self.nwords = i as i32 + 1;
                        return;
                    }
                }
                self.nwords = 0;
            }
        }
    }

    pub fn union(&self, other: &Self, mcx: Mcx<'mcx>) -> PgResult<Self> {
        let (longer, shorter) =
            if self.nwords <= other.nwords { (other, self) } else { (self, other) };
        let result = longer.clone_in(mcx)?;
        let dst = result.words.as_ptr();
        for (i, &w) in shorter.word_slice().iter().enumerate() {
            // SAFETY: i < shorter.nwords <= result.nwords.
            unsafe { *dst.add(i) |= w };
        }
        Ok(result)
    }

    /// bms_add_members: recycling union into self.
    pub fn add_members(&mut self, mcx: Mcx<'mcx>, other: &Self) -> PgResult<()> {
        if other.nwords > self.nwords {
            self.enlarge(mcx, other.nwords as usize)?;
        }
        let dst = self.words.as_ptr();
        for (i, &w) in other.word_slice().iter().enumerate() {
            // SAFETY: i < other.nwords <= self.nwords.
            unsafe { *dst.add(i) |= w };
        }
        Ok(())
    }

    /// bms_join: recycling union — ORs the shorter input into the longer and
    /// returns the longer (C pfrees the shorter; arena memory just dies).
    pub fn join(a: Self, b: Self) -> Self {
        if a.is_empty() {
            return b;
        }
        if b.is_empty() {
            return a;
        }
        let (result, other) = if a.nwords < b.nwords { (b, a) } else { (a, b) };
        let dst = result.words.as_ptr();
        for (i, &w) in other.word_slice().iter().enumerate() {
            // SAFETY: i < other.nwords <= result.nwords.
            unsafe { *dst.add(i) |= w };
        }
        result
    }

    /// bms_replace_members: recycling assignment of other's members into self.
    pub fn replace_members(&mut self, mcx: Mcx<'mcx>, other: &Self) -> PgResult<()> {
        if other.is_empty() {
            *self = Self::empty();
            return Ok(());
        }
        if self.is_empty() {
            *self = other.clone_in(mcx)?;
            return Ok(());
        }
        if other.nwords > self.nwords {
            self.enlarge(mcx, other.nwords as usize)?;
        }
        // SAFETY: other.nwords <= self.awords after the enlarge above.
        unsafe {
            core::ptr::copy_nonoverlapping(
                other.words.as_ptr(),
                self.words.as_ptr(),
                other.nwords as usize,
            );
        }
        self.nwords = other.nwords;
        Ok(())
    }

    pub fn intersect(&self, other: &Self, mcx: Mcx<'mcx>) -> PgResult<Self> {
        if self.is_empty() || other.is_empty() {
            return Ok(Self::empty());
        }
        let (shorter, longer) =
            if self.nwords <= other.nwords { (self, other) } else { (other, self) };
        let mut result = shorter.clone_in(mcx)?;
        let dst = result.words.as_ptr();
        let mut lastnonzero: i32 = -1;
        for i in 0..result.nwords as usize {
            // SAFETY: i < result.nwords; longer has at least as many words.
            unsafe {
                *dst.add(i) &= *longer.words.as_ptr().add(i);
                if *dst.add(i) != 0 {
                    lastnonzero = i as i32;
                }
            }
        }
        result.nwords = lastnonzero + 1;
        Ok(result)
    }

    /// bms_int_members: recycling intersect into self.
    pub fn int_members(&mut self, other: &Self) {
        let shortlen = core::cmp::min(self.nwords, other.nwords) as usize;
        let dst = self.words.as_ptr();
        let mut lastnonzero: i32 = -1;
        for i in 0..shortlen {
            // SAFETY: i < shortlen <= both nwords.
            unsafe {
                *dst.add(i) &= *other.words.as_ptr().add(i);
                if *dst.add(i) != 0 {
                    lastnonzero = i as i32;
                }
            }
        }
        self.nwords = lastnonzero + 1;
    }

    pub fn difference(&self, other: &Self, mcx: Mcx<'mcx>) -> PgResult<Self> {
        // C fast path: an empty difference avoids the copy entirely.
        if !self.nonempty_difference(other) {
            return Ok(Self::empty());
        }
        let mut result = self.clone_in(mcx)?;
        result.del_members(other);
        Ok(result)
    }

    /// bms_del_members: recycling difference into self.
    pub fn del_members(&mut self, other: &Self) {
        let dst = self.words.as_ptr();
        if self.nwords > other.nwords {
            for (i, &w) in other.word_slice().iter().enumerate() {
                // SAFETY: i < other.nwords < self.nwords.
                unsafe { *dst.add(i) &= !w };
            }
        } else {
            let mut lastnonzero: i32 = -1;
            for i in 0..self.nwords as usize {
                // SAFETY: i < self.nwords <= other.nwords.
                unsafe {
                    *dst.add(i) &= !*other.words.as_ptr().add(i);
                    if *dst.add(i) != 0 {
                        lastnonzero = i as i32;
                    }
                }
            }
            self.nwords = lastnonzero + 1;
        }
    }

    pub fn nonempty_difference(&self, other: &Self) -> bool {
        if self.is_empty() {
            return false;
        }
        if self.nwords > other.nwords {
            return true;
        }
        self.word_slice()
            .iter()
            .zip(other.word_slice())
            .any(|(&a, &b)| a & !b != 0)
    }

    pub fn is_subset(&self, other: &Self) -> bool {
        if self.nwords > other.nwords {
            return false;
        }
        self.word_slice()
            .iter()
            .zip(other.word_slice())
            .all(|(&a, &b)| a & !b == 0)
    }

    pub fn subset_compare(&self, other: &Self) -> BmsComparison {
        let mut result = BmsComparison::BmsEqual;
        let shortlen = core::cmp::min(self.nwords, other.nwords) as usize;
        for i in 0..shortlen {
            // SAFETY: i < both nwords.
            let (aw, bw) = unsafe {
                (*self.words.as_ptr().add(i), *other.words.as_ptr().add(i))
            };
            if aw & !bw != 0 {
                if result == BmsComparison::BmsSubset1 {
                    return BmsComparison::BmsDifferent;
                }
                result = BmsComparison::BmsSubset2;
            }
            if bw & !aw != 0 {
                if result == BmsComparison::BmsSubset2 {
                    return BmsComparison::BmsDifferent;
                }
                result = BmsComparison::BmsSubset1;
            }
        }
        if self.nwords > other.nwords {
            if result == BmsComparison::BmsSubset1 {
                return BmsComparison::BmsDifferent;
            }
            return BmsComparison::BmsSubset2;
        }
        if self.nwords < other.nwords {
            if result == BmsComparison::BmsSubset2 {
                return BmsComparison::BmsDifferent;
            }
            return BmsComparison::BmsSubset1;
        }
        result
    }

    /// bms_member_index: 0-based index of x among the members, -1 if absent.
    pub fn member_index(&self, x: i32) -> i32 {
        if !self.is_member(x) {
            return -1;
        }
        let wn = wordnum(x);
        let mut result: i32 = 0;
        for &w in &self.word_slice()[..wn] {
            if w != 0 {
                result += w.count_ones() as i32;
            }
        }
        let mask = ((1 as bitmapword) << bitnum(x)) - 1;
        // SAFETY: wn < nwords since x is a member.
        result += (unsafe { *self.words.as_ptr().add(wn) } & mask).count_ones() as i32;
        result
    }

    pub fn overlap(&self, other: &Self) -> bool {
        self.word_slice()
            .iter()
            .zip(other.word_slice())
            .any(|(&a, &b)| a & b != 0)
    }


    pub fn overlap_list(&self, xs: &[i32]) -> bool {
        if self.is_empty() || xs.is_empty() {
            return false;
        }
        for &x in xs {
            if x < 0 {
                negative_member();
            }
            let wn = wordnum(x);
            if wn < self.nwords as usize
                // SAFETY: wn < nwords.
                && unsafe { *self.words.as_ptr().add(wn) } & ((1 as bitmapword) << bitnum(x)) != 0
            {
                return true;
            }
        }
        false
    }

    pub fn num_members(&self) -> i32 {
        self.word_slice().iter().map(|&w| w.count_ones() as i32).sum()
    }

    pub fn membership(&self) -> BmsMembership {
        let mut result = BmsMembership::BmsEmptySet;
        for &w in self.word_slice() {
            if w != 0 {
                if result != BmsMembership::BmsEmptySet || has_multiple_ones(w) {
                    return BmsMembership::BmsMultiple;
                }
                result = BmsMembership::BmsSingleton;
            }
        }
        result
    }

    pub fn get_singleton_member(&self) -> Option<i32> {
        let mut result: i32 = -1;
        for (i, &w) in self.word_slice().iter().enumerate() {
            if w != 0 {
                if result >= 0 || has_multiple_ones(w) {
                    return None;
                }
                result = (i * BITS_PER_BITMAPWORD) as i32 + rightmost_one_pos(w);
            }
        }
        if result >= 0 { Some(result) } else { None }
    }

    /// C shape: `x = -1; while ((x = bms_next_member(a, x)) >= 0) ...`;
    /// returns -2 when exhausted.
    #[inline]
    pub fn next_member(&self, prevbit: i32) -> i32 {
        let nwords = self.nwords as usize;
        let prevbit = prevbit + 1;
        let mut mask: bitmapword = !0 << bitnum(prevbit);
        for wn in wordnum(prevbit)..nwords {
            // SAFETY: wn < nwords.
            let w = unsafe { *self.words.as_ptr().add(wn) } & mask;
            if w != 0 {
                return (wn * BITS_PER_BITMAPWORD) as i32 + rightmost_one_pos(w);
            }
            mask = !0;
        }
        -2
    }

    pub fn prev_member(&self, prevbit: i32) -> i32 {
        if self.is_empty() || prevbit == 0 {
            return -2;
        }
        let prevbit = if prevbit == -1 {
            self.nwords * BITS_PER_BITMAPWORD as i32 - 1
        } else {
            prevbit - 1
        };
        let ushiftbits = BITS_PER_BITMAPWORD - (bitnum(prevbit) + 1);
        let mut mask: bitmapword = !0 >> ushiftbits;
        for wn in (0..=wordnum(prevbit)).rev() {
            // SAFETY: wn <= wordnum(prevbit) < nwords (prev_member contract:
            // prevbit is at most one above the highest representable bit).
            let w = unsafe { *self.words.as_ptr().add(wn) } & mask;
            if w != 0 {
                return (wn * BITS_PER_BITMAPWORD) as i32 + leftmost_one_pos(w);
            }
            mask = !0;
        }
        -2
    }

    pub fn iter(&self) -> BmsIter<'_, 'mcx> {
        BmsIter { bms: self, prev: -1 }
    }
}

pub struct BmsIter<'a, 'mcx> {
    bms: &'a Bitmapset<'mcx>,
    prev: i32,
}

impl Iterator for BmsIter<'_, '_> {
    type Item = i32;
    #[inline]
    fn next(&mut self) -> Option<i32> {
        let x = self.bms.next_member(self.prev);
        if x >= 0 {
            self.prev = x;
            Some(x)
        } else {
            None
        }
    }
}

impl Default for Bitmapset<'_> {
    fn default() -> Self {
        Bitmapset::empty()
    }
}

impl core::fmt::Debug for Bitmapset<'_> {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_set().entries(self.iter()).finish()
    }
}

impl PartialEq for Bitmapset<'_> {
    fn eq(&self, other: &Self) -> bool {
        self.equal(other)
    }
}
impl Eq for Bitmapset<'_> {}
