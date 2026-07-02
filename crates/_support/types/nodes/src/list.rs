use core::alloc::Layout;
use core::marker::PhantomData;
use core::ptr::NonNull;

use mcx::{check_alloc_size, Allocator, Mcx};
use types_core::{Oid, TransactionId};
use types_error::PgResult;

use crate::node_tree::Node;
use crate::tags::NodeTag;

pub trait ListFlavor {
    type Cell<'mcx>: Copy + 'mcx;
    const TAG: NodeTag;
}

pub enum PtrFlavor {}
pub enum IntFlavor {}
pub enum OidFlavor {}
pub enum XidFlavor {}

impl ListFlavor for PtrFlavor {
    type Cell<'mcx> = Node<'mcx>;
    const TAG: NodeTag = NodeTag::T_List;
}
impl ListFlavor for IntFlavor {
    type Cell<'mcx> = i32;
    const TAG: NodeTag = NodeTag::T_IntList;
}
impl ListFlavor for OidFlavor {
    type Cell<'mcx> = Oid;
    const TAG: NodeTag = NodeTag::T_OidList;
}
impl ListFlavor for XidFlavor {
    type Cell<'mcx> = TransactionId;
    const TAG: NodeTag = NodeTag::T_XidList;
}

// C divergence: NIL is length == 0 (C uses a NULL pointer); the list flavor is
// the type parameter, not a stored tag; int/oid/xid cells are 4 bytes, not a
// pointer-wide union.
pub struct List<'mcx, F: ListFlavor> {
    length: i32,
    max_length: i32,
    elements: NonNull<F::Cell<'mcx>>,
    _flavor: PhantomData<F>,
}

pub type NodeList<'mcx> = List<'mcx, PtrFlavor>;
pub type IntList<'mcx> = List<'mcx, IntFlavor>;
pub type OidList<'mcx> = List<'mcx, OidFlavor>;
pub type XidList<'mcx> = List<'mcx, XidFlavor>;

// SAFETY: no Drop anywhere; the cell buffer is arena memory reclaimed at reset.
unsafe impl<'mcx, F: ListFlavor> mcx::ArenaSafe for List<'mcx, F> {}

const _: () = assert!(!core::mem::needs_drop::<NodeList<'static>>());
const _: () = assert!(core::mem::size_of::<NodeList<'static>>() == 16);

// list.c LIST_HEADER_OVERHEAD: 24-byte header / 8-byte ListCell on 64-bit.
const LIST_HEADER_OVERHEAD: i32 = 3;

#[inline]
fn nextpower2(v: i32) -> i32 {
    (v as u32).next_power_of_two() as i32
}

impl<'mcx, F: ListFlavor> List<'mcx, F> {
    #[inline]
    pub const fn nil() -> Self {
        List {
            length: 0,
            max_length: 0,
            elements: NonNull::dangling(),
            _flavor: PhantomData,
        }
    }

    #[inline]
    pub const fn tag(&self) -> NodeTag {
        F::TAG
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.length as usize
    }

    #[inline]
    pub fn is_nil(&self) -> bool {
        self.length == 0
    }

    #[inline]
    pub fn capacity(&self) -> usize {
        self.max_length as usize
    }

    #[inline]
    pub fn as_slice(&self) -> &[F::Cell<'mcx>] {
        // SAFETY: the first `length` cells are initialized; dangling+0 is valid.
        unsafe { core::slice::from_raw_parts(self.elements.as_ptr(), self.length as usize) }
    }

    #[inline]
    pub fn as_mut_slice(&mut self) -> &mut [F::Cell<'mcx>] {
        // SAFETY: as as_slice; &mut self gives exclusive access to the cells.
        unsafe { core::slice::from_raw_parts_mut(self.elements.as_ptr(), self.length as usize) }
    }

    #[inline]
    pub fn iter(&self) -> core::iter::Copied<core::slice::Iter<'_, F::Cell<'mcx>>> {
        self.as_slice().iter().copied()
    }

    #[inline]
    pub fn nth(&self, n: usize) -> F::Cell<'mcx> {
        self.as_slice()[n]
    }

    /// # Safety: `n < self.len()` (C list_nth checks only under Assert).
    #[inline]
    pub unsafe fn nth_unchecked(&self, n: usize) -> F::Cell<'mcx> {
        debug_assert!(n < self.len());
        unsafe { *self.elements.as_ptr().add(n) }
    }

    #[inline]
    pub fn first(&self) -> Option<F::Cell<'mcx>> {
        self.as_slice().first().copied()
    }

    #[inline]
    pub fn last(&self) -> Option<F::Cell<'mcx>> {
        self.as_slice().last().copied()
    }

    // new_list(1) shape: a first append always allocates the constant initial
    // capacity, so the size formula folds away.
    #[inline(never)]
    fn first_cell_alloc(&mut self, mcx: Mcx<'mcx>) -> PgResult<()> {
        const FIRST_CAP: usize = 5;
        debug_assert!(self.max_length == 0);
        let layout = Layout::array::<F::Cell<'mcx>>(FIRST_CAP).expect("const layout");
        let ptr = Allocator::allocate(&mcx, layout).map_err(|_| crate::oom(mcx, layout.size()))?;
        self.elements = ptr.cast();
        self.max_length = FIRST_CAP as i32;
        Ok(())
    }

    #[inline]
    fn grow_for_append(&mut self, mcx: Mcx<'mcx>) -> PgResult<()> {
        if self.max_length == 0 {
            self.first_cell_alloc(mcx)
        } else {
            self.enlarge(mcx, self.length + 1)
        }
    }

    // Out of line: inlined, its frame taxes every lappend fast path.
    #[inline(never)]
    fn enlarge(&mut self, mcx: Mcx<'mcx>, min_size: i32) -> PgResult<()> {
        debug_assert!(min_size > self.max_length);
        let cell = core::mem::size_of::<F::Cell<'mcx>>();
        // new_list caps the first allocation with the header folded in; a
        // non-empty list grows on the enlarge_list min-16 curve (list.c).
        let new_max = if self.max_length == 0 {
            nextpower2(core::cmp::max(8, min_size + LIST_HEADER_OVERHEAD)) - LIST_HEADER_OVERHEAD
        } else {
            nextpower2(core::cmp::max(16, min_size))
        };
        check_alloc_size(new_max as usize * cell)?;
        let new_layout = Layout::array::<F::Cell<'mcx>>(new_max as usize)
            .map_err(|_| crate::oom(mcx, new_max as usize * cell))?;
        let ptr = if self.max_length == 0 {
            Allocator::allocate(&mcx, new_layout)
        } else {
            let old_layout =
                Layout::array::<F::Cell<'mcx>>(self.max_length as usize).expect("valid old layout");
            // SAFETY: elements holds max_length cells allocated from this arena
            // with old_layout; new_layout is strictly larger.
            unsafe { Allocator::grow(&mcx, self.elements.cast(), old_layout, new_layout) }
        };
        self.elements = ptr.map_err(|_| crate::oom(mcx, new_layout.size()))?.cast();
        self.max_length = new_max;
        Ok(())
    }

    #[inline]
    pub fn lappend(&mut self, mcx: Mcx<'mcx>, v: F::Cell<'mcx>) -> PgResult<()> {
        if self.length >= self.max_length {
            self.grow_for_append(mcx)?;
        }
        // SAFETY: capacity > length after the enlarge check.
        unsafe { self.elements.as_ptr().add(self.length as usize).write(v) };
        self.length += 1;
        Ok(())
    }

    pub fn lcons(&mut self, mcx: Mcx<'mcx>, v: F::Cell<'mcx>) -> PgResult<()> {
        if self.length >= self.max_length {
            self.grow_for_append(mcx)?;
        }
        // SAFETY: capacity > length; shifting the live prefix up one slot.
        unsafe {
            let base = self.elements.as_ptr();
            core::ptr::copy(base, base.add(1), self.length as usize);
            base.write(v);
        }
        self.length += 1;
        Ok(())
    }

    pub fn insert_nth(&mut self, mcx: Mcx<'mcx>, pos: usize, v: F::Cell<'mcx>) -> PgResult<()> {
        assert!(pos <= self.len());
        if self.length >= self.max_length {
            self.grow_for_append(mcx)?;
        }
        // SAFETY: capacity > length; shifting cells [pos, length) up one slot.
        unsafe {
            let base = self.elements.as_ptr().add(pos);
            core::ptr::copy(base, base.add(1), self.length as usize - pos);
            base.write(v);
        }
        self.length += 1;
        Ok(())
    }

    pub fn from_slice(mcx: Mcx<'mcx>, cells: &[F::Cell<'mcx>]) -> PgResult<Self> {
        if cells.is_empty() {
            return Ok(Self::nil());
        }
        let mut list = Self::nil();
        list.enlarge(mcx, cells.len() as i32)?;
        // SAFETY: capacity >= cells.len(); fresh disjoint buffer.
        unsafe {
            core::ptr::copy_nonoverlapping(cells.as_ptr(), list.elements.as_ptr(), cells.len());
        }
        list.length = cells.len() as i32;
        Ok(list)
    }

    #[inline]
    pub fn make1(mcx: Mcx<'mcx>, c1: F::Cell<'mcx>) -> PgResult<Self> {
        Self::from_slice(mcx, &[c1])
    }

    #[inline]
    pub fn make2(mcx: Mcx<'mcx>, c1: F::Cell<'mcx>, c2: F::Cell<'mcx>) -> PgResult<Self> {
        Self::from_slice(mcx, &[c1, c2])
    }

    #[inline]
    pub fn make3(
        mcx: Mcx<'mcx>,
        c1: F::Cell<'mcx>,
        c2: F::Cell<'mcx>,
        c3: F::Cell<'mcx>,
    ) -> PgResult<Self> {
        Self::from_slice(mcx, &[c1, c2, c3])
    }

    pub fn concat(&mut self, mcx: Mcx<'mcx>, other: &Self) -> PgResult<()> {
        let n = other.length;
        if n == 0 {
            return Ok(());
        }
        if self.length + n > self.max_length {
            self.enlarge(mcx, self.length + n)?;
        }
        // SAFETY: capacity >= length + n; `other` is a distinct borrow, and even
        // self-concat (C allows list_concat(a, a)) copies from the live prefix
        // into cells past it.
        unsafe {
            core::ptr::copy_nonoverlapping(
                other.elements.as_ptr(),
                self.elements.as_ptr().add(self.length as usize),
                n as usize,
            );
        }
        self.length += n;
        Ok(())
    }

    pub fn truncate(&mut self, new_len: usize) {
        if new_len < self.len() {
            self.length = new_len as i32;
        }
    }

    pub fn clone_in(&self, mcx: Mcx<'mcx>) -> PgResult<Self> {
        Self::from_slice(mcx, self.as_slice())
    }
}

impl<'a, 'mcx, F: ListFlavor> IntoIterator for &'a List<'mcx, F> {
    type Item = F::Cell<'mcx>;
    type IntoIter = core::iter::Copied<core::slice::Iter<'a, F::Cell<'mcx>>>;
    #[inline]
    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

impl<'mcx, F: ListFlavor> Default for List<'mcx, F> {
    fn default() -> Self {
        Self::nil()
    }
}

impl<'mcx, F: ListFlavor> core::fmt::Debug for List<'mcx, F>
where
    F::Cell<'mcx>: core::fmt::Debug,
{
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_list().entries(self.as_slice()).finish()
    }
}
