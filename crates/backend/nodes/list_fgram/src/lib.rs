#![allow(non_snake_case)]
#![allow(non_camel_case_types)]

use std::cmp::Ordering;
use std::ffi::c_void;
use std::marker::PhantomData;
use std::mem::size_of;
use std::ptr::NonNull;

use ::error_fgram::{PgError, PgResult};
use ::mmgr_fgram::{
    palloc, pfree, GetMemoryChunkContext, MemoryContextAlloc, MemoryContextScope,
};
use ::pg_ffi_fgram::{
    List, ListCell, NodeTag, Oid, Size, T_IntList, T_List, T_OidList, T_XidList, TransactionId,
};

const MIN_LIST_ALLOCATION_CELLS: usize = 8;

pub trait PgListKind {
    type Item: Copy + PartialEq;

    const TAG: NodeTag;

    fn into_cell(value: Self::Item) -> ListCell;
    fn from_cell(cell: &ListCell) -> Self::Item;
}

#[derive(Debug)]
pub enum PtrKind<T> {
    _Marker(PhantomData<T>),
}

#[derive(Debug)]
pub enum IntKind {}

#[derive(Debug)]
pub enum OidKind {}

#[derive(Debug)]
pub enum XidKind {}

impl<T> PgListKind for PtrKind<T> {
    type Item = *mut T;

    const TAG: NodeTag = T_List;

    fn into_cell(value: Self::Item) -> ListCell {
        ListCell::ptr_value(value)
    }

    fn from_cell(cell: &ListCell) -> Self::Item {
        cell.ptr()
    }
}

impl PgListKind for IntKind {
    type Item = i32;

    const TAG: NodeTag = T_IntList;

    fn into_cell(value: Self::Item) -> ListCell {
        ListCell::int_value(value)
    }

    fn from_cell(cell: &ListCell) -> Self::Item {
        cell.int()
    }
}

impl PgListKind for OidKind {
    type Item = Oid;

    const TAG: NodeTag = T_OidList;

    fn into_cell(value: Self::Item) -> ListCell {
        ListCell::oid_value(value)
    }

    fn from_cell(cell: &ListCell) -> Self::Item {
        cell.oid()
    }
}

impl PgListKind for XidKind {
    type Item = TransactionId;

    const TAG: NodeTag = T_XidList;

    fn into_cell(value: Self::Item) -> ListCell {
        ListCell::xid_value(value)
    }

    fn from_cell(cell: &ListCell) -> Self::Item {
        cell.xid()
    }
}

pub type PtrList<'ctx, T> = PgList<'ctx, PtrKind<T>>;
pub type IntList<'ctx> = PgList<'ctx, IntKind>;
pub type OidList<'ctx> = PgList<'ctx, OidKind>;
pub type XidList<'ctx> = PgList<'ctx, XidKind>;

#[derive(Debug)]
/// A PostgreSQL list allocated in a memory context.
///
/// The memory context owns the allocation. This handle carries the lifetime of
/// the `MemoryContextScope` used to allocate it, so safe Rust cannot return the
/// list after the owning memory context has gone out of scope. Dropping the
/// handle does not free memory; `pfree(self)` or `list_free(Some(self))`
/// consumes the handle and frees the underlying PostgreSQL list storage.
///
/// ```compile_fail
/// use list_fgram::{lappend_int, IntList};
/// use ::mmgr_fgram::{
///     OwnedMemoryContext, PgMemoryContext, ALLOCSET_DEFAULT_INITSIZE,
///     ALLOCSET_DEFAULT_MAXSIZE, ALLOCSET_DEFAULT_MINSIZE,
/// };
///
/// fn list_outlives_context<'ctx>() -> IntList<'ctx> {
///     let context = OwnedMemoryContext::alloc_set(
///         Some(PgMemoryContext::top().unwrap()),
///         "too short",
///         ALLOCSET_DEFAULT_MINSIZE,
///         ALLOCSET_DEFAULT_INITSIZE,
///         ALLOCSET_DEFAULT_MAXSIZE,
///     )
///     .unwrap();
///     let scope = context.scope();
///
///     lappend_int(&scope, None, 1).unwrap()
/// }
/// ```
pub struct PgList<'ctx, K: PgListKind> {
    raw: NonNull<List>,
    _ctx: PhantomData<&'ctx ()>,
    _marker: PhantomData<K>,
}

impl<'ctx, K: PgListKind> PgList<'ctx, K> {
    /// # Safety
    ///
    /// `raw` must be either null or a valid PostgreSQL `List` with `K::TAG`.
    /// The returned wrapper borrows the memory context represented by `scope`.
    pub unsafe fn from_raw(
        _scope: &MemoryContextScope<'ctx>,
        raw: *mut List,
    ) -> PgResult<Option<Self>> {
        let Some(raw) = NonNull::new(raw) else {
            return Ok(None);
        };
        let list = unsafe { raw.as_ref() };
        if list.list_type() != K::TAG {
            return Err(type_mismatch(K::TAG, list.list_type()));
        }
        Ok(Some(Self {
            raw,
            _ctx: PhantomData,
            _marker: PhantomData,
        }))
    }

    pub fn into_raw(self) -> *mut List {
        let raw = self.raw.as_ptr();
        std::mem::forget(self);
        raw
    }

    pub fn as_raw(&self) -> *const List {
        self.raw.as_ptr()
    }

    pub fn as_mut_raw(&mut self) -> *mut List {
        self.raw.as_ptr()
    }

    pub fn pfree(self) -> PgResult<()> {
        let raw = self.into_raw();
        unsafe { list_free_raw(raw, false) }
    }

    pub fn len(&self) -> usize {
        self.list().len() as usize
    }

    pub fn is_empty(&self) -> bool {
        false
    }

    pub fn capacity(&self) -> usize {
        self.list().max_length() as usize
    }

    pub fn get(&self, index: usize) -> Option<K::Item> {
        self.cells().get(index).map(K::from_cell)
    }

    pub fn iter(&self) -> impl ExactSizeIterator<Item = K::Item> + '_ {
        self.cells().iter().map(K::from_cell)
    }

    fn list(&self) -> &List {
        unsafe { self.raw.as_ref() }
    }

    fn list_mut(&mut self) -> &mut List {
        unsafe { self.raw.as_mut() }
    }

    fn cells(&self) -> &[ListCell] {
        self.list().cells()
    }

    fn cells_mut(&mut self) -> &mut [ListCell] {
        self.list_mut().cells_mut()
    }
}

pub type list_sort_comparator = fn(&ListCell, &ListCell) -> i32;

pub fn list_make1_impl<'ctx, K: PgListKind>(
    scope: &MemoryContextScope<'ctx>,
    datum1: K::Item,
) -> PgResult<PgList<'ctx, K>> {
    make_list(scope, [K::into_cell(datum1)])
}

pub fn list_make2_impl<'ctx, K: PgListKind>(
    scope: &MemoryContextScope<'ctx>,
    datum1: K::Item,
    datum2: K::Item,
) -> PgResult<PgList<'ctx, K>> {
    make_list(scope, [K::into_cell(datum1), K::into_cell(datum2)])
}

pub fn list_make3_impl<'ctx, K: PgListKind>(
    scope: &MemoryContextScope<'ctx>,
    datum1: K::Item,
    datum2: K::Item,
    datum3: K::Item,
) -> PgResult<PgList<'ctx, K>> {
    make_list(
        scope,
        [
            K::into_cell(datum1),
            K::into_cell(datum2),
            K::into_cell(datum3),
        ],
    )
}

pub fn list_make4_impl<'ctx, K: PgListKind>(
    scope: &MemoryContextScope<'ctx>,
    datum1: K::Item,
    datum2: K::Item,
    datum3: K::Item,
    datum4: K::Item,
) -> PgResult<PgList<'ctx, K>> {
    make_list(
        scope,
        [
            K::into_cell(datum1),
            K::into_cell(datum2),
            K::into_cell(datum3),
            K::into_cell(datum4),
        ],
    )
}

pub fn list_make5_impl<'ctx, K: PgListKind>(
    scope: &MemoryContextScope<'ctx>,
    datum1: K::Item,
    datum2: K::Item,
    datum3: K::Item,
    datum4: K::Item,
    datum5: K::Item,
) -> PgResult<PgList<'ctx, K>> {
    make_list(
        scope,
        [
            K::into_cell(datum1),
            K::into_cell(datum2),
            K::into_cell(datum3),
            K::into_cell(datum4),
            K::into_cell(datum5),
        ],
    )
}

pub fn lappend<'ctx, T>(
    scope: &MemoryContextScope<'ctx>,
    list: Option<PtrList<'ctx, T>>,
    datum: *mut T,
) -> PgResult<PtrList<'ctx, T>> {
    append::<PtrKind<T>>(scope, list, datum)
}

pub fn lappend_int<'ctx>(
    scope: &MemoryContextScope<'ctx>,
    list: Option<IntList<'ctx>>,
    datum: i32,
) -> PgResult<IntList<'ctx>> {
    append::<IntKind>(scope, list, datum)
}

pub fn lappend_oid<'ctx>(
    scope: &MemoryContextScope<'ctx>,
    list: Option<OidList<'ctx>>,
    datum: Oid,
) -> PgResult<OidList<'ctx>> {
    append::<OidKind>(scope, list, datum)
}

pub fn lappend_xid<'ctx>(
    scope: &MemoryContextScope<'ctx>,
    list: Option<XidList<'ctx>>,
    datum: TransactionId,
) -> PgResult<XidList<'ctx>> {
    append::<XidKind>(scope, list, datum)
}

pub fn lcons<'ctx, T>(
    scope: &MemoryContextScope<'ctx>,
    datum: *mut T,
    list: Option<PtrList<'ctx, T>>,
) -> PgResult<PtrList<'ctx, T>> {
    prepend::<PtrKind<T>>(scope, datum, list)
}

pub fn lcons_int<'ctx>(
    scope: &MemoryContextScope<'ctx>,
    datum: i32,
    list: Option<IntList<'ctx>>,
) -> PgResult<IntList<'ctx>> {
    prepend::<IntKind>(scope, datum, list)
}

pub fn lcons_oid<'ctx>(
    scope: &MemoryContextScope<'ctx>,
    datum: Oid,
    list: Option<OidList<'ctx>>,
) -> PgResult<OidList<'ctx>> {
    prepend::<OidKind>(scope, datum, list)
}

pub fn list_insert_nth<'ctx, T>(
    scope: &MemoryContextScope<'ctx>,
    list: Option<PtrList<'ctx, T>>,
    pos: usize,
    datum: *mut T,
) -> PgResult<PtrList<'ctx, T>> {
    insert_nth::<PtrKind<T>>(scope, list, pos, datum)
}

pub fn list_insert_nth_int<'ctx>(
    scope: &MemoryContextScope<'ctx>,
    list: Option<IntList<'ctx>>,
    pos: usize,
    datum: i32,
) -> PgResult<IntList<'ctx>> {
    insert_nth::<IntKind>(scope, list, pos, datum)
}

pub fn list_insert_nth_oid<'ctx>(
    scope: &MemoryContextScope<'ctx>,
    list: Option<OidList<'ctx>>,
    pos: usize,
    datum: Oid,
) -> PgResult<OidList<'ctx>> {
    insert_nth::<OidKind>(scope, list, pos, datum)
}

pub fn list_concat<'ctx, K: PgListKind>(
    scope: &MemoryContextScope<'ctx>,
    list1: Option<PgList<'ctx, K>>,
    list2: Option<&PgList<'ctx, K>>,
) -> PgResult<Option<PgList<'ctx, K>>> {
    match (list1, list2) {
        (None, None) => Ok(None),
        (None, Some(list2)) => list_copy(scope, Some(list2)),
        (Some(list1), None) => Ok(Some(list1)),
        (Some(mut list1), Some(list2)) => {
            let old_len = list1.len();
            let new_len = old_len + list2.len();
            if new_len > list1.capacity() {
                enlarge_list(&mut list1, new_len)?;
            }
            let source = list2.cells();
            list1.list_mut().set_len(new_len as i32);
            list1.cells_mut()[old_len..new_len].copy_from_slice(source);
            Ok(Some(list1))
        }
    }
}

pub fn list_concat_copy<'ctx, K: PgListKind>(
    scope: &MemoryContextScope<'ctx>,
    list1: Option<&PgList<'ctx, K>>,
    list2: Option<&PgList<'ctx, K>>,
) -> PgResult<Option<PgList<'ctx, K>>> {
    match (list1, list2) {
        (None, None) => Ok(None),
        (Some(list), None) | (None, Some(list)) => list_copy(scope, Some(list)),
        (Some(list1), Some(list2)) => {
            let mut result = new_list::<K>(scope, list1.len() + list2.len())?;
            result.cells_mut()[..list1.len()].copy_from_slice(list1.cells());
            result.cells_mut()[list1.len()..].copy_from_slice(list2.cells());
            Ok(Some(result))
        }
    }
}

pub fn list_truncate<'ctx, K: PgListKind>(
    mut list: Option<PgList<'ctx, K>>,
    new_size: usize,
) -> Option<PgList<'ctx, K>> {
    let mut list = list.take()?;
    if new_size == 0 {
        return None;
    }
    if new_size < list.len() {
        list.list_mut().set_len(new_size as i32);
    }
    Some(list)
}

pub fn list_member_ptr<T>(list: Option<&PtrList<'_, T>>, datum: *const T) -> bool {
    list.is_some_and(|list| list.iter().any(|item| item.cast_const() == datum))
}

pub fn list_member_int(list: Option<&IntList<'_>>, datum: i32) -> bool {
    list.is_some_and(|list| list.iter().any(|item| item == datum))
}

pub fn list_member_oid(list: Option<&OidList<'_>>, datum: Oid) -> bool {
    list.is_some_and(|list| list.iter().any(|item| item == datum))
}

pub fn list_member_xid(list: Option<&XidList<'_>>, datum: TransactionId) -> bool {
    list.is_some_and(|list| list.iter().any(|item| item == datum))
}

pub fn list_delete_nth_cell<'ctx, K: PgListKind>(
    list: Option<PgList<'ctx, K>>,
    n: usize,
) -> PgResult<Option<PgList<'ctx, K>>> {
    let Some(mut list) = list else {
        return Ok(None);
    };
    if n >= list.len() {
        return Err(index_error(n, list.len()));
    }
    if list.len() == 1 {
        return Ok(None);
    }
    let old_len = list.len();
    list.cells_mut().copy_within(n + 1..old_len, n);
    list.list_mut().set_len((old_len - 1) as i32);
    Ok(Some(list))
}

pub fn list_delete_cell<'ctx, K: PgListKind>(
    list: Option<PgList<'ctx, K>>,
    cell_index: usize,
) -> PgResult<Option<PgList<'ctx, K>>> {
    list_delete_nth_cell(list, cell_index)
}

pub fn list_delete_ptr<'ctx, T>(
    list: Option<PtrList<'ctx, T>>,
    datum: *const T,
) -> PgResult<Option<PtrList<'ctx, T>>> {
    delete_first_matching(list, |item| item.cast_const() == datum)
}

pub fn list_delete_int<'ctx>(
    list: Option<IntList<'ctx>>,
    datum: i32,
) -> PgResult<Option<IntList<'ctx>>> {
    delete_first_matching(list, |item| item == datum)
}

pub fn list_delete_oid<'ctx>(
    list: Option<OidList<'ctx>>,
    datum: Oid,
) -> PgResult<Option<OidList<'ctx>>> {
    delete_first_matching(list, |item| item == datum)
}

pub fn list_delete_first<'ctx, K: PgListKind>(
    list: Option<PgList<'ctx, K>>,
) -> PgResult<Option<PgList<'ctx, K>>> {
    list_delete_nth_cell(list, 0)
}

pub fn list_delete_last<'ctx, K: PgListKind>(
    list: Option<PgList<'ctx, K>>,
) -> Option<PgList<'ctx, K>> {
    let list = list?;
    if list.len() <= 1 {
        None
    } else {
        let new_len = list.len() - 1;
        list_truncate(Some(list), new_len)
    }
}

pub fn list_delete_first_n<'ctx, K: PgListKind>(
    list: Option<PgList<'ctx, K>>,
    n: usize,
) -> Option<PgList<'ctx, K>> {
    let mut list = list?;
    if n == 0 {
        return Some(list);
    }
    if n >= list.len() {
        return None;
    }
    let old_len = list.len();
    list.cells_mut().copy_within(n..old_len, 0);
    list.list_mut().set_len((old_len - n) as i32);
    Some(list)
}

pub fn list_union_ptr<'ctx, T>(
    scope: &MemoryContextScope<'ctx>,
    list1: Option<&PtrList<'ctx, T>>,
    list2: Option<&PtrList<'ctx, T>>,
) -> PgResult<Option<PtrList<'ctx, T>>> {
    union_by(scope, list1, list2, |list, datum| {
        list_member_ptr(Some(list), datum.cast_const())
    })
}

pub fn list_union_int<'ctx>(
    scope: &MemoryContextScope<'ctx>,
    list1: Option<&IntList<'ctx>>,
    list2: Option<&IntList<'ctx>>,
) -> PgResult<Option<IntList<'ctx>>> {
    union_by(scope, list1, list2, |list, datum| {
        list_member_int(Some(list), datum)
    })
}

pub fn list_union_oid<'ctx>(
    scope: &MemoryContextScope<'ctx>,
    list1: Option<&OidList<'ctx>>,
    list2: Option<&OidList<'ctx>>,
) -> PgResult<Option<OidList<'ctx>>> {
    union_by(scope, list1, list2, |list, datum| {
        list_member_oid(Some(list), datum)
    })
}

pub fn list_intersection_int<'ctx>(
    scope: &MemoryContextScope<'ctx>,
    list1: Option<&IntList<'ctx>>,
    list2: Option<&IntList<'ctx>>,
) -> PgResult<Option<IntList<'ctx>>> {
    let (Some(list1), Some(list2)) = (list1, list2) else {
        return Ok(None);
    };
    let mut result = None;
    for datum in list1.iter() {
        if list_member_int(Some(list2), datum) {
            result = Some(lappend_int(scope, result, datum)?);
        }
    }
    Ok(result)
}

pub fn list_difference_ptr<'ctx, T>(
    scope: &MemoryContextScope<'ctx>,
    list1: Option<&PtrList<'ctx, T>>,
    list2: Option<&PtrList<'ctx, T>>,
) -> PgResult<Option<PtrList<'ctx, T>>> {
    difference_by(scope, list1, list2, |list, datum| {
        list_member_ptr(Some(list), datum.cast_const())
    })
}

pub fn list_difference_int<'ctx>(
    scope: &MemoryContextScope<'ctx>,
    list1: Option<&IntList<'ctx>>,
    list2: Option<&IntList<'ctx>>,
) -> PgResult<Option<IntList<'ctx>>> {
    difference_by(scope, list1, list2, |list, datum| {
        list_member_int(Some(list), datum)
    })
}

pub fn list_difference_oid<'ctx>(
    scope: &MemoryContextScope<'ctx>,
    list1: Option<&OidList<'ctx>>,
    list2: Option<&OidList<'ctx>>,
) -> PgResult<Option<OidList<'ctx>>> {
    difference_by(scope, list1, list2, |list, datum| {
        list_member_oid(Some(list), datum)
    })
}

pub fn list_append_unique_ptr<'ctx, T>(
    scope: &MemoryContextScope<'ctx>,
    list: Option<PtrList<'ctx, T>>,
    datum: *mut T,
) -> PgResult<PtrList<'ctx, T>> {
    if list_member_ptr(list.as_ref(), datum.cast_const()) {
        list.ok_or_else(|| PgError::error("list_append_unique_ptr: member in nonempty list"))
    } else {
        lappend(scope, list, datum)
    }
}

pub fn list_append_unique_int<'ctx>(
    scope: &MemoryContextScope<'ctx>,
    list: Option<IntList<'ctx>>,
    datum: i32,
) -> PgResult<IntList<'ctx>> {
    if list_member_int(list.as_ref(), datum) {
        list.ok_or_else(|| PgError::error("list_append_unique_int: member in nonempty list"))
    } else {
        lappend_int(scope, list, datum)
    }
}

pub fn list_append_unique_oid<'ctx>(
    scope: &MemoryContextScope<'ctx>,
    list: Option<OidList<'ctx>>,
    datum: Oid,
) -> PgResult<OidList<'ctx>> {
    if list_member_oid(list.as_ref(), datum) {
        list.ok_or_else(|| PgError::error("list_append_unique_oid: member in nonempty list"))
    } else {
        lappend_oid(scope, list, datum)
    }
}

pub fn list_concat_unique_ptr<'ctx, T>(
    scope: &MemoryContextScope<'ctx>,
    mut list1: Option<PtrList<'ctx, T>>,
    list2: Option<&PtrList<'ctx, T>>,
) -> PgResult<Option<PtrList<'ctx, T>>> {
    let Some(list2) = list2 else {
        return Ok(list1);
    };
    for datum in list2.iter() {
        list1 = Some(list_append_unique_ptr(scope, list1, datum)?);
    }
    Ok(list1)
}

pub fn list_concat_unique_int<'ctx>(
    scope: &MemoryContextScope<'ctx>,
    mut list1: Option<IntList<'ctx>>,
    list2: Option<&IntList<'ctx>>,
) -> PgResult<Option<IntList<'ctx>>> {
    let Some(list2) = list2 else {
        return Ok(list1);
    };
    for datum in list2.iter() {
        list1 = Some(list_append_unique_int(scope, list1, datum)?);
    }
    Ok(list1)
}

pub fn list_concat_unique_oid<'ctx>(
    scope: &MemoryContextScope<'ctx>,
    mut list1: Option<OidList<'ctx>>,
    list2: Option<&OidList<'ctx>>,
) -> PgResult<Option<OidList<'ctx>>> {
    let Some(list2) = list2 else {
        return Ok(list1);
    };
    for datum in list2.iter() {
        list1 = Some(list_append_unique_oid(scope, list1, datum)?);
    }
    Ok(list1)
}

pub fn list_deduplicate_oid(list: &mut OidList<'_>) {
    let len = list.len();
    if len <= 1 {
        return;
    }

    let mut write_index = 0;
    for read_index in 1..len {
        if list.cells()[write_index].oid() != list.cells()[read_index].oid() {
            write_index += 1;
            list.cells_mut()[write_index] = list.cells()[read_index];
        }
    }
    list.list_mut().set_len((write_index + 1) as i32);
}

pub fn list_free<K: PgListKind>(list: Option<PgList<'_, K>>) -> PgResult<()> {
    let Some(list) = list else {
        return Ok(());
    };
    list.pfree()
}

/// # Safety
///
/// Every pointer element in `list` must either be null or point to memory
/// allocated by PostgreSQL memory contexts and be freeable with `pfree`.
pub unsafe fn list_free_deep<T>(list: Option<PtrList<'_, T>>) -> PgResult<()> {
    let Some(list) = list else {
        return Ok(());
    };
    let raw = list.into_raw();
    unsafe { list_free_raw(raw, true) }
}

pub fn list_copy<'ctx, K: PgListKind>(
    scope: &MemoryContextScope<'ctx>,
    oldlist: Option<&PgList<'ctx, K>>,
) -> PgResult<Option<PgList<'ctx, K>>> {
    let Some(oldlist) = oldlist else {
        return Ok(None);
    };
    let mut newlist = new_list::<K>(scope, oldlist.len())?;
    newlist.cells_mut().copy_from_slice(oldlist.cells());
    Ok(Some(newlist))
}

pub fn list_copy_head<'ctx, K: PgListKind>(
    scope: &MemoryContextScope<'ctx>,
    oldlist: Option<&PgList<'ctx, K>>,
    len: usize,
) -> PgResult<Option<PgList<'ctx, K>>> {
    let Some(oldlist) = oldlist else {
        return Ok(None);
    };
    if len == 0 {
        return Ok(None);
    }
    let copy_len = oldlist.len().min(len);
    let mut newlist = new_list::<K>(scope, copy_len)?;
    newlist
        .cells_mut()
        .copy_from_slice(&oldlist.cells()[..copy_len]);
    Ok(Some(newlist))
}

pub fn list_copy_tail<'ctx, K: PgListKind>(
    scope: &MemoryContextScope<'ctx>,
    oldlist: Option<&PgList<'ctx, K>>,
    nskip: usize,
) -> PgResult<Option<PgList<'ctx, K>>> {
    let Some(oldlist) = oldlist else {
        return Ok(None);
    };
    if nskip >= oldlist.len() {
        return Ok(None);
    }
    let mut newlist = new_list::<K>(scope, oldlist.len() - nskip)?;
    newlist
        .cells_mut()
        .copy_from_slice(&oldlist.cells()[nskip..]);
    Ok(Some(newlist))
}

pub fn list_sort<K: PgListKind>(list: &mut PgList<'_, K>, cmp: list_sort_comparator) {
    list.cells_mut()
        .sort_by(|left, right| cmp_to_ordering(cmp(left, right)));
}

pub fn list_int_cmp(left: &ListCell, right: &ListCell) -> i32 {
    compare_ordering(left.int().cmp(&right.int()))
}

pub fn list_oid_cmp(left: &ListCell, right: &ListCell) -> i32 {
    compare_ordering(left.oid().cmp(&right.oid()))
}

fn append<'ctx, K: PgListKind>(
    scope: &MemoryContextScope<'ctx>,
    list: Option<PgList<'ctx, K>>,
    datum: K::Item,
) -> PgResult<PgList<'ctx, K>> {
    let Some(mut list) = list else {
        return list_make1_impl::<K>(scope, datum);
    };
    new_tail_cell(&mut list)?;
    let tail = list.len() - 1;
    list.cells_mut()[tail] = K::into_cell(datum);
    Ok(list)
}

fn prepend<'ctx, K: PgListKind>(
    scope: &MemoryContextScope<'ctx>,
    datum: K::Item,
    list: Option<PgList<'ctx, K>>,
) -> PgResult<PgList<'ctx, K>> {
    let Some(mut list) = list else {
        return list_make1_impl::<K>(scope, datum);
    };
    new_head_cell(&mut list)?;
    list.cells_mut()[0] = K::into_cell(datum);
    Ok(list)
}

fn insert_nth<'ctx, K: PgListKind>(
    scope: &MemoryContextScope<'ctx>,
    list: Option<PgList<'ctx, K>>,
    pos: usize,
    datum: K::Item,
) -> PgResult<PgList<'ctx, K>> {
    if list.is_none() {
        if pos != 0 {
            return Err(index_error(pos, 0));
        }
        return list_make1_impl::<K>(scope, datum);
    }
    let mut list = list.expect("checked above");
    if pos > list.len() {
        return Err(index_error(pos, list.len()));
    }
    insert_new_cell(&mut list, pos)?;
    list.cells_mut()[pos] = K::into_cell(datum);
    Ok(list)
}

fn make_list<'ctx, K: PgListKind, const N: usize>(
    scope: &MemoryContextScope<'ctx>,
    cells: [ListCell; N],
) -> PgResult<PgList<'ctx, K>> {
    let mut list = new_list::<K>(scope, N)?;
    list.cells_mut().copy_from_slice(&cells);
    Ok(list)
}

fn new_list<'ctx, K: PgListKind>(
    scope: &MemoryContextScope<'ctx>,
    min_size: usize,
) -> PgResult<PgList<'ctx, K>> {
    if min_size == 0 {
        return Err(PgError::error(
            "cannot allocate a non-NIL list with zero length",
        ));
    }
    let max_length = initial_max_length(min_size)?;
    let memory = palloc(scope, list_allocation_size(max_length)?)?;
    let raw = memory.as_ptr().cast::<List>();
    unsafe {
        List::initialize(raw, K::TAG, min_size as i32, max_length as i32);
    }
    Ok(PgList {
        raw: NonNull::new(memory.into_raw().cast::<List>())
            .ok_or_else(|| PgError::error("new_list: palloc returned a null pointer"))?,
        _ctx: PhantomData,
        _marker: PhantomData,
    })
}

fn initial_max_length(min_size: usize) -> PgResult<usize> {
    let overhead = List::header_overhead_cells();
    let requested = min_size
        .checked_add(overhead)
        .ok_or_else(|| PgError::error("list size overflow"))?
        .max(MIN_LIST_ALLOCATION_CELLS);
    Ok(requested
        .checked_next_power_of_two()
        .ok_or_else(|| PgError::error("list size overflow"))?
        - overhead)
}

fn list_allocation_size(max_length: usize) -> PgResult<Size> {
    List::header_size()
        .checked_add(
            max_length
                .checked_mul(size_of::<ListCell>())
                .ok_or_else(|| PgError::error("list allocation size overflow"))?,
        )
        .ok_or_else(|| PgError::error("list allocation size overflow"))
}

fn cells_allocation_size(length: usize) -> PgResult<Size> {
    length
        .checked_mul(size_of::<ListCell>())
        .ok_or_else(|| PgError::error("list cell allocation size overflow"))
}

fn new_tail_cell<K: PgListKind>(list: &mut PgList<'_, K>) -> PgResult<()> {
    if list.len() >= list.capacity() {
        enlarge_list(list, list.len() + 1)?;
    }
    let new_len = list.len() + 1;
    list.list_mut().set_len(new_len as i32);
    Ok(())
}

fn new_head_cell<K: PgListKind>(list: &mut PgList<'_, K>) -> PgResult<()> {
    if list.len() >= list.capacity() {
        enlarge_list(list, list.len() + 1)?;
    }
    let old_len = list.len();
    list.list_mut().set_len((old_len + 1) as i32);
    list.cells_mut().copy_within(0..old_len, 1);
    Ok(())
}

fn insert_new_cell<K: PgListKind>(list: &mut PgList<'_, K>, pos: usize) -> PgResult<()> {
    if list.len() >= list.capacity() {
        enlarge_list(list, list.len() + 1)?;
    }
    let old_len = list.len();
    list.list_mut().set_len((old_len + 1) as i32);
    if pos < old_len {
        list.cells_mut().copy_within(pos..old_len, pos + 1);
    }
    Ok(())
}

fn enlarge_list<K: PgListKind>(list: &mut PgList<'_, K>, min_size: usize) -> PgResult<()> {
    if min_size <= list.capacity() {
        return Ok(());
    }
    let new_max_len = min_size
        .max(16)
        .checked_next_power_of_two()
        .ok_or_else(|| PgError::error("list size overflow"))?;
    let new_size = cells_allocation_size(new_max_len)?;
    let old_len = list.len();
    let context = GetMemoryChunkContext(list.as_mut_raw().cast())?;

    if list.list().uses_initial_elements() {
        let mut memory = unsafe { MemoryContextAlloc(context, new_size)? };
        let old_bytes = list.list().cells()[..old_len].as_bytes();
        memory.as_mut_slice()[..old_bytes.len()].copy_from_slice(old_bytes);
        unsafe {
            list.list_mut()
                .set_elements_ptr(memory.into_raw().cast::<ListCell>());
        }
    } else {
        let old_elements = list.list().elements_ptr();
        let mut memory = unsafe { MemoryContextAlloc(context, new_size)? };
        let old_bytes = list.list().cells()[..old_len].as_bytes();
        memory.as_mut_slice()[..old_bytes.len()].copy_from_slice(old_bytes);
        unsafe {
            list.list_mut()
                .set_elements_ptr(memory.into_raw().cast::<ListCell>());
            pfree(old_elements.cast())?;
        }
    }
    list.list_mut().set_max_length(new_max_len as i32);
    Ok(())
}

fn delete_first_matching<'ctx, K: PgListKind>(
    list: Option<PgList<'ctx, K>>,
    matches: impl Fn(K::Item) -> bool,
) -> PgResult<Option<PgList<'ctx, K>>> {
    let Some(list_ref) = list.as_ref() else {
        return Ok(None);
    };
    let Some(index) = list_ref.iter().position(matches) else {
        return Ok(list);
    };
    list_delete_nth_cell(list, index)
}

fn union_by<'ctx, K: PgListKind>(
    scope: &MemoryContextScope<'ctx>,
    list1: Option<&PgList<'ctx, K>>,
    list2: Option<&PgList<'ctx, K>>,
    contains: impl Fn(&PgList<'ctx, K>, K::Item) -> bool,
) -> PgResult<Option<PgList<'ctx, K>>> {
    let mut result = list_copy(scope, list1)?;
    let Some(list2) = list2 else {
        return Ok(result);
    };
    for datum in list2.iter() {
        if !result.as_ref().is_some_and(|list| contains(list, datum)) {
            result = Some(append(scope, result, datum)?);
        }
    }
    Ok(result)
}

fn difference_by<'ctx, K: PgListKind>(
    scope: &MemoryContextScope<'ctx>,
    list1: Option<&PgList<'ctx, K>>,
    list2: Option<&PgList<'ctx, K>>,
    contains: impl Fn(&PgList<'ctx, K>, K::Item) -> bool,
) -> PgResult<Option<PgList<'ctx, K>>> {
    let Some(list1) = list1 else {
        return Ok(None);
    };
    let Some(list2) = list2 else {
        return list_copy(scope, Some(list1));
    };
    let mut result = None;
    for datum in list1.iter() {
        if !contains(list2, datum) {
            result = Some(append(scope, result, datum)?);
        }
    }
    Ok(result)
}

unsafe fn list_free_raw(raw: *mut List, deep: bool) -> PgResult<()> {
    if raw.is_null() {
        return Ok(());
    }

    let list = unsafe { &mut *raw };
    if deep {
        for cell in list.cells() {
            let pointer = cell.ptr::<c_void>();
            if !pointer.is_null() {
                unsafe { pfree(pointer.cast())? };
            }
        }
    }
    if !list.uses_initial_elements() {
        unsafe { pfree(list.elements_ptr().cast())? };
    }
    unsafe { pfree(raw.cast()) }
}

fn index_error(index: usize, len: usize) -> PgError {
    PgError::error(format!(
        "list index {index} is out of bounds for length {len}"
    ))
}

fn type_mismatch(expected: NodeTag, actual: NodeTag) -> PgError {
    PgError::error(format!(
        "expected list tag {expected}, found list tag {actual}"
    ))
}

fn compare_ordering(ordering: Ordering) -> i32 {
    match ordering {
        Ordering::Less => -1,
        Ordering::Equal => 0,
        Ordering::Greater => 1,
    }
}

fn cmp_to_ordering(value: i32) -> Ordering {
    value.cmp(&0)
}

trait ListCellSliceExt {
    fn as_bytes(&self) -> &[u8];
}

impl ListCellSliceExt for [ListCell] {
    fn as_bytes(&self) -> &[u8] {
        unsafe {
            std::slice::from_raw_parts(self.as_ptr().cast::<u8>(), std::mem::size_of_val(self))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ::mmgr_fgram::{
        OwnedMemoryContext, PgMemoryContext, ALLOCSET_DEFAULT_INITSIZE, ALLOCSET_DEFAULT_MAXSIZE,
        ALLOCSET_DEFAULT_MINSIZE,
    };
    use std::ffi::c_void;

    fn ptr(value: usize) -> *mut c_void {
        value as *mut c_void
    }

    fn test_context(name: &str) -> OwnedMemoryContext {
        OwnedMemoryContext::alloc_set(
            Some(PgMemoryContext::top().unwrap()),
            name,
            ALLOCSET_DEFAULT_MINSIZE,
            ALLOCSET_DEFAULT_INITSIZE,
            ALLOCSET_DEFAULT_MAXSIZE,
        )
        .unwrap()
    }

    fn int_values(list: Option<&IntList<'_>>) -> Vec<i32> {
        list.map(|list| list.iter().collect()).unwrap_or_default()
    }

    fn oid_values(list: Option<&OidList<'_>>) -> Vec<Oid> {
        list.map(|list| list.iter().collect()).unwrap_or_default()
    }

    #[test]
    fn append_and_prepend_keep_types_and_order() {
        let context = test_context("list append");
        let scope = context.scope();
        let list = lappend_int(&scope, None, 2).unwrap();
        let list = lappend_int(&scope, Some(list), 3).unwrap();
        let list = lcons_int(&scope, 1, Some(list)).unwrap();

        assert_eq!(list.list().list_type(), T_IntList);
        assert_eq!(int_values(Some(&list)), [1, 2, 3]);
        assert!(list.capacity() >= 5);
    }

    #[test]
    fn pointer_lists_are_typed() {
        let context = test_context("ptr list");
        let scope = context.scope();
        let list = lappend::<c_void>(&scope, None, ptr(1)).unwrap();
        let list = lappend(&scope, Some(list), ptr(2)).unwrap();

        assert_eq!(list.get(0), Some(ptr(1)));
        assert!(list_member_ptr(Some(&list), ptr(2)));
        assert!(!list_member_ptr(Some(&list), ptr(3)));
    }

    #[test]
    fn insert_delete_and_truncate_match_nil_behavior() {
        let context = test_context("list insert");
        let scope = context.scope();
        let list = lappend_int(&scope, None, 1).unwrap();
        let list = list_insert_nth_int(&scope, Some(list), 1, 3).unwrap();
        let list = list_insert_nth_int(&scope, Some(list), 1, 2).unwrap();
        assert_eq!(int_values(Some(&list)), [1, 2, 3]);

        let list = list_delete_int(Some(list), 2).unwrap();
        assert_eq!(int_values(list.as_ref()), [1, 3]);

        let list = list_delete_first(list).unwrap();
        assert_eq!(int_values(list.as_ref()), [3]);

        let list = list_delete_last(list);
        assert!(list.is_none());

        let list = lappend_int(&scope, None, 1).unwrap();
        assert!(list_truncate(Some(list), 0).is_none());
    }

    #[test]
    fn concat_and_copy_do_not_modify_second_list() {
        let context = test_context("list concat");
        let scope = context.scope();
        let left = lappend_int(&scope, Some(lappend_int(&scope, None, 1).unwrap()), 2).unwrap();
        let right = lappend_int(&scope, Some(lappend_int(&scope, None, 3).unwrap()), 4).unwrap();

        let combined = list_concat(&scope, Some(left), Some(&right))
            .unwrap()
            .unwrap();

        assert_eq!(int_values(Some(&combined)), [1, 2, 3, 4]);
        assert_eq!(int_values(Some(&right)), [3, 4]);

        let copied = list_concat_copy(&scope, Some(&combined), Some(&right))
            .unwrap()
            .unwrap();
        assert_eq!(int_values(Some(&copied)), [1, 2, 3, 4, 3, 4]);
    }

    #[test]
    fn set_operations_cover_pointer_int_and_oid_variants() {
        let context = test_context("list set operations");
        let scope = context.scope();
        let p1 = lappend(
            &scope,
            Some(lappend::<c_void>(&scope, None, ptr(1)).unwrap()),
            ptr(2),
        )
        .unwrap();
        let p2 = lappend(
            &scope,
            Some(lappend::<c_void>(&scope, None, ptr(2)).unwrap()),
            ptr(3),
        )
        .unwrap();
        let punion = list_union_ptr(&scope, Some(&p1), Some(&p2))
            .unwrap()
            .unwrap();
        assert_eq!(
            punion.iter().map(|p| p as usize).collect::<Vec<_>>(),
            [1, 2, 3]
        );

        let i1 = lappend_int(&scope, Some(lappend_int(&scope, None, 1).unwrap()), 2).unwrap();
        let i2 = lappend_int(&scope, Some(lappend_int(&scope, None, 2).unwrap()), 3).unwrap();
        let intersection = list_intersection_int(&scope, Some(&i1), Some(&i2))
            .unwrap()
            .unwrap();
        assert_eq!(int_values(Some(&intersection)), [2]);

        let o1 = lappend_oid(&scope, Some(lappend_oid(&scope, None, 1).unwrap()), 2).unwrap();
        let o2 = lappend_oid(&scope, Some(lappend_oid(&scope, None, 2).unwrap()), 3).unwrap();
        let difference = list_difference_oid(&scope, Some(&o1), Some(&o2))
            .unwrap()
            .unwrap();
        assert_eq!(oid_values(Some(&difference)), [1]);
    }

    #[test]
    fn unique_concat_and_deduplicate_oid_preserve_postgres_semantics() {
        let context = test_context("list unique");
        let scope = context.scope();
        let left = lappend_oid(&scope, Some(lappend_oid(&scope, None, 1).unwrap()), 2).unwrap();
        let right = lappend_oid(&scope, Some(lappend_oid(&scope, None, 2).unwrap()), 3).unwrap();
        let mut combined = list_concat_unique_oid(&scope, Some(left), Some(&right))
            .unwrap()
            .unwrap();
        assert_eq!(oid_values(Some(&combined)), [1, 2, 3]);

        combined = lappend_oid(&scope, Some(combined), 3).unwrap();
        combined = lappend_oid(&scope, Some(combined), 4).unwrap();
        list_deduplicate_oid(&mut combined);
        assert_eq!(oid_values(Some(&combined)), [1, 2, 3, 4]);
    }

    #[test]
    fn copy_head_tail_and_sort_match_original_shapes() {
        let context = test_context("list copy");
        let scope = context.scope();
        let mut list = None;
        for value in [3, 1, 2] {
            list = Some(lappend_int(&scope, list, value).unwrap());
        }
        let mut list = list.unwrap();

        let head = list_copy_head(&scope, Some(&list), 2).unwrap().unwrap();
        assert_eq!(int_values(Some(&head)), [3, 1]);

        let tail = list_copy_tail(&scope, Some(&list), 1).unwrap().unwrap();
        assert_eq!(int_values(Some(&tail)), [1, 2]);

        list_sort(&mut list, list_int_cmp);
        assert_eq!(int_values(Some(&list)), [1, 2, 3]);
    }

    #[test]
    fn raw_round_trip_preserves_ownership() {
        let context = test_context("list raw");
        let scope = context.scope();
        let list = lappend_int(&scope, None, 7).unwrap();
        let raw = list.into_raw();
        let list = unsafe { IntList::from_raw(&scope, raw) }.unwrap().unwrap();

        assert_eq!(list.get(0), Some(7));
    }

    #[test]
    fn explicit_list_free_releases_storage() {
        let context = test_context("list free");
        let scope = context.scope();
        let list = lappend_int(&scope, None, 7).unwrap();
        let raw = list.as_raw();

        list_free(Some(list)).unwrap();
        let next = lappend_int(&scope, None, 8).unwrap();

        assert_eq!(next.as_raw(), raw);
    }
}
