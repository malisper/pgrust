use core::ffi::{c_int, c_void};
use core::mem::offset_of;
use core::ptr::NonNull;
use core::slice;

use crate::types::{NodeTag, Oid, TransactionId};

pub const T_Invalid: NodeTag = 0;
pub const T_List: NodeTag = 1;
pub const T_IntList: NodeTag = 471;
pub const T_OidList: NodeTag = 472;
pub const T_XidList: NodeTag = 473;

#[repr(C)]
#[derive(Clone, Copy)]
pub union ListCell {
    pub ptr_value: *mut c_void,
    pub int_value: c_int,
    pub oid_value: Oid,
    pub xid_value: TransactionId,
}

impl ListCell {
    pub const fn ptr_value<T>(value: *mut T) -> Self {
        Self {
            ptr_value: value.cast(),
        }
    }

    pub const fn int_value(value: c_int) -> Self {
        Self { int_value: value }
    }

    pub const fn oid_value(value: Oid) -> Self {
        Self { oid_value: value }
    }

    pub const fn xid_value(value: TransactionId) -> Self {
        Self { xid_value: value }
    }

    pub fn ptr<T>(&self) -> *mut T {
        unsafe { self.ptr_value.cast() }
    }

    pub fn int(&self) -> c_int {
        unsafe { self.int_value }
    }

    pub fn oid(&self) -> Oid {
        unsafe { self.oid_value }
    }

    pub fn xid(&self) -> TransactionId {
        unsafe { self.xid_value }
    }
}

#[repr(C)]
pub struct List {
    type_: NodeTag,
    pub length: c_int,
    max_length: c_int,
    pub elements: *mut ListCell,
    initial_elements: [ListCell; 0],
}

impl List {
    pub fn header_size() -> usize {
        offset_of!(Self, initial_elements)
    }

    pub fn header_overhead_cells() -> usize {
        (Self::header_size() - 1) / core::mem::size_of::<ListCell>() + 1
    }

    /// # Safety
    ///
    /// `raw` must point at writable storage large enough for a `List` header
    /// plus `max_length` inline `ListCell`s.
    pub unsafe fn initialize(raw: *mut Self, type_: NodeTag, length: c_int, max_length: c_int) {
        unsafe {
            let elements = core::ptr::addr_of_mut!((*raw).initial_elements).cast::<ListCell>();
            core::ptr::addr_of_mut!((*raw).type_).write(type_);
            core::ptr::addr_of_mut!((*raw).length).write(length);
            core::ptr::addr_of_mut!((*raw).max_length).write(max_length);
            core::ptr::addr_of_mut!((*raw).elements).write(elements);
        }
    }

    pub fn list_type(&self) -> NodeTag {
        self.type_
    }

    pub fn len(&self) -> c_int {
        self.length
    }

    pub fn is_empty(&self) -> bool {
        self.length == 0
    }

    pub fn elements(&self) -> Option<NonNull<ListCell>> {
        NonNull::new(self.elements)
    }

    pub fn cells(&self) -> &[ListCell] {
        unsafe { slice::from_raw_parts(self.elements, self.length as usize) }
    }

    pub fn cells_mut(&mut self) -> &mut [ListCell] {
        unsafe { slice::from_raw_parts_mut(self.elements, self.length as usize) }
    }

    pub fn initial_elements_ptr(&self) -> *mut ListCell {
        core::ptr::addr_of!(self.initial_elements)
            .cast_mut()
            .cast::<ListCell>()
    }

    pub fn elements_ptr(&self) -> *mut ListCell {
        self.elements
    }

    pub fn uses_initial_elements(&self) -> bool {
        self.elements == self.initial_elements_ptr()
    }

    /// # Safety
    ///
    /// `elements` must point to storage for at least `self.max_length()` cells
    /// and must stay valid until the list is freed or changed again.
    pub unsafe fn set_elements_ptr(&mut self, elements: *mut ListCell) {
        self.elements = elements;
    }

    pub fn max_length(&self) -> c_int {
        self.max_length
    }

    pub fn set_max_length(&mut self, max_length: c_int) {
        self.max_length = max_length;
    }

    pub fn set_len(&mut self, length: c_int) {
        self.length = length;
    }

    pub fn as_ptr(&self) -> *const Self {
        self
    }

    pub fn as_mut_ptr(&mut self) -> *mut Self {
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use core::mem::{align_of, offset_of, size_of};

    #[test]
    fn list_layout_matches_postgres_abi() {
        assert_eq!(size_of::<ListCell>(), size_of::<*mut c_void>());
        assert_eq!(align_of::<ListCell>(), align_of::<*mut c_void>());
        assert_eq!(offset_of!(List, type_), 0);
        assert_eq!(offset_of!(List, length), 4);
        assert_eq!(offset_of!(List, max_length), 8);
        assert_eq!(offset_of!(List, elements), 16);
        assert_eq!(List::header_size(), 24);
        assert_eq!(List::header_overhead_cells(), 3);
    }
}
