// Circular intrusive dlist (lib/ilist.h inline semantics) over raw pointers.
// types_storage::ilist is NULL-terminated; SSI's list surgery (container_of
// back-links across three lists per node) needs C's sentinel representation.

#![allow(dead_code)]

#[repr(C)]
pub struct dlist_node {
    pub prev: *mut dlist_node,
    pub next: *mut dlist_node,
}

#[repr(C)]
pub struct dlist_head {
    pub head: dlist_node,
}

#[inline]
pub unsafe fn dlist_node_init(node: *mut dlist_node) {
    (*node).next = core::ptr::null_mut();
    (*node).prev = core::ptr::null_mut();
}

#[inline]
pub unsafe fn dlist_init(head: *mut dlist_head) {
    let h = &raw mut (*head).head;
    (*head).head.next = h;
    (*head).head.prev = h;
}

#[inline]
pub unsafe fn dlist_is_empty(head: *const dlist_head) -> bool {
    (*head).head.next == (&raw const (*head).head) as *mut dlist_node
}

#[inline]
pub fn dlist_node_is_detached(node: *const dlist_node) -> bool {
    unsafe { (*node).next.is_null() }
}

#[inline]
pub unsafe fn dlist_push_tail(head: *mut dlist_head, node: *mut dlist_node) {
    let h = &raw mut (*head).head;
    if (*head).head.next.is_null() {
        dlist_init(head);
    }
    (*node).next = h;
    (*node).prev = (*head).head.prev;
    (*(*node).prev).next = node;
    (*head).head.prev = node;
}

#[inline]
pub unsafe fn dlist_delete(node: *mut dlist_node) {
    (*(*node).prev).next = (*node).next;
    (*(*node).next).prev = (*node).prev;
}

#[inline]
pub unsafe fn dlist_delete_thoroughly(node: *mut dlist_node) {
    dlist_delete(node);
    (*node).next = core::ptr::null_mut();
    (*node).prev = core::ptr::null_mut();
}

#[inline]
pub unsafe fn dlist_pop_head_node(head: *mut dlist_head) -> *mut dlist_node {
    debug_assert!(!dlist_is_empty(head));
    let node = (*head).head.next;
    dlist_delete(node);
    node
}

macro_rules! dlist_container {
    ($Type:ty, $member:ident, $ptr:expr) => {{
        let __ptr: *mut $crate::ilist::dlist_node = $ptr;
        (__ptr as *mut u8).sub(core::mem::offset_of!($Type, $member)) as *mut $Type
    }};
}
pub(crate) use dlist_container;
