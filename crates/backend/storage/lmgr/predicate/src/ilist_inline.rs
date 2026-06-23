//! Inline `dlist` helpers from `lib/ilist.h` (the C `static inline` functions
//! that `backend-lib-ilist` does NOT emit — it only carries the `ilist.c`
//! out-of-line functions). Ported 1:1, raw-pointer/`unsafe`, exactly mirroring
//! the C pointer surgery so the intrusive lists weave through the shmem structs
//! byte-for-byte.

#![allow(dead_code)]

use ilist::{dlist_head, dlist_node};

/// `dlist_node_init(node)`.
#[inline]
pub unsafe fn dlist_node_init(node: *mut dlist_node) {
    (*node).next = core::ptr::null_mut();
    (*node).prev = core::ptr::null_mut();
}

/// `dlist_init(head)`.
#[inline]
pub unsafe fn dlist_init(head: *mut dlist_head) {
    let h = &raw mut (*head).head;
    (*head).head.next = h;
    (*head).head.prev = h;
}

/// `dlist_is_empty(head)`.
#[inline]
pub unsafe fn dlist_is_empty(head: *const dlist_head) -> bool {
    (*head).head.next == (&raw const (*head).head) as *mut dlist_node
}

/// `dlist_node_is_detached(node)`.
#[inline]
pub fn dlist_node_is_detached(node: *const dlist_node) -> bool {
    unsafe { (*node).next.is_null() }
}

/// `dlist_push_head(head, node)`.
#[inline]
pub unsafe fn dlist_push_head(head: *mut dlist_head, node: *mut dlist_node) {
    let h = &raw mut (*head).head;
    if (*head).head.next.is_null() {
        dlist_init(head);
    }
    (*node).next = (*head).head.next;
    (*node).prev = h;
    (*(*node).next).prev = node;
    (*head).head.next = node;
}

/// `dlist_push_tail(head, node)`.
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

/// `dlist_delete(node)`.
#[inline]
pub unsafe fn dlist_delete(node: *mut dlist_node) {
    (*(*node).prev).next = (*node).next;
    (*(*node).next).prev = (*node).prev;
}

/// `dlist_delete_thoroughly(node)`.
#[inline]
pub unsafe fn dlist_delete_thoroughly(node: *mut dlist_node) {
    (*(*node).prev).next = (*node).next;
    (*(*node).next).prev = (*node).prev;
    (*node).next = core::ptr::null_mut();
    (*node).prev = core::ptr::null_mut();
}

/// `dlist_pop_head_node(head)`.
#[inline]
pub unsafe fn dlist_pop_head_node(head: *mut dlist_head) -> *mut dlist_node {
    debug_assert!(!dlist_is_empty(head));
    let node = (*head).head.next;
    dlist_delete(node);
    node
}

/// `dlist_container(type, membername, ptr)` — recover the enclosing struct
/// pointer from a pointer to its embedded `dlist_node` field.
#[macro_export]
macro_rules! dlist_container {
    ($Type:ty, $member:ident, $ptr:expr) => {{
        let __ptr: *mut ::ilist::dlist_node = $ptr;
        (__ptr as *mut u8).sub(core::mem::offset_of!($Type, $member)) as *mut $Type
    }};
}

/// `dlist_head_element(type, membername, head)` — the container of the first
/// node in the list (caller must ensure non-empty).
#[macro_export]
macro_rules! dlist_head_element {
    ($Type:ty, $member:ident, $head:expr) => {{
        let __head: *mut ::ilist::dlist_head = $head;
        $crate::dlist_container!($Type, $member, (*__head).head.next)
    }};
}
