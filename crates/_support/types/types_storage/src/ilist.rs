//! Intrusive doubly-linked lists (`lib/ilist.h`), the embedded-link list type
//! used by `PGPROC`, `PROC_HDR`, `LOCK`, and `PROCLOCK`.
//!
//! In C a `dlist_node` is two raw pointers spliced directly into the
//! containing struct, and a list is reached by `dlist_container` pointer
//! arithmetic from a node back to its owner. The idiomatic translation keeps
//! the embedded node as a real field (so the struct layout mirrors C
//! field-for-field) and represents the links as owning boxes; the list
//! algorithms that splice nodes live in the crate that owns the list.

use alloc::boxed::Box;

/// `dlist_node` (`lib/ilist.h`) — a node embedded in a list element. A node
/// not currently in any list has both links `None` (C: zero-initialized).
#[derive(Clone, Debug, Default)]
pub struct dlist_node {
    /// `dlist_node *prev`.
    pub prev: Option<Box<dlist_node>>,
    /// `dlist_node *next`.
    pub next: Option<Box<dlist_node>>,
}

impl dlist_node {
    pub const fn new() -> Self {
        Self {
            prev: None,
            next: None,
        }
    }
}

/// `dlist_head` (`lib/ilist.h`) — the head of a doubly-linked list. `head.next`
/// points to the first element (or to `&head` for a circular empty list, or
/// `None` for a zero-initialized empty list); `head.prev` points to the last.
#[derive(Clone, Debug, Default)]
pub struct dlist_head {
    /// `dlist_node head`.
    pub head: dlist_node,
}

impl dlist_head {
    pub const fn new() -> Self {
        Self {
            head: dlist_node::new(),
        }
    }
}

/// `dclist_head` (`lib/ilist.h`) — a doubly-linked list head that also tracks
/// the element count.
#[derive(Clone, Debug, Default)]
pub struct dclist_head {
    /// `dlist_head dlist` — the actual list header.
    pub dlist: dlist_head,
    /// `uint32 count` — the number of items in the list.
    pub count: u32,
}

impl dclist_head {
    pub const fn new() -> Self {
        Self {
            dlist: dlist_head::new(),
            count: 0,
        }
    }
}
