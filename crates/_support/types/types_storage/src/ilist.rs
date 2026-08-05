use core::ptr::NonNull;

// C divergence from fabled: links are raw pointers (C's exact two-word node,
// no drop glue), not owning boxes. Nothing in this crate splices them; the
// owning unit's algorithms do.
#[derive(Clone, Copy, Debug, Default)]
pub struct dlist_node {
    pub prev: Option<NonNull<dlist_node>>,
    pub next: Option<NonNull<dlist_node>>,
}

impl dlist_node {
    pub const fn new() -> Self {
        Self {
            prev: None,
            next: None,
        }
    }
}

#[derive(Clone, Copy, Debug, Default)]
pub struct dlist_head {
    pub head: dlist_node,
}

impl dlist_head {
    pub const fn new() -> Self {
        Self {
            head: dlist_node::new(),
        }
    }
}

#[derive(Clone, Copy, Debug, Default)]
pub struct dclist_head {
    pub dlist: dlist_head,
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

const _: () = assert!(core::mem::size_of::<dlist_node>() == 2 * core::mem::size_of::<usize>());
