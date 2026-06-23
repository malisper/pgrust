//! Support for integrated/inline doubly- and singly-linked lists
//! (`src/backend/lib/ilist.c`).
//!
//! This is a C-faithful, raw-pointer port (the same approach as
//! `backend-lib-dshash` / `backend-utils-hash-dynahash`). The `ilist` family is
//! *intrusive*: a caller embeds a [`dlist_node`] / [`slist_node`] inside its own
//! struct and the list threads aliasing `*mut` links through those caller-owned
//! objects, reaching the embedding struct back via pointer arithmetic
//! (`dlist_container`). That is exactly the shared-mutable-aliasing graph the
//! C code relies on, so the links are modelled as real raw pointers and the node
//! storage stays caller-owned — `ilist` itself never allocates.
//!
//! `ilist.c` contains only the functions "too big to be considered for
//! inlining"; the bulk of the family (init / push / insert / delete-current /
//! iteration) lives as inline functions in `lib/ilist.h` and is emitted into
//! whichever crate uses it. The functions defined *in `ilist.c`* are:
//!
//! - [`slist_delete`] — always compiled.
//! - [`dlist_member_check`], [`dlist_check`], [`slist_check`] — compiled only
//!   under PostgreSQL's `ILIST_DEBUG`, mirrored here behind the `ilist_debug`
//!   cargo feature. Without the feature they are no-ops returning `Ok(())`,
//!   exactly like the C macros that expand to `((void) (head))`.
//!
//! The struct types ([`dlist_node`], [`dlist_head`], [`dclist_head`],
//! [`slist_node`], [`slist_head`]) mirror `lib/ilist.h` field-for-field with the
//! same `#[repr(C)]` layout so embedding/`container_of` arithmetic in consumer
//! crates is byte-compatible with C.
//!
//! # Failure surface
//!
//! In C the integrity checks call `elog(ERROR, ...)`, a non-local exit. The
//! project contract surfaces that as a [`PgResult`] error (see the
//! seam-signatures rule); `slist_delete` has no error path (only a debug
//! `Assert`) and so returns `()`.
#![allow(non_camel_case_types)]

use ::types_error::PgResult;
#[cfg(feature = "ilist_debug")]
use ::types_error::PgError;

/* ============================================================================
 * Struct types (lib/ilist.h), repr(C) field-for-field mirrors.
 * ========================================================================= */

/// `dlist_node` (`lib/ilist.h`) — a node of a doubly linked list. Embed this in
/// structs that need to be part of a doubly linked list.
#[derive(Copy, Clone, Debug)]
#[repr(C)]
pub struct dlist_node {
    /// `dlist_node *prev`.
    pub prev: *mut dlist_node,
    /// `dlist_node *next`.
    pub next: *mut dlist_node,
}

/// `dlist_head` (`lib/ilist.h`) — head of a doubly linked list.
///
/// Non-empty lists are internally circularly linked. `head.next` either points
/// to the first element; to `&head` if it's a circular empty list; or to NULL if
/// empty and not circular (`head.prev` symmetrically for the last element).
#[derive(Copy, Clone, Debug)]
#[repr(C)]
pub struct dlist_head {
    /// `dlist_node head`.
    pub head: dlist_node,
}

/// `dclist_head` (`lib/ilist.h`) — head of a doubly linked list that also keeps
/// a count of the number of items.
#[derive(Copy, Clone, Debug)]
#[repr(C)]
pub struct dclist_head {
    /// `dlist_head dlist` — the actual list header.
    pub dlist: dlist_head,
    /// `uint32 count` — the number of items in the list.
    pub count: u32,
}

/// `slist_node` (`lib/ilist.h`) — a node of a singly linked list. Embed this in
/// structs that need to be part of a singly linked list.
#[derive(Copy, Clone, Debug)]
#[repr(C)]
pub struct slist_node {
    /// `slist_node *next`.
    pub next: *mut slist_node,
}

/// `slist_head` (`lib/ilist.h`) — head of a singly linked list. Singly linked
/// lists are not circular; `head.next` is NULL when empty.
#[derive(Copy, Clone, Debug)]
#[repr(C)]
pub struct slist_head {
    /// `slist_node head`.
    pub head: slist_node,
}

/* ============================================================================
 * ilist.c functions
 * ========================================================================= */

/// Delete `node` from list `head` (`ilist.c:slist_delete`).
///
/// It is not allowed to delete a `node` which is not in the list `head`.
///
/// Caution: this is O(n); consider using `slist_delete_current()` instead.
///
/// # Safety
///
/// `head` must point to a valid `slist_head` and `node` to a node that is a
/// member of that list (the C contract; in C a missing node trips an
/// `Assert(found)` under `USE_ASSERT_CHECKING`). All nodes in the chain must be
/// live for the duration of the walk.
pub unsafe fn slist_delete(head: *mut slist_head, node: *const slist_node) {
    let mut last: *mut slist_node = &raw mut (*head).head;
    let mut cur: *mut slist_node;
    // bool found PG_USED_FOR_ASSERTS_ONLY = false;
    #[cfg(debug_assertions)]
    let mut found: bool = false;

    loop {
        cur = (*last).next;
        if cur.is_null() {
            break;
        }
        if cur == node as *mut slist_node {
            (*last).next = (*cur).next;
            #[cfg(debug_assertions)]
            {
                found = true;
            }
            break;
        }
        last = cur;
    }
    #[cfg(debug_assertions)]
    debug_assert!(found);

    // slist_check(head): no-op unless ILIST_DEBUG; matches the C call, whose
    // elog(ERROR) on corruption is a non-local exit. slist_delete cannot
    // propagate a Result, so a detected corruption aborts (panics), mirroring
    // elog(ERROR) -> longjmp.
    abort_on_corruption(slist_check(head));
}

/// Validate that `node` is a member of `head` (`ilist.c:dlist_member_check`,
/// `#ifdef ILIST_DEBUG`).
///
/// Returns `Ok(())` if `node` is found; otherwise the C code does
/// `elog(ERROR, "double linked list member check failure")`, surfaced here as an
/// `Err`. Without the `ilist_debug` feature this is a no-op returning `Ok(())`,
/// exactly like the C macro.
///
/// # Safety
///
/// `head` and `node` must point to valid, live structures with an intact chain.
pub unsafe fn dlist_member_check(head: *const dlist_head, node: *const dlist_node) -> PgResult<()> {
    #[cfg(feature = "ilist_debug")]
    {
        let mut cur: *const dlist_node;
        // iteration open-coded to due to the use of const
        cur = (*head).head.next;
        while cur != &raw const (*head).head {
            if cur == node {
                return Ok(());
            }
            cur = (*cur).next;
        }
        return Err(PgError::error("double linked list member check failure"));
    }
    #[cfg(not(feature = "ilist_debug"))]
    {
        let _ = (head, node);
        Ok(())
    }
}

/// Verify integrity of a doubly linked list (`ilist.c:dlist_check`,
/// `#ifdef ILIST_DEBUG`).
///
/// Returns `Ok(())` when the list is well-formed (or NULL-initialized as
/// zeroes); a NULL `head` or a corrupted chain reproduces the corresponding
/// C `elog(ERROR, ...)` as an `Err`. Without the `ilist_debug` feature this is a
/// no-op returning `Ok(())`, exactly like the C macro.
///
/// # Safety
///
/// `head`, if non-NULL, must point to a valid structure whose chain is live for
/// the walk.
pub unsafe fn dlist_check(head: *const dlist_head) -> PgResult<()> {
    #[cfg(feature = "ilist_debug")]
    {
        let mut cur: *mut dlist_node;

        if head.is_null() {
            return Err(PgError::error("doubly linked list head address is NULL"));
        }

        if (*head).head.next.is_null() && (*head).head.prev.is_null() {
            return Ok(()); // OK, initialized as zeroes
        }

        // iterate in forward direction
        cur = (*head).head.next;
        while cur != &raw const (*head).head as *mut dlist_node {
            if cur.is_null()
                || (*cur).next.is_null()
                || (*cur).prev.is_null()
                || (*(*cur).prev).next != cur
                || (*(*cur).next).prev != cur
            {
                return Err(PgError::error("doubly linked list is corrupted"));
            }
            cur = (*cur).next;
        }

        // iterate in backward direction
        cur = (*head).head.prev;
        while cur != &raw const (*head).head as *mut dlist_node {
            if cur.is_null()
                || (*cur).next.is_null()
                || (*cur).prev.is_null()
                || (*(*cur).prev).next != cur
                || (*(*cur).next).prev != cur
            {
                return Err(PgError::error("doubly linked list is corrupted"));
            }
            cur = (*cur).prev;
        }

        Ok(())
    }
    #[cfg(not(feature = "ilist_debug"))]
    {
        let _ = head;
        Ok(())
    }
}

/// Verify integrity of a singly linked list (`ilist.c:slist_check`,
/// `#ifdef ILIST_DEBUG`).
///
/// There isn't much we can test in a singly linked list except that it actually
/// ends sometime, i.e. hasn't introduced a cycle or similar; a NULL `head`
/// reproduces the C `elog(ERROR, ...)` as an `Err`. Without the `ilist_debug`
/// feature this is a no-op returning `Ok(())`, exactly like the C macro.
///
/// # Safety
///
/// `head`, if non-NULL, must point to a valid structure whose chain is live for
/// the walk.
pub unsafe fn slist_check(head: *const slist_head) -> PgResult<()> {
    #[cfg(feature = "ilist_debug")]
    {
        if head.is_null() {
            return Err(PgError::error("singly linked list head address is NULL"));
        }

        // there isn't much we can test in a singly linked list except that it
        // actually ends sometime, i.e. hasn't introduced a cycle or similar
        let mut cur: *mut slist_node = (*head).head.next;
        while !cur.is_null() {
            cur = (*cur).next;
        }

        Ok(())
    }
    #[cfg(not(feature = "ilist_debug"))]
    {
        let _ = head;
        Ok(())
    }
}

/* ============================================================================
 * ILIST_DEBUG abort helper
 * ========================================================================= */

/// Consume the result of an `ilist_debug` integrity check from a path that, like
/// the C inline call sites, cannot propagate a [`PgResult`].
///
/// In C, when `slist_check` detects corruption it calls `elog(ERROR, ...)`,
/// which `longjmp`s out and aborts the enclosing operation. The closest analogue
/// for a Rust function that returns `()` is a panic, so under `ilist_debug` this
/// panics carrying the same message text. With the feature off the checks never
/// fail (they return `Ok`), so this is a no-op too, matching the C
/// `((void) (head))` macros.
#[inline]
fn abort_on_corruption(result: PgResult<()>) {
    match result {
        Ok(()) => {}
        #[cfg(feature = "ilist_debug")]
        Err(err) => panic!("{}", err.message()),
        #[cfg(not(feature = "ilist_debug"))]
        Err(_) => {}
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::ptr;

    /// Build a heap-allocated chain of `slist_node`s linked behind a `head`, in
    /// the given push order (each pushed at the front, like `slist_push_head`),
    /// and return the head plus the node pointers (in push order).
    fn build_slist(values: usize) -> (Box<slist_head>, Vec<*mut slist_node>) {
        let mut head = Box::new(slist_head {
            head: slist_node { next: ptr::null_mut() },
        });
        let mut nodes = Vec::new();
        for _ in 0..values {
            // slist_push_head: node->next = head->head.next; head->head.next = node
            let node = Box::into_raw(Box::new(slist_node { next: head.head.next }));
            head.head.next = node;
            nodes.push(node);
        }
        (head, nodes)
    }

    fn slist_to_vec(head: &slist_head) -> Vec<*mut slist_node> {
        let mut out = Vec::new();
        let mut cur = head.head.next;
        while !cur.is_null() {
            out.push(cur);
            cur = unsafe { (*cur).next };
        }
        out
    }

    fn free_nodes(nodes: &[*mut slist_node]) {
        for &n in nodes {
            unsafe { drop(Box::from_raw(n)) };
        }
    }

    #[test]
    fn slist_delete_head_node() {
        // nodes pushed: n0, n1, n2 -> chain order: n2, n1, n0
        let (mut head, nodes) = build_slist(3);
        let order = slist_to_vec(&head);
        assert_eq!(order, vec![nodes[2], nodes[1], nodes[0]]);

        // delete the chain-head (n2)
        unsafe { slist_delete(&mut *head, nodes[2]) };
        assert_eq!(slist_to_vec(&head), vec![nodes[1], nodes[0]]);

        free_nodes(&nodes);
    }

    #[test]
    fn slist_delete_middle_node() {
        let (mut head, nodes) = build_slist(3); // chain: n2, n1, n0
        unsafe { slist_delete(&mut *head, nodes[1]) };
        assert_eq!(slist_to_vec(&head), vec![nodes[2], nodes[0]]);
        free_nodes(&nodes);
    }

    #[test]
    fn slist_delete_tail_node() {
        let (mut head, nodes) = build_slist(3); // chain: n2, n1, n0
        unsafe { slist_delete(&mut *head, nodes[0]) };
        assert_eq!(slist_to_vec(&head), vec![nodes[2], nodes[1]]);
        free_nodes(&nodes);
    }

    #[test]
    fn slist_delete_only_node_empties_list() {
        let (mut head, nodes) = build_slist(1);
        unsafe { slist_delete(&mut *head, nodes[0]) };
        assert!(head.head.next.is_null());
        assert!(slist_to_vec(&head).is_empty());
        free_nodes(&nodes);
    }

    #[test]
    fn struct_layouts_match_c() {
        // repr(C) two-pointer node; head wraps a single node; dclist adds a u32.
        assert_eq!(
            core::mem::size_of::<dlist_node>(),
            2 * core::mem::size_of::<*mut dlist_node>()
        );
        assert_eq!(
            core::mem::size_of::<dlist_head>(),
            core::mem::size_of::<dlist_node>()
        );
        assert_eq!(
            core::mem::size_of::<slist_node>(),
            core::mem::size_of::<*mut slist_node>()
        );
        assert_eq!(
            core::mem::size_of::<slist_head>(),
            core::mem::size_of::<slist_node>()
        );
    }

    #[test]
    fn checks_are_noop_without_feature() {
        // Without ilist_debug, all checks unconditionally return Ok, even for a
        // NULL head (matching the C macro expanding to ((void) (head))).
        #[cfg(not(feature = "ilist_debug"))]
        unsafe {
            assert!(dlist_check(ptr::null()).is_ok());
            assert!(slist_check(ptr::null()).is_ok());
            assert!(dlist_member_check(ptr::null(), ptr::null()).is_ok());
        }
    }

    #[cfg(feature = "ilist_debug")]
    #[test]
    fn slist_check_null_head_errors() {
        unsafe {
            assert!(slist_check(ptr::null()).is_err());
        }
    }

    #[cfg(feature = "ilist_debug")]
    #[test]
    fn dlist_check_null_head_errors() {
        unsafe {
            assert!(dlist_check(ptr::null()).is_err());
        }
    }

    #[cfg(feature = "ilist_debug")]
    #[test]
    fn dlist_check_zeroed_head_ok() {
        let head = dlist_head {
            head: dlist_node {
                prev: ptr::null_mut(),
                next: ptr::null_mut(),
            },
        };
        unsafe {
            assert!(dlist_check(&head).is_ok());
        }
    }

    #[cfg(feature = "ilist_debug")]
    #[test]
    fn dlist_member_check_finds_and_misses() {
        // Build a 2-element circular dlist by hand: head <-> a <-> b <-> head
        let mut head = Box::new(dlist_head {
            head: dlist_node {
                prev: ptr::null_mut(),
                next: ptr::null_mut(),
            },
        });
        let hp: *mut dlist_node = &mut head.head;
        let a = Box::into_raw(Box::new(dlist_node {
            prev: ptr::null_mut(),
            next: ptr::null_mut(),
        }));
        let b = Box::into_raw(Box::new(dlist_node {
            prev: ptr::null_mut(),
            next: ptr::null_mut(),
        }));
        unsafe {
            (*hp).next = a;
            (*a).prev = hp;
            (*a).next = b;
            (*b).prev = a;
            (*b).next = hp;
            (*hp).prev = b;

            assert!(dlist_check(&*head).is_ok());
            assert!(dlist_member_check(&*head, a).is_ok());
            assert!(dlist_member_check(&*head, b).is_ok());

            // a non-member node
            let stray = Box::into_raw(Box::new(dlist_node {
                prev: ptr::null_mut(),
                next: ptr::null_mut(),
            }));
            assert!(dlist_member_check(&*head, stray).is_err());

            drop(Box::from_raw(a));
            drop(Box::from_raw(b));
            drop(Box::from_raw(stray));
        }
    }
}
