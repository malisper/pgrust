//! Port of `src/backend/lib/rbtree.c` — PostgreSQL's generic Red-Black binary
//! tree package (adopted from Thomas Niemann's "Sorting and Searching
//! Algorithms: a Cookbook").
//!
//! Red-black trees are balanced binary trees in which (1) any child of a red
//! node is always black, and (2) every path from root to leaf traverses an equal
//! number of black nodes, so the longest root-to-leaf path is only about twice
//! the shortest and lookups run in O(lg n).
//!
//! # C-faithful raw-pointer port
//!
//! The C `rbtree` API is *intrusive and caller-allocated*: the caller embeds an
//! [`RBTNode`] (color + `parent`/`left`/`right` links) as the first field of its
//! own larger struct, supplies the comparator/combiner/allocfunc/freefunc as
//! `extern`-style callbacks together with a passthrough `arg`, and the tree
//! threads aliasing `*mut RBTNode` pointers through those caller-owned objects
//! with a single shared `RBTNIL` sentinel standing in for NULL. The total node
//! size is passed to [`rbt_create`] and `rbt_copy_data` `memcpy`s the trailing
//! payload bytes around.
//!
//! That contract — caller-owned node storage, opaque function-pointer callbacks
//! carrying a `void *arg`, a shared mutable sentinel, and raw aliasing links —
//! is exactly the C surface the rest of PostgreSQL links against, and it is the
//! one shape Rust's `&mut`/`Box` ownership model forbids. Following the blessed
//! raw-pointer precedent of `backend-lib-dshash` and `backend-utils-hash-dynahash`
//! in this repo, this crate transcribes `rbtree.c` line-for-line through `*mut`
//! pointers (the [`opacity-inherited`] rule: the typed `RBTNode *` of C stays a
//! `*mut RBTNode`, the `void *arg` stays a `*mut c_void`), preserving every
//! behavioural property the C callers rely on. There are no invented handles and
//! no restructuring of the algorithm.
//!
//! [`opacity-inherited`]: https://example.invalid/
//!
//! # Sanctioned divergences (audit against these)
//!
//! 1. **`palloc` of the control struct -> `Box::leak`.** C `rbt_create` `palloc`s
//!    the [`RBTree`] control struct in the caller's memory context and never
//!    frees it ("you can pfree the RBTree node if you feel the urge"). Here it is
//!    `Box::leak`ed, which is the same "allocate once, owned by the surrounding
//!    context, not reclaimed by this package" semantics; like the tiny C `palloc`
//!    it does not surface an allocation failure, so `rbt_create` returns
//!    `*mut RBTree` exactly as the C does.
//! 2. **`elog(ERROR, ...)` -> `PgResult`.** The only `ereport`/`elog` in
//!    `rbtree.c` is the `default:` arm of [`rbt_begin_iterate`]'s order switch
//!    ("unrecognized rbtree iteration order"). Per the project error contract
//!    (C's nonlocal `elog(ERROR)` exit becomes `Err(PgError)` propagation), that
//!    function returns [`PgResult<()>`]; all other functions cannot ereport and
//!    keep their C return type.
//! 3. **`rbt_copy_data` `memcpy` -> raw `copy_nonoverlapping`.** The payload move
//!    is the same byte copy of `node_size - sizeof(RBTNode)` bytes starting one
//!    `RBTNode` past the node header.

#![no_std]
#![allow(non_camel_case_types)]
#![allow(non_upper_case_globals)]
#![allow(clippy::missing_safety_doc)]

extern crate alloc;

use core::ffi::c_void;
use core::ptr;

use backend_utils_error::elog;
use types_core::Size;
use types_error::{PgResult, ERROR};

/*
 * Colors of nodes (values of RBTNode.color)
 */
const RBTBLACK: core::ffi::c_char = 0;
const RBTRED: core::ffi::c_char = 1;

/// `RBTNode` is intended to be used as the first field of a larger struct, whose
/// additional fields carry whatever payload data the caller needs for a tree
/// entry. (The total size of that larger struct is passed to [`rbt_create`].)
/// `RBTNode` is declared here to support this usage, but callers must treat it as
/// an opaque struct.
#[derive(Copy, Clone)]
#[repr(C)]
pub struct RBTNode {
    /// node's current color, red or black
    pub color: core::ffi::c_char,
    /// left child, or RBTNIL if none
    pub left: *mut RBTNode,
    /// right child, or RBTNIL if none
    pub right: *mut RBTNode,
    /// parent, or NULL (not RBTNIL!) if none
    pub parent: *mut RBTNode,
}

/// Available tree iteration orderings (`RBTOrderControl`).
pub type RBTOrderControl = core::ffi::c_uint;
/// inorder: left child, node, right child
pub const LeftRightWalk: RBTOrderControl = 0;
/// reverse inorder: right, node, left
pub const RightLeftWalk: RBTOrderControl = 1;

/// Support function: compare two RBTNodes for less/equal/greater.
pub type rbt_comparator =
    Option<unsafe extern "C" fn(*const RBTNode, *const RBTNode, *mut c_void) -> core::ffi::c_int>;
/// Support function: merge an existing tree entry with a new one.
pub type rbt_combiner =
    Option<unsafe extern "C" fn(*mut RBTNode, *const RBTNode, *mut c_void) -> ()>;
/// Support function: allocate a new RBTNode.
pub type rbt_allocfunc = Option<unsafe extern "C" fn(*mut c_void) -> *mut RBTNode>;
/// Support function: free an old RBTNode.
pub type rbt_freefunc = Option<unsafe extern "C" fn(*mut RBTNode, *mut c_void) -> ()>;

/// RBTree control structure (opaque to callers).
#[repr(C)]
pub struct RBTree {
    /// root node, or RBTNIL if tree is empty
    pub root: *mut RBTNode,

    /* Remaining fields are constant after rbt_create */
    /// actual size of tree nodes
    pub node_size: Size,
    /* The caller-supplied manipulation functions */
    pub comparator: rbt_comparator,
    pub combiner: rbt_combiner,
    pub allocfunc: rbt_allocfunc,
    pub freefunc: rbt_freefunc,
    /// Passthrough arg passed to all manipulation functions
    pub arg: *mut c_void,
}

/// `RBTreeIterator` holds state while traversing a tree. This is declared here so
/// that callers can stack-allocate it, but must otherwise be treated as an opaque
/// struct.
#[repr(C)]
pub struct RBTreeIterator {
    pub rbt: *mut RBTree,
    pub iterate: Option<unsafe extern "C" fn(*mut RBTreeIterator) -> *mut RBTNode>,
    pub last_visited: *mut RBTNode,
    pub is_over: bool,
}

/*
 * all leafs are sentinels, use customized NIL name to prevent
 * collision with system-wide constant NIL which is actually NULL
 */

/// The single shared sentinel node (`RBTNIL`).
///
/// C declares `static RBTNode sentinel` with `.left`/`.right` self-referencing
/// `&sentinel` and `.parent = NULL`. A `static mut` cannot hold a pointer to
/// itself at initialization in Rust, so the self-referencing links are installed
/// once (idempotently) by [`rbtnil`], and `RBTNIL` is read through that accessor.
static mut SENTINEL: RBTNode = RBTNode {
    color: RBTBLACK,
    left: ptr::null_mut(),
    right: ptr::null_mut(),
    parent: ptr::null_mut(),
};

/// `RBTNIL`: the address of the shared sentinel, with its self-referencing
/// `left`/`right` links established (`parent` stays NULL, as in C).
#[inline]
unsafe fn rbtnil() -> *mut RBTNode {
    let nil = &raw mut SENTINEL;
    // Idempotently install the self-loops the C static initializer encodes
    // (`.left = RBTNIL, .right = RBTNIL`); `parent` is NULL, never RBTNIL.
    (*nil).left = nil;
    (*nil).right = nil;
    nil
}

/*
 * rbt_create: create an empty RBTree
 *
 * Arguments are:
 *	node_size: actual size of tree nodes (> sizeof(RBTNode))
 *	The manipulation functions:
 *	comparator: compare two RBTNodes for less/equal/greater
 *	combiner: merge an existing tree entry with a new one
 *	allocfunc: allocate a new RBTNode
 *	freefunc: free an old RBTNode
 *	arg: passthrough pointer that will be passed to the manipulation functions
 *
 * Note that the combiner's righthand argument will be a "proposed" tree node,
 * ie the input to rbt_insert, in which the RBTNode fields themselves aren't
 * valid.  Similarly, either input to the comparator may be a "proposed" node.
 * This shouldn't matter since the functions aren't supposed to look at the
 * RBTNode fields, only the extra fields of the struct the RBTNode is embedded
 * in.
 *
 * The freefunc should just be pfree or equivalent; it should NOT attempt
 * to free any subsidiary data, because the node passed to it may not contain
 * valid data!	freefunc can be NULL if caller doesn't require retail
 * space reclamation.
 *
 * The RBTree node is palloc'd in the caller's memory context.  Note that
 * all contents of the tree are actually allocated by the caller, not here.
 *
 * Since tree contents are managed by the caller, there is currently not
 * an explicit "destroy" operation; typically a tree would be freed by
 * resetting or deleting the memory context it's stored in.  You can pfree
 * the RBTree node if you feel the urge.
 */
pub unsafe fn rbt_create(
    node_size: Size,
    comparator: rbt_comparator,
    combiner: rbt_combiner,
    allocfunc: rbt_allocfunc,
    freefunc: rbt_freefunc,
    arg: *mut c_void,
) -> *mut RBTree {
    // palloc(sizeof(RBTree)): allocate the control struct in the caller's
    // memory context. Box::leak gives the same "owned by the surrounding
    // context, never freed by this package" lifetime as the C palloc.
    let tree: *mut RBTree = alloc::boxed::Box::leak(alloc::boxed::Box::new(RBTree {
        root: ptr::null_mut(),
        node_size: 0,
        comparator: None,
        combiner: None,
        allocfunc: None,
        freefunc: None,
        arg: ptr::null_mut(),
    }));

    debug_assert!(node_size > core::mem::size_of::<RBTNode>());

    (*tree).root = rbtnil();
    (*tree).node_size = node_size;
    (*tree).comparator = comparator;
    (*tree).combiner = combiner;
    (*tree).allocfunc = allocfunc;
    (*tree).freefunc = freefunc;

    (*tree).arg = arg;

    tree
}

/// Copy the additional data fields from one RBTNode to another.
#[inline]
unsafe fn rbt_copy_data(rbt: *mut RBTree, dest: *mut RBTNode, src: *const RBTNode) {
    ptr::copy_nonoverlapping(
        src.add(1) as *const u8,
        dest.add(1) as *mut u8,
        (*rbt).node_size - core::mem::size_of::<RBTNode>(),
    );
}

/**********************************************************************
 *						  Search									  *
 **********************************************************************/

/*
 * rbt_find: search for a value in an RBTree
 *
 * data represents the value to try to find.  Its RBTNode fields need not
 * be valid, it's the extra data in the larger struct that is of interest.
 *
 * Returns the matching tree entry, or NULL if no match is found.
 */
pub unsafe fn rbt_find(rbt: *mut RBTree, data: *const RBTNode) -> *mut RBTNode {
    let mut node: *mut RBTNode = (*rbt).root;

    while node != rbtnil() {
        let cmp: core::ffi::c_int = ((*rbt).comparator.unwrap_unchecked())(data, node, (*rbt).arg);

        if cmp == 0 {
            return node;
        } else if cmp < 0 {
            node = (*node).left;
        } else {
            node = (*node).right;
        }
    }

    ptr::null_mut()
}

/*
 * rbt_find_great: search for a greater value in an RBTree
 *
 * If equal_match is true, this will be a great or equal search.
 *
 * Returns the matching tree entry, or NULL if no match is found.
 */
pub unsafe fn rbt_find_great(
    rbt: *mut RBTree,
    data: *const RBTNode,
    equal_match: bool,
) -> *mut RBTNode {
    let mut node: *mut RBTNode = (*rbt).root;
    let mut greater: *mut RBTNode = ptr::null_mut();

    while node != rbtnil() {
        let cmp: core::ffi::c_int = ((*rbt).comparator.unwrap_unchecked())(data, node, (*rbt).arg);

        if equal_match && cmp == 0 {
            return node;
        } else if cmp < 0 {
            greater = node;
            node = (*node).left;
        } else {
            node = (*node).right;
        }
    }

    greater
}

/*
 * rbt_find_less: search for a lesser value in an RBTree
 *
 * If equal_match is true, this will be a less or equal search.
 *
 * Returns the matching tree entry, or NULL if no match is found.
 */
pub unsafe fn rbt_find_less(
    rbt: *mut RBTree,
    data: *const RBTNode,
    equal_match: bool,
) -> *mut RBTNode {
    let mut node: *mut RBTNode = (*rbt).root;
    let mut lesser: *mut RBTNode = ptr::null_mut();

    while node != rbtnil() {
        let cmp: core::ffi::c_int = ((*rbt).comparator.unwrap_unchecked())(data, node, (*rbt).arg);

        if equal_match && cmp == 0 {
            return node;
        } else if cmp > 0 {
            lesser = node;
            node = (*node).right;
        } else {
            node = (*node).left;
        }
    }

    lesser
}

/*
 * rbt_leftmost: fetch the leftmost (smallest-valued) tree node.
 * Returns NULL if tree is empty.
 *
 * Note: in the original implementation this included an unlink step, but
 * that's a bit awkward.  Just call rbt_delete on the result if that's what
 * you want.
 */
pub unsafe fn rbt_leftmost(rbt: *mut RBTree) -> *mut RBTNode {
    let mut node: *mut RBTNode = (*rbt).root;
    let mut leftmost: *mut RBTNode = (*rbt).root;

    while node != rbtnil() {
        leftmost = node;
        node = (*node).left;
    }

    if leftmost != rbtnil() {
        return leftmost;
    }

    ptr::null_mut()
}

/**********************************************************************
 *							  Insertion								  *
 **********************************************************************/

/*
 * Rotate node x to left.
 *
 * x's right child takes its place in the tree, and x becomes the left
 * child of that node.
 */
unsafe fn rbt_rotate_left(rbt: *mut RBTree, x: *mut RBTNode) {
    let y: *mut RBTNode = (*x).right;

    /* establish x->right link */
    (*x).right = (*y).left;
    if (*y).left != rbtnil() {
        (*(*y).left).parent = x;
    }

    /* establish y->parent link */
    if y != rbtnil() {
        (*y).parent = (*x).parent;
    }
    if !(*x).parent.is_null() {
        if x == (*(*x).parent).left {
            (*(*x).parent).left = y;
        } else {
            (*(*x).parent).right = y;
        }
    } else {
        (*rbt).root = y;
    }

    /* link x and y */
    (*y).left = x;
    if x != rbtnil() {
        (*x).parent = y;
    }
}

/*
 * Rotate node x to right.
 *
 * x's left right child takes its place in the tree, and x becomes the right
 * child of that node.
 */
unsafe fn rbt_rotate_right(rbt: *mut RBTree, x: *mut RBTNode) {
    let y: *mut RBTNode = (*x).left;

    /* establish x->left link */
    (*x).left = (*y).right;
    if (*y).right != rbtnil() {
        (*(*y).right).parent = x;
    }

    /* establish y->parent link */
    if y != rbtnil() {
        (*y).parent = (*x).parent;
    }
    if !(*x).parent.is_null() {
        if x == (*(*x).parent).right {
            (*(*x).parent).right = y;
        } else {
            (*(*x).parent).left = y;
        }
    } else {
        (*rbt).root = y;
    }

    /* link x and y */
    (*y).right = x;
    if x != rbtnil() {
        (*x).parent = y;
    }
}

/*
 * Maintain Red-Black tree balance after inserting node x.
 *
 * The newly inserted node is always initially marked red.  That may lead to
 * a situation where a red node has a red child, which is prohibited.  We can
 * always fix the problem by a series of color changes and/or "rotations",
 * which move the problem progressively higher up in the tree.  If one of the
 * two red nodes is the root, we can always fix the problem by changing the
 * root from red to black.
 *
 * (This does not work lower down in the tree because we must also maintain
 * the invariant that every leaf has equal black-height.)
 */
unsafe fn rbt_insert_fixup(rbt: *mut RBTree, mut x: *mut RBTNode) {
    /*
     * x is always a red node.  Initially, it is the newly inserted node. Each
     * iteration of this loop moves it higher up in the tree.
     */
    while x != (*rbt).root && (*(*x).parent).color == RBTRED {
        /*
         * x and x->parent are both red.  Fix depends on whether x->parent is
         * a left or right child.  In either case, we define y to be the
         * "uncle" of x, that is, the other child of x's grandparent.
         *
         * If the uncle is red, we flip the grandparent to red and its two
         * children to black.  Then we loop around again to check whether the
         * grandparent still has a problem.
         *
         * If the uncle is black, we will perform one or two "rotations" to
         * balance the tree.  Either x or x->parent will take the
         * grandparent's position in the tree and recolored black, and the
         * original grandparent will be recolored red and become a child of
         * that node. This always leaves us with a valid red-black tree, so
         * the loop will terminate.
         */
        if (*x).parent == (*(*(*x).parent).parent).left {
            let y: *mut RBTNode = (*(*(*x).parent).parent).right;

            if (*y).color == RBTRED {
                /* uncle is RBTRED */
                (*(*x).parent).color = RBTBLACK;
                (*y).color = RBTBLACK;
                (*(*(*x).parent).parent).color = RBTRED;

                x = (*(*x).parent).parent;
            } else {
                /* uncle is RBTBLACK */
                if x == (*(*x).parent).right {
                    /* make x a left child */
                    x = (*x).parent;
                    rbt_rotate_left(rbt, x);
                }

                /* recolor and rotate */
                (*(*x).parent).color = RBTBLACK;
                (*(*(*x).parent).parent).color = RBTRED;

                rbt_rotate_right(rbt, (*(*x).parent).parent);
            }
        } else {
            /* mirror image of above code */
            let y: *mut RBTNode = (*(*(*x).parent).parent).left;

            if (*y).color == RBTRED {
                /* uncle is RBTRED */
                (*(*x).parent).color = RBTBLACK;
                (*y).color = RBTBLACK;
                (*(*(*x).parent).parent).color = RBTRED;

                x = (*(*x).parent).parent;
            } else {
                /* uncle is RBTBLACK */
                if x == (*(*x).parent).left {
                    x = (*x).parent;
                    rbt_rotate_right(rbt, x);
                }
                (*(*x).parent).color = RBTBLACK;
                (*(*(*x).parent).parent).color = RBTRED;

                rbt_rotate_left(rbt, (*(*x).parent).parent);
            }
        }
    }

    /*
     * The root may already have been black; if not, the black-height of every
     * node in the tree increases by one.
     */
    (*(*rbt).root).color = RBTBLACK;
}

/*
 * rbt_insert: insert a new value into the tree.
 *
 * data represents the value to insert.  Its RBTNode fields need not
 * be valid, it's the extra data in the larger struct that is of interest.
 *
 * If the value represented by "data" is not present in the tree, then
 * we copy "data" into a new tree entry and return that node, setting *isNew
 * to true.
 *
 * If the value represented by "data" is already present, then we call the
 * combiner function to merge data into the existing node, and return the
 * existing node, setting *isNew to false.
 *
 * "data" is unmodified in either case; it's typically just a local
 * variable in the caller.
 */
pub unsafe fn rbt_insert(rbt: *mut RBTree, data: *const RBTNode, is_new: *mut bool) -> *mut RBTNode {
    let mut current: *mut RBTNode;
    let mut parent: *mut RBTNode;
    let x: *mut RBTNode;
    let mut cmp: core::ffi::c_int;

    /* find where node belongs */
    current = (*rbt).root;
    parent = ptr::null_mut();
    cmp = 0; /* just to prevent compiler warning */

    while current != rbtnil() {
        cmp = ((*rbt).comparator.unwrap_unchecked())(data, current, (*rbt).arg);
        if cmp == 0 {
            /*
             * Found node with given key.  Apply combiner.
             */
            ((*rbt).combiner.unwrap_unchecked())(current, data, (*rbt).arg);
            *is_new = false;
            return current;
        }
        parent = current;
        current = if cmp < 0 { (*current).left } else { (*current).right };
    }

    /*
     * Value is not present, so create a new node containing data.
     */
    *is_new = true;

    x = ((*rbt).allocfunc.unwrap_unchecked())((*rbt).arg);

    (*x).color = RBTRED;

    (*x).left = rbtnil();
    (*x).right = rbtnil();
    (*x).parent = parent;
    rbt_copy_data(rbt, x, data);

    /* insert node in tree */
    if !parent.is_null() {
        if cmp < 0 {
            (*parent).left = x;
        } else {
            (*parent).right = x;
        }
    } else {
        (*rbt).root = x;
    }

    rbt_insert_fixup(rbt, x);

    x
}

/**********************************************************************
 *							Deletion								  *
 **********************************************************************/

/*
 * Maintain Red-Black tree balance after deleting a black node.
 */
unsafe fn rbt_delete_fixup(rbt: *mut RBTree, mut x: *mut RBTNode) {
    /*
     * x is always a black node.  Initially, it is the former child of the
     * deleted node.  Each iteration of this loop moves it higher up in the
     * tree.
     */
    while x != (*rbt).root && (*x).color == RBTBLACK {
        /*
         * Left and right cases are symmetric.  Any nodes that are children of
         * x have a black-height one less than the remainder of the nodes in
         * the tree.  We rotate and recolor nodes to move the problem up the
         * tree: at some stage we'll either fix the problem, or reach the root
         * (where the black-height is allowed to decrease).
         */
        if x == (*(*x).parent).left {
            let mut w: *mut RBTNode = (*(*x).parent).right;

            if (*w).color == RBTRED {
                (*w).color = RBTBLACK;
                (*(*x).parent).color = RBTRED;

                rbt_rotate_left(rbt, (*x).parent);
                w = (*(*x).parent).right;
            }

            if (*(*w).left).color == RBTBLACK && (*(*w).right).color == RBTBLACK {
                (*w).color = RBTRED;

                x = (*x).parent;
            } else {
                if (*(*w).right).color == RBTBLACK {
                    (*(*w).left).color = RBTBLACK;
                    (*w).color = RBTRED;

                    rbt_rotate_right(rbt, w);
                    w = (*(*x).parent).right;
                }
                (*w).color = (*(*x).parent).color;
                (*(*x).parent).color = RBTBLACK;
                (*(*w).right).color = RBTBLACK;

                rbt_rotate_left(rbt, (*x).parent);
                x = (*rbt).root; /* Arrange for loop to terminate. */
            }
        } else {
            let mut w: *mut RBTNode = (*(*x).parent).left;

            if (*w).color == RBTRED {
                (*w).color = RBTBLACK;
                (*(*x).parent).color = RBTRED;

                rbt_rotate_right(rbt, (*x).parent);
                w = (*(*x).parent).left;
            }

            if (*(*w).right).color == RBTBLACK && (*(*w).left).color == RBTBLACK {
                (*w).color = RBTRED;

                x = (*x).parent;
            } else {
                if (*(*w).left).color == RBTBLACK {
                    (*(*w).right).color = RBTBLACK;
                    (*w).color = RBTRED;

                    rbt_rotate_left(rbt, w);
                    w = (*(*x).parent).left;
                }
                (*w).color = (*(*x).parent).color;
                (*(*x).parent).color = RBTBLACK;
                (*(*w).left).color = RBTBLACK;

                rbt_rotate_right(rbt, (*x).parent);
                x = (*rbt).root; /* Arrange for loop to terminate. */
            }
        }
    }
    (*x).color = RBTBLACK;
}

/*
 * Delete node z from tree.
 */
unsafe fn rbt_delete_node(rbt: *mut RBTree, z: *mut RBTNode) {
    let x: *mut RBTNode;
    let y: *mut RBTNode;

    /* This is just paranoia: we should only get called on a valid node */
    if z.is_null() || z == rbtnil() {
        return;
    }

    /*
     * y is the node that will actually be removed from the tree.  This will
     * be z if z has fewer than two children, or the tree successor of z
     * otherwise.
     */
    if (*z).left == rbtnil() || (*z).right == rbtnil() {
        /* y has a RBTNIL node as a child */
        y = z;
    } else {
        /* find tree successor */
        let mut yy: *mut RBTNode = (*z).right;
        while (*yy).left != rbtnil() {
            yy = (*yy).left;
        }
        y = yy;
    }

    /* x is y's only child */
    if (*y).left != rbtnil() {
        x = (*y).left;
    } else {
        x = (*y).right;
    }

    /* Remove y from the tree. */
    (*x).parent = (*y).parent;
    if !(*y).parent.is_null() {
        if y == (*(*y).parent).left {
            (*(*y).parent).left = x;
        } else {
            (*(*y).parent).right = x;
        }
    } else {
        (*rbt).root = x;
    }

    /*
     * If we removed the tree successor of z rather than z itself, then move
     * the data for the removed node to the one we were supposed to remove.
     */
    if y != z {
        rbt_copy_data(rbt, z, y);
    }

    /*
     * Removing a black node might make some paths from root to leaf contain
     * fewer black nodes than others, or it might make two red nodes adjacent.
     */
    if (*y).color == RBTBLACK {
        rbt_delete_fixup(rbt, x);
    }

    /* Now we can recycle the y node */
    if (*rbt).freefunc.is_some() {
        ((*rbt).freefunc.unwrap_unchecked())(y, (*rbt).arg);
    }
}

/*
 * rbt_delete: remove the given tree entry
 *
 * "node" must have previously been found via rbt_find or rbt_leftmost.
 * It is caller's responsibility to free any subsidiary data attached
 * to the node before calling rbt_delete.  (Do *not* try to push that
 * responsibility off to the freefunc, as some other physical node
 * may be the one actually freed!)
 */
pub unsafe fn rbt_delete(rbt: *mut RBTree, node: *mut RBTNode) {
    rbt_delete_node(rbt, node);
}

/**********************************************************************
 *						  Traverse									  *
 **********************************************************************/

unsafe extern "C" fn rbt_left_right_iterator(iter: *mut RBTreeIterator) -> *mut RBTNode {
    if (*iter).last_visited.is_null() {
        (*iter).last_visited = (*(*iter).rbt).root;
        while (*(*iter).last_visited).left != rbtnil() {
            (*iter).last_visited = (*(*iter).last_visited).left;
        }

        return (*iter).last_visited;
    }

    if (*(*iter).last_visited).right != rbtnil() {
        (*iter).last_visited = (*(*iter).last_visited).right;
        while (*(*iter).last_visited).left != rbtnil() {
            (*iter).last_visited = (*(*iter).last_visited).left;
        }

        return (*iter).last_visited;
    }

    loop {
        let came_from: *mut RBTNode = (*iter).last_visited;

        (*iter).last_visited = (*(*iter).last_visited).parent;
        if (*iter).last_visited.is_null() {
            (*iter).is_over = true;
            break;
        }

        if (*(*iter).last_visited).left == came_from {
            break; /* came from left sub-tree, return current node */
        }

        /* else - came from right sub-tree, continue to move up */
    }

    (*iter).last_visited
}

unsafe extern "C" fn rbt_right_left_iterator(iter: *mut RBTreeIterator) -> *mut RBTNode {
    if (*iter).last_visited.is_null() {
        (*iter).last_visited = (*(*iter).rbt).root;
        while (*(*iter).last_visited).right != rbtnil() {
            (*iter).last_visited = (*(*iter).last_visited).right;
        }

        return (*iter).last_visited;
    }

    if (*(*iter).last_visited).left != rbtnil() {
        (*iter).last_visited = (*(*iter).last_visited).left;
        while (*(*iter).last_visited).right != rbtnil() {
            (*iter).last_visited = (*(*iter).last_visited).right;
        }

        return (*iter).last_visited;
    }

    loop {
        let came_from: *mut RBTNode = (*iter).last_visited;

        (*iter).last_visited = (*(*iter).last_visited).parent;
        if (*iter).last_visited.is_null() {
            (*iter).is_over = true;
            break;
        }

        if (*(*iter).last_visited).right == came_from {
            break; /* came from right sub-tree, return current node */
        }

        /* else - came from left sub-tree, continue to move up */
    }

    (*iter).last_visited
}

/*
 * rbt_begin_iterate: prepare to traverse the tree in any of several orders
 *
 * After calling rbt_begin_iterate, call rbt_iterate repeatedly until it
 * returns NULL or the traversal stops being of interest.
 *
 * If the tree is changed during traversal, results of further calls to
 * rbt_iterate are unspecified.  Multiple concurrent iterators on the same
 * tree are allowed.
 *
 * The iterator state is stored in the 'iter' struct.  The caller should
 * treat it as an opaque struct.
 */
pub unsafe fn rbt_begin_iterate(
    rbt: *mut RBTree,
    ctrl: RBTOrderControl,
    iter: *mut RBTreeIterator,
) -> PgResult<()> {
    /* Common initialization for all traversal orders */
    (*iter).rbt = rbt;
    (*iter).last_visited = ptr::null_mut();
    (*iter).is_over = (*rbt).root == rbtnil();

    match ctrl {
        LeftRightWalk => {
            /* visit left, then self, then right */
            (*iter).iterate = Some(rbt_left_right_iterator);
        }
        RightLeftWalk => {
            /* visit right, then self, then left */
            (*iter).iterate = Some(rbt_right_left_iterator);
        }
        _ => {
            elog(
                ERROR,
                alloc::format!("unrecognized rbtree iteration order: {ctrl}"),
            )?;
        }
    }

    Ok(())
}

/*
 * rbt_iterate: return the next node in traversal order, or NULL if no more
 */
pub unsafe fn rbt_iterate(iter: *mut RBTreeIterator) -> *mut RBTNode {
    if (*iter).is_over {
        return ptr::null_mut();
    }

    ((*iter).iterate.unwrap_unchecked())(iter)
}

#[cfg(test)]
extern crate std;

#[cfg(test)]
mod tests;
