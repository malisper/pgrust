//! Port of `src/backend/utils/adt/tsquery_cleanup.c` — "Cleanup query from NOT
//! values and/or stopword".

use alloc::vec::Vec;

use ::mcx::{alloc_in, Mcx, PgBox};
use ::utils_error::ereport;
use ::types_error::{PgError, PgResult, NOTICE};
use ::tsearch::tsearch::{
    QueryItem, QueryItemType, HDRSIZETQ, OP_AND, OP_NOT, OP_OR, OP_PHRASE, QI_OPR, QI_VAL,
    QI_VALSTOP,
};

use postgres_seams as tcop;

use crate::util::{
    encode_record, oom, operand_distance as op_distance, operand_length, tsq_size, QI_SIZE,
};

/// `Max(a, b)` (c.h).
#[inline]
fn max_i32(a: i32, b: i32) -> i32 {
    if a > b {
        a
    } else {
        b
    }
}

// ===========================================================================
// QueryItem field accessors (mirror the C union member accesses)
// ===========================================================================

/// `item->type` — the shared leading type tag.
#[inline]
fn qi_type(item: &QueryItem) -> QueryItemType {
    item.item_type()
}

/// `item->qoperator.oper`. Read only for `type == QI_OPR`.
#[inline]
fn qi_oper(item: &QueryItem) -> i8 {
    match item {
        QueryItem::Qoperator(o) => o.oper,
        _ => 0,
    }
}

/// `item->qoperator.left`. Read only for `type == QI_OPR`.
#[inline]
fn qi_left(item: &QueryItem) -> u32 {
    match item {
        QueryItem::Qoperator(o) => o.left,
        _ => 0,
    }
}

/// `item->qoperator.distance`. Read only for an `OP_PHRASE` operator node.
#[inline]
fn qi_distance(item: &QueryItem) -> i16 {
    match item {
        QueryItem::Qoperator(o) => o.distance,
        _ => 0,
    }
}

/// `item->qoperand.length`. Read only for `type == QI_VAL`.
#[inline]
fn qi_operand_length(item: &QueryItem) -> u32 {
    operand_length(item)
}

/// `op->distance` for a `QI_VAL` operand node.
#[inline]
fn qi_operand_distance(item: &QueryItem) -> u32 {
    op_distance(item)
}

/// `item->qoperator.left = v` (write through the operator view).
#[inline]
fn set_qi_left(item: &mut QueryItem, v: u32) {
    if let QueryItem::Qoperator(o) = item {
        o.left = v;
    }
}

/// `node->valnode->qoperator.distance += v` (write the phrase distance).
#[inline]
fn add_qi_distance(item: &mut QueryItem, v: i32) {
    if let QueryItem::Qoperator(o) = item {
        o.distance = o.distance.wrapping_add(v as i16);
    }
}

/// `op->distance = v` for a `QI_VAL` operand node.
#[inline]
fn set_qi_operand_distance(item: &mut QueryItem, v: u32) {
    if let QueryItem::Qoperand(o) = item {
        o.set_distance(v);
    }
}

// ===========================================================================
// NODE tree (the C `struct NODE`)
// ===========================================================================

/// C:
/// ```c
/// typedef struct NODE {
///     struct NODE *left;
///     struct NODE *right;
///     QueryItem  *valnode;
/// } NODE;
/// ```
///
/// `valnode` in C is a *pointer into* the caller's [`QueryItem`] array; the
/// tree code reads through it and, in `clean_stopword_intree`, writes back to
/// it. We keep an owned copy of the [`QueryItem`] in each node and reproduce
/// those writes on the copy; the rebuilt array is materialized from the copies
/// by `plainnode`. Each node is a [`PgBox`] charged to `mcx`
/// (`palloc(sizeof(NODE))`); the whole tree's heap use is tracked and released
/// on drop (`pfree`/`freetree`).
struct Node<'mcx> {
    left: Option<PgBox<'mcx, Node<'mcx>>>,
    right: Option<PgBox<'mcx, Node<'mcx>>>,
    valnode: QueryItem,
}

/// `palloc(sizeof(NODE))` — allocate a node, charged to `mcx`.
fn alloc_node<'mcx>(mcx: Mcx<'mcx>, valnode: QueryItem) -> PgResult<PgBox<'mcx, Node<'mcx>>> {
    alloc_in(
        mcx,
        Node {
            left: None,
            right: None,
            valnode,
        },
    )
}

// ===========================================================================
// maketree
// ===========================================================================

/// `maketree` (tsquery_cleanup.c:33) — make a query tree from the plain
/// (prefix) view of a query. `pos` is the C `QueryItem *in` as an index into
/// the full `items` array.
fn maketree<'mcx>(
    mcx: Mcx<'mcx>,
    items: &[QueryItem],
    pos: usize,
) -> PgResult<PgBox<'mcx, Node<'mcx>>> {
    /* since this function recurses, it could be driven to stack overflow. */
    tcop::check_stack_depth::call()?;

    let in_ = &items[pos];
    let mut node = alloc_node(mcx, in_.clone())?;

    if qi_type(in_) == QI_OPR {
        node.right = Some(maketree(mcx, items, pos + 1)?);
        if qi_oper(in_) != OP_NOT {
            node.left = Some(maketree(mcx, items, pos + qi_left(in_) as usize)?);
        }
    }
    Ok(node)
}

// ===========================================================================
// plainnode / plaintree
// ===========================================================================

/// Internal state for `plaintree` / `plainnode` (the C `PLAINTREE` struct).
/// `cur` tracks the number of populated elements; the vector is grown in
/// lockstep so `ptr[cur]` writes are always in range (growth is bounded by the
/// tree's node count and is `try_reserve`-guarded).
struct PlainTree {
    ptr: Vec<QueryItem>,
    cur: usize,
}

/// `plainnode` (tsquery_cleanup.c:62) — flatten one [`Node`] (and its children)
/// back into the plain prefix array, filling in operator `left` offsets.
/// Consumes `node` (C `pfree(node)` at the end — the box drops here, the
/// children are recursed into first).
fn plainnode(state: &mut PlainTree, node: PgBox<'_, Node<'_>>) -> PgResult<()> {
    /* since this function recurses, it could be driven to stack overflow. */
    tcop::check_stack_depth::call()?;

    let mut node = node;

    // C grows `ptr` when `cur == len`; make room for the element at `cur`.
    if state.cur == state.ptr.len() {
        state.ptr.try_reserve(1).map_err(|_| oom())?;
        state.ptr.push(QueryItem::default());
    }
    // memcpy(&ptr[cur], node->valnode, sizeof(QueryItem));
    state.ptr[state.cur] = node.valnode.clone();

    if qi_type(&node.valnode) == QI_VAL {
        state.cur += 1;
    } else if qi_oper(&node.valnode) == OP_NOT {
        // ptr[cur].qoperator.left = 1;
        set_qi_left(&mut state.ptr[state.cur], 1);
        state.cur += 1;
        let right = node
            .right
            .take()
            .ok_or_else(|| PgError::error("plainnode: OP_NOT node has no right child"))?;
        plainnode(state, right)?;
    } else {
        let cur = state.cur;
        state.cur += 1;
        let right = node
            .right
            .take()
            .ok_or_else(|| PgError::error("plainnode: operator node has no right child"))?;
        plainnode(state, right)?;
        // ptr[cur].qoperator.left = state->cur - cur;
        set_qi_left(&mut state.ptr[cur], (state.cur - cur) as u32);
        let left = node
            .left
            .take()
            .ok_or_else(|| PgError::error("plainnode: non-NOT operator node has no left child"))?;
        plainnode(state, left)?;
    }
    // pfree(node): the now-childless `Node` box drops here.
    Ok(())
}

/// `plaintree` (tsquery_cleanup.c:97) — make the plain (prefix) view of a tree
/// from a [`Node`] tree, returning the populated [`QueryItem`] array and its
/// length. Empty (C `NULL`) when `root` is `None` or is neither a `QI_VAL` nor
/// a `QI_OPR` node.
fn plaintree(root: Option<PgBox<'_, Node<'_>>>) -> PgResult<(Vec<QueryItem>, i32)> {
    let mut pl = PlainTree {
        ptr: Vec::new(),
        cur: 0,
    };

    match root {
        Some(root) if qi_type(&root.valnode) == QI_VAL || qi_type(&root.valnode) == QI_OPR => {
            plainnode(&mut pl, root)?;
        }
        Some(_root) => {
            // pl.ptr = NULL: the lone non-VAL/OPR root box drops here.
        }
        None => {}
    }

    let len = pl.cur as i32;
    pl.ptr.truncate(pl.cur);
    Ok((pl.ptr, len))
}

// ===========================================================================
// freetree
// ===========================================================================

/// `freetree` (tsquery_cleanup.c:115) — recursively free a [`Node`] subtree.
/// We walk it (consuming `node`) under `check_stack_depth`, faithful to the C
/// recursion / stack-overflow guard; each box drops as it is consumed.
fn freetree(node: Option<PgBox<'_, Node<'_>>>) -> PgResult<()> {
    /* since this function recurses, it could be driven to stack overflow. */
    tcop::check_stack_depth::call()?;

    let Some(mut node) = node else {
        return Ok(());
    };
    if let Some(left) = node.left.take() {
        freetree(Some(left))?;
    }
    if let Some(right) = node.right.take() {
        freetree(Some(right))?;
    }
    Ok(())
}

// ===========================================================================
// clean_NOT_intree / clean_NOT
// ===========================================================================

/// `clean_NOT_intree` (tsquery_cleanup.c:136) — clean a tree of `!` operators.
/// "Operator `!` always returns TRUE": every `OP_NOT` subtree is dropped, and
/// the surrounding `&`/`|`/`<->` operators are collapsed when an operand goes
/// away. Returns `None` when the whole subtree cancels out.
fn clean_NOT_intree<'mcx>(
    node: PgBox<'mcx, Node<'mcx>>,
) -> PgResult<Option<PgBox<'mcx, Node<'mcx>>>> {
    /* since this function recurses, it could be driven to stack overflow. */
    tcop::check_stack_depth::call()?;

    if qi_type(&node.valnode) == QI_VAL {
        return Ok(Some(node));
    }

    if qi_oper(&node.valnode) == OP_NOT {
        freetree(Some(node))?;
        return Ok(None);
    }

    /* operator & or | */
    if qi_oper(&node.valnode) == OP_OR {
        let mut node = node;
        let left = node
            .left
            .take()
            .ok_or_else(|| PgError::error("clean_NOT_intree: OR node has no left child"))?;
        let right = node
            .right
            .take()
            .ok_or_else(|| PgError::error("clean_NOT_intree: OR node has no right child"))?;

        node.left = clean_NOT_intree(left)?;
        if node.left.is_none() {
            // Restore the (uncleaned) right child so freetree frees it too,
            // matching C, where node->right still points at the original
            // subtree when we bail out.
            node.right = Some(right);
            freetree(Some(node))?;
            return Ok(None);
        }
        node.right = clean_NOT_intree(right)?;
        if node.right.is_none() {
            freetree(Some(node))?;
            return Ok(None);
        }
        Ok(Some(node))
    } else {
        let mut node = node;

        debug_assert!(qi_oper(&node.valnode) == OP_AND || qi_oper(&node.valnode) == OP_PHRASE);

        let left = node
            .left
            .take()
            .ok_or_else(|| PgError::error("clean_NOT_intree: AND/PHRASE node has no left child"))?;
        let right = node
            .right
            .take()
            .ok_or_else(|| PgError::error("clean_NOT_intree: AND/PHRASE node has no right child"))?;

        node.left = clean_NOT_intree(left)?;
        node.right = clean_NOT_intree(right)?;

        let res = if node.left.is_none() && node.right.is_none() {
            // pfree(node); res = NULL;
            None
        } else if node.left.is_none() {
            // res = node->right; pfree(node);
            node.right.take()
        } else if node.right.is_none() {
            // res = node->left; pfree(node);
            node.left.take()
        } else {
            Some(node)
        };
        Ok(res)
    }
}

/// `clean_NOT` (tsquery_cleanup.c:190) — entry point. `ptr` is the input
/// [`QueryItem`] array; returns the cleaned plain array and its length (C
/// writes `*len`). An empty result with length 0 corresponds to C returning
/// `NULL`.
pub fn clean_NOT(mcx: Mcx<'_>, ptr: &[QueryItem]) -> PgResult<(Vec<QueryItem>, i32)> {
    let root = maketree(mcx, ptr, 0)?;
    let cleaned = clean_NOT_intree(root)?;
    plaintree(cleaned)
}

// ===========================================================================
// clean_stopword_intree
// ===========================================================================

/// `clean_stopword_intree` (tsquery_cleanup.c:238) — remove `QI_VALSTOP`
/// (stopword) nodes from a query tree, adjusting adjacent phrase distances.
/// `ladd`/`radd` are the C output parameters: the amount to add to a phrase
/// distance to the left / right of this node.
fn clean_stopword_intree<'mcx>(
    node: PgBox<'mcx, Node<'mcx>>,
    ladd: &mut i32,
    radd: &mut i32,
) -> PgResult<Option<PgBox<'mcx, Node<'mcx>>>> {
    /* since this function recurses, it could be driven to stack overflow. */
    tcop::check_stack_depth::call()?;

    /* default output parameters indicate no change in parent distance */
    *ladd = 0;
    *radd = 0;

    if qi_type(&node.valnode) == QI_VAL {
        return Ok(Some(node));
    } else if qi_type(&node.valnode) == QI_VALSTOP {
        // pfree(node): the stopword leaf box drops here.
        return Ok(None);
    }

    debug_assert!(qi_type(&node.valnode) == QI_OPR);

    if qi_oper(&node.valnode) == OP_NOT {
        let mut node = node;
        /* NOT doesn't change pattern width, so just report child distances */
        let right = node
            .right
            .take()
            .ok_or_else(|| PgError::error("clean_stopword_intree: NOT node has no right child"))?;
        node.right = clean_stopword_intree(right, ladd, radd)?;
        if node.right.is_none() {
            freetree(Some(node))?;
            return Ok(None);
        }
        Ok(Some(node))
    } else {
        let mut node = node;
        let isphrase: bool;
        let ndistance: i32;
        let mut lladd = 0;
        let mut lradd = 0;
        let mut rladd = 0;
        let mut rradd = 0;

        /* First, recurse */
        let left = node
            .left
            .take()
            .ok_or_else(|| PgError::error("clean_stopword_intree: operator node has no left child"))?;
        let right = node.right.take().ok_or_else(|| {
            PgError::error("clean_stopword_intree: operator node has no right child")
        })?;
        node.left = clean_stopword_intree(left, &mut lladd, &mut lradd)?;
        node.right = clean_stopword_intree(right, &mut rladd, &mut rradd)?;

        /* Check if current node is OP_PHRASE, get its distance */
        isphrase = qi_oper(&node.valnode) == OP_PHRASE;
        ndistance = if isphrase {
            qi_distance(&node.valnode) as i32
        } else {
            0
        };

        let res: Option<PgBox<'mcx, Node<'mcx>>>;

        if node.left.is_none() && node.right.is_none() {
            /*
             * When we collapse out a phrase node entirely, propagate its own
             * distance into both *ladd and *radd; it is the responsibility of
             * the parent node to count it only once. For a phrase node,
             * distances coming from children are summed and propagated up. But
             * if this isn't a phrase node, take the larger of the two child
             * distances; that corresponds to what TS_execute will do in
             * non-stopword cases.
             */
            if isphrase {
                *ladd = lladd + ndistance + rladd;
                *radd = *ladd;
            } else {
                *ladd = max_i32(lladd, rladd);
                *radd = *ladd;
            }
            freetree(Some(node))?;
            return Ok(None);
        } else if node.left.is_none() {
            /* Removing this operator and left subnode */
            /* lladd and lradd are equal/redundant, don't count both */
            if isphrase {
                /* operator's own distance must propagate to left */
                *ladd = lladd + ndistance + rladd;
                *radd = rradd;
            } else {
                /* at non-phrase op, just forget the left subnode entirely */
                *ladd = rladd;
                *radd = rradd;
            }
            res = node.right.take();
            // pfree(node): the now-childless operator box drops at end of scope.
        } else if node.right.is_none() {
            /* Removing this operator and right subnode */
            /* rladd and rradd are equal/redundant, don't count both */
            if isphrase {
                /* operator's own distance must propagate to right */
                *ladd = lladd;
                *radd = lradd + ndistance + rradd;
            } else {
                /* at non-phrase op, just forget the right subnode entirely */
                *ladd = lladd;
                *radd = lradd;
            }
            res = node.left.take();
            // pfree(node): the now-childless operator box drops at end of scope.
        } else if isphrase {
            /* Absorb appropriate corrections at this level */
            add_qi_distance(&mut node.valnode, lradd + rladd);
            /* Propagate up any unaccounted-for corrections */
            *ladd = lladd;
            *radd = rradd;
            res = Some(node);
        } else {
            /* We're keeping a non-phrase operator, so ladd/radd remain 0 */
            res = Some(node);
        }

        Ok(res)
    }
}

// ===========================================================================
// calcstrlen
// ===========================================================================

/// `calcstrlen` (tsquery_cleanup.c:363) — total length of operand strings in
/// the tree (each operand counted as `length + 1` for its NUL terminator).
fn calcstrlen(node: &Node<'_>) -> i32 {
    let mut size: i32;

    if qi_type(&node.valnode) == QI_VAL {
        size = qi_operand_length(&node.valnode) as i32 + 1;
    } else {
        debug_assert!(qi_type(&node.valnode) == QI_OPR);

        size = calcstrlen(node.right.as_ref().expect("operator node has a right child"));
        if qi_oper(&node.valnode) != OP_NOT {
            size += calcstrlen(
                node.left
                    .as_ref()
                    .expect("non-NOT operator node has a left child"),
            );
        }
    }

    size
}

// ===========================================================================
// cleanup_tsquery_stopwords
// ===========================================================================

/// `cleanup_tsquery_stopwords` (tsquery_cleanup.c:387) — remove `QI_VALSTOP`
/// (stopword) nodes from a serialized `TSQuery` datum. `in_` is the serialized
/// `tsquery` byte image; `noisy` controls the "only stop words" NOTICE.
/// Returns the new serialized datum (for the `in->size == 0` early return it
/// returns a copy of the input, as C returns the input unchanged).
pub fn cleanup_tsquery_stopwords(mcx: Mcx<'_>, in_: &[u8], noisy: bool) -> PgResult<Vec<u8>> {
    let in_size = tsq_size(in_);

    if in_size == 0 {
        // return in;
        return try_clone_bytes(in_);
    }

    // Decode the QueryItem array (GETQUERY(in)) so the tree code can index it.
    let in_items = crate::util::get_query(in_)?;

    /* eliminate stop words */
    let mut ladd = 0;
    let mut radd = 0;
    let root = clean_stopword_intree(maketree(mcx, &in_items, 0)?, &mut ladd, &mut radd)?;

    let Some(root) = root else {
        if noisy {
            ereport(NOTICE)
                .errmsg(
                    "text-search query contains only stop words or doesn't contain lexemes, ignored",
                )
                .finish(::types_error::ErrorLocation::new(
                    "tsquery_cleanup.c",
                    409,
                    "cleanup_tsquery_stopwords",
                ))?;
        }
        // out = palloc(HDRSIZETQ); out->size = 0; SET_VARSIZE(out, HDRSIZETQ);
        let mut out = try_zeroed(HDRSIZETQ)?;
        set_varsize(&mut out, HDRSIZETQ);
        return Ok(out);
    };

    /*
     * Build TSQuery from plain view
     */

    let lenstr = calcstrlen(&root);
    let (items, len) = plaintree(Some(root))?;
    let commonlen = computesize(len as usize, lenstr as usize);

    // out = palloc(commonlen); SET_VARSIZE(out, commonlen); out->size = len;
    let mut out = try_zeroed(commonlen)?;
    set_varsize(&mut out, commonlen);
    set_tsq_size(&mut out, len);

    // memcpy(GETQUERY(out), items, len * sizeof(QueryItem));
    putquery(&mut out, &items[..len as usize]);

    // Relocate operand strings from `in` into `out`, rewriting distances.
    let mut out_items = crate::util::get_query(&out)?;
    let in_operand = getoperand(in_);

    let mut operands: usize = 0; // operands - GETOPERAND(out), in bytes
    let mut out_operand: Vec<u8> = try_zeroed(lenstr as usize)?;

    for i in 0..tsq_size(&out) as usize {
        // QueryOperand *op = (QueryOperand *) &items[i];
        let op = &mut out_items[i];
        if qi_type(op) != QI_VAL {
            continue;
        }

        let src_off = qi_operand_distance(op) as usize;
        let op_len = qi_operand_length(op) as usize;

        // memcpy(operands, GETOPERAND(in) + op->distance, op->length);
        out_operand[operands..operands + op_len]
            .copy_from_slice(&in_operand[src_off..src_off + op_len]);
        // operands[op->length] = '\0';
        out_operand[operands + op_len] = 0;
        // op->distance = operands - GETOPERAND(out);
        set_qi_operand_distance(op, operands as u32);
        // operands += op->length + 1;
        operands += op_len + 1;
    }

    // Write the (distance-rewritten) items and the operand storage back.
    putquery(&mut out, &out_items);
    let operand_off = HDRSIZETQ + tsq_size(&out) as usize * QI_SIZE;
    out[operand_off..operand_off + lenstr as usize].copy_from_slice(&out_operand);

    Ok(out)
}

// ===========================================================================
// Guarded data-derived allocation helpers
// ===========================================================================

/// `palloc0(n)` — allocate `n` zero bytes, OOM-guarded.
fn try_zeroed(n: usize) -> PgResult<Vec<u8>> {
    let mut v: Vec<u8> = Vec::new();
    v.try_reserve(n).map_err(|_| oom())?;
    v.resize(n, 0);
    Ok(v)
}

/// Clone a byte slice with an OOM guard (`return in;` early-out path).
fn try_clone_bytes(src: &[u8]) -> PgResult<Vec<u8>> {
    let mut v = try_zeroed(src.len())?;
    v.copy_from_slice(src);
    Ok(v)
}

// ===========================================================================
// Serialized TSQuery byte-image codec
// ===========================================================================

/// `SET_VARSIZE(p, len)` — write the 4-byte varlena header for `len` bytes.
#[inline]
fn set_varsize(buf: &mut [u8], len: usize) {
    let header: u32 = (len as u32) << 2;
    buf[0..4].copy_from_slice(&header.to_ne_bytes());
}

/// Write `TSQueryData.size`.
#[inline]
fn set_tsq_size(buf: &mut [u8], size: i32) {
    buf[4..8].copy_from_slice(&size.to_ne_bytes());
}

/// Write a [`QueryItem`] array into the `QueryItem` region of a serialized
/// datum (`memcpy(GETQUERY(out), items, n * sizeof(QueryItem))`).
fn putquery(buf: &mut [u8], items: &[QueryItem]) {
    for (i, item) in items.iter().enumerate() {
        let off = HDRSIZETQ + i * QI_SIZE;
        encode_record(item, &mut buf[off..off + QI_SIZE]);
    }
}

/// `GETOPERAND(x)` — the operand C-string storage of a serialized datum.
#[inline]
fn getoperand(buf: &[u8]) -> &[u8] {
    let size = tsq_size(buf) as usize;
    let off = HDRSIZETQ + size * QI_SIZE;
    &buf[off..]
}

/// `COMPUTESIZE(size, lenofoperand)` (ts_type.h).
#[inline]
fn computesize(size: usize, lenofoperand: usize) -> usize {
    HDRSIZETQ + size * QI_SIZE + lenofoperand
}
