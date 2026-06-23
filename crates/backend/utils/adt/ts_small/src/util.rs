//! Port of `src/backend/utils/adt/tsquery_util.c` — the `QTNode` expression-tree
//! toolkit.

use alloc::vec::Vec;

use ::mcx::{vec_with_capacity_in, Mcx, PgVec};
use ::utils_error::ereport;
use ::types_error::{PgError, PgResult, ERRCODE_PROGRAM_LIMIT_EXCEEDED, ERROR};
use ::tsearch::tsearch::{
    QueryItem, QueryOperand, QueryOperator, HDRSIZETQ, OP_AND, OP_NOT, OP_OR, OP_PHRASE, QI_OPR,
    QI_VAL,
};

use postgres_seams as tcop;

/// `MaxAllocSize` (`memutils.h`).
const MAX_ALLOC_SIZE: usize = ::mcx::MAX_ALLOC_SIZE;

/// `sizeof(QueryItem)` — the on-disk ABI record size (12 bytes).
pub const QI_SIZE: usize = 12;

/// Out-of-memory error for a guarded, data-derived allocation.
pub fn oom() -> PgError {
    PgError::new(
        ::types_error::ERROR,
        "out of memory",
    )
    .with_sqlstate(::types_error::ERRCODE_OUT_OF_MEMORY)
}

/// `query->size` — the number of [`QueryItem`]s.
#[inline]
pub fn tsq_size(q: &[u8]) -> i32 {
    i32::from_ne_bytes([q[4], q[5], q[6], q[7]])
}

// ===========================================================================
// QueryItem ABI codec (GETQUERY / GETOPERAND, ts_type.h)
// ===========================================================================

/// Decode one 12-byte ABI [`QueryItem`] record into the idiomatic `enum`.
pub fn decode_record(rec: &[u8]) -> QueryItem {
    let type_ = rec[0] as i8;
    if type_ == QI_OPR {
        QueryItem::Qoperator(QueryOperator {
            type_,
            oper: rec[1] as i8,
            distance: i16::from_ne_bytes([rec[2], rec[3]]),
            left: u32::from_ne_bytes([rec[4], rec[5], rec[6], rec[7]]),
        })
    } else if type_ == QI_VAL {
        QueryItem::Qoperand(QueryOperand {
            type_,
            weight: rec[1],
            prefix: rec[2] != 0,
            valcrc: i32::from_ne_bytes([rec[4], rec[5], rec[6], rec[7]]),
            len_dist: u32::from_ne_bytes([rec[8], rec[9], rec[10], rec[11]]),
        })
    } else {
        // QI_VALSTOP or any other bare-tag value: only the type byte matters.
        QueryItem::Type_(type_)
    }
}

/// Encode one idiomatic [`QueryItem`] into a 12-byte ABI record. Zeroes the
/// whole record first (C nodes are fully-initialized 12-byte slots).
pub fn encode_record(item: &QueryItem, rec: &mut [u8]) {
    for b in rec.iter_mut() {
        *b = 0;
    }
    match item {
        QueryItem::Qoperator(o) => {
            rec[0] = o.type_ as u8;
            rec[1] = o.oper as u8;
            rec[2..4].copy_from_slice(&o.distance.to_ne_bytes());
            rec[4..8].copy_from_slice(&o.left.to_ne_bytes());
        }
        QueryItem::Qoperand(o) => {
            rec[0] = o.type_ as u8;
            rec[1] = o.weight;
            rec[2] = o.prefix as u8;
            rec[4..8].copy_from_slice(&o.valcrc.to_ne_bytes());
            rec[8..12].copy_from_slice(&o.len_dist.to_ne_bytes());
        }
        QueryItem::Type_(t) => {
            rec[0] = *t as u8;
        }
    }
}

/// `GETQUERY(x)` (ts_type.h) — decode the [`QueryItem`] array into an owned
/// `Vec` (a transient working buffer). The count comes from the datum's own
/// `->size` field, so the reservation is against a validated bound.
pub fn get_query(q: &[u8]) -> PgResult<Vec<QueryItem>> {
    let size = tsq_size(q) as usize;
    let mut items: Vec<QueryItem> = Vec::new();
    items.try_reserve(size).map_err(|_| oom())?;
    for i in 0..size {
        let base = HDRSIZETQ + i * QI_SIZE;
        items.push(decode_record(&q[base..base + QI_SIZE]));
    }
    Ok(items)
}

/// `GETOPERAND(x)` (ts_type.h) — the operand storage following the array.
#[inline]
pub fn get_operand(q: &[u8]) -> &[u8] {
    let off = HDRSIZETQ + (tsq_size(q) as usize) * QI_SIZE;
    &q[off..]
}

/// `item->qoperand.distance`, for a `QI_VAL` node.
#[inline]
pub fn operand_distance(item: &QueryItem) -> u32 {
    match item {
        QueryItem::Qoperand(o) => o.distance(),
        _ => 0,
    }
}

/// `item->qoperand.length`, for a `QI_VAL` node.
#[inline]
pub fn operand_length(item: &QueryItem) -> u32 {
    match item {
        QueryItem::Qoperand(o) => o.length(),
        _ => 0,
    }
}

/// `item->qoperand.valcrc`, for a `QI_VAL` node.
#[inline]
pub fn operand_valcrc(item: &QueryItem) -> i32 {
    match item {
        QueryItem::Qoperand(o) => o.valcrc,
        _ => 0,
    }
}

// ===========================================================================
// QTN flags (ts_utils.h)
// ===========================================================================

/// `QTN_NEEDFREE` (ts_utils.h) — valnode is transient (C-only freeing hint;
/// inert under Rust ownership, kept for fidelity to `QTNBinary` / `QTNCopy`).
pub const QTN_NEEDFREE: u32 = 0x01;
/// `QTN_NOCHANGE` (ts_utils.h) — node was just substituted; don't recurse into
/// it. The only flag that affects control flow.
pub const QTN_NOCHANGE: u32 = 0x02;
/// `QTN_WORDFREE` (ts_utils.h) — word is transient (C-only freeing hint; inert
/// under Rust ownership). Kept for completeness of the flag set.
pub const QTN_WORDFREE: u32 = 0x04;

// ===========================================================================
// QTNode tree (tsquery_util.c)
// ===========================================================================

/// `QTNode` (ts_utils.h) — a node of the tsquery expression tree.
///
/// ```c
/// typedef struct QTNode {
///     QueryItem      *valnode;
///     uint32          flags;
///     int32           nchild;
///     char           *word;
///     uint32          sign;
///     struct QTNode **child;
/// } QTNode;
/// ```
///
/// `word` / `child` are [`PgVec`]s charged to the per-call [`Mcx`]; `QTNFree`
/// is `Drop` (charge released on drop), `QTNCopy` is [`QTNode::clone_in`]. Only
/// [`QTN_NOCHANGE`] is tracked (the `QTN_NEEDFREE`/`QTN_WORDFREE` hints are
/// inert under Rust ownership).
pub struct QTNode<'mcx> {
    /// the node's [`QueryItem`] (`*valnode` in C)
    pub valnode: QueryItem,
    /// `QTN_*` flag bits (only [`QTN_NOCHANGE`] affects control flow)
    pub flags: u32,
    /// operand text (only meaningful for `QI_VAL` nodes)
    pub word: PgVec<'mcx, u8>,
    /// OR of operand-CRC signature bits
    pub sign: u32,
    /// sub-trees (1 or 2 for an operator node, empty for an operand node)
    pub child: PgVec<'mcx, QTNode<'mcx>>,
}

impl<'mcx> QTNode<'mcx> {
    /// A fresh empty node carrying `valnode`, charged to `mcx` (empty `word` /
    /// `child` spines).
    fn empty(mcx: Mcx<'mcx>, valnode: QueryItem) -> QTNode<'mcx> {
        QTNode {
            valnode,
            flags: 0,
            word: PgVec::new_in(mcx),
            sign: 0,
            child: PgVec::new_in(mcx),
        }
    }

    /// `QTNCopy(QTNode *in)` (tsquery_util.c:396) — a deep copy charged to
    /// `mcx`. Mirrors C's recursive `palloc`/`memcpy` clone, including the
    /// `QTN_NEEDFREE` (+ `QTN_WORDFREE` for an operand node) stamping and the
    /// preservation of `sign` and the source `flags` (incl. [`QTN_NOCHANGE`]).
    pub fn clone_in(&self, mcx: Mcx<'mcx>) -> PgResult<QTNode<'mcx>> {
        // C: *out = *in; out->flags |= QTN_NEEDFREE;
        let mut flags = self.flags | QTN_NEEDFREE;

        let mut word: PgVec<'mcx, u8> = PgVec::new_in(mcx);
        let mut child: PgVec<'mcx, QTNode<'mcx>> = PgVec::new_in(mcx);

        if self.valnode_type() == QI_VAL {
            // out->word = palloc(len + 1); memcpy(word, in->word, len); out->word[len]
            // = '\0'; out->flags |= QTN_WORDFREE. The idiomatic `word` carries
            // exactly the `len` operand bytes (no embedded NUL).
            let len = operand_length(&self.valnode) as usize;
            if len > 0 {
                word = ::mcx::slice_in(mcx, &self.word[..len]).map_err(|_| oom())?;
            }
            flags |= QTN_WORDFREE;
        } else {
            child = vec_with_capacity_in(mcx, self.child.len()).map_err(|_| oom())?;
            for c in self.child.iter() {
                child.push(c.clone_in(mcx)?);
            }
        }
        Ok(QTNode {
            valnode: self.valnode.clone(),
            flags,
            word,
            sign: self.sign,
            child,
        })
    }

    /// `in->nchild` — number of children.
    #[inline]
    pub fn nchild(&self) -> i32 {
        self.child.len() as i32
    }

    /// `in->valnode->type` — the shared leading type tag.
    #[inline]
    pub fn valnode_type(&self) -> i8 {
        self.valnode.item_type()
    }

    /// `in->valnode->type == QI_OPR`.
    #[inline]
    pub fn is_opr(&self) -> bool {
        self.valnode_type() == QI_OPR
    }

    /// `in->valnode->qoperator.oper` (only valid for operator nodes).
    #[inline]
    pub fn oper(&self) -> i8 {
        match &self.valnode {
            QueryItem::Qoperator(o) => o.oper,
            _ => 0,
        }
    }

    /// `in->valnode->qoperand.valcrc` (only valid for operand nodes).
    #[inline]
    pub fn valcrc(&self) -> i32 {
        operand_valcrc(&self.valnode)
    }
}

// ---------------------------------------------------------------------------
// QT2QTN (tsquery_util.c:24)
// ---------------------------------------------------------------------------

/// `QT2QTN(GETQUERY(q), GETOPERAND(q))` (tsquery_util.c:24) — build a [`QTNode`]
/// tree from a flat tsquery datum.
pub fn qt2qtn<'mcx>(mcx: Mcx<'mcx>, q: &[u8]) -> PgResult<QTNode<'mcx>> {
    let items = get_query(q)?;
    qt2qtn_inner(mcx, &items, get_operand(q), 0)
}

/// `QT2QTN(QueryItem *in, char *operand)` (tsquery_util.c:24) — the recursive
/// builder over the polish-order [`QueryItem`] array starting at `items[idx]`.
fn qt2qtn_inner<'mcx>(
    mcx: Mcx<'mcx>,
    items: &[QueryItem],
    operand: &[u8],
    idx: usize,
) -> PgResult<QTNode<'mcx>> {
    // since this function recurses, it could be driven to stack overflow.
    tcop::check_stack_depth::call()?;

    let valnode = items[idx].clone();
    let type_ = valnode.item_type();

    let mut node = QTNode::empty(mcx, valnode);

    if type_ == QI_OPR {
        let oper = node.oper();
        // node->child[0] = QT2QTN(in + 1, operand);
        let c0 = qt2qtn_inner(mcx, items, operand, idx + 1)?;
        node.sign = c0.sign;
        if oper == OP_NOT {
            // node->nchild = 1;
            node.child.push(c0);
        } else {
            // node->nchild = 2;
            // node->child[1] = QT2QTN(in + in->qoperator.left, operand);
            let left = match &node.valnode {
                QueryItem::Qoperator(o) => o.left as usize,
                _ => 0,
            };
            let c1 = qt2qtn_inner(mcx, items, operand, idx + left)?;
            node.sign |= c1.sign;
            node.child.push(c0);
            node.child.push(c1);
        }
    } else {
        // else if (operand) — GETOPERAND() is always non-NULL at every call
        // site, so the operand-leaf branch is always taken for a QI_VAL node.
        let dist = operand_distance(&node.valnode) as usize;
        let len = operand_length(&node.valnode) as usize;
        // node->word = operand + in->qoperand.distance;
        node.word = ::mcx::slice_in(mcx, &operand[dist..dist + len]).map_err(|_| oom())?;
        // node->sign = ((uint32) 1) << (((unsigned int) valcrc) % 32);
        node.sign = 1u32 << ((operand_valcrc(&node.valnode) as u32) % 32);
    }

    Ok(node)
}

// ---------------------------------------------------------------------------
// tsCompareString (tsvector_op.c:1152) — needed by QTNodeCompare
// ---------------------------------------------------------------------------

/// `tsCompareString` (tsvector_op.c:1152) — compare two operand strings by
/// tsvector rules. With `prefix == true` it returns 0 iff `b` has prefix `a`.
fn ts_compare_string(a: &[u8], b: &[u8], prefix: bool) -> i32 {
    let lena = a.len();
    let lenb = b.len();
    let cmp;

    if lena == 0 {
        if prefix {
            cmp = 0; // empty string is prefix of anything
        } else {
            cmp = if lenb > 0 { -1 } else { 0 };
        }
    } else if lenb == 0 {
        cmp = if lena > 0 { 1 } else { 0 };
    } else {
        let min = lena.min(lenb);
        let mut c = memcmp(&a[..min], &b[..min]);

        if prefix {
            if c == 0 && lena > lenb {
                c = 1; // a is longer, so not a prefix of b
            }
        } else if c == 0 && lena != lenb {
            c = if lena < lenb { -1 } else { 1 };
        }
        cmp = c;
    }

    cmp
}

/// C `memcmp` returning negative / zero / positive.
#[inline]
fn memcmp(a: &[u8], b: &[u8]) -> i32 {
    match a.cmp(b) {
        core::cmp::Ordering::Less => -1,
        core::cmp::Ordering::Equal => 0,
        core::cmp::Ordering::Greater => 1,
    }
}

// ---------------------------------------------------------------------------
// QTNodeCompare (tsquery_util.c:96)
// ---------------------------------------------------------------------------

/// `QTNodeCompare(QTNode *an, QTNode *bn)` (tsquery_util.c:96) — the (arbitrary
/// but total) sort order on [`QTNode`] trees.
pub fn QTNodeCompare(an: &QTNode<'_>, bn: &QTNode<'_>) -> PgResult<i32> {
    // since this function recurses, it could be driven to stack overflow.
    tcop::check_stack_depth::call()?;

    let at = an.valnode_type();
    let bt = bn.valnode_type();

    if at != bt {
        return Ok(if at > bt { -1 } else { 1 });
    }

    if at == QI_OPR {
        let ao = match &an.valnode {
            QueryItem::Qoperator(o) => *o,
            _ => QueryOperator::default(),
        };
        let bo = match &bn.valnode {
            QueryItem::Qoperator(o) => *o,
            _ => QueryOperator::default(),
        };

        if ao.oper != bo.oper {
            return Ok(if ao.oper > bo.oper { -1 } else { 1 });
        }

        if an.nchild() != bn.nchild() {
            return Ok(if an.nchild() > bn.nchild() { -1 } else { 1 });
        }

        for i in 0..an.nchild() as usize {
            let res = QTNodeCompare(&an.child[i], &bn.child[i])?;
            if res != 0 {
                return Ok(res);
            }
        }

        if ao.oper == OP_PHRASE && ao.distance != bo.distance {
            return Ok(if ao.distance > bo.distance { -1 } else { 1 });
        }

        Ok(0)
    } else if at == QI_VAL {
        let acrc = operand_valcrc(&an.valnode);
        let bcrc = operand_valcrc(&bn.valnode);

        // C tsquery_util.c:135 — `valcrc` is a *signed* int32 (ts_type.h notes
        // it deliberately uses signed comparisons), and cmpQTN returns -1 when
        // a.valcrc > b.valcrc, so qsort orders the child array by *descending*
        // valcrc. QTN2QT serializes child[0] to in+1 (the right-printed
        // operand), so the highest valcrc prints rightmost — matching PG's
        // canonical ts_rewrite output exactly. (This relies on the LEGACY CRC
        // being computed correctly; see port-crc32c::legacy.)
        if acrc != bcrc {
            return Ok(if acrc > bcrc { -1 } else { 1 });
        }

        Ok(ts_compare_string(&an.word, &bn.word, false))
    } else {
        // elog(ERROR, "unrecognized QueryItem type: %d", an->valnode->type);
        Err(PgError::error(alloc::format!(
            "unrecognized QueryItem type: {}",
            at
        )))
    }
}

// ---------------------------------------------------------------------------
// QTNSort (tsquery_util.c:163) + cmpQTN (tsquery_util.c:152)
// ---------------------------------------------------------------------------

/// `QTNSort(QTNode *in)` (tsquery_util.c:163) — canonicalize a tree by sorting
/// the children of AND/OR nodes (recursively).
///
/// `cmpQTN` (tsquery_util.c:152) just calls [`QTNodeCompare`], which can fail
/// (`check_stack_depth`); since Rust's `sort_by` comparator cannot return an
/// error, the first failure is captured and the comparator returns `Equal`
/// thereafter, then propagated after the sort. The order is total, so the
/// stable sort is a harmless refinement of `qsort`.
pub fn QTNSort(node: &mut QTNode<'_>) -> PgResult<()> {
    // since this function recurses, it could be driven to stack overflow.
    tcop::check_stack_depth::call()?;

    if node.valnode_type() != QI_OPR {
        return Ok(());
    }

    for i in 0..node.child.len() {
        QTNSort(&mut node.child[i])?;
    }

    // if (in->nchild > 1 && in->valnode->qoperator.oper != OP_PHRASE)
    if node.nchild() > 1 && node.oper() != OP_PHRASE {
        let failed: core::cell::Cell<bool> = core::cell::Cell::new(false);
        let err: core::cell::Cell<Option<PgError>> = core::cell::Cell::new(None);
        node.child.sort_by(|a, b| {
            if failed.get() {
                return core::cmp::Ordering::Equal;
            }
            match QTNodeCompare(a, b) {
                Ok(c) => c.cmp(&0),
                Err(e) => {
                    failed.set(true);
                    err.set(Some(e));
                    core::cmp::Ordering::Equal
                }
            }
        });
        if let Some(e) = err.into_inner() {
            return Err(e);
        }
    }

    Ok(())
}

// ---------------------------------------------------------------------------
// QTNEq (tsquery_util.c:183)
// ---------------------------------------------------------------------------

/// `QTNEq(QTNode *a, QTNode *b)` (tsquery_util.c:183) — are two trees equal
/// according to [`QTNodeCompare`]?
#[allow(clippy::nonminimal_bool)]
pub fn QTNEq(a: &QTNode<'_>, b: &QTNode<'_>) -> PgResult<bool> {
    let sign = a.sign & b.sign;

    if !(sign == a.sign && sign == b.sign) {
        return Ok(false);
    }

    Ok(QTNodeCompare(a, b)? == 0)
}

// ---------------------------------------------------------------------------
// QTNTernary (tsquery_util.c:201)
// ---------------------------------------------------------------------------

/// `QTNTernary(QTNode *in)` (tsquery_util.c:201) — flatten chains of the same
/// associative operator (AND/OR) into a single multi-child node.
pub fn QTNTernary<'mcx>(mcx: Mcx<'mcx>, node: &mut QTNode<'mcx>) -> PgResult<()> {
    // since this function recurses, it could be driven to stack overflow.
    tcop::check_stack_depth::call()?;

    if node.valnode_type() != QI_OPR {
        return Ok(());
    }

    for i in 0..node.child.len() {
        QTNTernary(mcx, &mut node.child[i])?;
    }

    // Only AND and OR are associative, so don't flatten other node types
    if node.oper() != OP_AND && node.oper() != OP_OR {
        return Ok(());
    }

    // C splices each same-operator child node's children into `in->child` in
    // place (`i += cc->nchild - 1`). Equivalently: for each original child, if
    // it is a same-operator OPR node emit its children, else emit it unchanged.
    // (Grandchildren were already flattened by the recursive calls above.)
    let oper = node.oper();
    let old_children = core::mem::replace(&mut node.child, PgVec::new_in(mcx));
    let mut new_children: PgVec<'mcx, QTNode<'mcx>> =
        vec_with_capacity_in(mcx, old_children.len()).map_err(|_| oom())?;
    for mut cc in old_children {
        if cc.valnode_type() == QI_OPR && oper == cc.oper() {
            let gcs = core::mem::replace(&mut cc.child, PgVec::new_in(mcx));
            for gc in gcs {
                new_children.push(gc);
            }
        } else {
            new_children.push(cc);
        }
    }
    node.child = new_children;

    Ok(())
}

// ---------------------------------------------------------------------------
// QTNBinary (tsquery_util.c:250)
// ---------------------------------------------------------------------------

/// `QTNBinary(QTNode *in)` (tsquery_util.c:250) — convert a flattened tree back
/// to a binary tree by inserting intermediate same-operator nodes (the inverse
/// of [`QTNTernary`]).
pub fn QTNBinary<'mcx>(mcx: Mcx<'mcx>, node: &mut QTNode<'mcx>) -> PgResult<()> {
    // since this function recurses, it could be driven to stack overflow.
    tcop::check_stack_depth::call()?;

    if node.valnode_type() != QI_OPR {
        return Ok(());
    }

    for i in 0..node.child.len() {
        QTNBinary(mcx, &mut node.child[i])?;
    }

    // while (in->nchild > 2) {
    //   nn = new node over (child[0], child[1]); nn->flags = QTN_NEEDFREE;
    //   in->child[0] = nn; in->child[1] = in->child[in->nchild - 1]; in->nchild--;
    // }
    //
    // Each iteration's net effect, with child = [c0, c1, c2, ..., c_{m-1}]:
    //   nn = OPR(c0, c1);  child becomes [nn, c_{m-1}, c2, ..., c_{m-2}].
    let in_type = node.valnode_type();
    let in_oper = node.oper();
    while node.nchild() > 2 {
        let mut old = core::mem::replace(&mut node.child, PgVec::new_in(mcx));
        // c_last = in->child[in->nchild - 1]
        let c_last = old.pop().unwrap();
        // Now old = [c0, c1, c2, ..., c_{m-2}]. Pull c0, c1 off the front.
        let mut middle = old.into_iter();
        let child0 = middle.next().unwrap();
        let child1 = middle.next().unwrap();
        let nn_sign = child0.sign | child1.sign;

        // nn->valnode is a freshly palloc0'd QueryItem with only type+oper set.
        let nn_valnode = QueryItem::Qoperator(QueryOperator {
            type_: in_type,
            oper: in_oper,
            distance: 0,
            left: 0,
        });
        let mut nn_child: PgVec<'mcx, QTNode<'mcx>> =
            vec_with_capacity_in(mcx, 2).map_err(|_| oom())?;
        nn_child.push(child0);
        nn_child.push(child1);
        let nn = QTNode {
            valnode: nn_valnode,
            flags: QTN_NEEDFREE,
            word: PgVec::new_in(mcx),
            sign: nn_sign,
            child: nn_child,
        };

        // Rebuild child = [nn, c_last, c2, c3, ..., c_{m-2}].
        let mut new_child: PgVec<'mcx, QTNode<'mcx>> =
            vec_with_capacity_in(mcx, middle.len() + 2).map_err(|_| oom())?;
        new_child.push(nn);
        new_child.push(c_last);
        for c in middle {
            new_child.push(c);
        }
        node.child = new_child;
    }

    Ok(())
}

// ---------------------------------------------------------------------------
// cntsize / fillQT / QTN2QT (tsquery_util.c:292..388)
// ---------------------------------------------------------------------------

/// `cntsize` (tsquery_util.c:292) — accumulate the total operand length
/// (including `'\0'`-terminators) and the node count of the tree. Caller must
/// initialize `*sumlen` and `*nnode` to zero.
pub fn cntsize(node: &QTNode<'_>, sumlen: &mut i32, nnode: &mut i32) -> PgResult<()> {
    // since this function recurses, it could be driven to stack overflow.
    tcop::check_stack_depth::call()?;

    *nnode += 1;
    if node.valnode_type() == QI_OPR {
        for c in node.child.iter() {
            cntsize(c, sumlen, nnode)?;
        }
    } else {
        *sumlen += operand_length(&node.valnode) as i32 + 1;
    }
    Ok(())
}

/// `QTN2QTState` (tsquery_util.c:311) — the fill cursor for [`fill_qt`].
struct Qtn2QtState<'a> {
    items: &'a mut [QueryItem],
    curitem: usize,
    operand: &'a mut [u8],
    curoperand: usize,
}

/// `fillQT` (tsquery_util.c:323) — recursively emit a [`QTNode`] tree into the
/// flat [`QueryItem`] array + operand buffer.
fn fill_qt(state: &mut Qtn2QtState<'_>, node: &QTNode<'_>) -> PgResult<()> {
    // since this function recurses, it could be driven to stack overflow.
    tcop::check_stack_depth::call()?;

    if node.valnode_type() == QI_VAL {
        // memcpy(curitem, valnode, sizeof(QueryOperand)); copy word; set distance.
        let mut qop = match &node.valnode {
            QueryItem::Qoperand(o) => *o,
            _ => QueryOperand::default(),
        };
        let len = qop.length() as usize;
        state.operand[state.curoperand..state.curoperand + len].copy_from_slice(&node.word[..len]);
        qop.set_distance(state.curoperand as u32);
        state.operand[state.curoperand + len] = 0;
        state.curoperand += len + 1;
        state.items[state.curitem] = QueryItem::Qoperand(qop);
        state.curitem += 1;
    } else {
        // QI_OPR
        let curitem = state.curitem;
        // memcpy(curitem, valnode, sizeof(QueryOperator));
        let mut qopr = match &node.valnode {
            QueryItem::Qoperator(o) => *o,
            _ => QueryOperator::default(),
        };
        state.curitem += 1;

        fill_qt(state, &node.child[0])?;

        if node.nchild() == 2 {
            // curitem->qoperator.left = state->curitem - curitem;
            qopr.left = (state.curitem - curitem) as u32;
            fill_qt(state, &node.child[1])?;
        }
        state.items[curitem] = QueryItem::Qoperator(qopr);
    }
    Ok(())
}

/// `COMPUTESIZE(size, lenofoperand)` (ts_type.h).
#[inline]
fn computesize(nnode: i32, sumlen: i32) -> usize {
    HDRSIZETQ + (nnode as usize) * QI_SIZE + (sumlen as usize)
}

/// `TSQUERY_TOO_BIG(size, lenofoperand)` (ts_type.h) — would the flat form
/// overflow `MaxAllocSize`?
#[inline]
fn tsquery_too_big(nnode: i32, sumlen: i32) -> bool {
    (nnode as i64) > ((MAX_ALLOC_SIZE - HDRSIZETQ - sumlen as usize) / QI_SIZE) as i64
}

/// `QTN2QT(QTNode *in)` (tsquery_util.c:363) — serialize a [`QTNode`] tree to a
/// flat `tsquery` datum. The returned `Vec<u8>` is the OWNED result datum
/// (uncharged — the caller's).
pub fn qtn2qt(mcx: Mcx<'_>, node: &QTNode<'_>) -> PgResult<Vec<u8>> {
    let mut sumlen = 0i32;
    let mut nnode = 0i32;

    cntsize(node, &mut sumlen, &mut nnode)?;

    if tsquery_too_big(nnode, sumlen) {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED)
            .errmsg("tsquery is too large")
            .into_error());
    }
    let len = computesize(nnode, sumlen);

    let mut out: Vec<u8> = Vec::new();
    out.try_reserve(len).map_err(|_| oom())?;
    out.resize(len, 0u8);
    // SET_VARSIZE(out, len)
    out[0..4].copy_from_slice(&((len as u32) << 2).to_ne_bytes());
    // out->size = nnode
    out[4..8].copy_from_slice(&nnode.to_ne_bytes());

    // Build the QueryItem array + operand buffer (charged working buffers,
    // zero-filled to final length, palloc0), then splice them in.
    let mut items: PgVec<'_, QueryItem> =
        vec_with_capacity_in(mcx, nnode as usize).map_err(|_| oom())?;
    for _ in 0..nnode {
        items.push(QueryItem::default());
    }
    let mut operand: PgVec<'_, u8> = vec_with_capacity_in(mcx, sumlen as usize).map_err(|_| oom())?;
    for _ in 0..sumlen {
        operand.push(0u8);
    }
    {
        let mut state = Qtn2QtState {
            items: items.as_mut_slice(),
            curitem: 0,
            operand: operand.as_mut_slice(),
            curoperand: 0,
        };
        fill_qt(&mut state, node)?;
    }
    for (i, it) in items.iter().enumerate() {
        let base = HDRSIZETQ + i * QI_SIZE;
        encode_record(it, &mut out[base..base + QI_SIZE]);
    }
    let opbase = HDRSIZETQ + (nnode as usize) * QI_SIZE;
    out[opbase..opbase + sumlen as usize].copy_from_slice(&operand);

    Ok(out)
}

// ---------------------------------------------------------------------------
// QTNClearFlags (tsquery_util.c:434)
// ---------------------------------------------------------------------------

/// `QTNClearFlags(QTNode *in, uint32 flags)` (tsquery_util.c:434) — clear the
/// specified flag bit(s) in all nodes of the tree.
///
/// C calls `check_stack_depth()` here too, but the function is `void`; the
/// signature stays infallible because the bounded tree depth is already
/// validated by the `QTNTernary`/`QTNSort` that precede every call site.
pub fn QTNClearFlags(node: &mut QTNode<'_>, flags: u32) {
    node.flags &= !flags;

    if node.valnode_type() != QI_VAL {
        for c in node.child.iter_mut() {
            QTNClearFlags(c, flags);
        }
    }
}
