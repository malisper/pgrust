//! Port of `src/backend/utils/adt/tsquery_op.c` — various operations on
//! `tsquery` values.
//!
//! The `QTNode` toolkit (`QT2QTN`/`QTN2QT`/`QTNodeCompare`) lives in
//! `backend-utils-adt-ts-small`; this module reuses it. A `tsquery` is its flat
//! varlena image (`&[u8]`); the constructors return the new image (`Vec<u8>`).

extern crate alloc;

use alloc::vec::Vec;

use ::ts_small::util::{
    self, get_operand, get_query, operand_distance, operand_length, qt2qtn, qtn2qt, tsq_size,
    QTNode, QTNodeCompare, QTN_NEEDFREE,
};
use ::mcx::{vec_with_capacity_in, Mcx, PgVec};
use ::types_error::{PgResult, ERRCODE_INVALID_PARAMETER_VALUE, ERROR};
use ::tsearch::tsearch::{
    QueryItem, QueryOperator, TSQuerySign, MAXENTRYPOS, OP_AND, OP_NOT, OP_OR, OP_PHRASE, QI_OPR,
    QI_VAL, TSQS_SIGLEN,
};

use ::utils_error::ereport;

/// `query->size` of a flat `tsquery` image.
fn varsize(q: &[u8]) -> u32 {
    // VARSIZE: the 4-byte length word, with the low 2 bits being flags.
    (u32::from_ne_bytes([q[0], q[1], q[2], q[3]])) >> 2
}

/// `tsquery_numnode(query)` (tsquery_op.c:22) — the node count.
pub fn tsquery_numnode(query: &[u8]) -> i32 {
    tsq_size(query)
}

/// `join_tsqueries(a, b, operator, distance)` (tsquery_op.c:32) — build a
/// two-child operator `QTNode` over `b` (child[0]) and `a` (child[1]).
fn join_tsqueries<'mcx>(
    mcx: Mcx<'mcx>,
    a: &[u8],
    b: &[u8],
    operator: i8,
    distance: u16,
) -> PgResult<QTNode<'mcx>> {
    let valnode = QueryItem::Qoperator(QueryOperator {
        type_: QI_OPR,
        oper: operator,
        distance: if operator == OP_PHRASE { distance as i16 } else { 0 },
        left: 0,
    });

    // res->child[0] = QT2QTN(GETQUERY(b), GETOPERAND(b));
    // res->child[1] = QT2QTN(GETQUERY(a), GETOPERAND(a));
    let c0 = qt2qtn(mcx, b)?;
    let c1 = qt2qtn(mcx, a)?;
    let mut child: PgVec<'mcx, QTNode<'mcx>> = vec_with_capacity_in(mcx, 2).map_err(|_| util::oom())?;
    child.push(c0);
    child.push(c1);

    Ok(QTNode {
        valnode,
        flags: QTN_NEEDFREE,
        word: PgVec::new_in(mcx),
        sign: 0,
        child,
    })
}

/// `tsquery_and(a, b)` (tsquery_op.c:53). Returns the `&`-joined image, or a
/// clone of the non-empty operand when one side is empty.
pub fn tsquery_and(mcx: Mcx<'_>, a: &[u8], b: &[u8]) -> PgResult<Vec<u8>> {
    if tsq_size(a) == 0 {
        return clone_bytes(b);
    } else if tsq_size(b) == 0 {
        return clone_bytes(a);
    }
    let res = join_tsqueries(mcx, a, b, OP_AND, 0)?;
    qtn2qt(mcx, &res)
}

/// `tsquery_or(a, b)` (tsquery_op.c:83).
pub fn tsquery_or(mcx: Mcx<'_>, a: &[u8], b: &[u8]) -> PgResult<Vec<u8>> {
    if tsq_size(a) == 0 {
        return clone_bytes(b);
    } else if tsq_size(b) == 0 {
        return clone_bytes(a);
    }
    let res = join_tsqueries(mcx, a, b, OP_OR, 0)?;
    qtn2qt(mcx, &res)
}

/// `tsquery_phrase_distance(a, b, distance)` (tsquery_op.c:113).
pub fn tsquery_phrase_distance(mcx: Mcx<'_>, a: &[u8], b: &[u8], distance: i32) -> PgResult<Vec<u8>> {
    if distance < 0 || distance > MAXENTRYPOS as i32 {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_INVALID_PARAMETER_VALUE)
            .errmsg(alloc::format!(
                "distance in phrase operator must be an integer value between zero and {} inclusive",
                MAXENTRYPOS
            ))
            .into_error());
    }
    if tsq_size(a) == 0 {
        return clone_bytes(b);
    } else if tsq_size(b) == 0 {
        return clone_bytes(a);
    }
    let res = join_tsqueries(mcx, a, b, OP_PHRASE, distance as u16)?;
    qtn2qt(mcx, &res)
}

/// `tsquery_phrase(a, b)` (tsquery_op.c:149) — `<->` (distance 1).
pub fn tsquery_phrase(mcx: Mcx<'_>, a: &[u8], b: &[u8]) -> PgResult<Vec<u8>> {
    tsquery_phrase_distance(mcx, a, b, 1)
}

/// `tsquery_not(a)` (tsquery_op.c:158) — `!a`.
pub fn tsquery_not(mcx: Mcx<'_>, a: &[u8]) -> PgResult<Vec<u8>> {
    if tsq_size(a) == 0 {
        return clone_bytes(a);
    }

    let valnode = QueryItem::Qoperator(QueryOperator {
        type_: QI_OPR,
        oper: OP_NOT,
        distance: 0,
        left: 0,
    });
    let c0 = qt2qtn(mcx, a)?;
    let mut child: PgVec<'_, QTNode<'_>> = vec_with_capacity_in(mcx, 1).map_err(|_| util::oom())?;
    child.push(c0);

    let res = QTNode {
        valnode,
        flags: QTN_NEEDFREE,
        word: PgVec::new_in(mcx),
        sign: 0,
        child,
    };
    qtn2qt(mcx, &res)
}

/// `CompareTSQ(a, b)` (tsquery_op.c:188) — total order over `tsquery` images.
fn compare_tsq(mcx: Mcx<'_>, a: &[u8], b: &[u8]) -> PgResult<i32> {
    if tsq_size(a) != tsq_size(b) {
        return Ok(if tsq_size(a) < tsq_size(b) { -1 } else { 1 });
    } else if varsize(a) != varsize(b) {
        return Ok(if varsize(a) < varsize(b) { -1 } else { 1 });
    } else if tsq_size(a) != 0 {
        let an = qt2qtn(mcx, a)?;
        let bn = qt2qtn(mcx, b)?;
        return QTNodeCompare(&an, &bn);
    }
    Ok(0)
}

/// `tsquery_cmp(a, b)` (tsquery_op.c:214).
pub fn tsquery_cmp(mcx: Mcx<'_>, a: &[u8], b: &[u8]) -> PgResult<i32> {
    compare_tsq(mcx, a, b)
}

/// `tsquery_lt` (tsquery_op.c:242).
pub fn tsquery_lt(mcx: Mcx<'_>, a: &[u8], b: &[u8]) -> PgResult<bool> {
    Ok(compare_tsq(mcx, a, b)? < 0)
}
/// `tsquery_le` (tsquery_op.c:243).
pub fn tsquery_le(mcx: Mcx<'_>, a: &[u8], b: &[u8]) -> PgResult<bool> {
    Ok(compare_tsq(mcx, a, b)? <= 0)
}
/// `tsquery_eq` (tsquery_op.c:244).
pub fn tsquery_eq(mcx: Mcx<'_>, a: &[u8], b: &[u8]) -> PgResult<bool> {
    Ok(compare_tsq(mcx, a, b)? == 0)
}
/// `tsquery_ge` (tsquery_op.c:245).
pub fn tsquery_ge(mcx: Mcx<'_>, a: &[u8], b: &[u8]) -> PgResult<bool> {
    Ok(compare_tsq(mcx, a, b)? >= 0)
}
/// `tsquery_gt` (tsquery_op.c:246).
pub fn tsquery_gt(mcx: Mcx<'_>, a: &[u8], b: &[u8]) -> PgResult<bool> {
    Ok(compare_tsq(mcx, a, b)? > 0)
}
/// `tsquery_ne` (tsquery_op.c:247).
pub fn tsquery_ne(mcx: Mcx<'_>, a: &[u8], b: &[u8]) -> PgResult<bool> {
    Ok(compare_tsq(mcx, a, b)? != 0)
}

/// `makeTSQuerySign(a)` (tsquery_op.c:249) — the lossy GiST bit signature.
pub fn makeTSQuerySign(a: &[u8]) -> PgResult<TSQuerySign> {
    let items = get_query(a)?;
    let mut sign: TSQuerySign = 0;
    for item in &items {
        if item.item_type() == QI_VAL {
            // sign |= ((TSQuerySign) 1) << (((unsigned int) valcrc) % TSQS_SIGLEN);
            let valcrc = util::operand_valcrc(item) as u32;
            sign |= (1u64) << (valcrc % TSQS_SIGLEN);
        }
    }
    Ok(sign)
}

/// `collectTSQueryValues(a, &nvalues)` (tsquery_op.c:266) — extract the
/// distinct operand strings (one owned copy per `QI_VAL` node, in array order).
fn collect_tsquery_values<'mcx>(mcx: Mcx<'mcx>, a: &[u8]) -> PgResult<PgVec<'mcx, PgVec<'mcx, u8>>> {
    let items = get_query(a)?;
    let operand = get_operand(a);

    let mut values: PgVec<'mcx, PgVec<'mcx, u8>> =
        vec_with_capacity_in(mcx, items.len()).map_err(|_| util::oom())?;
    for item in &items {
        if item.item_type() == QI_VAL {
            let dist = operand_distance(item) as usize;
            let len = operand_length(item) as usize;
            let val = ::mcx::slice_in(mcx, &operand[dist..dist + len]).map_err(|_| util::oom())?;
            values.push(val);
        }
    }
    Ok(values)
}

/// `tsq_mcontains(query, ex)` (tsquery_op.c:306) — does `query` contain every
/// distinct operand of `ex`? The C `qsort`+`qunique`+merge becomes a sorted
/// dedup + an ordered scan; `cmp_string` is plain `strcmp` (byte order).
pub fn tsq_mcontains(mcx: Mcx<'_>, query: &[u8], ex: &[u8]) -> PgResult<bool> {
    let mut query_values = collect_tsquery_values(mcx, query)?;
    let mut ex_values = collect_tsquery_values(mcx, ex)?;

    // qsort + qunique with cmp_string (strcmp == byte ordering).
    sort_dedup(&mut query_values);
    sort_dedup(&mut ex_values);

    let mut result = true;
    if ex_values.len() > query_values.len() {
        result = false;
    } else {
        let mut j = 0usize;
        for i in 0..ex_values.len() {
            while j < query_values.len() {
                if ex_values[i].as_slice() == query_values[j].as_slice() {
                    break;
                }
                j += 1;
            }
            if j == query_values.len() {
                result = false;
                break;
            }
        }
    }
    Ok(result)
}

/// `tsq_mcontained(query, ex)` (tsquery_op.c:353) — `tsq_mcontains(ex, query)`.
pub fn tsq_mcontained(mcx: Mcx<'_>, query: &[u8], ex: &[u8]) -> PgResult<bool> {
    tsq_mcontains(mcx, ex, query)
}

/// `qsort(values, cmp_string)` then `qunique(values, …, cmp_string)` — sort by
/// byte order and drop adjacent duplicates.
fn sort_dedup(values: &mut PgVec<'_, PgVec<'_, u8>>) {
    values.sort_by(|x, y| x.as_slice().cmp(y.as_slice()));
    values.dedup_by(|x, y| x.as_slice() == y.as_slice());
}

/// Clone a flat `tsquery` image (the C `PG_RETURN_POINTER(b)` of a detoasted
/// copy). Allocated fallibly into the caller's heap, like the rest of the
/// owned-result datums.
fn clone_bytes(q: &[u8]) -> PgResult<Vec<u8>> {
    let mut out: Vec<u8> = Vec::new();
    out.try_reserve(q.len()).map_err(|_| util::oom())?;
    out.extend_from_slice(q);
    Ok(out)
}
