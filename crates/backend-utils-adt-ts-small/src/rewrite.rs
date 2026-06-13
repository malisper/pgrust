//! Port of `src/backend/utils/adt/tsquery_rewrite.c` — the `ts_rewrite` family.

use alloc::string::String;
use alloc::vec::Vec;

use mcx::{vec_with_capacity_in, Mcx, PgVec};
use backend_utils_error::ereport;
use types_error::{PgError, PgResult, ERRCODE_INVALID_PARAMETER_VALUE, ERROR};
use types_tsearch::tsearch::{OP_AND, OP_NOT, OP_OR};
use types_tuple::heaptuple::TSQUERYOID;

use backend_tcop_postgres_seams as tcop;
use backend_executor_spi_seams as spi;

use crate::util::{
    oom, qt2qtn, qtn2qt, tsq_size, QTNBinary, QTNClearFlags, QTNEq, QTNSort, QTNTernary, QTNode,
    QTNodeCompare, QTN_NOCHANGE,
};

/// Build the empty-`tsquery` datum produced by C's `SET_VARSIZE(rewritten,
/// HDRSIZETQ); rewritten->size = 0;`: an 8-byte varlena (header + `size = 0`).
fn empty_tsquery() -> PgResult<Vec<u8>> {
    let hdr = types_tsearch::tsearch::HDRSIZETQ;
    let mut out: Vec<u8> = Vec::new();
    out.try_reserve(hdr).map_err(|_| oom())?;
    out.resize(hdr, 0u8);
    // SET_VARSIZE(out, HDRSIZETQ): 4-byte varlena header is (len << 2).
    out[0..4].copy_from_slice(&((hdr as u32) << 2).to_ne_bytes());
    // out->size = 0 (already zero from the resize fill).
    Ok(out)
}

/// Copy a fully-detoasted datum slice into an owned `Vec`, bounded by its own
/// validated length (the `rewritten = query` copy semantics).
fn copy_datum(q: &[u8]) -> PgResult<Vec<u8>> {
    let mut out: Vec<u8> = Vec::new();
    out.try_reserve(q.len()).map_err(|_| oom())?;
    out.extend_from_slice(q);
    Ok(out)
}

/// `findeq` (tsquery_rewrite.c:34) — if `node` equals `ex`, return a copy of
/// `subs` instead; if `ex` matches a commutative subset of `node`'s children,
/// return a modified `node` with those children replaced by a copy of `subs`;
/// otherwise return `node` unmodified.
///
/// The [`QTN_NOCHANGE`] bit is set in successfully modified nodes so we don't
/// uselessly recurse into them. `*isfind` is set true when a replacement is
/// made. Takes ownership of `node`; returns the substitute (with
/// [`QTN_NOCHANGE`] set), the (possibly subset-modified) `node`, or `None`
/// when `node` matched and `subs` is `None`.
fn findeq<'mcx>(
    mcx: Mcx<'mcx>,
    mut node: QTNode<'mcx>,
    ex: &QTNode<'mcx>,
    subs: Option<&QTNode<'mcx>>,
    isfind: &mut bool,
) -> PgResult<Option<QTNode<'mcx>>> {
    // Can't match unless signature matches and node type matches.
    if (node.sign & ex.sign) != ex.sign || node.valnode_type() != ex.valnode_type() {
        return Ok(Some(node));
    }

    // Ignore nodes marked NOCHANGE, too.
    if node.flags & QTN_NOCHANGE != 0 {
        return Ok(Some(node));
    }

    if node.is_opr() {
        // Must be same operator.
        if node.oper() != ex.oper() {
            return Ok(Some(node));
        }

        if node.nchild() == ex.nchild() {
            // Simple case: when same number of children, match if equal.
            // (This is reliable when the children were sorted earlier.)
            if QTNEq(&node, ex)? {
                // Match; delete node and return a copy of subs instead.
                *isfind = true;
                if let Some(subs) = subs {
                    let mut copy = subs.clone_in(mcx)?;
                    copy.flags |= QTN_NOCHANGE;
                    return Ok(Some(copy));
                } else {
                    return Ok(None);
                }
            }
        } else if node.nchild() > ex.nchild() && ex.nchild() > 0 {
            // AND and OR are commutative/associative, so check if a subset of
            // the children match (e.g. node A|B|C, ex B|C, match as A|(B|C)).
            // This does not work for NOT or PHRASE (fixed child counts, can't
            // reach here for those). Because the children are sorted, one pass
            // through the two lists finds the matches.

            debug_assert!(node.oper() == OP_AND || node.oper() == OP_OR);

            // matched[] records which children of node matched
            let mut matched: PgVec<'mcx, bool> =
                vec_with_capacity_in(mcx, node.nchild() as usize).map_err(|_| oom())?;
            for _ in 0..node.nchild() {
                matched.push(false);
            }
            let mut nmatched = 0i32;
            let mut i = 0usize;
            let mut j = 0usize;
            while i < node.nchild() as usize && j < ex.nchild() as usize {
                let cmp = QTNodeCompare(&node.child[i], &ex.child[j])?;

                if cmp == 0 {
                    // match!
                    matched[i] = true;
                    nmatched += 1;
                    i += 1;
                    j += 1;
                } else if cmp < 0 {
                    // node->child[i] has no match, ignore it
                    i += 1;
                } else {
                    // ex->child[j] has no match; we can give up immediately
                    break;
                }
            }

            if nmatched == ex.nchild() {
                // collapse out the matched children of node
                let old_children = core::mem::replace(&mut node.child, PgVec::new_in(mcx));
                let mut new_children: PgVec<'mcx, QTNode<'mcx>> =
                    vec_with_capacity_in(mcx, old_children.len()).map_err(|_| oom())?;
                for (idx, c) in old_children.into_iter().enumerate() {
                    if matched[idx] {
                        // QTNFree(node->child[i]) — `c` drops here.
                    } else {
                        new_children.push(c);
                    }
                }

                // and instead insert a copy of subs
                if let Some(subs) = subs {
                    let mut copy = subs.clone_in(mcx)?;
                    copy.flags |= QTN_NOCHANGE;
                    new_children.push(copy);
                }

                node.child = new_children;

                // At this point we might have a node with zero or one child,
                // which should be simplified. But we leave it to our caller
                // (dofindsubquery) to take care of that.

                // Re-sort the node to put new child in the right place. This is
                // a bit bogus, but needed to keep the results the same as the
                // regression tests expect.
                QTNSort(&mut node)?;

                *isfind = true;
            }

            // pfree(matched) — `matched` drops at end of scope.
        }
    } else {
        debug_assert!(node.valnode_type() == types_tsearch::tsearch::QI_VAL);

        if node.valcrc() != ex.valcrc() {
            return Ok(Some(node));
        }
        if QTNEq(&node, ex)? {
            *isfind = true;
            if let Some(subs) = subs {
                let mut copy = subs.clone_in(mcx)?;
                copy.flags |= QTN_NOCHANGE;
                return Ok(Some(copy));
            } else {
                return Ok(None);
            }
        }
    }

    Ok(Some(node))
}

/// `dofindsubquery` (tsquery_rewrite.c:205) — recursive guts of
/// [`findsubquery`]: attempt to replace `ex` with `subs` at the root node, and
/// if that failed recurse into the child nodes; delete any void subtrees.
fn dofindsubquery<'mcx>(
    mcx: Mcx<'mcx>,
    root: QTNode<'mcx>,
    ex: &QTNode<'mcx>,
    subs: Option<&QTNode<'mcx>>,
    isfind: &mut bool,
) -> PgResult<Option<QTNode<'mcx>>> {
    // since this function recurses, it could be driven to stack overflow.
    tcop::check_stack_depth::call()?;

    // also, since it's a bit expensive, let's check for query cancel.
    tcop::check_for_interrupts::call()?;

    // match at the node itself
    let mut root = match findeq(mcx, root, ex, subs, isfind)? {
        Some(r) => r,
        None => return Ok(None),
    };

    // unless we matched here, consider matches at child nodes
    if (root.flags & QTN_NOCHANGE) == 0 && root.is_opr() {
        // Any subtrees that are replaced by NULL must be dropped from the tree.
        let old_children = core::mem::replace(&mut root.child, PgVec::new_in(mcx));
        let mut new_children: PgVec<'mcx, QTNode<'mcx>> =
            vec_with_capacity_in(mcx, old_children.len()).map_err(|_| oom())?;
        for c in old_children {
            if let Some(nc) = dofindsubquery(mcx, c, ex, subs, isfind)? {
                new_children.push(nc);
            }
        }
        root.child = new_children;

        // If we have just zero or one remaining child node, simplify out this
        // operator node.
        if root.nchild() == 0 {
            // QTNFree(root) — `root` drops here.
            return Ok(None);
        } else if root.nchild() == 1 && root.oper() != OP_NOT {
            // pfree(root); root = nroot;
            let nroot = root.child.pop().unwrap();
            // the now-empty operator shell (root) drops here.
            return Ok(Some(nroot));
        }
    }

    Ok(Some(root))
}

/// `findsubquery` (tsquery_rewrite.c:266) — substitute `subs` for `ex`
/// throughout the [`QTNode`] tree at `root`. Both `root` and `ex` must have
/// been through [`QTNTernary`] and [`QTNSort`] to ensure reliable matching.
fn findsubquery<'mcx>(
    mcx: Mcx<'mcx>,
    root: QTNode<'mcx>,
    ex: &QTNode<'mcx>,
    subs: Option<&QTNode<'mcx>>,
    isfind: Option<&mut bool>,
) -> PgResult<Option<QTNode<'mcx>>> {
    let mut did_find = false;

    let root = dofindsubquery(mcx, root, ex, subs, &mut did_find)?;

    if let Some(isfind) = isfind {
        *isfind = did_find;
    }

    Ok(root)
}

/// `tsquery_rewrite` (tsquery_rewrite.c:409) — `ts_rewrite(query, target,
/// substitute)`: substitute the `subst` query for every occurrence of `ex` in
/// `query`. Takes the three fully-detoasted `tsquery` datums and returns the
/// rewritten `tsquery` datum. (The fmgr wrapper / `PG_GETARG_TSQUERY*` /
/// `PG_RETURN_POINTER` are the systemic fmgr-layer deferral.)
pub fn tsquery_rewrite(mcx: Mcx<'_>, query: &[u8], ex: &[u8], subst: &[u8]) -> PgResult<Vec<u8>> {
    // TSQuery rewritten = query;
    if tsq_size(query) == 0 || tsq_size(ex) == 0 {
        return copy_datum(query);
    }

    // tree = QT2QTN(GETQUERY(query), GETOPERAND(query));
    let mut tree = qt2qtn(mcx, query)?;
    QTNTernary(mcx, &mut tree)?;
    QTNSort(&mut tree)?;

    // qex = QT2QTN(GETQUERY(ex), GETOPERAND(ex));
    let mut qex = qt2qtn(mcx, ex)?;
    QTNTernary(mcx, &mut qex)?;
    QTNSort(&mut qex)?;

    // if (subst->size) subs = QT2QTN(GETQUERY(subst), GETOPERAND(subst));
    let subs = if tsq_size(subst) != 0 {
        Some(qt2qtn(mcx, subst)?)
    } else {
        None
    };

    // tree = findsubquery(tree, qex, subs, NULL);
    let tree = findsubquery(mcx, tree, &qex, subs.as_ref(), None)?;

    match tree {
        None => {
            // SET_VARSIZE(rewritten, HDRSIZETQ); rewritten->size = 0;
            empty_tsquery()
        }
        Some(mut tree) => {
            QTNBinary(mcx, &mut tree)?;
            qtn2qt(mcx, &tree)
        }
    }
}

/// `tsquery_rewrite_query` (tsquery_rewrite.c:279) — `ts_rewrite(query, text)`:
/// obtain `(target, substitute)` pairs by running the `buf` text through SPI
/// and substitute each pair throughout `query`. (The fmgr wrapper /
/// `text_to_cstring` / `PG_RETURN_POINTER` are deferred; `buf` here is the
/// already-decoded command text. The SPI execution is funneled through the
/// unit's seam, installed by the SPI owner.)
pub fn tsquery_rewrite_query(mcx: Mcx<'_>, query: &[u8], buf: &str) -> PgResult<Vec<u8>> {
    // TSQuery rewritten = query;
    if tsq_size(query) == 0 {
        return copy_datum(query);
    }

    // tree = QT2QTN(GETQUERY(query), GETOPERAND(query));
    let mut tree_opt: Option<QTNode<'_>> = Some(qt2qtn(mcx, query)?);
    {
        let tree = tree_opt.as_mut().unwrap();
        QTNTernary(mcx, tree)?;
        QTNSort(tree)?;
    }

    // SPI_connect(); SPI_prepare(buf,...); SPI_cursor_open(...);
    // SPI_cursor_fetch loop; per-column SPI_getbinval; SPI cleanup.
    let result = spi::tsquery_rewrite_run::call(String::from(buf))?;

    // if (SPI_tuptable == NULL || tupdesc->natts != 2 ||
    //     SPI_gettypeid(tupdesc, 1) != TSQUERYOID ||
    //     SPI_gettypeid(tupdesc, 2) != TSQUERYOID)
    //     ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE), ...));
    if result.natts != 2 || result.col1_type != TSQUERYOID || result.col2_type != TSQUERYOID {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_INVALID_PARAMETER_VALUE)
            .errmsg("ts_rewrite query must return two tsquery columns")
            .into_error());
    }

    // while (SPI_processed > 0 && tree)
    'batches: for batch in &result.batches {
        if tree_opt.is_none() {
            break;
        }
        // for (i = 0; i < SPI_processed && tree; i++)
        for (qdata, sdata) in batch {
            if tree_opt.is_none() {
                break 'batches;
            }

            // Datum qdata = SPI_getbinval(..., 1, &isnull); if (isnull) continue;
            let qdata = match qdata {
                Some(q) => q,
                None => continue,
            };

            // sdata = SPI_getbinval(..., 2, &isnull); if (!isnull) { ... }
            if let Some(sdata) = sdata {
                let qtex = qdata;
                let qtsubs = sdata;

                // if (qtex->size == 0) { ...; continue; }
                if tsq_size(qtex) == 0 {
                    continue;
                }

                // qex = QT2QTN(GETQUERY(qtex), GETOPERAND(qtex));
                let mut qex = qt2qtn(mcx, qtex)?;
                QTNTernary(mcx, &mut qex)?;
                QTNSort(&mut qex)?;

                // if (qtsubs->size) qsubs = QT2QTN(GETQUERY(qtsubs), ...);
                let qsubs = if tsq_size(qtsubs) != 0 {
                    Some(qt2qtn(mcx, qtsubs)?)
                } else {
                    None
                };

                // tree = findsubquery(tree, qex, qsubs, NULL);
                let tree = tree_opt.take().ok_or_else(|| {
                    PgError::error("tsquery_rewrite_query: tree is NULL inside row loop")
                })?;
                tree_opt = findsubquery(mcx, tree, &qex, qsubs.as_ref(), None)?;

                // QTNFree(qex); QTNFree(qsubs); — qex/qsubs drop at end of scope.

                // if (tree) { ready the tree for another pass }
                if let Some(tree) = tree_opt.as_mut() {
                    QTNClearFlags(tree, QTN_NOCHANGE);
                    QTNTernary(mcx, tree)?;
                    QTNSort(tree)?;
                }
            }
        }
    }

    // SPI cleanup is handled by the seam provider.

    match tree_opt {
        Some(mut tree) => {
            QTNBinary(mcx, &mut tree)?;
            qtn2qt(mcx, &tree)
        }
        None => {
            // SET_VARSIZE(rewritten, HDRSIZETQ); rewritten->size = 0;
            empty_tsquery()
        }
    }
}
