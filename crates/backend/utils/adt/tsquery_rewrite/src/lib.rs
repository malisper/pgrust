//! tsquery_rewrite.c — ts_rewrite over the tsquery_core QTNode machinery.

use ::adt_tsquery_core::util::{
    qt2qtn, qtn2qt, qtn_clear_flags, qtn_copy, qtn_sort, qtn_ternary, qtn_eq, qtnode_compare,
    QtNode, QTN_NOCHANGE,
};
use ::adt_tsvector_core::builtins::arg_tsquery;
use ::adt_tsvector_core::query::{Item, TsQueryRef, OP_AND, OP_NOT, OP_OR};
use ::datum::Datum;
use ::mcx::{vec_with_capacity_in, Mcx, PgVec};
use ::types_error::{PgError, PgResult, ERRCODE_INVALID_PARAMETER_VALUE};
use ::types_fmgr::{
    varlena_result, FmgrBuiltin, FmgrInfo, FunctionCallInfoBaseData as Fcinfo, PGFunction,
};

const TSQUERYOID: ::types_core::Oid = 3615;

// findeq (tsquery_rewrite.c): match `ex` at this node (or a subset of an
// AND/OR node's children) and substitute a copy of `subs`. None = the node
// was replaced by the empty substitution.
fn findeq<'mcx>(
    mcx: Mcx<'mcx>,
    mut node: QtNode<'mcx>,
    ex: &QtNode<'_>,
    subs: Option<&QtNode<'_>>,
    isfind: &mut bool,
) -> PgResult<Option<QtNode<'mcx>>> {
    if (node.sign & ex.sign) != ex.sign
        || core::mem::discriminant(&node.item) != core::mem::discriminant(&ex.item)
    {
        return Ok(Some(node));
    }
    if node.flags & QTN_NOCHANGE != 0 {
        return Ok(Some(node));
    }

    match (node.item, ex.item) {
        (Item::Opr(nopr), Item::Opr(eopr)) => {
            if nopr.oper != eopr.oper {
                return Ok(Some(node));
            }
            if node.children.len() == ex.children.len() {
                if qtn_eq(&node, ex) {
                    *isfind = true;
                    return Ok(match subs {
                        Some(s) => {
                            let mut n = qtn_copy(mcx, s)?;
                            n.flags |= QTN_NOCHANGE;
                            Some(n)
                        }
                        None => None,
                    });
                }
            } else if node.children.len() > ex.children.len() && !ex.children.is_empty() {
                // AND/OR are commutative/associative: match a subset of the
                // (sorted) children in one merge pass.
                debug_assert!(nopr.oper == OP_AND || nopr.oper == OP_OR);
                let mut matched = vec![false; node.children.len()];
                let mut nmatched = 0usize;
                let (mut i, mut j) = (0usize, 0usize);
                while i < node.children.len() && j < ex.children.len() {
                    let cmp = qtnode_compare(&node.children[i], &ex.children[j]);
                    if cmp == 0 {
                        matched[i] = true;
                        nmatched += 1;
                        i += 1;
                        j += 1;
                    } else if cmp < 0 {
                        i += 1;
                    } else {
                        break;
                    }
                }
                if nmatched == ex.children.len() {
                    let old: PgVec<'mcx, QtNode<'mcx>> =
                        core::mem::replace(&mut node.children, PgVec::new_in(mcx));
                    node.children
                        .try_reserve_exact(old.len() + 1)
                        .map_err(|_| mcx.oom(old.len() + 1))?;
                    for (i, c) in old.into_iter().enumerate() {
                        if !matched[i] {
                            node.children.push(c);
                        }
                    }
                    if let Some(s) = subs {
                        let mut sc = qtn_copy(mcx, s)?;
                        sc.flags |= QTN_NOCHANGE;
                        node.children.push(sc);
                    }
                    // Zero-or-one-child simplification is dofindsubquery's
                    // job; the re-sort keeps regression output stable.
                    qtn_sort(&mut node);
                    *isfind = true;
                }
            }
            Ok(Some(node))
        }
        (Item::Val(nop), Item::Val(eop)) => {
            if nop.valcrc != eop.valcrc || !qtn_eq(&node, ex) {
                return Ok(Some(node));
            }
            *isfind = true;
            Ok(match subs {
                Some(s) => {
                    let mut n = qtn_copy(mcx, s)?;
                    n.flags |= QTN_NOCHANGE;
                    Some(n)
                }
                None => None,
            })
        }
        _ => Ok(Some(node)),
    }
}

// dofindsubquery (tsquery_rewrite.c): substitute at the root, else recurse;
// drop voided subtrees and simplify zero/one-child operator nodes.
fn dofindsubquery<'mcx>(
    mcx: Mcx<'mcx>,
    root: QtNode<'mcx>,
    ex: &QtNode<'_>,
    subs: Option<&QtNode<'_>>,
    isfind: &mut bool,
) -> PgResult<Option<QtNode<'mcx>>> {
    ::postgres_seams::check_for_interrupts::call()?;

    let Some(mut root) = findeq(mcx, root, ex, subs, isfind)? else {
        return Ok(None);
    };

    if root.flags & QTN_NOCHANGE == 0 {
        if let Item::Opr(opr) = root.item {
            let old: PgVec<'mcx, QtNode<'mcx>> =
                core::mem::replace(&mut root.children, PgVec::new_in(mcx));
            root.children
                .try_reserve_exact(old.len())
                .map_err(|_| mcx.oom(old.len()))?;
            for c in old.into_iter() {
                if let Some(kept) = dofindsubquery(mcx, c, ex, subs, isfind)? {
                    root.children.push(kept);
                }
            }
            if root.children.is_empty() {
                return Ok(None);
            }
            if root.children.len() == 1 && opr.oper != OP_NOT {
                return Ok(Some(root.children.pop().expect("one child")));
            }
        }
    }
    Ok(Some(root))
}

// findsubquery (tsquery_rewrite.c). Both root and ex must be QTNTernary'd
// and QTNSort'ed.
pub fn findsubquery<'mcx>(
    mcx: Mcx<'mcx>,
    root: QtNode<'mcx>,
    ex: &QtNode<'_>,
    subs: Option<&QtNode<'_>>,
) -> PgResult<Option<QtNode<'mcx>>> {
    let mut did_find = false;
    dofindsubquery(mcx, root, ex, subs, &mut did_find)
}

fn image_result(img: PgVec<'_, u8>) -> Datum {
    varlena_result(::datum::Varlena::from_image(img))
}

fn copy_image<'mcx>(mcx: Mcx<'mcx>, q: TsQueryRef<'_>) -> PgResult<PgVec<'mcx, u8>> {
    let mut img = vec_with_capacity_in(mcx, q.payload.len() + 4)?;
    img.extend_from_slice(&[0u8; 4]);
    ::mcx::vec_append_bytes(&mut img, q.payload)?;
    Ok(img)
}

// C truncates the input copy to HDRSIZETQ with size = 0.
fn empty_tsquery<'mcx>(mcx: Mcx<'mcx>) -> PgResult<PgVec<'mcx, u8>> {
    ::adt_tsquery_core::parse::build_query_image(mcx, &[], &[])
}

fn prepared_tree<'mcx>(mcx: Mcx<'mcx>, q: TsQueryRef<'_>) -> PgResult<QtNode<'mcx>> {
    let mut t = qt2qtn(mcx, q, 0)?;
    qtn_ternary(&mut t);
    qtn_sort(&mut t);
    Ok(t)
}

fn finish_tree<'mcx>(mcx: Mcx<'mcx>, tree: Option<QtNode<'mcx>>) -> PgResult<Datum> {
    match tree {
        Some(mut t) => {
            ::adt_tsquery_core::util::qtn_binary(mcx, &mut t);
            Ok(image_result(qtn2qt(mcx, &t)?))
        }
        None => Ok(image_result(empty_tsquery(mcx)?)),
    }
}

// tsquery_rewrite (3684): ts_rewrite(tsquery, tsquery, tsquery).
pub fn fc_tsquery_rewrite(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let query = arg_tsquery(fcinfo, 0)?;
    let ex = arg_tsquery(fcinfo, 1)?;
    let subst = arg_tsquery(fcinfo, 2)?;
    // SAFETY: the armed result mcx outlives this call.
    let mcx = unsafe { fcinfo.result_mcx_detached() };

    if query.size() == 0 || ex.size() == 0 {
        return Ok(image_result(copy_image(mcx, query)?));
    }

    let tree = prepared_tree(mcx, query)?;
    let qex = prepared_tree(mcx, ex)?;
    let subs = if subst.size() != 0 { Some(qt2qtn(mcx, subst, 0)?) } else { None };

    let tree = findsubquery(mcx, tree, &qex, subs.as_ref())?;
    finish_tree(mcx, tree)
}

fn tsquery_ref_from_datum<'mcx>(mcx: Mcx<'mcx>, d: Datum) -> PgResult<TsQueryRef<'mcx>> {
    let p = d.as_usize() as *const u8;
    // SAFETY: a not-null tsquery column datum: a live varlena image readable
    // through its varsize_any extent.
    let image = unsafe { core::slice::from_raw_parts(p, ::types_tuple::varatt::varsize_any(p)) };
    // pg_detoast_datum: normalizes short headers (the payload needs int32
    // alignment) and detoasts stored values.
    let flat = detoast::detoast_attr(mcx, image)?;
    let flat = flat.leak();
    Ok(TsQueryRef { payload: &flat[::types_tuple::varatt::VARHDRSZ..] })
}

// tsquery_rewrite_query (3685): ts_rewrite(tsquery, text) — the SELECT must
// return two tsquery columns; each (target, substitute) row is applied in
// fetch order.
pub fn fc_tsquery_rewrite_query(
    _flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    let query = arg_tsquery(fcinfo, 0)?;
    // SAFETY: strict fn: arg 1 is a non-null live text varlena.
    let sql_v = unsafe { fcinfo.arg_varlena_packed(1) }?;
    let sql = core::str::from_utf8(sql_v.data())
        .map_err(|_| Box::new(PgError::error("invalid UTF-8 in ts_rewrite query text")))?;
    // SAFETY: the armed result mcx outlives this call.
    let mcx = unsafe { fcinfo.result_mcx_detached() };

    if query.size() == 0 {
        return Ok(image_result(copy_image(mcx, query)?));
    }

    let mut tree = Some(prepared_tree(mcx, query)?);

    spi::SPI_connect()?;
    let plan = spi::SPI_prepare(sql, &[])?;
    let cursor = spi::SPI_cursor_open(None, plan, &[], &[], true)?;

    let mut checked_shape = false;
    loop {
        spi::SPI_cursor_fetch(&cursor, true, 100)?;
        let processed = spi::SPI_processed();
        let Some(h) = spi::SPI_tuptable() else { break };
        if !checked_shape {
            let shape_ok = spi::tuptable_with(h, |t| {
                t.tupdesc.natts == 2
                    && spi::SPI_gettypeid(&t.tupdesc, 1) == TSQUERYOID
                    && spi::SPI_gettypeid(&t.tupdesc, 2) == TSQUERYOID
            });
            if !shape_ok {
                return Err(
                    PgError::error("ts_rewrite query must return two tsquery columns")
                        .with_sqlstate(ERRCODE_INVALID_PARAMETER_VALUE)
                        .into(),
                );
            }
            checked_shape = true;
        }
        if processed == 0 || tree.is_none() {
            spi::SPI_freetuptable(h)?;
            break;
        }

        let mut step = |tree: &mut Option<QtNode<'_>>| -> PgResult<()> {
            spi::tuptable_with(h, |t| -> PgResult<()> {
                for tup in t.vals.iter() {
                    if tree.is_none() {
                        break;
                    }
                    let (qdata, isnull) = spi::SPI_getbinval(tup, &t.tupdesc, 1);
                    if isnull {
                        continue;
                    }
                    let (sdata, isnull) = spi::SPI_getbinval(tup, &t.tupdesc, 2);
                    if isnull {
                        continue;
                    }
                    let qtex = tsquery_ref_from_datum(mcx, qdata)?;
                    let qtsubs = tsquery_ref_from_datum(mcx, sdata)?;
                    if qtex.size() == 0 {
                        continue;
                    }
                    let qex = prepared_tree(mcx, qtex)?;
                    let qsubs =
                        if qtsubs.size() != 0 { Some(qt2qtn(mcx, qtsubs, 0)?) } else { None };
                    *tree = findsubquery(mcx, tree.take().expect("tree"), &qex, qsubs.as_ref())?;
                    if let Some(t) = tree.as_mut() {
                        // Ready the tree for another pass.
                        qtn_clear_flags(t, QTN_NOCHANGE);
                        qtn_ternary(t);
                        qtn_sort(t);
                    }
                }
                Ok(())
            })
        };
        step(&mut tree)?;
        spi::SPI_freetuptable(h)?;
    }

    spi::SPI_cursor_close(cursor)?;
    spi::SPI_freeplan(plan);
    spi::SPI_finish()?;

    finish_tree(mcx, tree)
}

const fn b(foid: ::types_core::Oid, name: &'static str, nargs: i16, func: PGFunction) -> FmgrBuiltin {
    FmgrBuiltin { foid, name, nargs, strict: true, retset: false, func }
}

pub const TSQUERY_REWRITE_BUILTINS: &[FmgrBuiltin] = &[
    b(3684, "tsquery_rewrite", 3, fc_tsquery_rewrite),
    b(3685, "tsquery_rewrite_query", 2, fc_tsquery_rewrite_query),
];
