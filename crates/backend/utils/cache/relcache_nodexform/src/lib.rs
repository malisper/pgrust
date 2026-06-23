//! `backend-utils-cache-relcache-nodexform` — the owner of the relcache derived
//! caches whose payload is a raw `rd_indextuple` node-tree transform plus the
//! publication-membership descriptor.
//!
//! These bodies live in `utils/cache/relcache.c`
//! (`RelationGetIndexAttrBitmap`'s per-index step and
//! `RelationBuildPublicationDesc`), but they bottom out on cross-unit vocabulary
//! — the raw `pg_index.indexprs`/`indpred` `pg_node_tree` text (read via the
//! syscache owner, since the trimmed relcache entry carries only the decoded
//! `rd_index` form, not the raw `rd_indextuple`), `stringToNode` (node read
//! owner), `pull_varattnos` (var owner), `index_open`/`rd_indam` (indexam +
//! relcache owners), and the publication-catalog traversal + REPLICA IDENTITY
//! validity checks (pg_publication / publicationcmds owners). This crate
//! orchestrates them exactly as the C does and installs the seams from
//! [`init_seams`].
//!
//! Each function re-reads the relation's `pg_index` row by OID through the
//! syscache (the faithful analogue of the C `heap_getattr(rd_indextuple, ...)`,
//! which reads the same catcache-resident tuple): the relcache build does not
//! retain the raw tuple in the owned model.

#![allow(non_snake_case)]

extern crate alloc;

use alloc::format;
use alloc::string::String;

use ::mcx::{Mcx, MemoryContext, PgVec};
use ::types_catalog::pg_publication::{PublicationDesc, PublishGencolsType};
use ::types_core::primitive::Oid;
use ::types_error::{PgResult, ERROR};
use ::nodes::primnodes::Expr;
use ::rel::Relation;

use ::utils_error::ereport;

use common_relation_seams as relation_seam;
use indexam_seams as indexam_seam;
use partition_seams as partition_seam;
use pg_publication_seams as pubcat_seam;
use publicationcmds_seams as pubcmds_seam;
use read_seams as read_seam;
use init_subselect_ext_seams as clauses_seam;
use prepqual_seams as prepqual_seam;
use var_seams as var_seam;
use lsyscache_seams as lsyscache_seam;
use relcache_nodexform_seams as inward;
use relcache_seams as relcache_seam;
use syscache_seams as syscache_seam;

/// `AccessShareLock` (`storage/lockdefs.h`).
const ACCESS_SHARE_LOCK: types_storage::lock::LOCKMODE = 1;

/// Iterate the offset members of a `pull_varattnos` result bitmapset (allocated
/// in `mcx`) into a `Vec<i32>` (the `expr_attrs`/`pred_attrs` the
/// `IndexAttrInfo` carries — already offset by
/// `FirstLowInvalidHeapAttributeNumber`). `None` is the C empty/NULL set.
fn bms_members(bms: Option<&::nodes::bitmapset::Bitmapset<'_>>) -> alloc::vec::Vec<i32> {
    let mut out = alloc::vec::Vec::new();
    let mut i = -1;
    loop {
        i = nodes_core::bitmapset::bms_next_member(bms, i);
        if i < 0 {
            break;
        }
        out.push(i);
    }
    out
}

/// `pull_varattnos((Node *) stringToNode(text), 1, &attrs)` over a raw
/// `pg_index.indexprs`/`indpred` `pg_node_tree` text — collect the offset
/// attribute members referenced by every `Var` (varno 1) in the tree, into a
/// `Vec<i32>`. The C feeds the whole `List*` node to `pull_varattnos`; the owned
/// `pull_varattnos` seam takes a single `Expr`, so we walk the deserialized
/// `Node::List` and union each element's contributions (the same idiom
/// `indexcmds.c`'s `CheckPredicate`/expression scan uses).
fn pull_attrs_from_node_text<'mcx>(
    mcx: Mcx<'mcx>,
    text: &str,
) -> PgResult<alloc::vec::Vec<i32>> {
    let node = read_seam::string_to_node::call(mcx, text)?;
    let mut out = alloc::vec::Vec::new();
    // A `List*` of expressions (the normal `indexprs` shape and the
    // implicit-AND `indpred` list).
    if let Some(elems) = node.as_list() {
        for elem in elems.iter() {
            if let Some(e) = elem.as_expr() {
                let bms = var_seam::pull_varattnos::call(mcx, e, 1)?;
                out.extend(bms_members(bms.as_deref()));
            }
        }
    } else if let Some(e) = node.as_expr() {
        // A bare expression (defensive — `pull_varattnos` over the single node).
        let bms = var_seam::pull_varattnos::call(mcx, e, 1)?;
        out.extend(bms_members(bms.as_deref()));
    }
    Ok(out)
}

/// `RelationGetIndexAttrBitmap`'s per-index step (relcache.c): `index_open(
/// indexOid, AccessShareLock)`, extract `indkey`/`indisunique`/`indnkeyatts`/
/// `amsummarizing` + `pull_varattnos` over the raw `indexprs`/`indpred`, then
/// `index_close`. Returns the one index's attribute contributions.
///
/// The C reads the index's `rd_indextuple` directly; the owned relcache entry
/// keeps only the decoded `rd_index` form, so the raw `indexprs`/`indpred`
/// `pg_node_tree` text is read back by OID through the syscache owner (the same
/// catcache-resident `pg_index` row the C's `rd_indextuple` points at). The
/// `amsummarizing` flag comes from the opened index's `rd_indam` vtable (via the
/// relcache facade), and the full `indkey` vector / form scalars from the
/// projected `pg_index` row.
fn open_index_attrs(index_oid: Oid) -> PgResult<inward::IndexAttrInfo> {
    let scratch = MemoryContext::new("RelationGetIndexAttrBitmap open_index_attrs");
    let mcx = scratch.mcx();

    // indexDesc = index_open(indexOid, AccessShareLock);
    let index_rel = indexam_seam::index_open::call(mcx, index_oid, ACCESS_SHARE_LOCK)?;

    // Projected pg_index form scalars + full indkey (the C reads them off
    // `indexDesc->rd_index`/`rd_indextuple`).
    let idxinfo = syscache_seam::search_pg_index_info::call(mcx, index_oid)?.ok_or_else(|| {
        ereport(ERROR)
            .errmsg_internal(format!("cache lookup failed for index {index_oid}"))
            .into_error()
    })?;

    // indexDesc->rd_indam->amsummarizing.
    let amsummarizing = relcache_seam::relation_rd_indam::call(index_oid)
        .map(|am| am.amsummarizing)
        .unwrap_or(false);

    // datum = heap_getattr(rd_indextuple, Anum_pg_index_indexprs, ...); if
    // !isnull indexExpressions = stringToNode(...). `has_expressions` mirrors
    // the C `indexExpressions == NULL` test (the raw column being null).
    let exprs_text: Option<String> = syscache_seam::pg_index_exprs_text::call(index_oid)?;
    let pred_text: Option<String> = syscache_seam::pg_index_pred_text::call(index_oid)?;

    let has_expressions = exprs_text.is_some();
    let has_predicate = pred_text.is_some();

    let mut expr_attrs = alloc::vec::Vec::new();
    if let Some(text) = exprs_text {
        // pull_varattnos(indexExpressions, 1, attrs);
        expr_attrs = pull_attrs_from_node_text(mcx, &text)?;
    }
    let mut pred_attrs = alloc::vec::Vec::new();
    if let Some(text) = pred_text {
        // pull_varattnos(indexPredicate, 1, attrs);
        pred_attrs = pull_attrs_from_node_text(mcx, &text)?;
    }

    // index_close(indexDesc, AccessShareLock);
    index_rel.close(ACCESS_SHARE_LOCK)?;

    Ok(inward::IndexAttrInfo {
        indisunique: idxinfo.indisunique,
        indnkeyatts: idxinfo.indnkeyatts,
        amsummarizing,
        has_expressions,
        has_predicate,
        indkey: idxinfo.indkey.iter().copied().collect(),
        expr_attrs,
        pred_attrs,
    })
}

/// `RelationBuildPublicationDesc(relation, pubdesc)` (relcache.c): traverse all
/// publications the relation is in to build the per-relation publish-action +
/// REPLICA IDENTITY validity descriptor. Faithful port over the publication-
/// catalog seams (`GetRelationPublications`/`GetSchemaPublications`/
/// `GetAllTablesPublications`/`GetPublication`/`get_partition_ancestors`/
/// `get_rel_namespace`) and the two validity checks (`pub_rf_contains_invalid_
/// column` / `pub_contains_invalid_column`).
fn relation_build_publication_desc<'mcx>(
    mcx: Mcx<'mcx>,
    relation: &Relation<'mcx>,
) -> PgResult<PublicationDesc> {
    let relid = relation.rd_id;

    let mut pubdesc = PublicationDesc::default();

    // if (!is_publishable_relation(relation)) -> publishes no actions, all valid.
    if !pubcat_seam::is_publishable_relation::call(relation)? {
        pubdesc.rf_valid_for_update = true;
        pubdesc.rf_valid_for_delete = true;
        pubdesc.cols_valid_for_update = true;
        pubdesc.cols_valid_for_delete = true;
        pubdesc.gencols_valid_for_update = true;
        pubdesc.gencols_valid_for_delete = true;
        return Ok(pubdesc);
    }

    // memset(pubdesc, 0, ...); then set the six validity flags to true.
    pubdesc.rf_valid_for_update = true;
    pubdesc.rf_valid_for_delete = true;
    pubdesc.cols_valid_for_update = true;
    pubdesc.cols_valid_for_delete = true;
    pubdesc.gencols_valid_for_update = true;
    pubdesc.gencols_valid_for_delete = true;

    // puboids = GetRelationPublications(relid);
    let mut puboids: alloc::vec::Vec<Oid> =
        pubcat_seam::GetRelationPublications::call(mcx, relid)?
            .iter()
            .copied()
            .collect();

    // schemaid = RelationGetNamespace(relation);
    let schemaid = relation.rd_rel.relnamespace;
    // puboids = list_concat_unique_oid(puboids, GetSchemaPublications(schemaid));
    concat_unique_oid(
        &mut puboids,
        pubcat_seam::GetSchemaPublications::call(mcx, schemaid)?.iter().copied(),
    );

    // ancestors, used by the per-publication validity checks for partitions.
    let mut ancestors: alloc::vec::Vec<Oid> = alloc::vec::Vec::new();
    if relation.rd_rel.relispartition {
        // ancestors = get_partition_ancestors(relid);
        ancestors = partition_seam::get_partition_ancestors::call(mcx, relid)?
            .iter()
            .copied()
            .collect();

        for &ancestor in &ancestors {
            // puboids = list_concat_unique_oid(puboids, GetRelationPublications(ancestor));
            concat_unique_oid(
                &mut puboids,
                pubcat_seam::GetRelationPublications::call(mcx, ancestor)?
                    .iter()
                    .copied(),
            );
            // schemaid = get_rel_namespace(ancestor);
            let aschemaid = lsyscache_seam::get_rel_namespace::call(ancestor)?;
            // puboids = list_concat_unique_oid(puboids, GetSchemaPublications(schemaid));
            concat_unique_oid(
                &mut puboids,
                pubcat_seam::GetSchemaPublications::call(mcx, aschemaid)?
                    .iter()
                    .copied(),
            );
        }
    }

    // puboids = list_concat_unique_oid(puboids, GetAllTablesPublications());
    concat_unique_oid(
        &mut puboids,
        pubcat_seam::GetAllTablesPublications::call(mcx)?.iter().copied(),
    );

    for &pubid in &puboids {
        // pubform = SearchSysCache1(PUBLICATIONOID, pubid); (via GetPublication).
        let pubform = pubcat_seam::GetPublication::call(mcx, pubid)?;

        // pubdesc->pubactions.pub* |= pubform->pub*;
        pubdesc.pubactions.pubinsert |= pubform.pubactions.pubinsert;
        pubdesc.pubactions.pubupdate |= pubform.pubactions.pubupdate;
        pubdesc.pubactions.pubdelete |= pubform.pubactions.pubdelete;
        pubdesc.pubactions.pubtruncate |= pubform.pubactions.pubtruncate;

        // Row filter validity: skip FOR ALL TABLES (no row filters).
        if !pubform.alltables
            && (pubform.pubactions.pubupdate || pubform.pubactions.pubdelete)
            && pubcmds_seam::pub_rf_contains_invalid_column::call(
                mcx,
                pubid,
                relation,
                &ancestors,
                pubform.pubviaroot,
            )?
        {
            if pubform.pubactions.pubupdate {
                pubdesc.rf_valid_for_update = false;
            }
            if pubform.pubactions.pubdelete {
                pubdesc.rf_valid_for_delete = false;
            }
        }

        // Column-list / generated-column validity.
        if pubform.pubactions.pubupdate || pubform.pubactions.pubdelete {
            let pubgencols: i8 = match pubform.pubgencols_type {
                PublishGencolsType::None => b'n' as i8,
                PublishGencolsType::Stored => b's' as i8,
            };
            let (found, invalid_column_list, invalid_gen_col) =
                pubcmds_seam::pub_contains_invalid_column::call(
                    mcx,
                    pubid,
                    relation,
                    &ancestors,
                    pubform.pubviaroot,
                    pubgencols,
                )?;
            if found {
                if pubform.pubactions.pubupdate {
                    pubdesc.cols_valid_for_update = !invalid_column_list;
                    pubdesc.gencols_valid_for_update = !invalid_gen_col;
                }
                if pubform.pubactions.pubdelete {
                    pubdesc.cols_valid_for_delete = !invalid_column_list;
                    pubdesc.gencols_valid_for_delete = !invalid_gen_col;
                }
            }
        }

        // Early-out shortcuts: once everything is replicated and a category is
        // fully invalid for both update and delete, no further publication can
        // change the outcome.
        let all_actions = pubdesc.pubactions.pubinsert
            && pubdesc.pubactions.pubupdate
            && pubdesc.pubactions.pubdelete
            && pubdesc.pubactions.pubtruncate;
        if all_actions && !pubdesc.rf_valid_for_update && !pubdesc.rf_valid_for_delete {
            break;
        }
        if all_actions && !pubdesc.cols_valid_for_update && !pubdesc.cols_valid_for_delete {
            break;
        }
        if all_actions && !pubdesc.gencols_valid_for_update && !pubdesc.gencols_valid_for_delete {
            break;
        }
    }

    Ok(pubdesc)
}

/// `list_concat_unique_oid(dst, src)` — append each `src` OID not already in
/// `dst`.
fn concat_unique_oid(dst: &mut alloc::vec::Vec<Oid>, src: impl IntoIterator<Item = Oid>) {
    for oid in src {
        if !dst.contains(&oid) {
            dst.push(oid);
        }
    }
}

/// `RelationBuildPublicationDesc(relation)` driven from the relcache `Oid`
/// caller (`derived::RelationBuildPublicationDesc`). Opens the relation entry,
/// builds the descriptor, and acknowledges (the relcache caches it on the entry
/// behind the seam; the owned entry carries the `rd_has_pubdesc` presence flag).
fn publication_desc(relid: Oid) -> PgResult<()> {
    let scratch = MemoryContext::new("RelationBuildPublicationDesc");
    let mcx = scratch.mcx();
    // The C receives an already-open `Relation`; the OID-keyed relcache shell
    // (`derived::RelationBuildPublicationDesc(Oid)`) opens it for us.
    let rel = relation_seam::relation_open::call(mcx, relid, ACCESS_SHARE_LOCK)?;
    let _pubdesc = relation_build_publication_desc(mcx, &rel)?;
    // The C copies *pubdesc into rel->rd_pubdesc (CacheMemoryContext); the owned
    // entry carries only the rd_has_pubdesc presence flag, set by the relcache
    // caller on a successful return.
    rel.close(ACCESS_SHARE_LOCK)?;
    Ok(())
}

/// Decode a stored `pg_node_tree` text (`pg_index.indexprs` / `indpred`) into a
/// `Vec<Expr>`. The C stores both as a `List*` (`indexprs` is the expression
/// list, `indpred` the implicit-AND predicate list); `stringToNode` yields a
/// `Node::List` of `Expr` (defensively, a bare `Expr` is treated as a 1-element
/// list). Each element is owned out of the decoded tree.
fn decode_node_text_to_exprs(text: &str) -> PgResult<alloc::vec::Vec<Expr<'static>>> {
    // C's `RelationGetIndexExpressions`/`...Predicate` decode the catalog
    // `pg_node_tree` into a context that persists for the query's use of the
    // resulting trees (and re-derive per call). The owned model decodes into a
    // process-/thread-lifetime context (reclaimed at backend exit) so the returned
    // `Expr<'static>` honestly outlive every consumer — decoding into a caller's
    // transient `mcx` and erasing to `'static` would be a lie the borrow checker
    // now (correctly) rejects. Mirrors `pull_var_clause`'s result-context idiom.
    let mcx = nodexform_result_mcx();
    let node = read_seam::string_to_node::call(mcx, text)?;
    let node = ::mcx::PgBox::into_inner(node);
    let mut out = alloc::vec::Vec::new();
    match node.into_list() {
        Some(elems) => {
            for elem in elems {
                let inner = ::mcx::PgBox::into_inner(elem);
                if let Some(e) = inner.into_expr() {
                    out.push(e);
                }
            }
        }
        None => {
            // A bare expression node (not wrapped in a List).
            if let Some(e) = node_into_expr_fallback(text)? {
                out.push(e);
            }
        }
    }
    Ok(out)
}

/// Re-decode and unwrap a bare `Expr` from a `pg_node_tree` text (used only on
/// the defensive non-`List` path of [`decode_node_text_to_exprs`], where the
/// first decode was consumed by the `into_list` test).
fn node_into_expr_fallback(text: &str) -> PgResult<Option<Expr<'static>>> {
    let node = read_seam::string_to_node::call(nodexform_result_mcx(), text)?;
    Ok(::mcx::PgBox::into_inner(node).into_expr())
}

/// Process-/thread-lifetime context for decoded `pg_index.indexprs`/`indpred`
/// trees (and their const-folded forms). C keeps these in a query-lifetime
/// context; the faithful stand-in is a leaked per-thread `MemoryContext` whose
/// contents are reclaimed at backend exit, so the returned `Expr<'static>` (and
/// their by-reference children) never dangle.
fn nodexform_result_mcx() -> Mcx<'static> {
    use core::cell::Cell;
    thread_local! {
        static CTX: Cell<Option<&'static MemoryContext>> = const { Cell::new(None) };
    }
    CTX.with(|c| match c.get() {
        Some(ctx) => ctx.mcx(),
        None => {
            let ctx: &'static MemoryContext = alloc::boxed::Box::leak(alloc::boxed::Box::new(
                MemoryContext::new("relcache index nodexform result"),
            ));
            c.set(Some(ctx));
            ctx.mcx()
        }
    })
}

/// `RelationGetIndexExpressions(relation)` (relcache.c:5097): decode the raw
/// `pg_index.indexprs` text, run each expression through
/// `eval_const_expressions` (NOT `canonicalize_qual` — these are not quals),
/// then `fix_opfuncids`. Returns the expression list in `mcx`, or `None` when
/// the index has no expression columns. The C memoizes into `rd_indexprs`; the
/// owned entry does not carry that field, so the tree is re-derived per call.
fn index_expressions<'mcx>(
    mcx: Mcx<'mcx>,
    index_relid: Oid,
) -> PgResult<Option<PgVec<'static, Expr<'static>>>> {
    // heap_attisnull(rd_indextuple, Anum_pg_index_indexprs): the raw indexprs
    // text is read by OID through the syscache owner (the C `rd_indextuple`
    // analogue). NULL == no expression columns == NIL.
    let Some(text) = syscache_seam::pg_index_exprs_text::call(index_relid)? else {
        return Ok(None);
    };

    let raw = decode_node_text_to_exprs(&text)?;
    let mut result: PgVec<'static, Expr<'static>> = PgVec::new_in(nodexform_result_mcx());
    for e in raw {
        // eval_const_expressions(NULL, expr) — const-fold, no canonicalize.
        // Fold into the same query-lifetime result context as the decoded trees.
        let mut e = clauses_seam::eval_const_expressions_expr::call(nodexform_result_mcx(), e)?;
        // fix_opfuncids((Node *) result).
        nodes_core::nodefuncs::fix_opfuncids(&mut e)?;
        result.push(e);
    }
    Ok(Some(result))
}

/// `GetIndexInputType(index, indexcol)` (spgutils.c:120) — the EXPRESSION-key
/// polymorphic branch, which the SP-GiST core defers to this owner seam because
/// it needs `RelationGetIndexExpressions` + `exprType` + `getBaseType` (none
/// reachable from the SP-GiST core).
///
/// The SP-GiST core has already handled the non-polymorphic and
/// simple-(heap-)column cases; this is reached only when the index column
/// `indexcol` is an expression column (`indkey[indexcol-1] == 0`). C:
///
/// ```c
/// indexprs = RelationGetIndexExpressions(index);
/// indexpr_item = list_head(indexprs);
/// for (int i = 1; i <= indnkeyatts; i++)
///     if (indkey.values[i-1] == 0) {
///         if (indexpr_item == NULL) elog(ERROR, "wrong number of index expressions");
///         if (i == indexcol) return getBaseType(exprType(lfirst(indexpr_item)));
///         indexpr_item = lnext(indexprs, indexpr_item);
///     }
/// elog(ERROR, "wrong number of index expressions");
/// ```
fn get_index_input_type_expr(index_oid: Oid, indexcol: i16) -> PgResult<Oid> {
    let scratch = MemoryContext::new("GetIndexInputType index_expressions");
    let mcx = scratch.mcx();

    // indkey vector + indnkeyatts off the projected pg_index row.
    let idxinfo = syscache_seam::search_pg_index_info::call(mcx, index_oid)?.ok_or_else(|| {
        ereport(ERROR)
            .errmsg_internal(format!("cache lookup failed for index {index_oid}"))
            .into_error()
    })?;
    let indkey: alloc::vec::Vec<i16> = idxinfo.indkey.iter().copied().collect();
    let indnkeyatts = idxinfo.indnkeyatts as i32;

    // indexprs = RelationGetIndexExpressions(index); indexpr_item = list_head(..)
    let exprs = index_expressions(mcx, index_oid)?;
    let mut indexpr_iter = exprs.as_ref().map(|v| v.iter());

    for i in 1..=indnkeyatts {
        // indkey.values[i-1] == 0 ? (an expression column)
        if indkey.get((i - 1) as usize).copied().unwrap_or(0) == 0 {
            let Some(item) = indexpr_iter.as_mut().and_then(|it| it.next()) else {
                return Err(ereport(ERROR)
                    .errmsg_internal("wrong number of index expressions")
                    .into_error());
            };
            if i == indexcol as i32 {
                // getBaseType(exprType((Node *) lfirst(indexpr_item)))
                let etype = nodes_core::nodefuncs::expr_type(Some(item))?;
                return lsyscache_seam::get_base_type::call(etype);
            }
        }
    }

    Err(ereport(ERROR)
        .errmsg_internal("wrong number of index expressions")
        .into_error())
}

/// `RelationGetIndexPredicate(relation)` (relcache.c:5210): decode the raw
/// `pg_index.indpred` implicit-AND text, run it through `eval_const_expressions`,
/// `canonicalize_qual(.., false)`, `make_ands_implicit`, then `fix_opfuncids`.
/// Returns the implicit-AND predicate list in `mcx`, or `None` when the index is
/// not partial. The C memoizes into `rd_indpred`; the owned entry re-derives.
fn index_predicate<'mcx>(
    mcx: Mcx<'mcx>,
    index_relid: Oid,
) -> PgResult<Option<PgVec<'static, Expr<'static>>>> {
    // heap_attisnull(rd_indextuple, Anum_pg_index_indpred): NULL == not partial
    // == NIL.
    let Some(text) = syscache_seam::pg_index_pred_text::call(index_relid)? else {
        return Ok(None);
    };

    // The stored predicate is an implicit-AND `List*`; rebuild the single
    // boolean clause (`make_ands_explicit`) so the qual transforms (which work
    // over one `Expr`) match the C's `(Expr *) result` cast over the list.
    let clauses = decode_node_text_to_exprs(&text)?;
    let pred_expr = nodes_core::makefuncs::make_ands_explicit(clauses);

    // result = eval_const_expressions(NULL, (Node *) result);
    // Const-fold/canonicalize into the same query-lifetime result context as the
    // decoded trees (C keeps the whole predicate in one query context).
    let rmcx = nodexform_result_mcx();
    let folded = clauses_seam::eval_const_expressions_expr::call(rmcx, pred_expr)?;
    // result = canonicalize_qual((Expr *) result, false);
    let canon = prepqual_seam::canonicalize_qual::call(rmcx, Some(folded), false)?;
    // result = make_ands_implicit((Expr *) result);
    let implicit = nodes_core::makefuncs::make_ands_implicit(canon);

    let mut result: PgVec<'static, Expr<'static>> = PgVec::new_in(nodexform_result_mcx());
    for mut e in implicit {
        // fix_opfuncids((Node *) result).
        nodes_core::nodefuncs::fix_opfuncids(&mut e)?;
        result.push(e);
    }
    Ok(Some(result))
}

/// `index_concurrently_create_copy` expression source (catalog/index.c:1359-1369):
/// `indexExprs = (List *) stringToNode(exprString)` — the RAW `pg_index.indexprs`
/// expression list, with NO `eval_const_expressions` / `fix_opfuncids`. C uses
/// this (not the IndexInfo expressions) precisely because the IndexInfo lists are
/// flattened for the planner; the new index must store the original tree.
fn index_raw_expressions<'mcx>(
    mcx: Mcx<'mcx>,
    index_relid: Oid,
) -> PgResult<Option<PgVec<'static, Expr<'static>>>> {
    let Some(text) = syscache_seam::pg_index_exprs_text::call(index_relid)? else {
        return Ok(None);
    };
    let raw = decode_node_text_to_exprs(&text)?;
    let mut result: PgVec<'static, Expr<'static>> = PgVec::new_in(nodexform_result_mcx());
    for e in raw {
        result.push(e);
    }
    Ok(Some(result))
}

/// `index_concurrently_create_copy` predicate source (catalog/index.c:1370-1383):
/// `indexPreds = make_ands_implicit((Expr *) stringToNode(predString))` — the RAW
/// `pg_index.indpred`, reduced to implicit-AND form, with NO
/// `eval_const_expressions` / `canonicalize_qual` flattening (the stored predicate
/// is already an implicit-AND `List*`, so `make_ands_implicit` over the rebuilt
/// `make_ands_explicit` clause is identity-preserving).
fn index_raw_predicate<'mcx>(
    mcx: Mcx<'mcx>,
    index_relid: Oid,
) -> PgResult<Option<PgVec<'static, Expr<'static>>>> {
    let Some(text) = syscache_seam::pg_index_pred_text::call(index_relid)? else {
        return Ok(None);
    };
    let clauses = decode_node_text_to_exprs(&text)?;
    let pred_expr = nodes_core::makefuncs::make_ands_explicit(clauses);
    let implicit = nodes_core::makefuncs::make_ands_implicit(Some(pred_expr));
    let mut result: PgVec<'static, Expr<'static>> = PgVec::new_in(nodexform_result_mcx());
    for e in implicit {
        result.push(e);
    }
    Ok(Some(result))
}

/// `RelationGetDummyIndexExpressions(relation)` (relcache.c:5156): decode the
/// raw `pg_index.indexprs` text, then build, per raw sub-expression, a null
/// `Const` carrying the same type/typmod/collation —
/// `makeConst(exprType(rawExpr), exprTypmod(rawExpr), exprCollation(rawExpr), 1,
/// (Datum) 0, true /*isnull*/, true /*byval*/)`. Used by `BuildDummyIndexInfo`
/// (catalog/index.c) when truncating an index so no user-defined expression code
/// runs (the typlen/typbyval are arbitrary, as the value is null). Returns the
/// dummy-Const list in `mcx`, or `None` when the index has no expression columns.
fn dummy_index_expressions<'mcx>(
    mcx: Mcx<'mcx>,
    index_relid: Oid,
) -> PgResult<Option<PgVec<'static, Expr<'static>>>> {
    // heap_attisnull(rd_indextuple, Anum_pg_index_indexprs): the raw indexprs
    // text is read by OID through the syscache owner (the C `rd_indextuple`
    // analogue). NULL == no expression columns == NIL.
    let Some(text) = syscache_seam::pg_index_exprs_text::call(index_relid)? else {
        return Ok(None);
    };

    let raw = decode_node_text_to_exprs(&text)?;
    let mut result: PgVec<'static, Expr<'static>> = PgVec::new_in(nodexform_result_mcx());
    for rawexpr in &raw {
        // makeConst(exprType, exprTypmod, exprCollation, 1, (Datum) 0, true, true)
        // — a null Const of the same type/typmod/collation as the real expr. The
        // constlen (1) and constbyval (true) are arbitrary, per the C comment,
        // because the value is null.
        let consttype = nodes_core::nodefuncs::expr_type(Some(rawexpr))?;
        let consttypmod = nodes_core::nodefuncs::expr_typmod(Some(rawexpr))?;
        let constcollid = nodes_core::nodefuncs::expr_collation(Some(rawexpr))?;
        let cons = nodes_core::makefuncs::make_const(
            nodexform_result_mcx(),
            consttype,
            consttypmod,
            constcollid,
            1,
            types_tuple::heaptuple::Datum::ByVal(0),
            true,
            true,
        )?;
        result.push(Expr::Const(cons));
    }
    Ok(Some(result))
}

/// Install this unit's seams. `index_expressions` / `index_predicate` /
/// `dummy_index_expressions` decode the raw `pg_index.indexprs` / `indpred`
/// node trees and return the transformed expression lists; the owned relcache
/// entry does not retain the C's `rd_indexprs` / `rd_indpred` memoization, so
/// each call re-derives the tree (faithful behavior, minus the cache).
pub fn init_seams() {
    inward::open_index_attrs::set(open_index_attrs);
    inward::relation_build_publication_desc::set(relation_build_publication_desc);
    inward::publication_desc::set(publication_desc);
    inward::index_expressions::set(index_expressions);
    inward::index_predicate::set(index_predicate);
    inward::index_raw_expressions::set(index_raw_expressions);
    inward::index_raw_predicate::set(index_raw_predicate);
    inward::dummy_index_expressions::set(dummy_index_expressions);
    // `GetIndexInputType` EXPRESSION-key branch (spgutils.c) — owned here because
    // it needs `RelationGetIndexExpressions` + `exprType` + `getBaseType`.
    spg_core_seams::get_index_input_type_expr::set(get_index_input_type_expr);
}
