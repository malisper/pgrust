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

use mcx::{Mcx, MemoryContext};
use types_catalog::pg_publication::{PublicationDesc, PublishGencolsType};
use types_core::primitive::Oid;
use types_error::{PgResult, ERROR};
use types_nodes::nodes::Node;
use types_rel::Relation;

use backend_utils_error::ereport;

use backend_access_common_relation_seams as relation_seam;
use backend_access_index_indexam_seams as indexam_seam;
use backend_catalog_partition_seams as partition_seam;
use backend_catalog_pg_publication_seams as pubcat_seam;
use backend_commands_publicationcmds_seams as pubcmds_seam;
use backend_nodes_read_seams as read_seam;
use backend_optimizer_util_var_seams as var_seam;
use backend_utils_cache_lsyscache_seams as lsyscache_seam;
use backend_utils_cache_relcache_nodexform_seams as inward;
use backend_utils_cache_relcache_seams as relcache_seam;
use backend_utils_cache_syscache_seams as syscache_seam;

/// `AccessShareLock` (`storage/lockdefs.h`).
const ACCESS_SHARE_LOCK: types_storage::lock::LOCKMODE = 1;

/// Iterate the offset members of a `pull_varattnos` result bitmapset (allocated
/// in `mcx`) into a `Vec<i32>` (the `expr_attrs`/`pred_attrs` the
/// `IndexAttrInfo` carries — already offset by
/// `FirstLowInvalidHeapAttributeNumber`). `None` is the C empty/NULL set.
fn bms_members(bms: Option<&types_nodes::bitmapset::Bitmapset<'_>>) -> alloc::vec::Vec<i32> {
    let mut out = alloc::vec::Vec::new();
    let mut i = -1;
    loop {
        i = backend_nodes_core::bitmapset::bms_next_member(bms, i);
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
    match &*node {
        // A `List*` of expressions (the normal `indexprs` shape and the
        // implicit-AND `indpred` list).
        Node::List(elems) => {
            for elem in elems.iter() {
                if let Some(e) = elem.as_expr() {
                    let bms = var_seam::pull_varattnos::call(mcx, e, 1)?;
                    out.extend(bms_members(bms.as_deref()));
                }
            }
        }
        // A bare expression (defensive — `pull_varattnos` over the single node).
        Node::Expr(e) => {
            let bms = var_seam::pull_varattnos::call(mcx, e, 1)?;
            out.extend(bms_members(bms.as_deref()));
        }
        _ => {}
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

/// Install this unit's seams. The three node-tree caching seams
/// (`index_expressions` / `index_predicate` / `dummy_index_expressions`) are NOT
/// installed here: they cache a built node tree into the relcache entry's
/// `rd_indexprs` / `rd_indpred` / dummy-Const fields, which the trimmed owned
/// entry does not carry, and their only consumers (`get_index_expressions` /
/// `get_index_predicate` in the planner-catalog read path) panic on the
/// unmodeled planner-arena node projection regardless — that is the #159
/// planner-values keystone, not this lane.
pub fn init_seams() {
    inward::open_index_attrs::set(open_index_attrs);
    inward::relation_build_publication_desc::set(relation_build_publication_desc);
    inward::publication_desc::set(publication_desc);
}
