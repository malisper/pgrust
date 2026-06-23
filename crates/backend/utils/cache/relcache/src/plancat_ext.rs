//! Installers for the relcache-owned externals that `optimizer/util/plancat.c`'s
//! `get_relation_info` reads, declared in the consumer-side
//! `backend-optimizer-util-plancat-ext-seams` crate.
//!
//! `get_relation_info`'s index loop, parallel-workers read, index-list read and
//! per-index block-count are all `RelationGetParallelWorkers` /
//! `RelationGetIndexList` / `index_open` + `rd_index`/`rd_indam`/`rd_op*` reads /
//! `RelationGetNumberOfBlocks` — i.e. relcache reads over the owned entry. They
//! are homed here (relcache OWNS these reads) rather than left as panicking
//! no-owner stubs. The node-vocabulary reads (`get_index_expressions`/
//! `get_index_predicate`) and the catalog scans (`get_stat_ext_list`) route to
//! the relcache derived family / their node-tree owner seams exactly as the
//! relcache derived adapters do; for the empty (no expressions / no partial
//! predicate / no extended stats) cases the entry's own cached state answers.
//!
//! The table-AM capability probes (`table_has_scan_bitmap`/`table_has_tid_range`)
//! and the size estimators are NOT installed here — they belong to the table-AM
//! owner (heapam_handler.c) and are installed there.

#![allow(unused_variables)]

use plancat_ext_seams as px;
use relcache_seams as sx;
use types_core::primitive::{BlockNumber, Oid};
use types_error::{PgError, PgResult};

use crate::core_entry_store::{self, RelationClose, RelationIdGetRelation, with_relation};

/// `index_open(indexoid, NoLock)` (the relcache-pin half) + run `read` while the
/// entry is pinned + `index_close(indexRelation, NoLock)`.
///
/// In C `get_relation_info` / `infer_arbiter_indexes` open each index with
/// `index_open(indexoid, lmode)`, read the descriptor, then
/// `index_close(indexRelation, NoLock)` — the close drops the relcache **pin**
/// (`RelationDecrementReferenceCount`) but keeps the lock (held to xact end).
/// The pgrust by-OID accessors each need that same pin/unpin pairing so the
/// pin is not leaked onto the resource owner (otherwise it surfaces as
/// "resource was not closed" at statement/transaction end). The caller does the
/// `LockRelationOid(.., lmode).keep()` separately when a lock is wanted; this
/// helper takes no lock and always unpins, on both the `Ok` and `Err` paths,
/// mirroring `index_close`.
fn with_index_open<R>(
    indexoid: Oid,
    read: impl FnOnce() -> PgResult<R>,
) -> PgResult<R> {
    let built = RelationIdGetRelation(indexoid)?;
    if built == types_core::InvalidOid {
        return Err(PgError::error(format!(
            "could not open index with OID {indexoid}"
        )));
    }
    let result = read();
    // `index_close(indexRelation, NoLock)` — drop the relcache pin (keep the
    // lock). Run on the error path too, like C's resource cleanup, so a failed
    // read does not leak the pin.
    RelationClose(indexoid)?;
    result
}

/// Install every plancat-ext seam relcache owns.
pub fn init_seams() {
    px::relation_parallel_workers::set(relation_parallel_workers);
    px::ignore_system_indexes_for::set(ignore_system_indexes_for);
    px::relation_get_index_list_oids::set(relation_get_index_list_oids);
    px::get_relation_fkey_list::set(get_relation_fkey_list);
    px::get_index_cat_info::set(get_index_cat_info);
    px::get_index_expressions::set(get_index_expressions);
    px::get_index_predicate::set(get_index_predicate);
    px::index_number_of_blocks::set(index_number_of_blocks);
    px::index_get_tree_height::set(index_get_tree_height);
    px::get_stat_ext_list::set(get_stat_ext_list);
    px::relation_is_partition::set(relation_is_partition);
    px::relation_has_not_null::set(relation_has_not_null);
    px::relation_has_stored_generated_columns::set(relation_has_stored_generated_columns);
    px::not_null_attnums::set(not_null_attnums);
    px::get_check_constraints::set(get_check_constraints);
    px::relation_has_row_triggers::set(relation_has_row_triggers);
    px::relation_has_transition_tables::set(relation_has_transition_tables);
    px::get_infer_index_info::set(get_infer_index_info);
    px::infer_collation_opclass_match::set(infer_collation_opclass_match);
    sx::get_replident_index_info::set(get_replident_index_info);
}

/// `ATExecReplicaIdentity`'s `index_open(indexOid, ShareLock)` reads, projected
/// into a [`sx::ReplidentIndexInfo`] (tablecmds.c:18490). The caller already
/// holds the `ShareLock`; this pins the relcache entry (`relation_open(..,
/// NoLock)` half of `index_open`), reads `rd_index`/`rd_indam` + the
/// expression/predicate presence, and unpins (`index_close(.., NoLock)`).
///
/// A relation with `rd_index == NULL` (not an index) yields `is_index == false`
/// and default fields; the caller raises the `is not an index for table` error.
fn get_replident_index_info(index_oid: Oid) -> PgResult<sx::ReplidentIndexInfo> {
    with_index_open(index_oid, || {
        // `rd_index` flags + key-column vector off the owned entry.
        let (is_index, indrelid, indisunique, indisexclusion, indimmediate, key_columns, has_expr) =
            with_relation(index_oid, |rd| match rd.rd_index.as_ref() {
                None => (false, types_core::InvalidOid, false, false, false, Vec::new(), false),
                Some(index) => {
                    let nkey = index.indnkeyatts as usize;
                    let key_columns = index
                        .indkey
                        .iter()
                        .take(nkey)
                        .map(|&k| sx::ReplidentKeyColumn { attno: k as i16 })
                        .collect::<Vec<_>>();
                    // `RelationGetIndexExpressions(indexRel) != NIL`: an index
                    // carries expression columns iff some `indkey[i] == 0`
                    // (`InvalidAttrNumber`), the on-disk marker for an expression
                    // column (relcache.c:5108 quick-exit proxy).
                    let has_expr = index
                        .indkey
                        .iter()
                        .any(|&k| k == types_core::primitive::InvalidAttrNumber);
                    (
                        true,
                        index.indrelid,
                        index.indisunique,
                        index.indisexclusion,
                        index.indimmediate,
                        key_columns,
                        has_expr,
                    )
                }
            })?;

        // `indexRel->rd_indam->amcanunique`. A partitioned index has `rd_indam ==
        // NULL` (no AM routine); the C reads `indexRel->rd_indam->amcanunique`
        // unconditionally, but a partitioned index reaches here only with
        // `indisunique` already set by a valid AM, so absent `rd_indam` => false
        // is the faithful "AM does not support uniqueness" outcome.
        let amcanunique =
            with_relation(index_oid, |rd| rd.rd_indam.as_ref().map(|am| am.amcanunique))?
                .unwrap_or(false);

        // `RelationGetIndexPredicate(indexRel) != NIL`: a partial index. The
        // owned relcache entry does not carry `indpred`; read its presence off
        // pg_index (`heap_attisnull(.., Anum_pg_index_indpred)`), exactly as the
        // FK/logical-replication callers do.
        let has_predicate =
            syscache_seams::pg_index_has_predicate::call(index_oid)?
                .unwrap_or(false);

        Ok(sx::ReplidentIndexInfo {
            is_index,
            indrelid,
            amcanunique,
            indisunique,
            indisexclusion,
            indimmediate,
            has_expressions: has_expr,
            has_predicate,
            key_columns,
        })
    })
}

/// `index_open(indexoid, rellockmode)` + the `idxForm`/expression/predicate reads
/// `infer_arbiter_indexes` (plancat.c) needs, with the index left closed at
/// return. Opens (locks + builds) the index relcache entry, reads the
/// pg_index-form fields the inference loop examines, and materializes the index
/// expressions/predicate into `root`'s node arena (empty for a plain or non-
/// partial index — every catalog and plain-column unique index).
fn get_infer_index_info(
    mcx: mcx::Mcx<'_>,
    root: &mut pathnodes::PlannerInfo,
    indexoid: Oid,
    rellockmode: i32,
) -> PgResult<px::InferIndexInfo> {
    // `index_open(indexoid, rellockmode)` = lock then build/return the cached
    // entry; the lock is held to transaction end (the executor needs the same
    // lock), mirroring get_index_cat_info.
    if rellockmode != 0 {
        lmgr_seams::lock_relation_oid::call(indexoid, rellockmode)?.keep();
    }

    // pg_index-form fields off the owned entry, with the index pinned for the
    // read and unpinned afterwards (`index_open`/`index_close(.., NoLock)`).
    let (indexrelid, indisvalid, indisunique, indisexclusion, indnkeyatts, indkey) =
        with_index_open(indexoid, || {
            with_relation(indexoid, |rd| {
                let index = rd.rd_index.as_ref().ok_or_else(|| {
                    PgError::error(format!("relation {indexoid} is not an index"))
                })?;
                Ok::<_, PgError>((
                    index.indexrelid,
                    index.indisvalid,
                    index.indisunique,
                    index.indisexclusion,
                    index.indnkeyatts as i32,
                    index
                        .indkey
                        .iter()
                        .map(|&k| k as types_core::primitive::AttrNumber)
                        .collect::<Vec<_>>(),
                ))
            })?
        })?;

    // `RelationGetIndexExpressions` / `RelationGetIndexPredicate` as arena node
    // handles. These reuse the same read paths get_index_expressions /
    // get_index_predicate use: empty for an index with no expression columns /
    // no partial predicate (the heap_attisnull quick-exits), and a loud "not
    // modeled" error for the real arena projection of an expression/partial
    // index — exactly mirroring those seams.
    let idx_exprs = get_index_expressions(mcx, root, indexoid)?;
    let idx_predicate = get_index_predicate(mcx, root, indexoid)?;

    Ok(px::InferIndexInfo {
        indexrelid,
        indisvalid,
        indisunique,
        indisexclusion,
        indnkeyatts,
        indkey,
        idx_exprs,
        idx_predicate,
    })
}

/// `infer_collation_opclass_match(elem, idxRel, idxExprs)` (plancat.c): when the
/// inference element specifies a collation or opclass, verify at least one of the
/// opened index's attributes matches it (opfamily + input type for the opclass,
/// collation for the collation), and that the matching attribute is the one the
/// element refers to (the Var's attno, or — for an expression element — the
/// cataloged index expression by node-equality). Returns true immediately when
/// the element specifies neither (the common case).
fn infer_collation_opclass_match(
    root: &pathnodes::PlannerInfo,
    indexoid: Oid,
    elem: &px::InferenceElemInfo,
    idx_exprs: &[pathnodes::NodeId],
) -> PgResult<bool> {
    use ::nodes::primnodes::Expr;

    // No collation/opclass specified -> no exact match needed.
    if elem.infercollid == types_core::InvalidOid && elem.inferopclass == types_core::InvalidOid {
        return Ok(true);
    }

    // Lookup opfamily and input type for the specified opclass (if any).
    let (inferopfamily, inferopcinputtype) = if elem.inferopclass != types_core::InvalidOid {
        (
            lsyscache_seams::get_opclass_family::call(elem.inferopclass)?,
            lsyscache_seams::get_opclass_input_type::call(elem.inferopclass)?,
        )
    } else {
        (types_core::InvalidOid, types_core::InvalidOid)
    };

    // Open the index (pin), read its per-attribute catalog data + run the match
    // loop, then unpin (`index_open`/`index_close(.., NoLock)`).
    with_index_open(indexoid, || {
        // Per-attribute opfamily/opcintype/collation + indkey, off the owned entry.
        let (natts, opfamilies, opcintypes, collations, indkeys) =
            with_relation(indexoid, |rd| {
                let index = rd.rd_index.as_ref().ok_or_else(|| {
                    PgError::error(format!("relation {indexoid} is not an index"))
                })?;
                Ok::<_, PgError>((
                    rd.rd_att.natts() as usize,
                    rd.rd_opfamily.clone(),
                    rd.rd_opcintype.clone(),
                    rd.rd_indcollation.clone(),
                    index.indkey.iter().map(|&k| k as i32).collect::<Vec<_>>(),
                ))
            })??;

        // The inference element's expression, resolved through `root`'s arena (the
        // same arena `idx_exprs` index into).
        let elem_expr = root.node(elem.expr);
        let elem_is_var = matches!(elem_expr, Expr::Var(_));
        let elem_varattno = match elem_expr {
            Expr::Var(v) => v.varattno as i32,
            _ => 0,
        };

        let mut nplain = 0usize; // # plain attrs observed (C: nplain).
        for natt in 1..=natts {
            let opfamily = *opfamilies.get(natt - 1).unwrap_or(&types_core::InvalidOid);
            let opcinputtype = *opcintypes.get(natt - 1).unwrap_or(&types_core::InvalidOid);
            let collation = *collations.get(natt - 1).unwrap_or(&types_core::InvalidOid);
            let attno = *indkeys.get(natt - 1).unwrap_or(&0);

            if attno != 0 {
                nplain += 1;
            }

            // Attribute needed to match opclass, but didn't.
            if elem.inferopclass != types_core::InvalidOid
                && (inferopfamily != opfamily || inferopcinputtype != opcinputtype)
            {
                continue;
            }
            // Attribute needed to match collation, but didn't.
            if elem.infercollid != types_core::InvalidOid && elem.infercollid != collation {
                continue;
            }

            // One matching index att found -> good enough.
            if elem_is_var {
                if elem_varattno == attno {
                    return Ok(true);
                }
            } else if attno == 0 {
                // Expression column: compare the element expr to the cataloged index
                // expression at position (natt-1)-nplain by node-equality.
                let idx_pos = (natt - 1) - nplain;
                if let Some(&natt_expr) = idx_exprs.get(idx_pos) {
                    if px::node_equal::call(root, elem.expr, natt_expr) {
                        return Ok(true);
                    }
                }
            }
        }

        Ok(false)
    })
}

/// `has_row_triggers` (plancat.c): whether the relation has any row-level
/// trigger for `event`, read off the owned entry's `trigdesc`.
fn relation_has_row_triggers(
    relid: Oid,
    event: pathnodes::CmdType,
) -> PgResult<bool> {
    with_relation(relid, |rd| {
        let td = match rd.rd_trigdesc.as_ref() {
            Some(td) => td,
            None => return false,
        };
        match event {
            pathnodes::CMD_INSERT => {
                td.trig_insert_after_row || td.trig_insert_before_row
            }
            pathnodes::CMD_UPDATE => {
                td.trig_update_after_row || td.trig_update_before_row
            }
            pathnodes::CMD_DELETE => {
                td.trig_delete_after_row || td.trig_delete_before_row
            }
            _ => false, // CMD_MERGE has no separate event.
        }
    })
}

/// `has_transition_tables` (plancat.c): whether the relation has any transition
/// table for `event`. Foreign tables cannot have transition tables.
fn relation_has_transition_tables(
    relid: Oid,
    event: pathnodes::CmdType,
) -> PgResult<bool> {
    with_relation(relid, |rd| {
        if rd.rd_rel.relkind == (b'f' as i8) {
            return false; // RELKIND_FOREIGN_TABLE
        }
        let td = match rd.rd_trigdesc.as_ref() {
            Some(td) => td,
            None => return false,
        };
        match event {
            pathnodes::CMD_INSERT => td.trig_insert_new_table,
            pathnodes::CMD_UPDATE => {
                td.trig_update_old_table || td.trig_update_new_table
            }
            pathnodes::CMD_DELETE => td.trig_delete_old_table,
            _ => false,
        }
    })
}

/// `relation->rd_rel->relispartition` (`plancat.c`'s `include_partition` test).
fn relation_is_partition(relid: Oid) -> PgResult<bool> {
    with_relation(relid, |rd| rd.rd_rel.relispartition)
}

/// `relation->rd_att->constr->has_not_null` (`get_relation_constraints`).
fn relation_has_not_null(relid: Oid) -> PgResult<bool> {
    with_relation(relid, |rd| {
        rd.rd_att.constr().map(|c| c.has_not_null).unwrap_or(false)
    })
}

/// `tupdesc->constr && tupdesc->constr->has_generated_stored`
/// (`has_stored_generated_columns`).
fn relation_has_stored_generated_columns(relid: Oid) -> PgResult<bool> {
    with_relation(relid, |rd| {
        rd.rd_att.constr().map(|c| c.has_generated_stored).unwrap_or(false)
    })
}

/// The relation's valid not-null columns as `(attno, atttypid, atttypmod,
/// attcollation)`, in attno order — the `IS NOT NULL` NullTest data
/// `get_relation_constraints` builds: per-column `att->attnullability ==
/// ATTNULLABLE_VALID && !att->attisdropped`.
fn not_null_attnums(relid: Oid) -> PgResult<Vec<(types_core::primitive::AttrNumber, Oid, i32, Oid)>> {
    with_relation(relid, |rd| {
        let mut out = Vec::new();
        for att in rd.rd_att.attrs.iter() {
            if att.attnullability == types_tuple::heaptuple::ATTNULLABLE_VALID && !att.attisdropped {
                out.push((att.attnum, att.atttypid, att.atttypmod, att.attcollation));
            }
        }
        out
    })
}

/// The relation's fully-validated check constraints (`rd_att->constr->check[i]`
/// where `ccvalid`), in catalog order — `(ccbin, ccnoinherit)`. NOT ENFORCED
/// constraints are always invalid, so the `ccvalid` filter subsumes the C
/// `Assert(ccenforced)`.
fn get_check_constraints(relid: Oid) -> PgResult<Vec<px::CheckConstraintInfo>> {
    with_relation(relid, |rd| {
        let mut out = Vec::new();
        if let Some(constr) = rd.rd_att.constr() {
            for c in constr.check.iter() {
                if !c.ccvalid {
                    continue;
                }
                out.push(px::CheckConstraintInfo {
                    ccbin: c.ccbin.clone(),
                    ccnoinherit: c.ccnoinherit,
                });
            }
        }
        out
    })
}

/// `RelationGetParallelWorkers(relation, -1)` (rel.h): the macro is
/// `(relation)->rd_options ? ((StdRdOptions *) rd_options)->parallel_workers :
/// defaultpw`. plancat calls it with `defaultpw == -1`.
fn relation_parallel_workers(relid: Oid) -> PgResult<i32> {
    with_relation(relid, |rd| {
        rd.rd_options
            .as_ref()
            .and_then(|o| o.std())
            .map(|o| o.parallel_workers)
            .unwrap_or(-1)
    })
}

/// `IgnoreSystemIndexes && IsSystemRelation(relation)` (the `hasindex`-gating
/// read in `get_relation_info`). `IgnoreSystemIndexes` is the miscinit.c GUC;
/// `IsSystemRelation` is catalog.c over the relation. We project the entry to a
/// `Relation` value-slice and call the catalog seam (its real owner).
fn ignore_system_indexes_for(relid: Oid) -> PgResult<bool> {
    if !miscinit_seams::get_ignore_system_indexes::call() {
        return Ok(false);
    }
    // IsSystemRelation needs a Relation; project the owned entry and wrap it
    // (no release authority — a transient read handle).
    let relcx = mcx::MemoryContext::new("ignore_system_indexes_for");
    let data = with_relation(relid, |rd| crate::build::project_relation_data(relcx.mcx(), rd))??;
    let rel = rel::Relation::open(data, None);
    catalog_seams::is_system_relation::call(&rel)
}

/// `RelationGetIndexList(relation)` as a lifetime-free `Vec<Oid>` (relcache.c).
fn relation_get_index_list_oids(relid: Oid) -> PgResult<Vec<Oid>> {
    crate::derived::RelationGetIndexList(relid)
}

/// `RelationGetFKeyList(relation)` (relcache.c) as the planner-ready
/// [`px::CachedFkInfo`] rows `get_relation_foreign_keys` (plancat.c) walks.
///
/// In C `RelationGetFKeyList(Relation relation)` takes the caller's
/// already-open `Relation` and never pins it itself; `get_relation_info` holds
/// the relation open across this call (it `table_open`ed it and closes it after
/// FK collection). So this must NOT take a fresh `rd_refcnt` pin: a
/// `RelationIdGetRelation` here would `RelationIncrementReferenceCount` a pin
/// that nothing releases — this function builds a value `Vec` and returns, with
/// no `Relation` carrier to drop — leaking one reference per `get_relation_info`
/// and making `CheckTableNotInUse` later refuse a same-session `DROP` of the
/// table. The entry is guaranteed already built and pinned (the caller opened
/// it), so a pin-free `cache_lookup` is the faithful presence check.
fn get_relation_fkey_list(relid: Oid) -> PgResult<Vec<px::CachedFkInfo>> {
    if core_entry_store::cache_lookup(relid).is_none() {
        return Err(PgError::error(format!(
            "could not open relation with OID {relid}"
        )));
    }
    let fkeys = crate::derived::RelationGetFKeyList(relid)?;
    Ok(fkeys
        .into_iter()
        .map(|fk| px::CachedFkInfo {
            conrelid: fk.conrelid,
            confrelid: fk.confrelid,
            conenforced: fk.conenforced,
            nkeys: fk.nkeys,
            conkey: fk.conkey,
            confkey: fk.confkey,
            conpfeqop: fk.conpfeqop,
        })
        .collect())
}

/// Open the index `indexoid` (forcing a relcache build) and extract everything
/// `get_relation_info` reads into a planner-ready [`px::IndexCatInfo`]. This is
/// the `index_open(indexoid, lmode)` + `rd_index`/`rd_indam`/`rd_opfamily`/
/// `rd_opcintype`/`rd_indcollation`/`rd_indoption` + `index_can_return` reads,
/// over the owned relcache index entry (which `RelationInitIndexAccessInfo`
/// fully populated). The table-AM half of `amhasgetbitmap` is supplied
/// separately by `table_has_scan_bitmap`.
fn get_index_cat_info(indexoid: Oid, lmode: i32) -> PgResult<px::IndexCatInfo> {
    // C: `indexRelation = index_open(indexoid, lmode)` = `LockRelationOid(indexoid,
    // lmode)` then `relation_open(indexoid, NoLock)` (which builds/returns the
    // cached entry). The lock is held to transaction end (`index_close(..,
    // NoLock)` keeps it), matching the executor's later need for the same lock.
    if lmode != 0 {
        lmgr_seams::lock_relation_oid::call(indexoid, lmode)?.keep();
    }

    // Force the index entry to be built/cached (the `relation_open(.., NoLock)`
    // half of `index_open`), read the descriptor, then drop the relcache pin
    // (`index_close(indexRelation, NoLock)`).
    with_index_open(indexoid, || {
        // Read the bulk of the descriptor from the owned entry.
        let mut info = with_relation(indexoid, |rd| build_index_cat_info(rd))??;

        // `index_can_return(indexRelation, attno)` (indexam.c) per column: read the
        // AM's `amcanreturn` callback off the cached `rd_indam` vtable. NULL means
        // the AM never supports index-only scans (`canreturn[i] = false`). We need a
        // `Relation` to invoke the callback; project the owned entry and wrap it.
        let amcanreturn = with_relation(indexoid, |rd| {
            rd.rd_indam.as_ref().and_then(|am| am.amcanreturn)
        })?;
        if let Some(amcanreturn) = amcanreturn {
            let relcx = mcx::MemoryContext::new("index_can_return");
            let data = with_relation(indexoid, |rd| {
                crate::build::project_relation_data(relcx.mcx(), rd)
            })??;
            let rel = rel::Relation::open(data, None);
            let mut canreturn = Vec::with_capacity(info.indnatts as usize);
            for i in 0..info.indnatts {
                canreturn.push(amcanreturn(&rel, i + 1)?);
            }
            info.canreturn = canreturn;
        } else {
            info.canreturn = vec![false; info.indnatts as usize];
        }

        Ok(info)
    })
}

/// Build the [`px::IndexCatInfo`] from the owned relcache index entry's cached
/// `rd_index`/`rd_indam`/`rd_op*` fields (everything except `canreturn`, which
/// needs a projected `Relation` for the `amcanreturn` callback, and the
/// `has_opclassoptions` presence, read from `rd_opcoptions`).
fn build_index_cat_info(rd: &core_entry_store::RelationData) -> PgResult<px::IndexCatInfo> {
    let index = rd.rd_index.as_ref().ok_or_else(|| {
        PgError::error(format!("relation {} is not an index", rd.rd_id))
    })?;

    let indnatts = index.indnatts as i32;
    let indnkeyatts = index.indnkeyatts as i32;

    // indcheckxmin: for a built/cached catalog index this is false (the index
    // was created at initdb with no HOT-recheck need). When true, C compares
    // `HeapTupleHeaderGetXmin(rd_indextuple) < TransactionXmin`; the owned entry
    // does not carry the raw index tuple's xmin, so that comparison is not
    // modeled — surface it loudly rather than silently guess (mirror-and-panic).
    if index.indcheckxmin {
        return Err(PgError::error(
            "get_index_cat_info: indcheckxmin recheck not modeled (rd_indextuple xmin absent)",
        ));
    }

    let relam = rd.rd_rel.relam;
    let is_partitioned = rd.rd_rel.relkind == (b'I' as i8); // RELKIND_PARTITIONED_INDEX

    // rd_indam capability flags (the C reads them only for non-partitioned
    // indexes; a partitioned index has rd_indam == NULL).
    let (
        amcanorder,
        amcanorderbyop,
        amoptionalkey,
        amsearcharray,
        amsearchnulls,
        amcanparallel,
        amhasgettuple,
        amhasgetbitmap_am,
        amcanmarkpos,
        amhasgettreeheight,
    ) = match rd.rd_indam.as_ref() {
        Some(am) => (
            am.amcanorder,
            am.amcanorderbyop,
            am.amoptionalkey,
            am.amsearcharray,
            am.amsearchnulls,
            am.amcanparallel,
            am.amgettuple.is_some(),
            am.amgetbitmap.is_some(),
            am.ammarkpos.is_some() && am.amrestrpos.is_some(),
            am.amgettreeheight.is_some(),
        ),
        None => (false, false, false, false, false, false, false, false, false, false),
    };

    let has_opclassoptions = rd
        .rd_opcoptions
        .as_ref()
        .is_some_and(|cols| cols.iter().any(|c| c.is_some()));

    Ok(px::IndexCatInfo {
        indexrelid: index.indexrelid,
        reltablespace: rd.rd_rel.reltablespace,
        relam,
        is_partitioned,
        indisvalid: index.indisvalid,
        indcheckxmin: index.indcheckxmin,
        indcheckxmin_passes: true, // unreached: indcheckxmin is false above.
        indnatts,
        indnkeyatts,
        indkey: index.indkey.iter().map(|&k| k as i32).collect(),
        indisunique: index.indisunique,
        indisexclusion: index.indisexclusion,
        indnullsnotdistinct: index.indnullsnotdistinct,
        indimmediate: index.indimmediate,
        opfamily: rd.rd_opfamily.clone(),
        opcintype: rd.rd_opcintype.clone(),
        indcollation: rd.rd_indcollation.clone(),
        indoption: rd.rd_indoption.clone(),
        canreturn: Vec::new(), // filled by get_index_cat_info via amcanreturn.
        has_opclassoptions,
        amcanorder,
        amcanorderbyop,
        amoptionalkey,
        amsearcharray,
        amsearchnulls,
        amcanparallel,
        amhasgettuple,
        amhasgetbitmap: amhasgetbitmap_am,
        amcanmarkpos,
        amhasgettreeheight,
    })
}

/// `RelationGetIndexExpressions(indexRelation)` (relcache.c) as fresh arena node
/// handles in the planner arena, in indkey order. An index with no expression
/// columns yields the empty list (the `indexprs` quick exit); for an expression
/// index the decoded `Expr` list (from the node-tree owner seam) is interned
/// into `root`'s node arena and returned as `NodeId` handles.
///
/// `mcx` is the planner-run arena context (`root->planner_cxt`): the decoded
/// trees are allocated there (the same arena that backs `root`'s `node_arena`),
/// so the returned handles outlive this call. This mirrors the C
/// `RelationGetIndexExpressions`, which builds in `CacheMemoryContext` and the
/// caller (`get_relation_info`) `copyObject`s the result into `root->planner_cxt`.
fn get_index_expressions(
    mcx: mcx::Mcx<'_>,
    root: &mut pathnodes::PlannerInfo,
    indexoid: Oid,
) -> PgResult<Vec<pathnodes::NodeId>> {
    with_index_open(indexoid, || {
        // `RelationGetIndexExpressions(index)` quick-exits to NIL when the index has
        // no expression columns (the C `heap_attisnull(rd_indextuple,
        // Anum_pg_index_indexprs)` short-circuit). A zero in `indkey` marks an
        // expression column; a plain index — every pg_class system index — has none,
        // so the list is empty without invoking the node-tree builder.
        let has_exprs = with_relation(indexoid, |rd| {
            rd.rd_index
                .as_ref()
                .is_some_and(|i| i.indkey.iter().any(|&k| k == 0))
        })?;
        if !has_exprs {
            return Ok(Vec::new());
        }
        // Decode the raw `pg_index.indexprs` tree (`stringToNode` +
        // `eval_const_expressions` + `fix_opfuncids`) via the node-tree owner seam,
        // allocating directly in the planner arena `mcx`, then intern each `Expr`
        // into `root`'s node arena and return the `NodeId` handles in indkey order.
        let exprs = relcache_nodexform_seams::index_expressions::call(
            mcx, indexoid,
        )?;
        let exprs = match exprs {
            Some(v) => v,
            None => return Ok(Vec::new()),
        };
        let mut out = Vec::with_capacity(exprs.len());
        for e in exprs {
            out.push(root.alloc_node(e));
        }
        Ok(out)
    })
}

/// `RelationGetIndexPredicate(indexRelation)` (relcache.c) as fresh arena node
/// handles (empty if the index is not partial). For a partial index the decoded
/// implicit-AND predicate list (from the node-tree owner seam) is interned into
/// `root`'s node arena. `mcx` is the planner-run arena context (see
/// `get_index_expressions`).
fn get_index_predicate(
    mcx: mcx::Mcx<'_>,
    root: &mut pathnodes::PlannerInfo,
    indexoid: Oid,
) -> PgResult<Vec<pathnodes::NodeId>> {
    with_index_open(indexoid, || {
        // `RelationGetIndexPredicate(index)` quick-exits to NIL unless the index is
        // partial (the C `heap_attisnull(rd_indextuple, Anum_pg_index_indpred)`
        // short-circuit). Read the presence via the syscache owner's test (the same
        // `rd_index_has_indpred` uses) and skip the node-tree builder when absent.
        let has_pred = syscache_seams::pg_index_has_predicate::call(indexoid)?
            .unwrap_or(false);
        if !has_pred {
            return Ok(Vec::new());
        }
        // Decode the raw `pg_index.indpred` tree (`stringToNode` +
        // `eval_const_expressions` + `canonicalize_qual` + `make_ands_implicit` +
        // `fix_opfuncids`) via the node-tree owner seam, allocating in the planner
        // arena `mcx`, then intern each implicit-AND conjunct into `root`'s arena.
        let pred = relcache_nodexform_seams::index_predicate::call(
            mcx, indexoid,
        )?;
        let pred = match pred {
            Some(v) => v,
            None => return Ok(Vec::new()),
        };
        let mut out = Vec::with_capacity(pred.len());
        for e in pred {
            out.push(root.alloc_node(e));
        }
        Ok(out)
    })
}

/// `RelationGetNumberOfBlocks(indexRelation)` (bufmgr.c) for an index — the main
/// fork block count via smgr, off the entry's locator/backend (the same read as
/// the table `relation_get_number_of_blocks` seam).
fn index_number_of_blocks(indexoid: Oid) -> PgResult<BlockNumber> {
    with_index_open(indexoid, || {
        let (locator, backend) = with_relation(indexoid, |rd| (rd.rd_locator, rd.rd_backend))?;
        smgr_seams::smgrnblocks::call(
            locator,
            backend,
            types_core::primitive::MAIN_FORKNUM,
        )
    })
}

/// `amroutine->amgettreeheight(indexRelation)` (index AM) — the index tree
/// height; only called when `IndexCatInfo::amhasgettreeheight` is true.
fn index_get_tree_height(indexoid: Oid) -> PgResult<i32> {
    with_index_open(indexoid, || {
        let amgettreeheight = with_relation(indexoid, |rd| {
            rd.rd_indam.as_ref().and_then(|am| am.amgettreeheight)
        })?;
        let amgettreeheight = amgettreeheight
            .ok_or_else(|| PgError::error("index_get_tree_height: amgettreeheight not set"))?;
        let relcx = mcx::MemoryContext::new("index_get_tree_height");
        let data =
            with_relation(indexoid, |rd| crate::build::project_relation_data(relcx.mcx(), rd))??;
        let rel = rel::Relation::open(data, None);
        amgettreeheight(relcx.mcx(), &rel)
    })
}

/// `RelationGetStatExtList(relation)` (relcache.c) — OIDs of the relation's
/// extended-statistics objects (empty for a relation with no statistics).
fn get_stat_ext_list(relid: Oid) -> PgResult<Vec<Oid>> {
    crate::derived::RelationGetStatExtList(relid)
}
