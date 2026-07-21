//! plancat.c slice: get_relation_info for plain heap relations with btree
//! indexes, estimate_rel_size, has_unique_index, restriction_selectivity.

use std::cell::{Cell, RefCell};

use mcx::{vec_from_elem_in, PgVec};
use types_core::{BlockNumber, Oid, BTREE_AM_OID};
use types_error::PgResult;
use types_pathnodes::{IndexOptInfo, NodeId, RelId};
use types_rel::{NoLock, Relation, RELKIND_RELATION};
use types_tuple::htup::FirstLowInvalidHeapAttributeNumber;
use types_tuple::tupdesc::{ATTNULLABLE_UNKNOWN, ATTNULLABLE_VALID};

use crate::relnode::{relids_singleton, relids_union};
use crate::run::PlannerRun;

const INDOPTION_DESC: i16 = 1 << 0;
const INDOPTION_NULLS_FIRST: i16 = 1 << 1;
const RELKIND_MATVIEW: u8 = b'm';
const RELKIND_TOASTVALUE: u8 = b't';
const RELKIND_SEQUENCE: u8 = b'S';
pub(crate) const AMFLAG_HAS_TID_RANGE: u32 = types_pathnodes::AMFLAG_HAS_TID_RANGE;

fn relkind_has_table_am(relkind: u8) -> bool {
    matches!(relkind, RELKIND_RELATION | RELKIND_MATVIEW | RELKIND_TOASTVALUE)
}

pub fn get_relation_info<'mcx>(
    run: &mut PlannerRun<'mcx>,
    relation_object_id: Oid,
    inhparent: bool,
    rel: RelId,
) -> PgResult<()> {
    let mcx = run.mcx;
    let varno = run.root.rel(rel).relid;

    let relation = table::table_open(mcx, relation_object_id, NoLock)?;
    let relkind = relation.rd_rel.relkind;
    if !(relkind_has_table_am(relkind)
        || relkind == RELKIND_SEQUENCE
        || relkind == types_rel::RELKIND_FOREIGN_TABLE
        || relkind == types_rel::RELKIND_PARTITIONED_TABLE)
    {
        panic!("get_relation_info (plancat.c): relkind {relkind}; M2 foreign lane");
    }
    // C's !RelationIsPermanent && RecoveryInProgress guard: no hot-standby
    // sessions exist, so the recovery arm is compile-time false.


    let natts = relation.rd_att.natts;
    {
        let r = run.root.rel_mut(rel);
        r.min_attr = (FirstLowInvalidHeapAttributeNumber + 1) as i16;
        r.max_attr = natts as i16;
        r.reltablespace = relation.rd_rel.reltablespace;
        debug_assert!(r.max_attr >= r.min_attr);
        let span = (r.max_attr - r.min_attr + 1) as usize;
        r.attr_needed = PgVec::new_in(mcx);
        for _ in 0..span {
            r.attr_needed.push(crate::relnode::relids_empty());
        }
        r.attr_widths = vec_from_elem_in(mcx, 0i32, span);
    }

    // C leaves notnullattnums unpopulated for traditional inheritance parents.
    if !inhparent || relkind == types_rel::RELKIND_PARTITIONED_TABLE {
        for i in 0..natts as usize {
            let attr = relation.rd_att.compact_attr(i);
            debug_assert!(attr.attnullability != ATTNULLABLE_UNKNOWN);
            if attr.attnullability == ATTNULLABLE_VALID {
                debug_assert!(!attr.attisdropped);
                let nn = relids_singleton(mcx, (i + 1) as u32);
                let cur = crate::relnode::relids_take(&mut run.root.rel_mut(rel).notnullattnums);
                run.root.rel_mut(rel).notnullattnums = relids_union(mcx, &cur, &nn);
            }
        }
    }

    // An inheritance parent's size is the appendrel's, computed in
    // set_append_rel_size; pages/tuples stay zero here.
    if !inhparent {
        let min_attr = run.root.rel(rel).min_attr;
        let empty = PgVec::new_in(mcx);
        let mut widths = core::mem::replace(&mut run.root.rel_mut(rel).attr_widths, empty);
        let (pages, tuples, allvisfrac) =
            estimate_rel_size(&relation, Some(&mut widths), min_attr)?;
        let r = run.root.rel_mut(rel);
        r.attr_widths = widths;
        r.pages = pages;
        r.tuples = tuples;
        r.allvisfrac = allvisfrac;
    }

    run.root.rel_mut(rel).rel_parallel_workers = relation.get_parallel_workers(-1);

    // A partitioned parent keeps its (partitioned) indexes in indexlist for
    // uniqueness proofs; a traditional inheritance parent keeps none.
    let hasindex = if inhparent && relkind != types_rel::RELKIND_PARTITIONED_TABLE {
        false
    } else {
        relation.rd_rel.relhasindex
    };
    let mut indexinfos: PgVec<'mcx, &'mcx IndexOptInfo<'mcx>> = PgVec::new_in(mcx);
    if hasindex {
        let indexoidlist =
            relcache_seams::relation_get_index_list::call(mcx, relation_object_id)?;
        let lmode = run.rte(varno as usize).rellockmode;

        for &indexoid in indexoidlist.iter() {
            let index_rel = indexam::index_open(mcx, indexoid, lmode)?;
            let ind = index_rel
                .rd_index
                .as_ref()
                .expect("index relation carries rd_index");

            if !ind.indisvalid {
                indexam::index_close(index_rel, NoLock)?;
                continue;
            }
            // indcheckxmin gate: M2 concurrent-build lane (Form lacks it).

            let is_partitioned_index =
                index_rel.rd_rel.relkind == types_rel::RELKIND_PARTITIONED_INDEX;
            assert!(
                index_rel.rd_rel.relkind == types_rel::RELKIND_INDEX || is_partitioned_index,
                "get_relation_info (plancat.c): unexpected index relkind"
            );
            // C reads amroutine fields off rd_indam; from_relam covers
            // non-builtin relam values over builtin handlers (still loud for
            // genuinely unknown AMs).
            let relam = index_rel.rd_rel.relam;
            let am_kind = types_relscan::IndexAmKind::from_relam(relam);
            let am_is_btree = am_kind == types_relscan::IndexAmKind::Btree;
            let am_is_gin = am_kind == types_relscan::IndexAmKind::Gin;
            let am_is_gist = am_kind == types_relscan::IndexAmKind::Gist;
            let am_is_brin = am_kind == types_relscan::IndexAmKind::Brin;
            let am_is_spgist = am_kind == types_relscan::IndexAmKind::Spgist;
            let am_is_hnsw = am_kind == types_relscan::IndexAmKind::Hnsw;
            let am_is_bloom = am_kind == types_relscan::IndexAmKind::Bloom;
            let ncolumns = ind.indnatts as i32;
            let nkeycolumns = ind.indnkeyatts as i32;
            let mut info = IndexOptInfo::new(mcx);
            info.indexoid = ind.indexrelid;
            info.reltablespace = index_rel.rd_rel.reltablespace;
            info.rel = Some(rel);
            info.ncolumns = ncolumns;
            info.nkeycolumns = nkeycolumns;
            // canreturn spans all columns (plancat.c index_can_return loop);
            // opfamily/opcintype are key-column-only.
            for i in 0..ncolumns as usize {
                info.indexkeys.push(ind.indkey[i] as i32);
                info.indexcollations.push(
                    index_rel.rd_indcollation.get(i).copied().unwrap_or(0),
                );
                info.canreturn.push(match am_kind {
                    types_relscan::IndexAmKind::Btree => btcanreturn(),
                    types_relscan::IndexAmKind::Gist => {
                        gist::gistcanreturn(&index_rel, i as i32 + 1)
                    }
                    types_relscan::IndexAmKind::Spgist => {
                        spgist::spgcanreturn(&index_rel, i as i32 + 1)?
                    }
                    _ => false,
                });
            }
            for i in 0..nkeycolumns as usize {
                info.opfamily.push(index_rel.rd_opfamily[i]);
                info.opcintype.push(index_rel.rd_opcintype[i]);
            }
            info.relam = relam;
            // Per-AM IndexAmRoutine flags (bt/hash/gin/gist/brin handlers);
            // a partitioned index has no AM (C NULLifies these fields).
            if !is_partitioned_index {
                info.amcanorderbyop = am_is_gist || am_is_spgist || am_is_hnsw;
                info.amoptionalkey = am_is_btree
                    || am_is_gin
                    || am_is_gist
                    || am_is_spgist
                    || am_is_brin
                    || am_is_hnsw
                    || am_is_bloom;
                info.amsearcharray = am_is_btree;
                info.amsearchnulls = am_is_btree || am_is_gist || am_is_spgist || am_is_brin;
                info.amcanparallel = am_is_btree;
                info.amhasgettuple = !am_is_gin && !am_is_brin && !am_is_bloom;
                info.amhasgetbitmap = !am_is_hnsw;
                info.amcanmarkpos = am_is_btree;

                // amcanorder arm: a non-ordering AM leaves the sort vectors
                // empty (C's NULL sortopfamily).
                if am_is_btree {
                    for i in 0..nkeycolumns as usize {
                        let opt = index_rel.rd_indoption[i];
                        info.sortopfamily.push(info.opfamily[i]);
                        info.reverse_sort.push(opt & INDOPTION_DESC != 0);
                        info.nulls_first.push(opt & INDOPTION_NULLS_FIRST != 0);
                    }
                }
            }

            // RelationGetIndexExpressions/Predicate + ChangeVarNodes(1, varno):
            // parsed from the Form's nodeToString sources (pg_index.rs note).
            if let Some(src) = ind.indexprs_src.as_ref() {
                let node = readfuncs::stringToNode(mcx, src.as_str())?;
                let list = node.as_list().expect("indexprs is a List");
                for e in list.iter() {
                    let e = clauses::eval_const_expressions(mcx, e)?;
                    if varno != 1 {
                        change_var_nodes(e, varno as i32)?;
                    }
                    info.indexprs.push(run.intern_expr(e));
                }
            }
            if let Some(src) = ind.indpred_src.as_ref() {
                let node = readfuncs::stringToNode(mcx, src.as_str())?;
                let folded = clauses::eval_const_expressions(mcx, node)?;
                let canon = crate::prepqual::canonicalize_qual(mcx, folded, false)?;
                let implicit = clauses::make_ands_implicit(mcx, Some(canon))?;
                for e in implicit.iter() {
                    if varno != 1 {
                        change_var_nodes(e, varno as i32)?;
                    }
                    info.indpred.push(run.intern_expr(e));
                }
            }

            // build_index_tlist (plancat.c); system attrs are unreachable in
            // an index key.
            let mut indexpr_next = 0usize;
            for i in 0..ncolumns as usize {
                let indexkey = info.indexkeys[i];
                let expr = if indexkey != 0 {
                    assert!(indexkey > 0, "build_index_tlist: system-attribute index key");
                    let att = relation.rd_att.attrs[indexkey as usize - 1];
                    types_nodes::Node::mk_var(
                        mcx,
                        varno as i32,
                        indexkey as i16,
                        att.atttypid,
                        att.atttypmod,
                        att.attcollation,
                        0,
                    )?
                } else {
                    let id = *info
                        .indexprs
                        .get(indexpr_next)
                        .expect("wrong number of index expressions");
                    indexpr_next += 1;
                    *run.root.expr_node(id)
                };
                let tle =
                    types_nodes::Node::mk_target_entry(mcx, expr, (i + 1) as i16, None, false)?;
                info.indextlist.push(run.intern_expr(tle));
            }
            assert!(
                indexpr_next == info.indexprs.len(),
                "wrong number of index expressions"
            );

            info.indrestrictinfo = RefCell::new(PgVec::new_in(mcx));
            info.predOK = Cell::new(false);
            info.unique = ind.indisunique;
            info.nullsnotdistinct = ind.indnullsnotdistinct;
            info.immediate = ind.indimmediate;
            info.hypothetical = false;

            if is_partitioned_index {
                info.pages = 0;
                info.tuples = 0.0;
                info.tree_height = Cell::new(-1);
            } else {
                if info.indpred.is_empty() {
                    info.pages = bufmgr_seams::relation_get_number_of_blocks_in_fork::call(
                        &index_rel,
                        types_core::ForkNumber::MAIN_FORKNUM,
                    )?;
                    info.tuples = run.root.rel(rel).tuples;
                } else {
                    let (pages, tuples, _) = estimate_rel_size(&index_rel, None, 1)?;
                    info.pages = pages;
                    info.tuples = tuples.min(run.root.rel(rel).tuples);
                }
                info.tree_height = Cell::new(if am_is_btree {
                    nbtree::bt_getrootheight(&index_rel)?
                } else {
                    -1
                });
            }
            if am_is_gin && !is_partitioned_index {
                let gs = gin::ginGetStats(&index_rel)?;
                info.gin_stats = Some(types_pathnodes::GinIndexStats {
                    pending_pages: gs.nPendingPages,
                    total_pages: gs.nTotalPages,
                    entry_pages: gs.nEntryPages,
                    data_pages: gs.nDataPages,
                    entries: gs.nEntries,
                    version: gs.ginVersion,
                });
            }

            indexam::index_close(index_rel, NoLock)?;
            indexinfos.insert(0, &*mcx::forget_box_in(mcx, info)?);
        }
    }
    run.root.rel_mut(rel).indexlist = indexinfos;

    crate::extended_stats::get_relation_statistics(run, rel, relation.rd_id)?;

    if relkind == types_rel::RELKIND_FOREIGN_TABLE {
        // C: restrict_nonsystem_relation_kind guard (no built-in foreign
        // tables exist, so C's FirstNormalObjectId Assert is vacuous).
        if guc_tables::backing::restrict_nonsystem_relation_kind()
            & guc_tables::consts::RESTRICT_RELKIND_FOREIGN_TABLE
            != 0
        {
            return Err(Box::new(
                types_error::PgError::error(
                    "access to non-system foreign table is restricted".to_string(),
                )
                .with_sqlstate(types_error::ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
            ));
        }
        let serverid =
            foreigncmds_seams::get_foreign_server_id_by_rel_id::call(relation_object_id)?;
        let routine = foreigncmds_seams::get_fdw_routine_by_rel_id::call(mcx, relation_object_id)?;
        let r = run.root.rel_mut(rel);
        r.serverid = serverid;
        r.fdwroutine = Some(routine);
    } else {
        let is_pgrcolumnar = tableam_vocab::is_pgrcolumnar_am_oid(relation.rd_rel.relam);
        let sorted_attnos = if is_pgrcolumnar && ::costsize::gucs::pgrcolumnar_scan_pathkeys() {
            pgrcolumnar_sorted_pathkey_attnos(run, &relation)?
        } else {
            PgVec::new_in(run.mcx)
        };
        // Per-column on-disk bytes for column-fraction seqscan disk costing
        // (costsize::pgrcolumnar_scan_col_fraction); footer-less parts leave it
        // empty (fraction 1.0 = C behavior).
        let col_bytes = if is_pgrcolumnar && ::costsize::gucs::pgrcolumnar_colfrac_cost() {
            match ::tableam::pgrcolumnar_footer_col_bytes(&relation)? {
                Some(v) => {
                    let mut pv: PgVec<'_, u64> = PgVec::new_in(run.mcx);
                    for b in v {
                        pv.push(b);
                    }
                    pv
                }
                None => PgVec::new_in(run.mcx),
            }
        } else {
            PgVec::new_in(run.mcx)
        };
        // Ingest-time per-column NDV for no-pg_statistic group-key
        // estimation (selfuncs::add_unique_group_var); footer-less parts
        // leave it empty (ratio fallback = prior behavior).
        let col_ndv = if is_pgrcolumnar && ::costsize::gucs::pgrcolumnar_footer_ndv_est() {
            match ::tableam::pgrcolumnar_footer_ndv(&relation)? {
                Some(v) => {
                    let mut pv: PgVec<'_, u64> = PgVec::new_in(run.mcx);
                    for b in v {
                        pv.push(b);
                    }
                    pv
                }
                None => PgVec::new_in(run.mcx),
            }
        } else {
            PgVec::new_in(run.mcx)
        };
        // SE-TOPNNI text sort-key answerability: v7 per-column stitch dict
        // sizes (0 = no stitch) for the m5_suppress DictCode top-N key
        // probe. Knob-gated (default ON since the GL-TOPNNI-1 flip; the
        // kill spelling stands the walk down with the probe — knob
        // coherence). Served from the session part cache like its
        // siblings (one cached-Part vec clone per pgrcolumnar plancat).
        let stitch_gndv = if is_pgrcolumnar && crate::m5_suppress::topn_nonint_enabled() {
            match ::tableam::pgrcolumnar_footer_stitch_gndv(&relation)? {
                Some(v) => {
                    let mut pv: PgVec<'_, u64> = PgVec::new_in(run.mcx);
                    for b in v {
                        pv.push(b);
                    }
                    pv
                }
                None => PgVec::new_in(run.mcx),
            }
        } else {
            PgVec::new_in(run.mcx)
        };
        // Meta zero-count answerability (q2box lane): every committed RG
        // carries v7 zero/empty counts. Consumed by m5_suppress's
        // CbPlainAggFold keying — a qualed count-only shape may only be
        // suppressed to serial when the footer META answer can actually
        // serve it (v<=6-lineage banks measured 5x serial-instead-of-
        // Gather otherwise). Served from the session part cache (one flag
        // walk per cached Part lookup; the footer serves above already
        // pay the cache probe).
        let zerocnt_all = is_pgrcolumnar
            && ::tableam::pgrcolumnar_footer_zerocnt_all(&relation)?.unwrap_or(false);
        let r = run.root.rel_mut(rel);
        r.serverid = 0;
        r.fdwroutine = None;
        if is_pgrcolumnar {
            // pgrcolumnar refuses TID/TID-range and bitmap scans; the flag also
            // routes Gather costing to pgrcolumnar_parallel_setup_cost.
            r.amflags |= types_pathnodes::AMFLAG_PGRCOLUMNAR;
            if zerocnt_all {
                r.amflags |= types_pathnodes::AMFLAG_PGRCOLUMNAR_ZEROCNT;
            }
            r.pgrcolumnar_sorted_attnos = sorted_attnos;
            r.pgrcolumnar_col_bytes = col_bytes;
            r.pgrcolumnar_col_ndv = col_ndv;
            r.pgrcolumnar_stitch_gndv = stitch_gndv;
        } else {
            // Heap AM always provides scan_bitmap/scan_tid_range.
            r.amflags |= AMFLAG_HAS_TID_RANGE;
        }
    }

    get_relation_foreign_keys(run, rel, &relation, inhparent)?;

    if inhparent && relkind == types_rel::RELKIND_PARTITIONED_TABLE {
        set_relation_partition_info(run, rel, &relation)?;
    }

    relation.close(NoLock)?;
    Ok(())
}

// get_relation_foreign_keys (plancat.c): ForeignKeyOptInfos for FKs that
// reference some other RTE of the query, appended to root.fkey_list.
fn get_relation_foreign_keys<'mcx>(
    run: &mut PlannerRun<'mcx>,
    rel: RelId,
    relation: &Relation<'mcx>,
    inhparent: bool,
) -> PgResult<()> {
    let mcx = run.mcx;
    if run.root.rel(rel).reloptkind != types_pathnodes::RELOPT_BASEREL
        || run.parse().rtable.len() < 2
    {
        return Ok(());
    }
    // An inheritance parent's FKs would only help if every member had
    // equivalent constraints; C doesn't attempt that deduction either.
    if inhparent {
        return Ok(());
    }
    let cachedfkeys = relcache_seams::relation_get_fkey_list::call(relation.rd_id)?;
    let rel_relid = run.root.rel(rel).relid;
    for cachedfk in cachedfkeys.iter() {
        debug_assert!(cachedfk.conrelid == relation.rd_id);
        if !cachedfk.conenforced {
            continue;
        }
        for (idx, rte_node) in run.parse().rtable.iter().enumerate() {
            let rti = idx as u32 + 1;
            let rte = rte_node.as_range_tbl_entry().expect("rtable cell is a RangeTblEntry");
            if rte.rtekind != types_nodes::parsenodes::RTEKind::RTE_RELATION
                || rte.relid != cachedfk.confrelid
            {
                continue;
            }
            // An inheritance parent doesn't really match, nor does a
            // self-referential FK (joins only).
            if rte.inh || rti == rel_relid {
                continue;
            }
            let nkeys = cachedfk.nkeys as usize;
            let mut info = types_pathnodes::ForeignKeyOptInfo::new(mcx);
            info.con_relid = rel_relid;
            info.ref_relid = rti;
            info.nkeys = cachedfk.nkeys;
            info.conkey.extend_from_slice(&cachedfk.conkey[..nkeys]);
            info.confkey.extend_from_slice(&cachedfk.confkey[..nkeys]);
            info.conpfeqop.extend_from_slice(&cachedfk.conpfeqop[..nkeys]);
            for _ in 0..nkeys {
                info.eclass.push(None);
                info.fk_eclass_member.push(None);
                info.rinfos.push(PgVec::new_in(mcx));
            }
            let id = run.root.alloc_foreign_key(info);
            run.root.fkey_list.push(id);
        }
    }
    Ok(())
}

// set_relation_partition_info (plancat.c); the PartitionDirectory is subsumed
// by partdesc's relid-keyed cache (no concurrent-detach snapshot isolation).
fn set_relation_partition_info<'mcx>(
    run: &mut PlannerRun<'mcx>,
    rel: RelId,
    relation: &Relation<'mcx>,
) -> PgResult<()> {
    let partdesc = partdesc::RelationGetPartitionDesc(relation, true)?;
    let key = partcache::RelationGetPartitionKey(relation)?;
    let scheme = find_partition_scheme(run, &key)?;
    let bcopy = match partdesc.boundinfo.as_ref() {
        Some(bi) => Some(mcx::alloc_in(
            run.mcx,
            copy_boundinfo_for_planner(run.mcx, bi, &key, partdesc.nparts as i32)?,
        )?),
        None => None,
    };
    {
        let r = run.root.rel_mut(rel);
        r.part_scheme = Some(scheme);
        r.boundinfo = bcopy;
        r.nparts = partdesc.nparts as i32;
    }
    set_baserel_partition_key_exprs(run, rel, &key)?;
    set_baserel_partition_constraint(run, rel, relation)?;
    Ok(())
}

// find_partition_scheme (plancat.c). C shares one palloc'd scheme by pointer;
// here each rel owns an equal-by-value copy and root->part_schemes keeps the
// canonical set (PartitionSchemeData::PartialEq compares supfuncs by fn_oid).
fn find_partition_scheme<'mcx>(
    run: &mut PlannerRun<'mcx>,
    key: &partcache::PartitionKeyData,
) -> PgResult<mcx::PgBox<'mcx, types_pathnodes::PartitionSchemeData<'mcx>>> {
    let mcx = run.mcx;
    let build = |mcx: mcx::Mcx<'mcx>| -> PgResult<types_pathnodes::PartitionSchemeData<'mcx>> {
        let n = key.partnatts as usize;
        let mut ps = types_pathnodes::PartitionSchemeData::new(mcx);
        ps.strategy = key.strategy;
        ps.partnatts = key.partnatts;
        ps.partopfamily.reserve(n);
        ps.partopcintype.reserve(n);
        ps.partcollation.reserve(n);
        ps.parttyplen.reserve(n);
        ps.parttypbyval.reserve(n);
        ps.partsupfunc.reserve(n);
        for i in 0..n {
            ps.partopfamily.push(key.partopfamily[i]);
            ps.partopcintype.push(key.partopcintype[i]);
            ps.partcollation.push(key.partcollation[i]);
            ps.parttyplen.push(key.parttyplen[i]);
            ps.parttypbyval.push(key.parttypbyval[i]);
            // fn_oid-only record: the scheme's supfuncs are compared by oid
            // (PartialEq) and pruning resolves callables per step.
            let mut f = types_core::fmgr::FmgrInfo::default();
            f.fn_oid = key.partsupfunc[i].borrow().fn_oid;
            ps.partsupfunc.push(f);
        }
        Ok(ps)
    };
    let fresh = build(mcx)?;
    let found = run
        .root
        .part_schemes
        .iter()
        .any(|ps| ps.as_ref().is_some_and(|ps| **ps == fresh));
    if !found {
        run.root.part_schemes.push(Some(mcx::alloc_in(mcx, build(mcx)?)?));
    }
    mcx::alloc_in(mcx, fresh)
}

// partition_bounds_copy (partbounds.c) into the planner's DatumImage form;
// hash rows are two byval int4 datums regardless of the key types.
fn copy_boundinfo_for_planner<'mcx>(
    mcx: mcx::Mcx<'mcx>,
    bi: &partbounds::PartitionBoundInfoData<'_>,
    key: &partcache::PartitionKeyData,
    nparts: i32,
) -> PgResult<types_pathnodes::PartitionBoundInfoData<'mcx>> {
    use types_pathnodes::DatumImage;
    let hash = bi.strategy as u8 == b'h';
    let mut out = types_pathnodes::PartitionBoundInfoData::new(mcx);
    out.strategy = bi.strategy;
    out.ndatums = bi.ndatums as i32;
    out.nindexes = bi.indexes.len() as i32;
    out.null_index = bi.null_index;
    out.default_index = bi.default_index;
    out.indexes.reserve(bi.indexes.len());
    for &ix in bi.indexes.iter() {
        out.indexes.push(ix);
    }
    let width = bi.width;
    let has_kind = !bi.kind.is_empty();
    let mut kinds: PgVec<'mcx, PgVec<'mcx, i8>> = PgVec::new_in(mcx);
    out.datums.reserve(bi.ndatums);
    for i in 0..bi.ndatums {
        let mut row: PgVec<'mcx, DatumImage<'mcx>> = PgVec::new_in(mcx);
        row.reserve(width);
        let mut krow: PgVec<'mcx, i8> = PgVec::new_in(mcx);
        for j in 0..width {
            let kind = if has_kind { bi.kind_at(i, j) } else { partbounds::KIND_VALUE };
            if has_kind {
                krow.push(kind);
            }
            if kind != partbounds::KIND_VALUE {
                row.push(DatumImage::ByVal(0));
                continue;
            }
            let (byval, typlen) =
                if hash { (true, 4i16) } else { (key.parttypbyval[j], key.parttyplen[j]) };
            let d = bi.datum(i, j);
            if byval {
                row.push(DatumImage::ByVal(d.as_u64()));
            } else {
                let p = d.as_usize() as *const u8;
                // SAFETY: byref bound datums are live inline images owned by
                // the partdesc cache; length from typlen or varlena header.
                let len = unsafe {
                    match typlen {
                        l if l > 0 => l as usize,
                        -1 => {
                            let b0 = *p;
                            if b0 & 0x01 != 0 {
                                (b0 as usize >> 1) & 0x7F
                            } else {
                                (u32::from_ne_bytes(
                                    core::slice::from_raw_parts(p, 4).try_into().unwrap(),
                                ) as usize)
                                    >> 2
                            }
                        }
                        // datumGetSize (datum.c): cstring is its NUL
                        // terminator-inclusive strlen.
                        -2 => {
                            let mut l = 0usize;
                            while *p.add(l) != 0 {
                                l += 1;
                            }
                            l + 1
                        }
                        other => panic!("copy_boundinfo_for_planner: typlen {other} invalid"),
                    }
                };
                let mut buf: PgVec<'mcx, u8> = mcx::vec_with_capacity_in(mcx, len)?;
                // SAFETY: len derived from the datum's own image.
                buf.extend_from_slice(unsafe { core::slice::from_raw_parts(p, len) });
                row.push(DatumImage::Bytes(buf));
            }
        }
        out.datums.push(row);
        if has_kind {
            kinds.push(krow);
        }
    }
    out.kind = if has_kind { Some(kinds) } else { None };
    // Interleaved LIST partitions (create_list_bounds, partbounds.c): C
    // computes this at bounds-build time; the partdesc cache's boundinfo
    // predates the field, so it is derived here on the planner copy.
    if bi.strategy as u8 == b'l' && nparts > 1 {
        let accepts_nulls = i32::from(bi.null_index != -1);
        let has_default = i32::from(bi.default_index != -1);
        if out.ndatums + accepts_nulls + has_default != nparts {
            let mut last_index = -1;
            for i in 0..out.indexes.len() {
                let index = out.indexes[i];
                if index < last_index
                    || (bi.null_index != -1 && index == bi.null_index)
                {
                    types_pathnodes::relids::relids_add_member_mut(
                        mcx,
                        &mut out.interleaved_parts,
                        index as u32,
                    );
                }
                last_index = index;
            }
        }
        if bi.default_index != -1 {
            types_pathnodes::relids::relids_add_member_mut(
                mcx,
                &mut out.interleaved_parts,
                bi.default_index as u32,
            );
        }
    }
    Ok(out)
}

// set_baserel_partition_key_exprs (plancat.c).
fn set_baserel_partition_key_exprs<'mcx>(
    run: &mut PlannerRun<'mcx>,
    rel: RelId,
    key: &partcache::PartitionKeyData,
) -> PgResult<()> {
    let mcx = run.mcx;
    let varno = run.root.rel(rel).relid;
    let n = key.partnatts as usize;
    let mut ids: PgVec<'mcx, NodeId> = mcx::vec_with_capacity_in(mcx, n)?;
    let mut partexprs_item = key.partexprs.iter();
    for i in 0..n {
        let attno = key.partattrs[i];
        let partexpr = if attno != 0 {
            assert!(attno > 0);
            let mut v = types_nodes::Node::build::<types_nodes::primnodes::Var>(mcx)?;
            v.varno = varno as i32;
            v.varattno = attno;
            v.vartype = key.parttypid[i];
            v.vartypmod = key.parttypmod[i];
            v.varcollid = key.parttypcoll[i];
            v.varnosyn = varno;
            v.varattnosyn = attno;
            v.location = -1;
            v.seal()
        } else {
            let expr = partexprs_item
                .next()
                .unwrap_or_else(|| panic!("wrong number of partition key expressions"));
            // copyObject: the cache's tree is shared; ChangeVarNodes below
            // scribbles varno in place on the copy.
            let copied = rewrite_manip::copy_node(mcx, expr)?;
            rewrite_manip::ChangeVarNodes(mcx, copied, 1, varno as i32, 0)?;
            copied
        };
        ids.push(run.intern_expr(partexpr));
    }
    let mut partexprs: PgVec<'mcx, PgVec<'mcx, NodeId>> = PgVec::new_in(mcx);
    let mut nullable: PgVec<'mcx, PgVec<'mcx, NodeId>> = PgVec::new_in(mcx);
    partexprs.reserve(n);
    nullable.reserve(n);
    for &id in ids.iter() {
        let mut col: PgVec<'mcx, NodeId> = PgVec::new_in(mcx);
        col.reserve(1);
        col.push(id);
        partexprs.push(col);
        nullable.push(PgVec::new_in(mcx));
    }
    let r = run.root.rel_mut(rel);
    r.partexprs = partexprs;
    r.nullable_partexprs = nullable;
    Ok(())
}

// set_baserel_partition_constraint (plancat.c); canonicalize_qual skipped as
// in C (partition quals are already canonical).
fn set_baserel_partition_constraint<'mcx>(
    run: &mut PlannerRun<'mcx>,
    rel: RelId,
    relation: &Relation<'mcx>,
) -> PgResult<()> {
    if !run.root.rel(rel).partition_qual.is_empty() {
        return Ok(());
    }
    let mcx = run.mcx;
    let varno = run.root.rel(rel).relid;
    let partconstr = partdesc::RelationGetPartitionQual(mcx, relation)?;
    if partconstr.is_nil() {
        return Ok(());
    }
    let mut folded_ids: PgVec<'mcx, NodeId> = PgVec::new_in(mcx);
    for q in partconstr.iter() {
        let folded = clauses::eval_const_expressions(mcx, q)?;
        if varno != 1 {
            change_var_nodes(folded, varno as i32)?;
        }
        folded_ids.push(run.intern_expr(folded));
    }
    run.root.rel_mut(rel).partition_qual = folded_ids;
    Ok(())
}

fn btcanreturn() -> bool {
    true
}

// index_can_return (indexam.c) for the closed AM set; amutils' generic
// Returnable fallback rides the indexam_seams slot installed here.
pub fn index_can_return(mcx: mcx::Mcx<'_>, index_oid: Oid, attno: i32) -> PgResult<bool> {
    let rel = indexam::index_open(mcx, index_oid, types_rel::AccessShareLock)?;
    let res = match types_relscan::IndexAmKind::from_relam(rel.rd_rel.relam) {
        types_relscan::IndexAmKind::Btree => btcanreturn(),
        types_relscan::IndexAmKind::Gist => gist::gistcanreturn(&rel, attno),
        types_relscan::IndexAmKind::Spgist => spgist::spgcanreturn(&rel, attno)?,
        _ => false,
    };
    indexam::index_close(rel, types_rel::AccessShareLock)?;
    Ok(res)
}

// ChangeVarNodes (rewriteManip.c), rt_index 1 arm over freshly parsed index
// expression trees (exclusively owned, so in-place mutation is safe). The
// generic engine covers the full expression vocabulary; only Vars mutate.
pub(crate) fn change_var_nodes(node: types_nodes::Node<'_>, new_varno: i32) -> PgResult<()> {
    struct W {
        new_varno: i32,
    }
    impl<'mcx> nodes_core::NodeWalker<'mcx> for W {
        fn visit(&mut self, node: types_nodes::Node<'mcx>) -> PgResult<bool> {
            if node.node_tag() == types_nodes::NodeTag::T_Var {
                // SAFETY: tree is freshly parsed and exclusively owned here.
                unsafe {
                    node.with_mut::<types_nodes::primnodes::Var, _>(|v| {
                        if v.varno == 1 && v.varlevelsup == 0 {
                            v.varno = self.new_varno;
                        }
                    })
                }
                .expect("Var");
                return Ok(false);
            }
            nodes_core::expression_tree_walker(node, self)
        }
    }
    let mut w = W { new_varno };
    use nodes_core::NodeWalker as _;
    w.visit(node)?;
    Ok(())
}

const HEAP_OVERHEAD_BYTES_PER_TUPLE: usize = 24 + 4;
const HEAP_USABLE_BYTES_PER_PAGE: usize = 8192 - 24;

// estimate_rel_size (plancat.c), table-AM arm -> (pages, tuples, allvisfrac).
pub fn estimate_rel_size(
    rel: &Relation<'_>,
    attr_widths: Option<&mut [i32]>,
    min_attr: i16,
) -> PgResult<(BlockNumber, f64, f64)> {
    let relkind = rel.rd_rel.relkind;
    if !relkind_has_table_am(relkind) {
        if relkind == types_rel::RELKIND_INDEX {
            let reported_pages = bufmgr_seams::relation_get_number_of_blocks_in_fork::call(
                rel,
                types_core::ForkNumber::MAIN_FORKNUM,
            )?;
            if reported_pages == 0 {
                return Ok((0, 0.0, 0.0));
            }
            let mut curpages = reported_pages;
            let mut relpages = rel.rd_rel.relpages as BlockNumber;
            let reltuples = rel.rd_rel.reltuples as f64;
            let relallvisible = rel.rd_rel.relallvisible as BlockNumber;
            // Discount the metapage (OK for btree/hash/GIN, suspect for GiST).
            if relpages > 0 {
                curpages -= 1;
                relpages -= 1;
            }
            let density = if reltuples >= 0.0 && relpages > 0 {
                reltuples / relpages as f64
            } else {
                let tuple_width = get_rel_data_width(rel, None, 1)? as usize
                    + HEAP_OVERHEAD_BYTES_PER_TUPLE;
                (HEAP_USABLE_BYTES_PER_PAGE / tuple_width) as f64
            };
            let tuples = (density * curpages as f64).round_ties_even();
            let allvisfrac = if relallvisible == 0 || curpages == 0 {
                0.0
            } else if relallvisible as f64 >= curpages as f64 {
                1.0
            } else {
                relallvisible as f64 / curpages as f64
            };
            return Ok((reported_pages, tuples, allvisfrac));
        }
        if relkind == RELKIND_SEQUENCE
            || relkind == types_rel::RELKIND_FOREIGN_TABLE
            || relkind == types_rel::RELKIND_PARTITIONED_TABLE
        {
            // C foreign-table + final else arms: just use whatever's in
            // pg_class (partitioned tables are storageless; reached with
            // ONLY / zero partitions).
            return Ok((rel.rd_rel.relpages as BlockNumber, rel.rd_rel.reltuples as f64, 0.0));
        }
        panic!("estimate_rel_size (plancat.c): relkind {relkind}; M2 lane");
    }
    let mut pages: BlockNumber = 0;
    let mut tuples = 0.0f64;
    let mut allvisfrac = 0.0f64;
    tableam::table_relation_estimate_size(
        rel,
        HEAP_OVERHEAD_BYTES_PER_TUPLE,
        HEAP_USABLE_BYTES_PER_PAGE,
        |aw| get_rel_data_width(rel, aw, min_attr),
        attr_widths,
        &mut pages,
        &mut tuples,
        &mut allvisfrac,
    )?;
    Ok((pages, tuples, allvisfrac))
}

// get_rel_data_width (plancat.c); attr_widths[attno - min_attr] is the cache.
pub fn get_rel_data_width(
    rel: &Relation<'_>,
    mut attr_widths: Option<&mut [i32]>,
    min_attr: i16,
) -> PgResult<i32> {
    let mut tuple_width: i64 = 0;
    for i in 1..=rel.rd_att.natts {
        let att = rel.rd_att.attr((i - 1) as usize);
        if att.attisdropped {
            continue;
        }
        let ndx = (i - min_attr as i32) as usize;
        if let Some(aw) = attr_widths.as_deref() {
            if aw[ndx] > 0 {
                tuple_width += aw[ndx] as i64;
                continue;
            }
        }
        let mut item_width = lsyscache::get_attavgwidth(rel.rd_id, i as i16)?;
        if item_width <= 0 {
            item_width = lsyscache::get_typavgwidth(att.atttypid, att.atttypmod)?;
            debug_assert!(item_width > 0);
        }
        if let Some(aw) = attr_widths.as_deref_mut() {
            aw[ndx] = item_width;
        }
        tuple_width += item_width as i64;
    }
    Ok(crate::costsize::clamp_width_est(tuple_width))
}

// has_unique_index (plancat.c).
pub fn has_unique_index(run: &PlannerRun<'_>, rel: RelId, attno: i16) -> bool {
    for index in run.root.rel(rel).indexlist.iter() {
        if index.unique
            && index.nkeycolumns == 1
            && index.indexkeys[0] == attno as i32
            && (index.indpred.is_empty() || index.predOK.get())
        {
            return true;
        }
    }
    false
}

// Proname of a dynamic-oid (extension) estimator proc; None for builtins.
#[cold]
fn dynamic_estimator_name(procid: Oid) -> PgResult<Option<String>> {
    const FIRST_NORMAL_OBJECT_ID: Oid = 16384;
    if procid < FIRST_NORMAL_OBJECT_ID {
        return Ok(None);
    }
    let cx = ::mcx::MemoryContext::new("plancat estimator probe");
    let name = lsyscache::get_func_name(cx.mcx(), procid)?.map(|n| n.as_str().to_string());
    Ok(name)
}

// restriction_selectivity (plancat.c): closed-set oprrest dispatch.
pub fn restriction_selectivity<'mcx>(
    run: &mut PlannerRun<'mcx>,
    operatorid: Oid,
    args: &[NodeId],
    inputcollid: Oid,
    varrelid: i32,
) -> PgResult<f64> {
    const F_EQSEL: Oid = 101;
    let oprrest = crate::syscache_memo::get_oprrest(run, operatorid)?;
    if oprrest == 0 {
        return Ok(0.5);
    }
    const F_NEQSEL: Oid = 102;
    const F_SCALARLTSEL: Oid = 103;
    const F_SCALARGTSEL: Oid = 104;
    const F_SCALARLESEL: Oid = 336;
    const F_SCALARGESEL: Oid = 337;
    const F_ICLIKESEL: Oid = 1814;
    const F_ICNLIKESEL: Oid = 1815;
    const F_REGEXEQSEL: Oid = 1818;
    const F_LIKESEL: Oid = 1819;
    const F_ICREGEXEQSEL: Oid = 1820;
    const F_REGEXNESEL: Oid = 1821;
    const F_NLIKESEL: Oid = 1822;
    const F_ICREGEXNESEL: Oid = 1823;
    const F_PREFIXSEL: Oid = 3437;
    use crate::like_support::PatternType;
    const F_MATCHINGSEL: Oid = 5040;
    // geo_selfuncs.c constants
    const F_AREASEL: Oid = 139;
    const F_POSITIONSEL: Oid = 1300;
    const F_CONTSEL: Oid = 1302;
    let result = match oprrest {
        F_AREASEL => 0.005,
        F_POSITIONSEL => 0.1,
        F_CONTSEL => 0.001,
        F_EQSEL => crate::selfuncs::eqsel(run, operatorid, args, varrelid, inputcollid)?,
        F_MATCHINGSEL => {
            crate::selfuncs::matchingsel(run, operatorid, args, varrelid, inputcollid)?
        }
        F_NEQSEL => crate::selfuncs::neqsel(run, operatorid, args, varrelid, inputcollid)?,
        F_SCALARLTSEL | F_SCALARGTSEL | F_SCALARLESEL | F_SCALARGESEL => {
            let isgt = oprrest == F_SCALARGTSEL || oprrest == F_SCALARGESEL;
            let iseq = oprrest == F_SCALARLESEL || oprrest == F_SCALARGESEL;
            crate::selfuncs::scalarineqsel_wrapper(
                run, operatorid, args, varrelid, inputcollid, isgt, iseq,
            )?
        }
        F_REGEXEQSEL | F_ICREGEXEQSEL | F_LIKESEL | F_ICLIKESEL | F_PREFIXSEL | F_REGEXNESEL
        | F_ICREGEXNESEL | F_NLIKESEL | F_ICNLIKESEL => {
            let (ptype, negate) = match oprrest {
                F_REGEXEQSEL => (PatternType::Regex, false),
                F_ICREGEXEQSEL => (PatternType::RegexIc, false),
                F_LIKESEL => (PatternType::Like, false),
                F_ICLIKESEL => (PatternType::LikeIc, false),
                F_PREFIXSEL => (PatternType::Prefix, false),
                F_REGEXNESEL => (PatternType::Regex, true),
                F_ICREGEXNESEL => (PatternType::RegexIc, true),
                F_NLIKESEL => (PatternType::Like, true),
                _ => (PatternType::LikeIc, true),
            };
            crate::like_support::patternsel(
                run, operatorid, args, varrelid, inputcollid, ptype, negate,
            )?
        }
        3169 => crate::rangetypes_selfuncs::rangesel(run, operatorid, args, varrelid)?,
        4243 => crate::multirangetypes_selfuncs::multirangesel(run, operatorid, args, varrelid)?,
        3560 => crate::network_selfuncs::networksel(run, operatorid, args, varrelid)?,
        3686 => crate::ts_selfuncs::tsmatchsel(run, args, varrelid)?,
        3817 => crate::array_selfuncs::arraycontsel(run, operatorid, args, varrelid)?,
        // Extension estimators carry dynamic oids; match by proname. The
        // intarray _sel wrappers substitute the built-in operator OID and
        // call arraycontsel, exactly as their C bodies do.
        other => match dynamic_estimator_name(other)?.as_deref() {
            Some("_int_overlap_sel") => {
                crate::array_selfuncs::arraycontsel(run, 2750, args, varrelid)?
            }
            Some("_int_contains_sel") => {
                crate::array_selfuncs::arraycontsel(run, 2751, args, varrelid)?
            }
            Some("_int_contained_sel") => {
                crate::array_selfuncs::arraycontsel(run, 2752, args, varrelid)?
            }
            Some("_int_matchsel") => {
                crate::intarray_selfuncs::int_matchsel(run, args, varrelid, other)?
            }
            _ => panic!(
                "restriction_selectivity (plancat.c): oprrest {other}; M2 selfuncs lane"
            ),
        },
    };
    if !(0.0..=1.0).contains(&result) {
        panic!("invalid restriction selectivity: {result}");
    }
    Ok(result)
}

// join_selectivity (plancat.c): closed-set oprjoin dispatch. The scalar
// inequality estimators return DEFAULT_INEQ_SEL with no arg inspection.
pub fn join_selectivity<'mcx>(
    run: &mut PlannerRun<'mcx>,
    operatorid: Oid,
    args: &[NodeId],
    inputcollid: Oid,
    jointype: types_pathnodes::JoinType,
    sjinfo: Option<&types_pathnodes::SpecialJoinInfo<'mcx>>,
) -> PgResult<f64> {
    const F_EQJOINSEL: Oid = 105;
    const F_SCALARLTJOINSEL: Oid = 107;
    const F_SCALARGTJOINSEL: Oid = 108;
    const F_SCALARLEJOINSEL: Oid = 386;
    const F_SCALARGEJOINSEL: Oid = 398;
    const F_AREAJOINSEL: Oid = 140;
    const F_POSITIONJOINSEL: Oid = 1301;
    const F_CONTJOINSEL: Oid = 1303;
    const DEFAULT_INEQ_SEL: f64 = 0.3333333333333333;
    let oprjoin = lsyscache::get_oprjoin(operatorid)?;
    if oprjoin == 0 {
        return Ok(0.5);
    }
    let result = match oprjoin {
        F_EQJOINSEL => {
            crate::selfuncs::eqjoinsel(run, operatorid, args, jointype, sjinfo, inputcollid)?
        }
        F_SCALARLTJOINSEL | F_SCALARGTJOINSEL | F_SCALARLEJOINSEL | F_SCALARGEJOINSEL => {
            DEFAULT_INEQ_SEL
        }
        // patternjoinsel (like_support.c) punts for all pattern types.
        1816 | 1824 | 1825 | 1826 | 3438 => crate::selfuncs::DEFAULT_MATCH_SEL,
        1817 | 1827 | 1828 | 1829 => 1.0 - crate::selfuncs::DEFAULT_MATCH_SEL,
        F_AREAJOINSEL => 0.005,
        F_POSITIONJOINSEL => 0.1,
        F_CONTJOINSEL => 0.001,
        106 => crate::selfuncs::neqjoinsel(run, operatorid, args, jointype, sjinfo, inputcollid)?,
        3561 => crate::network_selfuncs::networkjoinsel(run, operatorid, args, sjinfo)?,
        // matchingjoinsel (selfuncs.c) punts.
        5041 => crate::selfuncs::DEFAULT_MATCHING_SEL,
        // tsmatchjoinsel (ts_selfuncs.c) punts.
        3687 => crate::ts_selfuncs::DEFAULT_TS_MATCH_SEL,
        // arraycontjoinsel (array_selfuncs.c) is a C stub.
        3818 => crate::array_selfuncs::arraycontjoinsel(operatorid),
        // intarray _joinsel wrappers: built-in operator OID substituted into
        // the arraycontjoinsel stub (matches the C wrappers).
        other => match dynamic_estimator_name(other)?.as_deref() {
            Some("_int_overlap_joinsel") => crate::array_selfuncs::arraycontjoinsel(2750),
            Some("_int_contains_joinsel") => crate::array_selfuncs::arraycontjoinsel(2751),
            Some("_int_contained_joinsel") => crate::array_selfuncs::arraycontjoinsel(2752),
            _ => panic!("join_selectivity (plancat.c): oprjoin {other}; M2 selfuncs lane"),
        },
    };
    if !(0.0..=1.0).contains(&result) {
        panic!("invalid join selectivity: {result}");
    }
    Ok(result)
}

// function_selectivity (plancat.c): SupportRequestSelectivity dispatch on the
// prosupport oid. The in-core like_regex_support providers (like_support.c)
// stay native; other prosupport functions get the request through fmgr with
// the restriction/join estimator pre-bound (planner state does not cross the
// fmgr boundary in this port).
pub fn function_selectivity<'mcx>(
    run: &mut PlannerRun<'mcx>,
    funcid: Oid,
    args: &[NodeId],
    inputcollid: Oid,
    is_join: bool,
    varrelid: i32,
    jointype: types_pathnodes::JoinType,
    sjinfo: Option<&types_pathnodes::SpecialJoinInfo<'mcx>>,
) -> PgResult<f64> {
    use crate::like_support::PatternType;
    let shape = syscache_seams::pg_proc_cost_shape::call(funcid)?
        .unwrap_or_else(|| panic!("cache lookup failed for function {funcid}"));
    let ptype = match shape.prosupport {
        0 => return Ok(0.3333333),
        1023 => PatternType::Like,
        1025 => PatternType::LikeIc,
        1364 => PatternType::Regex,
        1024 => PatternType::RegexIc,
        6242 => PatternType::Prefix,
        prosupport => {
            let mut estimate = |operatorid: Oid| -> PgResult<f64> {
                if is_join {
                    join_selectivity(run, operatorid, args, inputcollid, jointype, sjinfo)
                } else {
                    restriction_selectivity(run, operatorid, args, inputcollid, varrelid)
                }
            };
            let mut req = types_nodes::supportnodes::SupportRequestSelectivity::new(
                funcid,
                is_join,
                &mut estimate,
            );
            let addr = core::ptr::from_mut(&mut req) as usize;
            let result = fmgr_core::oid_function_call1_coll(
                prosupport,
                0,
                datum::Datum::from_usize(addr),
            )?;
            if result.as_usize() == addr {
                if !(0.0..=1.0).contains(&req.selectivity) {
                    panic!("invalid function selectivity: {}", req.selectivity);
                }
                return Ok(req.selectivity);
            }
            return Ok(0.3333333);
        }
    };
    if is_join {
        return Ok(crate::selfuncs::DEFAULT_MATCH_SEL);
    }
    crate::like_support::patternsel_common(
        run, 0, funcid, args, varrelid, inputcollid, ptype, false,
    )
}

// add_function_cost (plancat.c). DIVERGENCE: callers don't thread the calling
// node, so the support request carries node=None (in-core cost-support
// functions all tolerate that and fall back to procost).
pub fn add_function_cost(funcid: Oid, cost: &mut types_pathnodes::QualCost) -> PgResult<()> {
    let shape = syscache_seams::pg_proc_cost_shape::call(funcid)?
        .unwrap_or_else(|| panic!("cache lookup failed for function {funcid}"));
    if shape.prosupport != 0 {
        let mut req = types_nodes::supportnodes::SupportRequestCost::new(funcid, None);
        let addr = core::ptr::from_mut(&mut req) as usize;
        let result =
            fmgr_core::oid_function_call1_coll(shape.prosupport, 0, datum::Datum::from_usize(addr))?;
        if result.as_usize() == addr {
            cost.startup += req.startup;
            cost.per_tuple += req.per_tuple;
            return Ok(());
        }
    }
    cost.per_tuple += shape.procost as f64 * crate::gucs::cpu_operator_cost();
    Ok(())
}

// get_function_rows (plancat.c); root is not threaded (support functions on
// this lane read only Const args).
pub fn get_function_rows(funcid: Oid, node: Option<types_nodes::Node<'_>>) -> PgResult<f64> {
    let shape = syscache_seams::pg_proc_cost_shape::call(funcid)?
        .unwrap_or_else(|| panic!("cache lookup failed for function {funcid}"));
    if shape.prosupport != 0 {
        let mut req = types_nodes::supportnodes::SupportRequestRows::new(funcid, node);
        let addr = core::ptr::from_mut(&mut req) as usize;
        let result =
            fmgr_core::oid_function_call1_coll(shape.prosupport, 0, datum::Datum::from_usize(addr))?;
        if result.as_usize() == addr {
            return Ok(req.rows);
        }
    }
    Ok(shape.prorows as f64)
}

// infer_arbiter_indexes (plancat.c).
pub fn infer_arbiter_indexes<'mcx>(
    run: &crate::run::PlannerRun<'mcx>,
    oc: &types_nodes::primnodes::OnConflictExpr<'mcx>,
) -> PgResult<types_nodes::list::OidList<'mcx>> {
    use types_nodes::equal::equal;

    let mcx = run.mcx;
    let mut results = types_nodes::list::OidList::nil();
    if oc.arbiterElems.is_nil() && oc.constraint == 0 {
        return Ok(results);
    }

    let parse = run.parse();
    let varno = parse.resultRelation;
    let rte = run.rte(varno as usize);

    let mut infer_attrs = types_nodes::Bitmapset::empty();
    let mut infer_elems: Vec<types_nodes::Node<'mcx>> = Vec::new();
    for elem_node in &oc.arbiterElems {
        let elem = elem_node.as_inference_elem().expect("arbiterElems cell");
        let expr = elem.expr.expect("InferenceElem has expr");
        match expr.as_var() {
            None => infer_elems.push(expr),
            Some(var) => {
                if var.varattno == 0 {
                    return Err(Box::new(
                        types_error::PgError::error(
                            "whole row unique index inference specifications are not supported",
                        )
                        .with_sqlstate(types_error::ERRCODE_FEATURE_NOT_SUPPORTED),
                    ));
                }
                infer_attrs
                    .add_member(mcx, var.varattno as i32 - FirstLowInvalidHeapAttributeNumber)?;
            }
        }
    }

    let index_oid_from_constraint = if oc.constraint != 0 {
        let indexoid = lsyscache::get_constraint_index(oc.constraint)?;
        if indexoid == 0 {
            return Err(Box::new(
                types_error::PgError::error(
                    "constraint in ON CONFLICT clause has no associated index",
                )
                .with_sqlstate(types_error::ERRCODE_WRONG_OBJECT_TYPE),
            ));
        }
        indexoid
    } else {
        0
    };

    let relation = table::table_open(mcx, rte.relid, NoLock)?;
    let indexoidlist = relcache_seams::relation_get_index_list::call(mcx, rte.relid)?;
    for &indexoid in indexoidlist.iter() {
        let idx_rel = indexam::index_open(mcx, indexoid, rte.rellockmode)?;
        let _matched = 'matched: {
            let ind = idx_rel.rd_index.as_ref().expect("index relation carries rd_index");
            if !ind.indisvalid {
                break 'matched false;
            }
            if index_oid_from_constraint == ind.indexrelid {
                if ind.indisexclusion
                    && oc.action == types_nodes::primnodes::OnConflictAction::ONCONFLICT_UPDATE
                {
                    return Err(Box::new(
                        types_error::PgError::error(
                            "ON CONFLICT DO UPDATE not supported with exclusion constraints",
                        )
                        .with_sqlstate(types_error::ERRCODE_WRONG_OBJECT_TYPE),
                    ));
                }
                results.lappend(mcx, ind.indexrelid)?;
                indexam::index_close(idx_rel, NoLock)?;
                table::table_close(relation, NoLock)?;
                return Ok(results);
            } else if index_oid_from_constraint != 0 {
                break 'matched false;
            }
            if !ind.indisunique || ind.indisexclusion {
                break 'matched false;
            }
            let mut indexed_attrs = types_nodes::Bitmapset::empty();
            for natt in 0..ind.indnkeyatts as usize {
                let attno = ind.indkey[natt];
                if attno != 0 {
                    indexed_attrs
                        .add_member(mcx, attno as i32 - FirstLowInvalidHeapAttributeNumber)?;
                }
            }
            if !indexed_attrs.equal(&infer_attrs) {
                break 'matched false;
            }
            // RelationGetIndexExpressions: stringToNode + eval_const_expressions
            // — necessary, not just optimization, since arbiterElems went
            // through the same folding via preprocess_expression (relcache.c).
            let mut idx_exprs: Vec<types_nodes::Node<'mcx>> = Vec::new();
            if let Some(src) = ind.indexprs_src.as_ref() {
                let node = readfuncs::stringToNode(mcx, src.as_str())?;
                for e in node.as_list().expect("indexprs is a List").iter() {
                    let e = clauses::eval_const_expressions(mcx, e)?;
                    if varno != 1 {
                        change_var_nodes(e, varno)?;
                    }
                    idx_exprs.push(e);
                }
            }
            for elem_node in &oc.arbiterElems {
                let elem = elem_node.as_inference_elem().expect("arbiterElems cell");
                if !infer_collation_opclass_match(elem, &idx_rel, &idx_exprs)? {
                    break 'matched false;
                }
                let expr = elem.expr.expect("InferenceElem has expr");
                if expr.as_var().is_some() {
                    continue;
                }
                if elem.infercollid != 0
                    || elem.inferopclass != 0
                    || idx_exprs.iter().any(|&e| equal(e, expr))
                {
                    continue;
                }
                break 'matched false;
            }
            if idx_exprs
                .iter()
                .any(|&e| !infer_elems.iter().any(|&ie| equal(e, ie)))
            {
                break 'matched false;
            }
            // RelationGetIndexPredicate shape: const-fold + canonicalize +
            // implicit-AND, as relcache does.
            let mut pred_exprs: Vec<types_nodes::Node<'mcx>> = Vec::new();
            if let Some(src) = ind.indpred_src.as_ref() {
                let node = readfuncs::stringToNode(mcx, src.as_str())?;
                let folded = clauses::eval_const_expressions(mcx, node)?;
                let canon = crate::prepqual::canonicalize_qual(mcx, folded, false)?;
                let implicit = clauses::make_ands_implicit(mcx, Some(canon))?;
                for e in implicit.iter() {
                    if varno != 1 {
                        change_var_nodes(e, varno)?;
                    }
                    pred_exprs.push(e);
                }
            }
            let mut arbiter_where: Vec<types_nodes::Node<'mcx>> = Vec::new();
            if let Some(w) = oc.arbiterWhere {
                for e in w.as_list().expect("preprocessed arbiterWhere is a List").iter() {
                    arbiter_where.push(e);
                }
            }
            if !crate::predtest::predicate_implied_by(mcx, &pred_exprs, &arbiter_where, false)? {
                break 'matched false;
            }
            results.lappend(mcx, ind.indexrelid)?;
            true
        };
        indexam::index_close(idx_rel, NoLock)?;
    }
    table::table_close(relation, NoLock)?;

    if results.is_nil() {
        return Err(Box::new(
            types_error::PgError::error(
                "there is no unique or exclusion constraint matching the ON CONFLICT specification",
            )
            .with_sqlstate(types_error::ERRCODE_INVALID_COLUMN_REFERENCE),
        ));
    }
    Ok(results)
}

// infer_collation_opclass_match (plancat.c). rd_opfamily/rd_opcintype cover
// only key columns; INCLUDE columns read as 0 where C reads past the palloc
// (they can never satisfy an opclass/collation requirement either way).
fn infer_collation_opclass_match<'mcx>(
    elem: &types_nodes::primnodes::InferenceElem<'mcx>,
    idx_rel: &Relation<'mcx>,
    idx_exprs: &[types_nodes::Node<'mcx>],
) -> PgResult<bool> {
    use types_nodes::equal::equal;

    if elem.infercollid == 0 && elem.inferopclass == 0 {
        return Ok(true);
    }
    let mut inferopfamily = 0;
    let mut inferopcinputtype = 0;
    if elem.inferopclass != 0 {
        inferopfamily = lsyscache::get_opclass_family(elem.inferopclass)?;
        inferopcinputtype = lsyscache::get_opclass_input_type(elem.inferopclass)?;
    }
    let ind = idx_rel.rd_index.as_ref().expect("index relation carries rd_index");
    let elem_expr = elem.expr.expect("InferenceElem has expr");
    let mut nplain = 0usize;
    for natt in 1..=idx_rel.rd_att.natts as usize {
        let opfamily = idx_rel.rd_opfamily.get(natt - 1).copied().unwrap_or(0);
        let opcinputtype = idx_rel.rd_opcintype.get(natt - 1).copied().unwrap_or(0);
        let collation = idx_rel.rd_indcollation.get(natt - 1).copied().unwrap_or(0);
        let attno = ind.indkey[natt - 1];
        if attno != 0 {
            nplain += 1;
        }
        if elem.inferopclass != 0
            && (inferopfamily != opfamily || inferopcinputtype != opcinputtype)
        {
            continue;
        }
        if elem.infercollid != 0 && elem.infercollid != collation {
            continue;
        }
        match elem_expr.as_var() {
            Some(var) => {
                if var.varattno == attno {
                    return Ok(true);
                }
            }
            None => {
                if attno == 0 && equal(elem_expr, idx_exprs[(natt - 1) - nplain]) {
                    return Ok(true);
                }
            }
        }
    }
    Ok(false)
}

// get_relation_constraints (plancat.c) for the constraint-exclusion refutation
// leg.
pub fn get_relation_constraints<'mcx>(
    run: &mut PlannerRun<'mcx>,
    relation_object_id: Oid,
    rel: RelId,
    include_noinherit: bool,
    include_notnull: bool,
    include_partition: bool,
) -> PgResult<PgVec<'mcx, types_nodes::Node<'mcx>>> {
    let mcx = run.mcx;
    let varno = run.root.rel(rel).relid;
    let mut result: PgVec<'mcx, types_nodes::Node<'mcx>> = PgVec::new_in(mcx);

    let relation = table::table_open(mcx, relation_object_id, NoLock)?;
    if let Some(constr) = relation.rd_att.constr.as_deref() {
        for check in constr.check.iter() {
            if !check.ccvalid {
                continue;
            }
            debug_assert!(check.ccenforced);
            if check.ccnoinherit && !include_noinherit {
                continue;
            }
            let ccbin = check.ccbin.as_ref().expect("CHECK constraint has ccbin");
            let cexpr = readfuncs::stringToNode(mcx, ccbin.as_str())?;
            let cexpr = clauses::eval_const_expressions(mcx, cexpr)?;
            let cexpr = crate::prepqual::canonicalize_qual(mcx, cexpr, true)?;
            if varno != 1 {
                change_var_nodes(cexpr, varno as i32)?;
            }
            let implicit = clauses::make_ands_implicit(mcx, Some(cexpr))?;
            for item in implicit.iter() {
                result.push(item);
            }
        }
        if include_notnull && constr.has_not_null {
            let natts = relation.rd_att.natts;
            for i in 1..=natts {
                let att = &relation.rd_att.compact_attrs[(i - 1) as usize];
                if att.attnullability == ATTNULLABLE_VALID && !att.attisdropped {
                    let wholeatt = relation.rd_att.attrs[(i - 1) as usize];
                    let var = types_nodes::Node::mk_var(
                        mcx,
                        varno as i32,
                        i as i16,
                        wholeatt.atttypid,
                        wholeatt.atttypmod,
                        wholeatt.attcollation,
                        0,
                    )?;
                    // argisrow=false is correct even for a composite column
                    // (attnotnull is IS DISTINCT FROM NULL there, not SQL-spec).
                    let ntest = types_nodes::Node::mk(
                        mcx,
                        types_nodes::primnodes::NullTest {
                            arg: Some(var),
                            nulltesttype: types_nodes::primnodes::NullTestType::IS_NOT_NULL,
                            argisrow: false,
                            location: -1,
                        },
                    )?;
                    result.push(ntest);
                }
            }
        }
        if constr.has_generated_virtual {
            for item in result.iter_mut() {
                *item = crate::prepjointree::expand_generated_columns_in_expr(
                    mcx,
                    *item,
                    &relation,
                    varno as i32,
                )?;
            }
        }
    }
    if include_partition && relation.rd_rel.relispartition {
        set_baserel_partition_constraint(run, rel, &relation)?;
        for i in 0..run.root.rel(rel).partition_qual.len() {
            let id = run.root.rel(rel).partition_qual[i];
            result.push(*run.root.expr_node(id));
        }
    }
    relation.close(NoLock)?;
    Ok(result)
}

// get_relation_data_width (plancat.c).
pub fn get_relation_data_width(
    mcx: mcx::Mcx<'_>,
    relid: Oid,
    attr_widths: Option<&mut [i32]>,
) -> PgResult<i32> {
    let relation = table::table_open(mcx, relid, NoLock)?;
    let result = get_rel_data_width(&relation, attr_widths, 0)?;
    table::table_close(relation, NoLock)?;
    Ok(result)
}

// has_row_triggers (plancat.c).
pub fn has_row_triggers(
    run: &PlannerRun<'_>,
    rti: usize,
    event: types_nodes::CmdType,
) -> PgResult<bool> {
    use types_nodes::CmdType::*;
    let rte = run.rte(rti);
    let trig_desc = relcache_seams::relation_get_trigger_desc::call(rte.relid)?;
    let Some(t) = trig_desc else { return Ok(false) };
    Ok(match event {
        CMD_INSERT => t.trig_insert_after_row || t.trig_insert_before_row,
        CMD_UPDATE => t.trig_update_after_row || t.trig_update_before_row,
        CMD_DELETE => t.trig_delete_after_row || t.trig_delete_before_row,
        CMD_MERGE => false,
        other => panic!("unrecognized CmdType: {other:?}"),
    })
}

// has_transition_tables (plancat.c).
pub fn has_transition_tables(
    run: &PlannerRun<'_>,
    rti: usize,
    event: types_nodes::CmdType,
) -> PgResult<bool> {
    use types_nodes::CmdType::*;
    let rte = run.rte(rti);
    if rte.relkind == types_rel::RELKIND_FOREIGN_TABLE {
        return Ok(false);
    }
    let trig_desc = relcache_seams::relation_get_trigger_desc::call(rte.relid)?;
    let Some(t) = trig_desc else { return Ok(false) };
    Ok(match event {
        CMD_INSERT => t.trig_insert_new_table,
        CMD_UPDATE => t.trig_update_old_table || t.trig_update_new_table,
        CMD_DELETE => t.trig_delete_old_table,
        CMD_MERGE => false,
        other => panic!("unrecognized CmdType: {other:?}"),
    })
}

// has_stored_generated_columns (plancat.c).
pub fn has_stored_generated_columns(run: &PlannerRun<'_>, rti: usize) -> PgResult<bool> {
    let rte = run.rte(rti);
    let relation = table::table_open(run.mcx, rte.relid, NoLock)?;
    let result = relation
        .rd_att
        .constr
        .as_deref()
        .is_some_and(|c| c.has_generated_stored);
    table::table_close(relation, NoLock)?;
    Ok(result)
}

// get_dependent_generated_columns (plancat.c); attnos in both bitmapsets are
// offset by FirstLowInvalidHeapAttributeNumber.
pub fn get_dependent_generated_columns<'mcx>(
    run: &PlannerRun<'mcx>,
    rti: usize,
    target_cols: &types_nodes::Bitmapset<'mcx>,
) -> PgResult<types_nodes::Bitmapset<'mcx>> {
    let mcx = run.mcx;
    let mut dependent_cols = types_nodes::Bitmapset::empty();
    let rte = run.rte(rti);
    let relation = table::table_open(mcx, rte.relid, NoLock)?;
    if let Some(constr) = relation.rd_att.constr.as_deref() {
        if constr.has_generated_stored {
            for defval in constr.defval.iter() {
                if relation.rd_att.attrs[defval.adnum as usize - 1].attgenerated == 0 {
                    continue;
                }
                let adbin = defval.adbin.as_ref().expect("generated column has adbin");
                let expr = readfuncs::stringToNode(mcx, adbin.as_str())?;
                let mut attrs_used = types_nodes::Bitmapset::empty();
                vars::pull_varattnos(mcx, expr, 1, &mut attrs_used)?;
                if target_cols.overlap(&attrs_used) {
                    dependent_cols.add_member(
                        mcx,
                        defval.adnum as i32 - FirstLowInvalidHeapAttributeNumber,
                    )?;
                }
            }
        }
    }
    table::table_close(relation, NoLock)?;
    Ok(dependent_cols)
}

// pgrcolumnar v5 footer sorted columns admissible as ascending scan pathkeys.
// int/date/timestamp default btree order IS the footer tracker's signed
// order; text requires a memcmp-ordered collation (collate_is_c); bpchar's
// space-padded comparison never matches byte order and pgrcolumnar admits no
// other types.
fn pgrcolumnar_sorted_pathkey_attnos<'mcx>(
    run: &PlannerRun<'mcx>,
    relation: &Relation<'_>,
) -> PgResult<PgVec<'mcx, i16>> {
    let mut out: PgVec<'mcx, i16> = PgVec::new_in(run.mcx);
    let Some(sorted) = ::tableam::pgrcolumnar_footer_sorted(relation)? else {
        return Ok(out);
    };
    use types_core::catalog::{DATEOID, INT2OID, INT4OID, INT8OID, TEXTOID, TIMESTAMPOID,
        VARCHAROID};
    for (i, a) in relation.rd_att.attrs.iter().enumerate() {
        if !sorted.get(i).copied().unwrap_or(false) || a.attisdropped {
            continue;
        }
        let ok = match a.atttypid {
            INT2OID | INT4OID | INT8OID | DATEOID | TIMESTAMPOID => true,
            TEXTOID | VARCHAROID => collate_sorts_bytewise(a.attcollation),
            _ => false,
        };
        if ok {
            out.push(i as i16 + 1);
        }
    }
    Ok(out)
}

fn collate_sorts_bytewise(coll: Oid) -> bool {
    use types_core::catalog::{C_COLLATION_OID, DEFAULT_COLLATION_OID};
    if !types_core::OidIsValid(coll) {
        return false;
    }
    if coll == DEFAULT_COLLATION_OID && !::pg_locale::default_locale_installed() {
        return false;
    }
    if coll != C_COLLATION_OID
        && coll != DEFAULT_COLLATION_OID
        && !syscache_seams::lookup_pg_collation_locale_row::is_installed()
    {
        return false;
    }
    matches!(::pg_locale::pg_newlocale_from_collation(coll), Ok(l) if l.collate_is_c)
}
