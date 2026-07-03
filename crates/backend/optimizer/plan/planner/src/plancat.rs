//! plancat.c slice: get_relation_info for plain heap relations with btree
//! indexes, estimate_rel_size, has_unique_index, restriction_selectivity.

use std::cell::{Cell, RefCell};
use std::rc::Rc;

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
const AMFLAG_HAS_TID_RANGE: u32 = 1 << 0;

fn relkind_has_table_am(relkind: u8) -> bool {
    matches!(relkind, RELKIND_RELATION | RELKIND_MATVIEW | RELKIND_TOASTVALUE)
}

pub fn get_relation_info<'mcx>(
    run: &mut PlannerRun<'mcx>,
    relation_object_id: Oid,
    inhparent: bool,
    rel: RelId,
) -> PgResult<()> {
    assert!(!inhparent, "get_relation_info (plancat.c): inhparent; M2 partition lane");
    let mcx = run.mcx;
    let varno = run.root.rel(rel).relid;

    let relation = table::table_open(mcx, relation_object_id, NoLock)?;
    let relkind = relation.rd_rel.relkind;
    if !(relkind_has_table_am(relkind) || relkind == RELKIND_SEQUENCE) {
        panic!("get_relation_info (plancat.c): relkind {relkind}; M2 foreign/partitioned lane");
    }
    // The recovery guard needs RecoveryInProgress; permanent rels skip it.
    if relation.rd_rel.relpersistence != b'p' {
        panic!("get_relation_info (plancat.c): non-permanent relation; M2 recovery-guard lane");
    }

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
            r.attr_needed.push(None);
        }
        r.attr_widths = vec_from_elem_in(mcx, 0i32, span);
    }

    for i in 0..natts as usize {
        let attr = relation.rd_att.compact_attr(i);
        debug_assert!(attr.attnullability != ATTNULLABLE_UNKNOWN);
        if attr.attnullability == ATTNULLABLE_VALID {
            debug_assert!(!attr.attisdropped);
            let nn = relids_singleton(mcx, (i + 1) as u32);
            let cur = run.root.rel_mut(rel).notnullattnums.take();
            run.root.rel_mut(rel).notnullattnums = relids_union(mcx, &cur, &nn);
        }
    }

    {
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

    let hasindex = relation.rd_rel.relhasindex;
    let mut indexinfos: PgVec<'mcx, Rc<IndexOptInfo<'mcx>>> = PgVec::new_in(mcx);
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

            if index_rel.rd_rel.relkind != types_rel::RELKIND_INDEX {
                panic!("get_relation_info (plancat.c): partitioned index; M2 partition lane");
            }
            if index_rel.rd_rel.relam != BTREE_AM_OID {
                panic!(
                    "get_relation_info (plancat.c): index AM {}; M2 non-btree lane",
                    index_rel.rd_rel.relam
                );
            }
            if ind.has_indpred {
                panic!("get_relation_info (plancat.c): partial index; M2 partial-index lane");
            }

            let ncolumns = ind.indnatts as i32;
            let nkeycolumns = ind.indnkeyatts as i32;
            let mut info = IndexOptInfo::new(mcx);
            info.indexoid = ind.indexrelid;
            info.reltablespace = index_rel.rd_rel.reltablespace;
            info.rel = Some(rel);
            info.ncolumns = ncolumns;
            info.nkeycolumns = nkeycolumns;
            for i in 0..ncolumns as usize {
                let key = ind.indkey[i] as i32;
                if key == 0 {
                    panic!("get_relation_info (plancat.c): expression index; M2 lane");
                }
                info.indexkeys.push(key);
                info.indexcollations.push(
                    index_rel.rd_indcollation.get(i).copied().unwrap_or(0),
                );
            }
            for i in 0..nkeycolumns as usize {
                info.opfamily.push(index_rel.rd_opfamily[i]);
                info.opcintype.push(index_rel.rd_opcintype[i]);
                info.canreturn.push(btcanreturn());
            }
            info.relam = index_rel.rd_rel.relam;
            info.amcanorderbyop = false;
            info.amoptionalkey = true;
            info.amsearcharray = true;
            info.amsearchnulls = true;
            info.amcanparallel = true;
            info.amhasgettuple = true;
            info.amhasgetbitmap = true;
            info.amcanmarkpos = true;

            for i in 0..nkeycolumns as usize {
                let opt = index_rel.rd_indoption[i];
                info.sortopfamily.push(info.opfamily[i]);
                info.reverse_sort.push(opt & INDOPTION_DESC != 0);
                info.nulls_first.push(opt & INDOPTION_NULLS_FIRST != 0);
            }


            info.indrestrictinfo = RefCell::new(PgVec::new_in(mcx));
            info.predOK = Cell::new(false);
            info.unique = ind.indisunique;
            info.nullsnotdistinct = ind.indnullsnotdistinct;
            info.immediate = ind.indimmediate;
            info.hypothetical = false;

            info.pages = bufmgr_seams::relation_get_number_of_blocks_in_fork::call(
                &index_rel,
                types_core::ForkNumber::MAIN_FORKNUM,
            )?;
            info.tuples = run.root.rel(rel).tuples;
            info.tree_height = Cell::new(nbtree::bt_getrootheight(&index_rel)?);

            indexam::index_close(index_rel, NoLock)?;
            indexinfos.insert(0, Rc::new(info));
        }
    }
    run.root.rel_mut(rel).indexlist = indexinfos;

    // Divergence: RelationGetStatExtList unported; extended stats absent
    // (multi-clause selectivity panics upstream).
    debug_assert!(run.root.rel(rel).statlist.is_empty());

    {
        let r = run.root.rel_mut(rel);
        r.serverid = 0;
        r.has_fdwroutine = false;
        // Heap AM always provides scan_bitmap/scan_tid_range.
        r.amflags |= AMFLAG_HAS_TID_RANGE;
    }

    // Divergence: get_relation_foreign_keys is skipped (RelationGetFKeyList
    // unported), so fkey_list stays empty and join size estimation uses
    // fkselec = 1.0 even where C would match FK constraints. Estimate-only:
    // affects plan choice, never results. The plancat FK unit owns the fix.
    debug_assert!(run.root.fkey_list.is_empty());

    relation.close(NoLock)?;
    Ok(())
}
fn btcanreturn() -> bool {
    true
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

// restriction_selectivity (plancat.c): closed-set oprrest dispatch.
pub fn restriction_selectivity<'mcx>(
    run: &mut PlannerRun<'mcx>,
    operatorid: Oid,
    args: &[NodeId],
    inputcollid: Oid,
    varrelid: i32,
) -> PgResult<f64> {
    const F_EQSEL: Oid = 101;
    let oprrest = lsyscache::get_oprrest(operatorid)?;
    if oprrest == 0 {
        return Ok(0.5);
    }
    let result = match oprrest {
        F_EQSEL => crate::selfuncs::eqsel(run, operatorid, args, varrelid, inputcollid)?,
        other => panic!(
            "restriction_selectivity (plancat.c): oprrest {other}; M2 selfuncs lane"
        ),
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
    const DEFAULT_INEQ_SEL: f64 = 0.3333333333333333;
    let _ = inputcollid;
    let oprjoin = lsyscache::get_oprjoin(operatorid)?;
    if oprjoin == 0 {
        return Ok(0.5);
    }
    let result = match oprjoin {
        F_EQJOINSEL => crate::selfuncs::eqjoinsel(run, operatorid, args, jointype, sjinfo)?,
        F_SCALARLTJOINSEL | F_SCALARGTJOINSEL | F_SCALARLEJOINSEL | F_SCALARGEJOINSEL => {
            DEFAULT_INEQ_SEL
        }
        other => panic!("join_selectivity (plancat.c): oprjoin {other}; M2 selfuncs lane"),
    };
    if !(0.0..=1.0).contains(&result) {
        panic!("invalid join selectivity: {result}");
    }
    Ok(result)
}

// add_function_cost (plancat.c), no-prosupport arm.
pub fn add_function_cost(funcid: Oid, cost: &mut types_pathnodes::QualCost) -> PgResult<()> {
    let shape = syscache_seams::pg_proc_cost_shape::call(funcid)?
        .unwrap_or_else(|| panic!("cache lookup failed for function {funcid}"));
    if shape.prosupport != 0 {
        panic!("add_function_cost (plancat.c): SupportRequestCost for {funcid}; M2 lane");
    }
    cost.per_tuple += shape.procost as f64 * crate::gucs::cpu_operator_cost();
    Ok(())
}
