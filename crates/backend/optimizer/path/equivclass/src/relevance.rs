//! equivclass.c — the relids/index relevance helpers, the EC member iterator,
//! `select_equality_operator`, `find_join_domain`, and the redundancy tests.
//! All over the arena + `relids_*` seams; none constructs members.

extern crate alloc;

use alloc::vec::Vec;

use pathnodes::{
    EcId, EmId, EquivalenceClass, EquivalenceMemberIterator, IndexClause, JoinDomain, PlannerInfo,
    RelId, Relids, RinfoId, RELOPT_OTHER_JOINREL, RELOPT_OTHER_MEMBER_REL, RELOPT_OTHER_UPPER_REL,
};
use types_core::primitive::Oid;

use relnode_seams as bms;
use lsyscache_seams as cat;

const INVALID_OID: Oid = 0;
/// `COMPARE_EQ` (cmptype.h) — the btree "equal" compare type.
const COMPARE_EQ: i32 = 3;
/// `BMS_MULTIPLE` (bitmapset.h).
pub(crate) const BMS_MULTIPLE: i32 = 2;

#[inline]
pub(crate) fn oid_is_valid(oid: Oid) -> bool {
    oid != INVALID_OID
}

#[inline]
pub(crate) fn is_other_rel(rel: &pathnodes::RelOptInfo) -> bool {
    matches!(
        rel.reloptkind,
        RELOPT_OTHER_MEMBER_REL | RELOPT_OTHER_JOINREL | RELOPT_OTHER_UPPER_REL
    )
}

/* ======================================================================
 * select_equality_operator (equivclass.c:1948)
 * ==================================================================== */

/// `select_equality_operator(ec, lefttype, righttype)` — find an opfamily
/// equality operator for the two given datatypes; require leakproof if the EC's
/// max security level is positive. Returns `InvalidOid` on failure.
pub fn select_equality_operator(ec: &EquivalenceClass, lefttype: Oid, righttype: Oid) -> Oid {
    for &opfamily in &ec.ec_opfamilies {
        let opno = cat::get_opfamily_member_for_cmptype::call(opfamily, lefttype, righttype, COMPARE_EQ)
            .expect("get_opfamily_member_for_cmptype");
        if !oid_is_valid(opno) {
            continue; /* unsupported input type */
        }
        /* If no security restrictions, accept the first operator we find */
        if ec.ec_max_security == 0 {
            return opno;
        }
        /* Else, check whether all operators are leakproof */
        let opcode = cat::get_opcode::call(opno).expect("get_opcode");
        if cat::get_func_leakproof::call(opcode).expect("get_func_leakproof") {
            return opno;
        }
    }
    INVALID_OID
}

/* ======================================================================
 * find_join_domain (equivclass.c:2616)
 * ==================================================================== */

/// `find_join_domain(root, relids)` — the JoinDomain whose `jd_relids` is a
/// subset of `relids` (i.e. that contains the clause). `elog(ERROR)` if none.
pub fn find_join_domain(root: &PlannerInfo, relids: &Relids) -> JoinDomain {
    for jdomain in &root.join_domains {
        if bms::relids_is_subset::call(&jdomain.jd_relids, relids) {
            return jdomain.clone();
        }
    }
    panic!("failed to find appropriate JoinDomain");
}

/* ======================================================================
 * EquivalenceMemberIterator (equivclass.c:3156, 3175)
 * ==================================================================== */

/// `setup_eclass_member_iterator(it, ec, child_relids)` (equivclass.c:3156).
pub fn setup_eclass_member_iterator(
    it: &mut EquivalenceMemberIterator,
    root: &PlannerInfo,
    ec: EcId,
    child_relids: &Relids,
) {
    let ecref = root.ec(ec);
    it.ec = Some(ec);
    /* no need to set this if the class has no child members array set */
    it.child_relids = if !ecref.ec_childmembers.is_empty() {
        bms::relids_copy::call(child_relids)
    } else {
        None
    };
    it.current_relid = -1;
    it.current_list = ecref.ec_members.clone();
    it.current_cell = if it.current_list.is_empty() {
        None
    } else {
        Some(0)
    };
}

/// `eclass_member_iterator_next(it)` (equivclass.c:3175).
pub fn eclass_member_iterator_next(
    root: &PlannerInfo,
    it: &mut EquivalenceMemberIterator,
) -> Option<EmId> {
    loop {
        /* return from the current list if it has a pending cell */
        if let Some(cell) = it.current_cell {
            let em = it.current_list[cell];
            let next = cell + 1;
            it.current_cell = if next < it.current_list.len() {
                Some(next)
            } else {
                None
            };
            return Some(em);
        }

        /* Search for the next list to return members from */
        let ec = it.ec.expect("iterator not set up");
        let ecref = root.ec(ec);
        loop {
            it.current_relid = bms::relids_next_member::call(&it.child_relids, it.current_relid);
            if it.current_relid <= 0 {
                return None;
            }
            /* be paranoid about relids above the sized ec_childmembers array */
            if it.current_relid >= ecref.ec_childmembers_size {
                return None;
            }
            let list = &ecref.ec_childmembers[it.current_relid as usize];
            if !list.is_empty() {
                it.current_list = list.clone();
                it.current_cell = Some(0);
                break;
            }
        }
    }
}

/// Build a fresh iterator over `ec`'s members for `child_relids`.
pub(crate) fn new_iterator(
    root: &PlannerInfo,
    ec: EcId,
    child_relids: &Relids,
) -> EquivalenceMemberIterator {
    let mut it = EquivalenceMemberIterator::default();
    setup_eclass_member_iterator(&mut it, root, ec, child_relids);
    it
}

/* ======================================================================
 * eclass index helpers (equivclass.c:3612, 3646)
 * ==================================================================== */

/// `get_eclass_indexes_for_relids(root, relids)` (equivclass.c:3612).
pub fn get_eclass_indexes_for_relids(root: &PlannerInfo, relids: &Relids) -> Relids {
    debug_assert!(root.ec_merging_done);

    let mut ec_indexes: Relids = None;
    let mut i: i32 = -1;
    loop {
        i = bms::relids_next_member::call(relids, i);
        if i <= 0 {
            break;
        }
        /* ignore the RTE_GROUP RTE */
        if i == root.group_rtindex {
            continue;
        }
        match root.simple_rel_array[i as usize] {
            None => {
                /* must be an outer join */
                debug_assert!(bms::relids_is_member::call(i, &root.outer_join_rels));
                continue;
            }
            Some(rel_id) => {
                let rel_eci = root.rel(rel_id).eclass_indexes.clone();
                ec_indexes = bms::relids_add_members::call(ec_indexes, &rel_eci);
            }
        }
    }
    ec_indexes
}

/// `get_common_eclass_indexes(root, relids1, relids2)` (equivclass.c:3646).
pub fn get_common_eclass_indexes(root: &PlannerInfo, relids1: &Relids, relids2: &Relids) -> Relids {
    let rel1ecs = get_eclass_indexes_for_relids(root, relids1);

    /* singleton fast path */
    let rel2ecs: Relids = match bms::relids_get_singleton_member::call(relids2) {
        Some(relid) => {
            let rel_id = root.simple_rel_array[relid as usize]
                .expect("singleton relid must have a simple_rel_array entry");
            root.rel(rel_id).eclass_indexes.clone()
        }
        None => get_eclass_indexes_for_relids(root, relids2),
    };

    bms::relids_int_members::call(rel1ecs, &rel2ecs)
}

/* ======================================================================
 * relevance tests (equivclass.c:3369, 3445, 3489)
 * ==================================================================== */

/// `have_relevant_eclass_joinclause(root, rel1, rel2)` (equivclass.c:3370).
pub fn have_relevant_eclass_joinclause(root: &PlannerInfo, rel1: RelId, rel2: RelId) -> bool {
    let rel1_relids = root.rel(rel1).relids.clone();
    let rel2_relids = root.rel(rel2).relids.clone();
    let matching_ecs = get_common_eclass_indexes(root, &rel1_relids, &rel2_relids);

    let mut i: i32 = -1;
    loop {
        i = bms::relids_next_member::call(&matching_ecs, i);
        if i < 0 {
            break;
        }
        let ec = EcId(i as u32);

        debug_assert!(bms::relids_overlap::call(&rel1_relids, &root.ec(ec).ec_relids));
        debug_assert!(bms::relids_overlap::call(&rel2_relids, &root.ec(ec).ec_relids));

        /* won't generate joinclauses if single-member (covers volatile too) */
        if root.ec(ec).ec_members.len() <= 1 {
            continue;
        }
        return true;
    }
    false
}

/// `has_relevant_eclass_joinclause(root, rel1)` (equivclass.c:3446).
pub fn has_relevant_eclass_joinclause(root: &PlannerInfo, rel1: RelId) -> bool {
    let rel1_relids = root.rel(rel1).relids.clone();
    let matched_ecs = get_eclass_indexes_for_relids(root, &rel1_relids);

    let mut i: i32 = -1;
    loop {
        i = bms::relids_next_member::call(&matched_ecs, i);
        if i < 0 {
            break;
        }
        let ec = EcId(i as u32);

        if root.ec(ec).ec_members.len() <= 1 {
            continue;
        }
        if !bms::relids_is_subset::call(&root.ec(ec).ec_relids, &rel1_relids) {
            return true;
        }
    }
    false
}

/// `eclass_useful_for_merging(root, eclass, rel)` (equivclass.c:3490).
pub fn eclass_useful_for_merging(root: &PlannerInfo, eclass: EcId, rel: RelId) -> bool {
    let ec = root.ec(eclass);
    debug_assert!(ec.ec_merged.is_none());

    /* won't generate joinclauses if const or single-member (covers volatile) */
    if ec.ec_has_const || ec.ec_members.len() <= 1 {
        return false;
    }

    let relopt = root.rel(rel);
    let relids: Relids = if is_other_rel(relopt) {
        debug_assert!(!bms::relids_is_empty::call(&relopt.top_parent_relids));
        relopt.top_parent_relids.clone()
    } else {
        relopt.relids.clone()
    };

    /* if rel already includes all members of eclass, no point searching */
    if bms::relids_is_subset::call(&ec.ec_relids, &relids) {
        return false;
    }

    /* need a member not in the given rel; ignore children here */
    for &cur_em_id in &ec.ec_members {
        let cur_em = root.em(cur_em_id);
        debug_assert!(!cur_em.em_is_child);
        if !bms::relids_overlap::call(&cur_em.em_relids, &relids) {
            return true;
        }
    }
    false
}

/* ======================================================================
 * redundancy tests (equivclass.c:3549, 3576)
 * ==================================================================== */

/// `is_redundant_derived_clause(rinfo, clauselist)` (equivclass.c:3550).
pub fn is_redundant_derived_clause(
    root: &PlannerInfo,
    rinfo: RinfoId,
    clauselist: &[RinfoId],
) -> bool {
    let parent_ec = match root.rinfo(rinfo).parent_ec {
        None => return false,
        Some(ec) => ec,
    };

    for &otherrinfo in clauselist {
        if root.rinfo(otherrinfo).parent_ec == Some(parent_ec) {
            return true;
        }
    }
    false
}

/// `is_redundant_with_indexclauses(rinfo, indexclauses)` (equivclass.c:3576).
pub fn is_redundant_with_indexclauses(
    root: &PlannerInfo,
    rinfo: RinfoId,
    indexclauses: &[IndexClause],
) -> bool {
    let parent_ec = root.rinfo(rinfo).parent_ec;

    for iclause in indexclauses {
        if iclause.lossy {
            continue;
        }
        let otherrinfo = iclause.rinfo;

        /* match if same clause (pointer equality → handle equality) */
        if otherrinfo == Some(rinfo) {
            return true;
        }
        /* match if derived from same EC */
        if let (Some(pec), Some(other)) = (parent_ec, otherrinfo) {
            if root.rinfo(other).parent_ec == Some(pec) {
                return true;
            }
        }
    }
    false
}

/// Iterate every still-canonical EC index (skips `ec_merged` ECs, reproducing
/// C's post-`list_delete` iteration set), yielding `(EcId, &EquivalenceClass)`.
pub(crate) fn live_ec_ids(root: &PlannerInfo) -> Vec<EcId> {
    let mut out = Vec::new();
    for i in 0..root.eq_classes.len() {
        let id = EcId(i as u32);
        if root.ec(id).ec_merged.is_none() {
            out.push(id);
        }
    }
    out
}
