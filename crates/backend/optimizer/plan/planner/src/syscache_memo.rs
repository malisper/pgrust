//! Per-planning-cycle syscache memos (syscache-memo lane).
//!
//! The replanfix2 close-out's exact-Ir map of one custom-plan replan left a
//! 15.7% residual in the syscache/catcache machinery: operator-shape rows
//! re-searched 16x/replan (clausesel::merge_clause alone re-fetches the
//! same one or two operators 10x), the relation ACL mask recomputed 8x, and
//! pg_amop probed 14x — all against catalog rows that cannot change
//! observably within one planning cycle (see the divergence note below).
//! C pays a cheap hash probe per re-search; the port's seam + projection +
//! release ceremony is ~300-500 Ir per call, so repeats are pure tax.
//!
//! Pattern of record (= T1 att_stats_memo, selfuncs::get_att_stats):
//! PlannerRun-scoped keyed vecs (plan arena, die at planner exit — bounded,
//! zero cross-query state), struct values arena-leaked via forget_box_in
//! and stored OPAQUE (NonNull) in types_pathnodes so the _support crate
//! takes no backend dep; this module owns both sides of every cast. Linear
//! scan on the key vec: per-cycle key populations are single digits.
//!
//! COST ONLY / divergence note: the memos issue the same seam calls with
//! the same arguments and return the same values; plancache invalidation
//! policy is untouched (a relcache/syscache inval still discards the plan).
//! The only divergence class is a concurrent mid-planning-cycle catalog
//! change (ALTER OPERATOR / GRANT / REVOKE committing while we plan) that
//! C's per-call re-searches could observe on a later call within the same
//! cycle — nondeterministic interleaving in C, estimates-only here: the
//! ACL memo feeds vardata.acl_ok (whether stats are CONSULTED, never
//! whether access is ALLOWED — ExecCheckPermissions runs unmemoized at
//! executor startup), and the shape/amop memos feed selectivity/cost
//! machinery. Exactly the T1 divergence class, argued in
//! notes/syscache-memo-lane.md.
//!
//! Kill switches (each restores the exact incumbent per-call fetch):
//!   PGRUST_PLANNER_OPSHAPE_MEMO=0 | PGRUST_PLANNER_ACLMASK_MEMO=0 |
//!   PGRUST_PLANNER_AMOP_MEMO=0

use syscache_seams::{PgAmopShape, PgOperatorShape};
use types_core::{InvalidOid, Oid, RegProcedure};
use types_error::PgResult;

use crate::run::PlannerRun;

fn env_disabled(name: &'static str) -> bool {
    matches!(std::env::var(name).as_deref(), Ok("0") | Ok("off"))
}

fn opshape_memo_disabled() -> bool {
    static DISABLED: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *DISABLED.get_or_init(|| env_disabled("PGRUST_PLANNER_OPSHAPE_MEMO"))
}

fn aclmask_memo_disabled() -> bool {
    static DISABLED: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *DISABLED.get_or_init(|| env_disabled("PGRUST_PLANNER_ACLMASK_MEMO"))
}

fn amop_memo_disabled() -> bool {
    static DISABLED: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *DISABLED.get_or_init(|| env_disabled("PGRUST_PLANNER_AMOP_MEMO"))
}

// ---------------------------------------------------------------------------
// Operator shape: opno -> &'mcx PgOperatorShape (per-cycle)
// ---------------------------------------------------------------------------

// Memo-only (callers gate on the kill switch and take the exact incumbent
// lsyscache path when disabled — no memo read/write, no arena leak).
fn operator_shape<'mcx>(
    run: &PlannerRun<'mcx>,
    opno: Oid,
) -> PgResult<Option<&'mcx PgOperatorShape>> {
    let hit = run
        .op_shape_memo
        .borrow()
        .iter()
        .find_map(|(k, v)| (*k == opno).then_some(*v));
    if let Some(v) = hit {
        // SAFETY: the pointer was created below from &'mcx PgOperatorShape
        // leaked into run.mcx (this function is the only writer); the arena
        // outlives the run, and planning is single-threaded.
        return Ok(v.map(|p| unsafe { p.cast::<PgOperatorShape>().as_ref() }));
    }
    let fetched = match syscache_seams::lookup_pg_operator_shape::call(opno)? {
        Some(shape) => Some(&*mcx::forget_box_in(run.mcx, shape)?),
        None => None,
    };
    run.op_shape_memo
        .borrow_mut()
        .push((opno, fetched.map(|r| core::ptr::NonNull::from(r).cast())));
    Ok(fetched)
}

/// lsyscache::get_oprrest through the per-cycle shape memo.
pub(crate) fn get_oprrest(run: &PlannerRun<'_>, opno: Oid) -> PgResult<RegProcedure> {
    if opshape_memo_disabled() {
        return lsyscache::get_oprrest(opno);
    }
    Ok(match operator_shape(run, opno)? {
        Some(shape) => shape.oprrest,
        None => InvalidOid,
    })
}

/// lsyscache::get_opcode through the per-cycle shape memo.
pub(crate) fn get_opcode(run: &PlannerRun<'_>, opno: Oid) -> PgResult<RegProcedure> {
    if opshape_memo_disabled() {
        return lsyscache::get_opcode(opno);
    }
    Ok(match operator_shape(run, opno)? {
        Some(shape) => shape.oprcode,
        None => InvalidOid,
    })
}

/// lsyscache::get_commutator through the per-cycle shape memo.
pub(crate) fn get_commutator(run: &PlannerRun<'_>, opno: Oid) -> PgResult<Oid> {
    if opshape_memo_disabled() {
        return lsyscache::get_commutator(opno);
    }
    Ok(match operator_shape(run, opno)? {
        Some(shape) => shape.oprcom,
        None => InvalidOid,
    })
}

/// lsyscache::op_mergejoinable through the per-cycle shape memo. The
/// record/array-eq arms are typcache-keyed (by inputtype, not opno) —
/// those delegate to the incumbent untouched.
pub(crate) fn op_mergejoinable(
    run: &PlannerRun<'_>,
    opno: Oid,
    inputtype: Oid,
) -> PgResult<bool> {
    if opshape_memo_disabled()
        || opno == lsyscache::RECORD_EQ_OP
        || opno == lsyscache::ARRAY_EQ_OP
    {
        return lsyscache::op_mergejoinable(opno, inputtype);
    }
    Ok(match operator_shape(run, opno)? {
        Some(shape) => shape.oprcanmerge,
        None => false,
    })
}

// ---------------------------------------------------------------------------
// ACL mask: (relid, roleid, mask, how_all) -> pg_class_aclmask (per-cycle)
// ---------------------------------------------------------------------------

/// aclchk_seams::pg_class_aclmask through the per-cycle memo. Feeds
/// vardata.acl_ok (statistics gating) only; never permission enforcement.
pub(crate) fn class_aclmask(
    run: &PlannerRun<'_>,
    table_oid: Oid,
    roleid: Oid,
    mask: u64,
    how_all: bool,
) -> PgResult<u64> {
    if aclmask_memo_disabled() {
        return aclchk_seams::pg_class_aclmask::call(table_oid, roleid, mask, how_all);
    }
    let key = (table_oid, roleid, mask, how_all);
    let hit = run
        .aclmask_memo
        .borrow()
        .iter()
        .find_map(|(k, v)| (*k == key).then_some(*v));
    if let Some(v) = hit {
        return Ok(v);
    }
    let fetched = aclchk_seams::pg_class_aclmask::call(table_oid, roleid, mask, how_all)?;
    run.aclmask_memo.borrow_mut().push((key, fetched));
    Ok(fetched)
}

// ---------------------------------------------------------------------------
// pg_amop: by-operator shape and by-strategy member (per-cycle)
// ---------------------------------------------------------------------------

// Memo-only; callers gate on the kill switch (exact incumbent when off).
fn amop_by_operator<'mcx>(
    run: &PlannerRun<'mcx>,
    opno: Oid,
    purpose: u8,
    opfamily: Oid,
) -> PgResult<Option<&'mcx PgAmopShape>> {
    let key = (opno, purpose, opfamily);
    let hit = run
        .amop_by_op_memo
        .borrow()
        .iter()
        .find_map(|(k, v)| (*k == key).then_some(*v));
    if let Some(v) = hit {
        // SAFETY: created below from &'mcx PgAmopShape leaked into run.mcx
        // (this function is the only writer); arena outlives the run;
        // planning is single-threaded.
        return Ok(v.map(|p| unsafe { p.cast::<PgAmopShape>().as_ref() }));
    }
    let fetched = match syscache_seams::lookup_pg_amop_by_operator::call(opno, purpose, opfamily)? {
        Some(shape) => Some(&*mcx::forget_box_in(run.mcx, shape)?),
        None => None,
    };
    run.amop_by_op_memo
        .borrow_mut()
        .push((key, fetched.map(|r| core::ptr::NonNull::from(r).cast())));
    Ok(fetched)
}

/// lsyscache::get_op_opfamily_strategy through the per-cycle amop memo.
pub(crate) fn get_op_opfamily_strategy(
    run: &PlannerRun<'_>,
    opno: Oid,
    opfamily: Oid,
) -> PgResult<i32> {
    if amop_memo_disabled() {
        return lsyscache::get_op_opfamily_strategy(opno, opfamily);
    }
    Ok(match amop_by_operator(run, opno, lsyscache::amop::AMOP_SEARCH, opfamily)? {
        Some(amop) => amop.amopstrategy as i32,
        None => 0,
    })
}

/// lsyscache::get_opfamily_member through the per-cycle memo.
pub(crate) fn get_opfamily_member(
    run: &PlannerRun<'_>,
    opfamily: Oid,
    lefttype: Oid,
    righttype: Oid,
    strategy: i16,
) -> PgResult<Oid> {
    if amop_memo_disabled() {
        return syscache_seams::lookup_pg_amop_by_strategy::call(
            opfamily, lefttype, righttype, strategy,
        );
    }
    let key = (opfamily, lefttype, righttype, strategy);
    let hit = run
        .amop_member_memo
        .borrow()
        .iter()
        .find_map(|(k, v)| (*k == key).then_some(*v));
    if let Some(v) = hit {
        return Ok(v);
    }
    let fetched =
        syscache_seams::lookup_pg_amop_by_strategy::call(opfamily, lefttype, righttype, strategy)?;
    run.amop_member_memo.borrow_mut().push((key, fetched));
    Ok(fetched)
}
