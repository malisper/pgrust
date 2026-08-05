//! Per-planning-cycle catalog memos (syscache-memo lane; extended by the
//! replanfix3 lane, which HOISTED the module here from
//! `planner::syscache_memo` so that every crate below the planner that
//! already sees `types_pathnodes::run::PlannerRun` — this crate's own
//! `amop` internals and `indxpath` were the named run-inaccessible callers
//! of the close-out — can route repeat catalog probes through the same
//! per-cycle block. `planner::syscache_memo` is now a thin re-export.
//!
//! The prior increment's exact-Ir map left operator-shape rows re-searched
//! 16x/replan, the relation ACL mask recomputed 8x, and pg_amop probed
//! 14x — all against catalog rows that cannot change observably within one
//! planning cycle (divergence note below). C pays a cheap hash probe per
//! re-search; the port's seam + projection + release ceremony is ~300-500
//! Ir per call, so repeats are pure tax. The replanfix3 exact-Ir map at the
//! same shape found the residual concentrated in the pg_amop family
//! (by-operator probes from `ops_are_compatible` internals and the index
//! clause-matching walk, plus the members-list fetch repeated per
//! inequality clause): this revision adds a members-list memo and
//! run-threaded variants of the amop accessors, and flips the reachable
//! callers.
//!
//! Pattern of record (= T1 att_stats_memo, selfuncs::get_att_stats): the
//! keyed vecs live in a single MemoBlock that is arena-allocated LAZILY on
//! the first memo consultation, and PlannerRun carries one opaque
//! `Cell<Option<NonNull<()>>>` to it. Planning cycles that never repeat a
//! catalog lookup (the instr-guard shapes) pay a single None store; the
//! eager multi-field form measured +19 instr/q on that guard. This module
//! owns the block type and the only cast pair. Values are small Copy PODs
//! stored inline, except the members lists, which are arena-leaked shared
//! refs (the T1 bundle pattern). Linear scan on the key vecs: per-cycle
//! key populations are single digits.
//!
//! COST ONLY / divergence note: the memos issue the same seam calls with
//! the same arguments and return the same values; plancache invalidation
//! policy is untouched (a relcache/syscache inval still discards the plan,
//! and a fresh planning cycle starts with an empty block). The only
//! divergence class is a concurrent mid-planning-cycle catalog change
//! (ALTER OPERATOR / GRANT / REVOKE committing while we plan) that C's
//! per-call re-searches could observe on a later call within the same
//! cycle — nondeterministic interleaving in C, estimates-only here: the
//! ACL memo feeds vardata.acl_ok (whether stats are CONSULTED, never
//! whether access is ALLOWED — ExecCheckPermissions runs unmemoized at
//! executor startup), and the shape/amop memos feed selectivity/cost
//! machinery. Exactly the T1 divergence class, argued in
//! notes/syscache-memo-lane.md.
//!
//! Kill switches (each restores the exact incumbent per-call fetch):
//!   PGRUST_PLANNER_OPSHAPE_MEMO=0 | PGRUST_PLANNER_ACLMASK_MEMO=0 |
//!   PGRUST_PLANNER_AMOP_MEMO=0 | PGRUST_PLANNER_AMOPLIST_MEMO=0

use mcx::PgVec;
use syscache_seams::{PgAmopMemberShape, PgAmopShape, PgOperatorShape};
use types_core::{InvalidOid, Oid, RegProcedure};
use types_error::PgResult;
use types_pathnodes::run::PlannerRun;

use crate::amop::{AMOP_ORDER, AMOP_SEARCH};

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

fn amoplist_memo_disabled() -> bool {
    static DISABLED: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *DISABLED.get_or_init(|| env_disabled("PGRUST_PLANNER_AMOPLIST_MEMO"))
}

struct MemoBlock<'mcx> {
    /// opno -> pg_operator shape (None = operator absent).
    op_shape: core::cell::RefCell<PgVec<'mcx, (Oid, Option<PgOperatorShape>)>>,
    /// (relid, roleid, mask, how_all) -> pg_class_aclmask result.
    aclmask: core::cell::RefCell<PgVec<'mcx, ((Oid, Oid, u64, bool), u64)>>,
    /// (opno, amop purpose, opfamily) -> pg_amop shape (None = not a member).
    amop_by_op: core::cell::RefCell<PgVec<'mcx, ((Oid, u8, Oid), Option<PgAmopShape>)>>,
    /// (opfamily, lefttype, righttype, strategy) -> member operator oid.
    amop_member: core::cell::RefCell<PgVec<'mcx, ((Oid, Oid, Oid, i16), Oid)>>,
    /// opno -> the operator's full pg_amop members list, arena-leaked and
    /// shared (`ops_are_compatible` fetches the same list once per
    /// inequality clause — same key, repeat within one cycle).
    amop_members: core::cell::RefCell<
        PgVec<'mcx, (Oid, &'mcx PgVec<'mcx, PgAmopMemberShape>)>,
    >,
}

// Arena-leaked via forget_box_in: drop glue never runs; every member is
// arena-backed or Copy, so the run's arena reset reclaims everything.
mcx::forget_safe_struct!(
    MemoBlock<'_> { op_shape, aclmask, amop_by_op, amop_member, amop_members },
);

/// The only cast pair for PlannerRun.syscache_memos.
fn memos<'mcx>(run: &PlannerRun<'mcx>) -> PgResult<&'mcx MemoBlock<'mcx>> {
    if let Some(p) = run.syscache_memos.get() {
        // SAFETY: the pointer was created below from &'mcx MemoBlock<'mcx>
        // leaked into run.mcx (this function is the only writer); the arena
        // outlives the run, and planning is single-threaded.
        return Ok(unsafe { p.cast::<MemoBlock<'mcx>>().as_ref() });
    }
    let block = &*mcx::forget_box_in(
        run.mcx,
        MemoBlock {
            op_shape: core::cell::RefCell::new(PgVec::new_in(run.mcx)),
            aclmask: core::cell::RefCell::new(PgVec::new_in(run.mcx)),
            amop_by_op: core::cell::RefCell::new(PgVec::new_in(run.mcx)),
            amop_member: core::cell::RefCell::new(PgVec::new_in(run.mcx)),
            amop_members: core::cell::RefCell::new(PgVec::new_in(run.mcx)),
        },
    )?;
    run.syscache_memos
        .set(Some(core::ptr::NonNull::from(block).cast()));
    Ok(block)
}

// ---------------------------------------------------------------------------
// Operator shape: opno -> PgOperatorShape (per-cycle)
// ---------------------------------------------------------------------------

// Memo-only (callers gate on the kill switch and take the exact incumbent
// lsyscache path when disabled — no memo read/write, no block allocation).
fn operator_shape(run: &PlannerRun<'_>, opno: Oid) -> PgResult<Option<PgOperatorShape>> {
    let block = memos(run)?;
    let hit = block
        .op_shape
        .borrow()
        .iter()
        .find_map(|(k, v)| (*k == opno).then_some(*v));
    if let Some(v) = hit {
        return Ok(v);
    }
    let fetched = syscache_seams::lookup_pg_operator_shape::call(opno)?;
    block.op_shape.borrow_mut().push((opno, fetched));
    Ok(fetched)
}

/// lsyscache::get_oprrest through the per-cycle shape memo.
pub fn get_oprrest(run: &PlannerRun<'_>, opno: Oid) -> PgResult<RegProcedure> {
    if opshape_memo_disabled() {
        return crate::get_oprrest(opno);
    }
    Ok(match operator_shape(run, opno)? {
        Some(shape) => shape.oprrest,
        None => InvalidOid,
    })
}

/// lsyscache::get_opcode through the per-cycle shape memo.
pub fn get_opcode(run: &PlannerRun<'_>, opno: Oid) -> PgResult<RegProcedure> {
    if opshape_memo_disabled() {
        return crate::get_opcode(opno);
    }
    Ok(match operator_shape(run, opno)? {
        Some(shape) => shape.oprcode,
        None => InvalidOid,
    })
}

/// lsyscache::get_commutator through the per-cycle shape memo.
pub fn get_commutator(run: &PlannerRun<'_>, opno: Oid) -> PgResult<Oid> {
    if opshape_memo_disabled() {
        return crate::get_commutator(opno);
    }
    Ok(match operator_shape(run, opno)? {
        Some(shape) => shape.oprcom,
        None => InvalidOid,
    })
}

/// lsyscache::op_mergejoinable through the per-cycle shape memo. The
/// record/array-eq arms are typcache-keyed (by inputtype, not opno) —
/// those delegate to the incumbent untouched.
pub fn op_mergejoinable(
    run: &PlannerRun<'_>,
    opno: Oid,
    inputtype: Oid,
) -> PgResult<bool> {
    if opshape_memo_disabled()
        || opno == crate::RECORD_EQ_OP
        || opno == crate::ARRAY_EQ_OP
    {
        return crate::op_mergejoinable(opno, inputtype);
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
pub fn class_aclmask(
    run: &PlannerRun<'_>,
    table_oid: Oid,
    roleid: Oid,
    mask: u64,
    how_all: bool,
) -> PgResult<u64> {
    if aclmask_memo_disabled() {
        return aclchk_seams::pg_class_aclmask::call(table_oid, roleid, mask, how_all);
    }
    let block = memos(run)?;
    let key = (table_oid, roleid, mask, how_all);
    let hit = block
        .aclmask
        .borrow()
        .iter()
        .find_map(|(k, v)| (*k == key).then_some(*v));
    if let Some(v) = hit {
        return Ok(v);
    }
    let fetched = aclchk_seams::pg_class_aclmask::call(table_oid, roleid, mask, how_all)?;
    block.aclmask.borrow_mut().push((key, fetched));
    Ok(fetched)
}

// ---------------------------------------------------------------------------
// pg_amop: by-operator shape and by-strategy member (per-cycle)
// ---------------------------------------------------------------------------

// Memo-only; callers gate on the kill switch (exact incumbent when off).
fn amop_by_operator(
    run: &PlannerRun<'_>,
    opno: Oid,
    purpose: u8,
    opfamily: Oid,
) -> PgResult<Option<PgAmopShape>> {
    let block = memos(run)?;
    let key = (opno, purpose, opfamily);
    let hit = block
        .amop_by_op
        .borrow()
        .iter()
        .find_map(|(k, v)| (*k == key).then_some(*v));
    if let Some(v) = hit {
        return Ok(v);
    }
    let fetched = syscache_seams::lookup_pg_amop_by_operator::call(opno, purpose, opfamily)?;
    block.amop_by_op.borrow_mut().push((key, fetched));
    Ok(fetched)
}

/// lsyscache::get_op_opfamily_strategy through the per-cycle amop memo.
pub fn get_op_opfamily_strategy(
    run: &PlannerRun<'_>,
    opno: Oid,
    opfamily: Oid,
) -> PgResult<i32> {
    if amop_memo_disabled() {
        return crate::get_op_opfamily_strategy(opno, opfamily);
    }
    Ok(match amop_by_operator(run, opno, AMOP_SEARCH, opfamily)? {
        Some(amop) => amop.amopstrategy as i32,
        None => 0,
    })
}

/// lsyscache::get_opfamily_member through the per-cycle memo.
pub fn get_opfamily_member(
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
    let block = memos(run)?;
    let key = (opfamily, lefttype, righttype, strategy);
    let hit = block
        .amop_member
        .borrow()
        .iter()
        .find_map(|(k, v)| (*k == key).then_some(*v));
    if let Some(v) = hit {
        return Ok(v);
    }
    let fetched =
        syscache_seams::lookup_pg_amop_by_strategy::call(opfamily, lefttype, righttype, strategy)?;
    block.amop_member.borrow_mut().push((key, fetched));
    Ok(fetched)
}

/// lsyscache::op_in_opfamily through the per-cycle amop memo (replanfix3:
/// the index clause-matching walk probes the same one or two operators
/// against the same opfamily several times per cycle).
pub fn op_in_opfamily(run: &PlannerRun<'_>, opno: Oid, opfamily: Oid) -> PgResult<bool> {
    if amop_memo_disabled() {
        return crate::op_in_opfamily(opno, opfamily);
    }
    Ok(amop_by_operator(run, opno, AMOP_SEARCH, opfamily)?.is_some())
}

/// lsyscache::get_op_opfamily_properties through the per-cycle amop memo.
/// Same not-a-member error surface as the incumbent.
pub fn get_op_opfamily_properties(
    run: &PlannerRun<'_>,
    opno: Oid,
    opfamily: Oid,
    ordering_op: bool,
) -> PgResult<(i32, Oid, Oid)> {
    if amop_memo_disabled() {
        return crate::get_op_opfamily_properties(opno, opfamily, ordering_op);
    }
    let purpose = if ordering_op { AMOP_ORDER } else { AMOP_SEARCH };
    let amop = amop_by_operator(run, opno, purpose, opfamily)?.ok_or_else(|| {
        crate::cache_lookup_error(format!(
            "operator {opno} is not a member of opfamily {opfamily}"
        ))
    })?;
    Ok((amop.amopstrategy as i32, amop.amoplefttype, amop.amoprighttype))
}

/// lsyscache::get_op_opfamily_sortfamily through the per-cycle amop memo.
pub fn get_op_opfamily_sortfamily(
    run: &PlannerRun<'_>,
    opno: Oid,
    opfamily: Oid,
) -> PgResult<Oid> {
    if amop_memo_disabled() {
        return crate::get_op_opfamily_sortfamily(opno, opfamily);
    }
    Ok(match amop_by_operator(run, opno, AMOP_ORDER, opfamily)? {
        Some(amop) => amop.amopsortfamily,
        None => InvalidOid,
    })
}

// ---------------------------------------------------------------------------
// pg_amop members list: opno -> full member row list (per-cycle)
// ---------------------------------------------------------------------------

// Memo-only; callers gate on the kill switch. The list is fetched into the
// RUN arena (not the incumbent's scratch) and leaked — every consumer of
// one cycle shares the same rows.
fn amop_members<'mcx>(
    run: &PlannerRun<'mcx>,
    opno: Oid,
) -> PgResult<&'mcx PgVec<'mcx, PgAmopMemberShape>> {
    let block = memos(run)?;
    let hit = block
        .amop_members
        .borrow()
        .iter()
        .find_map(|(k, v)| (*k == opno).then_some(*v));
    if let Some(v) = hit {
        return Ok(v);
    }
    let fetched = syscache_seams::lookup_pg_amop_members_by_operator::call(run.mcx, opno)?;
    let leaked: &'mcx PgVec<'mcx, PgAmopMemberShape> =
        &*mcx::forget_box_in(run.mcx, fetched)?;
    block.amop_members.borrow_mut().push((opno, leaked));
    Ok(leaked)
}

fn ops_are_compatible_run(
    run: &PlannerRun<'_>,
    opno1: Oid,
    opno2: Oid,
    check_ordering: bool,
) -> PgResult<bool> {
    if opno1 == opno2 {
        return Ok(true);
    }
    // Same walk as the incumbent lsyscache::amop::ops_are_compatible, with
    // the members fetch and the per-member membership probe memoized.
    let members = amop_members(run, opno1)?;
    for op_form in members.iter() {
        if op_in_opfamily(run, opno2, op_form.amopfamily)? {
            let (consistent_eq, consistent_ord) =
                crate::amop::index_am_consistent_flags(op_form.amopmethod);
            if if check_ordering { consistent_ord } else { consistent_eq } {
                return Ok(true);
            }
        }
    }
    Ok(false)
}

/// lsyscache::comparison_ops_are_compatible through the per-cycle memos
/// (the histogram-applicability probe re-runs with the same staop for
/// every inequality clause of one cycle).
pub fn comparison_ops_are_compatible(
    run: &PlannerRun<'_>,
    opno1: Oid,
    opno2: Oid,
) -> PgResult<bool> {
    if amoplist_memo_disabled() {
        return crate::comparison_ops_are_compatible(opno1, opno2);
    }
    ops_are_compatible_run(run, opno1, opno2, true)
}

/// lsyscache::equality_ops_are_compatible through the per-cycle memos.
pub fn equality_ops_are_compatible(
    run: &PlannerRun<'_>,
    opno1: Oid,
    opno2: Oid,
) -> PgResult<bool> {
    if amoplist_memo_disabled() {
        return crate::equality_ops_are_compatible(opno1, opno2);
    }
    ops_are_compatible_run(run, opno1, opno2, false)
}
