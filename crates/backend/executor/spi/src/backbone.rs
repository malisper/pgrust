//! SPI connection / nesting machinery (`spi.c`): the `_SPI_stack`,
//! `SPI_connect_ext` / `SPI_finish`, the transaction-end cleanup
//! (`AtEOXact_SPI` / `AtEOSubXact_SPI`), `SPI_inside_nonatomic_context`, and
//! the `_SPI_begin_call` / `_SPI_end_call` / `_SPI_execmem` / `_SPI_procmem`
//! helpers.
//!
//! # Faithful model
//!
//! The C `static` backend-globals (`_SPI_stack`, `_SPI_current`,
//! `_SPI_stack_depth`, `_SPI_connected`, and the public `SPI_processed` /
//! `SPI_tuptable` / `SPI_result`) are modeled as `thread_local!` per the repo's
//! backend-global convention (cf. portalmem's `PORTAL_HASH_TABLE`).
//!
//! C keeps `_SPI_current` as a raw pointer *into* the `_SPI_stack` array (which
//! `repalloc` can move — hence the file-header "re-fetch `_SPI_current` after
//! any nested SPI call" caution). We instead represent the live stack level by
//! its **index** `_SPI_connected` and index into the `Vec` whenever we need the
//! current connection; this is the same observable behaviour without the
//! move-invalidation hazard.
//!
//! Each `_SPI_connection`'s `procCxt` / `execCxt` are owned `::mcx::MemoryContext`
//! values (C `AllocSetContextCreate`); `MemoryContextDelete` is dropping them,
//! `MemoryContextReset` is `.reset()`. The C parent-context choice
//! (`TopTransactionContext` vs `PortalContext`) is recorded as `atomic` but the
//! owned-context model roots them at a fresh malloc-backed context, matching the
//! repo's portalmem treatment of `TOP_PORTAL_CONTEXT` (the cross-xact lifetime
//! distinction is preserved by `atomic` + the explicit cleanup in
//! `AtEOSubXact_SPI` / `SPI_finish`, exactly as C documents).

use crate::result_code::{
    SPI_ERROR_UNCONNECTED, SPI_OK_CONNECT, SPI_OK_FINISH, SPI_OPT_NONATOMIC,
};
use transam_xact_seams as xact_seam;
use ::utils_error::ereport;
use ::mcx::MemoryContext;
use std::cell::RefCell;
use ::types_core::{SubTransactionId, InvalidSubTransactionId};
use ::types_error::{ErrorLocation, PgResult, WARNING};

/// A tuple table created during a SPI op. The public part (`tupdesc` / `vals` /
/// `numvals`) is what `SPI_tuptable` exposes; the private part mirrors
/// `SPITupleTable`'s bookkeeping (`spi.h`).
///
/// The actual tuple payload (`vals: HeapTuple*`) and the per-table memory
/// context are produced by the `spi_dest_startup` / `spi_printtup` DestSPI
/// receiver, which is **seam-and-panic** until the dest-router keystone lands
/// (see [`crate::exec`]); the struct shape is faithful so the connection
/// machinery (subxact cleanup, freetuptable) can be grounded now.
pub struct SpiTupleTable {
    /// `subid` — the subtransaction that created this table (for AtEOSubXact
    /// cleanup).
    pub subid: SubTransactionId,
    /// `tuptabcxt` — the table's own memory context (deleted to free it).
    pub tuptabcxt: Option<MemoryContext>,
    /// `numvals` — number of rows held.
    pub numvals: u64,
}

/// `_SPI_connection` (`spi_priv.h`): one entry per SPI nesting level.
pub struct SpiConnection {
    /// `processed` — `SPI_processed` value owned by this level.
    pub processed: u64,
    /// `tuptable` — this level's current result table index into `tuptables`,
    /// or `None`.
    pub tuptable: Option<usize>,
    /// `execSubid` — subxact in which the current executor op started.
    pub exec_subid: SubTransactionId,
    /// `tuptables` — the slist of tuple tables created at this level.
    pub tuptables: Vec<SpiTupleTable>,
    /// `procCxt` — the procedure (long-lived) memory context.
    pub proc_cxt: Option<MemoryContext>,
    /// `execCxt` — the executor (reset-per-op) memory context.
    pub exec_cxt: Option<MemoryContext>,
    /// `connectSubid` — the subxact that did `SPI_connect`.
    pub connect_subid: SubTransactionId,
    /// `atomic` — false in a non-atomic (procedure) context.
    pub atomic: bool,
    /// `internal_xact` — set across `SPI_commit` / `SPI_rollback`.
    pub internal_xact: bool,
    /// `outer_processed` — saved `SPI_processed` of the enclosing level.
    pub outer_processed: u64,
    /// `outer_result` — saved `SPI_result` of the enclosing level.
    pub outer_result: i32,
    // NOTE: C also saves `savedcxt` (the context to restore on SPI_finish) and
    // `outer_tuptable`. In the owned-context model the "restore" is implicit
    // (we never globally switch the current context the way C's
    // MemoryContextSwitchTo does), and `SPI_tuptable` is recomputed from the
    // live level, so these are not needed as stored fields. `queryEnv` is part
    // of the (seam-and-panic) execution leg and is added when that lands.
}

thread_local! {
    /// `_SPI_stack` — the heap array of connections. C grows it 16 → doubling;
    /// a `Vec` subsumes both `_SPI_stack` and `_SPI_stack_depth`.
    static SPI_STACK: RefCell<Vec<SpiConnection>> = const { RefCell::new(Vec::new()) };
    /// `_SPI_connected` — index of the current top level, `-1` when none.
    static SPI_CONNECTED: RefCell<i32> = const { RefCell::new(-1) };

    /// Public `SPI_processed` global.
    pub(crate) static SPI_PROCESSED: RefCell<u64> = const { RefCell::new(0) };
    /// Public `SPI_result` global.
    pub(crate) static SPI_RESULT: RefCell<i32> = const { RefCell::new(0) };
    // `SPI_tuptable` is derived from the current level on demand; the
    // execution legs (seam-and-panic) own its population.
}

fn loc(funcname: &'static str) -> ErrorLocation {
    ErrorLocation::new("../src/backend/executor/spi.c", 0, funcname)
}

/// Set the public `SPI_result` global (C's `SPI_result = code;`). The tuple
/// accessors ([`crate::accessors`]) report `SPI_ERROR_NOATTRIBUTE` /
/// `SPI_ERROR_TYPUNKNOWN` / a cleared `0` through here.
pub(crate) fn set_spi_result(code: i32) {
    SPI_RESULT.with(|r| *r.borrow_mut() = code);
}

/// Read the public `SPI_result` global (C's `SPI_result`).
pub fn SPI_result() -> i32 {
    SPI_RESULT.with(|r| *r.borrow())
}

/// Set the public `SPI_processed` global (C's `SPI_processed = my_processed;`).
pub(crate) fn set_spi_processed(n: u64) {
    SPI_PROCESSED.with(|p| *p.borrow_mut() = n);
}

/// Read the public `SPI_processed` global (C's `SPI_processed`).
pub fn SPI_processed() -> u64 {
    SPI_PROCESSED.with(|p| *p.borrow())
}

/// True when `_SPI_connected >= 0` (we are inside some SPI context).
fn is_connected() -> bool {
    SPI_CONNECTED.with(|c| *c.borrow() >= 0)
}

/// `SPI_connect(void)` — `SPI_connect_ext(0)`.
pub fn SPI_connect() -> PgResult<i32> {
    SPI_connect_ext(0)
}

/// `SPI_connect_ext(int options)` (`spi.c`): push a new nesting level.
///
/// Faithful to C: validates the stack, grows it (16 → doubling), pushes a fresh
/// `_SPI_connection`, creates the proc/exec memory contexts, and resets the
/// public `SPI_processed` / `SPI_result` globals.
pub fn SPI_connect_ext(options: i32) -> PgResult<i32> {
    let connected = SPI_CONNECTED.with(|c| *c.borrow());

    // C validates `_SPI_stack` / depth consistency; with a Vec the only
    // invariant is `connected == len - 1`. A corrupted state is an elog(ERROR).
    let len = SPI_STACK.with(|s| s.borrow().len()) as i32;
    if connected + 1 != len {
        ereport(::types_error::ERROR)
            .errmsg_internal("SPI stack corrupted")
            .finish(loc("SPI_connect_ext"))?;
    }

    let atomic = options & SPI_OPT_NONATOMIC == 0;
    let connect_subid = xact_seam::get_current_sub_transaction_id::call();

    let outer_processed = SPI_PROCESSED.with(|p| *p.borrow());
    let outer_result = SPI_RESULT.with(|r| *r.borrow());

    // Create the procedure / executor contexts (C AllocSetContextCreate). The
    // parent-context distinction (TopTransactionContext vs PortalContext) is
    // captured by `atomic`; the owned contexts are dropped in SPI_finish /
    // AtEOSubXact_SPI exactly as C deletes them.
    let proc_cxt = MemoryContext::new("SPI Proc");
    let exec_cxt = proc_cxt.new_child("SPI Exec");

    let conn = SpiConnection {
        processed: 0,
        tuptable: None,
        exec_subid: InvalidSubTransactionId,
        tuptables: Vec::new(),
        proc_cxt: Some(proc_cxt),
        exec_cxt: Some(exec_cxt),
        connect_subid,
        atomic,
        internal_xact: false,
        outer_processed,
        outer_result,
    };

    SPI_STACK.with(|s| s.borrow_mut().push(conn));
    SPI_CONNECTED.with(|c| *c.borrow_mut() += 1);

    // Reset API globals.
    SPI_PROCESSED.with(|p| *p.borrow_mut() = 0);
    SPI_RESULT.with(|r| *r.borrow_mut() = 0);

    Ok(SPI_OK_CONNECT)
}

/// `SPI_finish(void)` (`spi.c`): pop the current level.
///
/// Faithful to C: checks we are connected (`_SPI_begin_call(false)`), deletes
/// the exec/proc contexts (dropping the owned `MemoryContext`s, which frees all
/// tuptables under them), restores the outer API globals, and pops the stack.
pub fn SPI_finish() -> PgResult<i32> {
    if !is_connected() {
        return Ok(SPI_ERROR_UNCONNECTED);
    }

    // Pop the top connection; dropping it deletes execCxt + procCxt (and thus
    // every tuptable created at this level) — C MemoryContextDelete order is
    // execCxt then procCxt, but procCxt is the parent so a single drop is
    // equivalent.
    let popped = SPI_STACK.with(|s| s.borrow_mut().pop());
    let conn = match popped {
        Some(c) => c,
        None => return Ok(SPI_ERROR_UNCONNECTED),
    };
    drop(conn.exec_cxt);
    drop(conn.proc_cxt);

    // Restore outer API variables.
    SPI_PROCESSED.with(|p| *p.borrow_mut() = conn.outer_processed);
    SPI_RESULT.with(|r| *r.borrow_mut() = conn.outer_result);

    SPI_CONNECTED.with(|c| *c.borrow_mut() -= 1);

    Ok(SPI_OK_FINISH)
}

/// `AtEOXact_SPI(bool isCommit)` (`spi.c`): pop every non-internal-xact level
/// at transaction end; WARN on a leaked connection at commit.
///
/// Runs on the transaction hot path (called by xact's commit/abort). Faithful
/// to C: stop at the first `internal_xact` level (it belongs to the
/// SPI_commit/rollback caller), restore outer globals as we pop, and emit the
/// "transaction left non-empty SPI stack" WARNING if we popped anything during
/// a commit.
pub fn AtEOXact_SPI(is_commit: bool) -> PgResult<()> {
    let mut found = false;

    loop {
        let connected = SPI_CONNECTED.with(|c| *c.borrow());
        if connected < 0 {
            break;
        }
        let internal = SPI_STACK.with(|s| s.borrow()[connected as usize].internal_xact);
        if internal {
            break;
        }
        found = true;

        // C does NOT delete the contexts here — they go away with their parent
        // (TopTransactionContext / PortalContext) automatically. We drop the
        // owned contexts as we pop, which is the equivalent of the parent
        // disappearing; we must not touch a context that might already be gone,
        // and dropping the popped value is safe.
        let conn = SPI_STACK.with(|s| s.borrow_mut().pop()).expect("checked");
        SPI_PROCESSED.with(|p| *p.borrow_mut() = conn.outer_processed);
        SPI_RESULT.with(|r| *r.borrow_mut() = conn.outer_result);
        SPI_CONNECTED.with(|c| *c.borrow_mut() -= 1);
    }

    if found && is_commit {
        ereport(WARNING)
            .errcode(::types_error::ERRCODE_WARNING)
            .errmsg("transaction left non-empty SPI stack")
            .errhint("Check for missing \"SPI_finish\" calls.")
            .finish(loc("AtEOXact_SPI"))?;
    }
    Ok(())
}

/// `AtEOSubXact_SPI(bool isCommit, SubTransactionId mySubid)` (`spi.c`).
///
/// Pop levels created by the ending subxact (WARN on commit if any), then, if a
/// surrounding SPI context remains during an abort, force-reset its execCxt if
/// the current executor op started in the subxact and throw away any tuple
/// tables created within it.
pub fn AtEOSubXact_SPI(is_commit: bool, my_subid: SubTransactionId) -> PgResult<()> {
    let mut found = false;

    loop {
        let connected = SPI_CONNECTED.with(|c| *c.borrow());
        if connected < 0 {
            break;
        }
        let (connect_subid, internal) = SPI_STACK.with(|s| {
            let conn = &s.borrow()[connected as usize];
            (conn.connect_subid, conn.internal_xact)
        });
        if connect_subid != my_subid {
            break; // couldn't be any underneath it either
        }
        if internal {
            break;
        }
        found = true;

        // Release procedure memory explicitly (C deletes execCxt + procCxt).
        let conn = SPI_STACK.with(|s| s.borrow_mut().pop()).expect("checked");
        drop(conn.exec_cxt);
        drop(conn.proc_cxt);
        SPI_PROCESSED.with(|p| *p.borrow_mut() = conn.outer_processed);
        SPI_RESULT.with(|r| *r.borrow_mut() = conn.outer_result);
        SPI_CONNECTED.with(|c| *c.borrow_mut() -= 1);
    }

    if found && is_commit {
        ereport(WARNING)
            .errcode(::types_error::ERRCODE_WARNING)
            .errmsg("subtransaction left non-empty SPI stack")
            .errhint("Check for missing \"SPI_finish\" calls.")
            .finish(loc("AtEOSubXact_SPI"))?;
    }

    // If aborting a subxact and an outer SPI context surrounds it, clean up.
    if !is_commit {
        SPI_STACK.with(|s| {
            let mut stack = s.borrow_mut();
            if let Some(conn) = stack.last_mut() {
                // Throw away executor state started within this subxact
                // (force a _SPI_end_call(true)): reset execCxt.
                if conn.exec_subid >= my_subid && conn.exec_subid != InvalidSubTransactionId {
                    conn.exec_subid = InvalidSubTransactionId;
                    if let Some(cxt) = conn.exec_cxt.as_mut() {
                        cxt.reset();
                    }
                }
                // Throw away any tuple tables created within the current subxact.
                // (C uses a manual slist walk to avoid the O(N^2)
                // SPI_freetuptable search; the predicate `subid >= mySubid` is
                // the same.)
                let cur = conn.tuptable;
                let mut removed_cur = false;
                let mut idx = 0;
                conn.tuptables.retain(|t| {
                    let keep = t.subid < my_subid;
                    if !keep && Some(idx) == cur {
                        removed_cur = true;
                    }
                    idx += 1;
                    keep
                });
                if removed_cur {
                    conn.tuptable = None;
                }
            }
        });
    }
    Ok(())
}

/// `SPI_inside_nonatomic_context(void)` (`spi.c`): are we in a non-atomic
/// (procedure) SPI context, not nested inside a subtransaction?
///
/// These tests must match `_SPI_commit`'s notion of "atomic".
pub fn SPI_inside_nonatomic_context() -> bool {
    let connected = SPI_CONNECTED.with(|c| *c.borrow());
    if connected < 0 {
        return false; // not in any SPI context at all
    }
    let atomic = SPI_STACK.with(|s| s.borrow()[connected as usize].atomic);
    if atomic {
        return false; // it's atomic (function, not procedure)
    }
    if xact_seam::is_sub_transaction::call() {
        return false; // within a subtransaction → treat as atomic
    }
    true
}

// ----- internal helpers (`_SPI_begin_call` / `_SPI_end_call` etc.) -----

/// `_SPI_begin_call(bool use_exec)` (`spi.c`): enter a SPI op. Returns
/// `SPI_ERROR_UNCONNECTED` when not connected, else `0`. When `use_exec`,
/// records the executor-op start subxact and switches to the exec context.
///
/// Used by the (seam-and-panic) execution legs; the context switch is a no-op
/// in the owned-context model (allocation targets the level's `exec_cxt`
/// directly), so this only does the connect check + execSubid bookkeeping.
#[allow(dead_code)]
pub(crate) fn _SPI_begin_call(use_exec: bool) -> i32 {
    if !is_connected() {
        return SPI_ERROR_UNCONNECTED;
    }
    if use_exec {
        let sub = xact_seam::get_current_sub_transaction_id::call();
        SPI_STACK.with(|s| {
            if let Some(conn) = s.borrow_mut().last_mut() {
                conn.exec_subid = sub;
            }
        });
    }
    0
}

/// `_SPI_end_call(bool use_exec)` (`spi.c`): leave a SPI op. When `use_exec`,
/// marks the executor context no longer in use and resets it.
#[allow(dead_code)]
pub(crate) fn _SPI_end_call(use_exec: bool) -> i32 {
    if use_exec {
        SPI_STACK.with(|s| {
            if let Some(conn) = s.borrow_mut().last_mut() {
                conn.exec_subid = InvalidSubTransactionId;
                if let Some(cxt) = conn.exec_cxt.as_mut() {
                    cxt.reset();
                }
            }
        });
    }
    0
}

// ----- seam-body adapters (signature shims for the `*_seam` wiring) -----

/// Seam body for `spi_connect` — `SPI_connect()` returning `PgResult<()>`
/// (the seam carries `ereport(ERROR)` on `Err`; the `SPI_OK_CONNECT` code is
/// implied by `Ok`).
pub(crate) fn spi_connect_seam() -> PgResult<()> {
    SPI_connect()?;
    Ok(())
}

/// Seam body for `spi_finish` — `SPI_finish()` returning the SPI code.
pub(crate) fn spi_finish_seam() -> PgResult<i32> {
    SPI_finish()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::result_code::{SPI_ERROR_UNCONNECTED, SPI_OK_CONNECT, SPI_OK_FINISH};
    use std::sync::Once;

    static SEAMS: Once = Once::new();

    // The SPI backbone depends on two pure xact reads. Tests run on one thread
    // per process for the thread-locals; install simple stubs.
    fn install_seams() {
        SEAMS.call_once(|| {
            fn cur_subid() -> SubTransactionId {
                // TopSubTransactionId; the value only matters for relative
                // comparisons within a test.
                1
            }
            fn not_in_subxact() -> bool {
                false
            }
            xact_seam::get_current_sub_transaction_id::set(cur_subid);
            xact_seam::is_sub_transaction::set(not_in_subxact);
        });
    }

    fn reset_globals() {
        SPI_STACK.with(|s| s.borrow_mut().clear());
        SPI_CONNECTED.with(|c| *c.borrow_mut() = -1);
        SPI_PROCESSED.with(|p| *p.borrow_mut() = 0);
        SPI_RESULT.with(|r| *r.borrow_mut() = 0);
    }

    #[test]
    fn connect_finish_roundtrip() {
        install_seams();
        reset_globals();

        assert!(!is_connected());
        assert_eq!(SPI_finish().unwrap(), SPI_ERROR_UNCONNECTED);

        assert_eq!(SPI_connect().unwrap(), SPI_OK_CONNECT);
        assert!(is_connected());
        assert_eq!(SPI_CONNECTED.with(|c| *c.borrow()), 0);

        // Globals reset on connect.
        assert_eq!(SPI_PROCESSED.with(|p| *p.borrow()), 0);

        assert_eq!(SPI_finish().unwrap(), SPI_OK_FINISH);
        assert!(!is_connected());
    }

    #[test]
    fn nesting_preserves_outer_globals() {
        install_seams();
        reset_globals();

        SPI_connect().unwrap();
        // Simulate a result at the outer level.
        SPI_PROCESSED.with(|p| *p.borrow_mut() = 42);
        SPI_RESULT.with(|r| *r.borrow_mut() = SPI_OK_CONNECT);

        SPI_connect().unwrap();
        // Inner level reset the globals…
        assert_eq!(SPI_PROCESSED.with(|p| *p.borrow()), 0);
        assert_eq!(SPI_CONNECTED.with(|c| *c.borrow()), 1);

        SPI_finish().unwrap();
        // …and SPI_finish restored the outer ones.
        assert_eq!(SPI_PROCESSED.with(|p| *p.borrow()), 42);
        assert_eq!(SPI_RESULT.with(|r| *r.borrow()), SPI_OK_CONNECT);

        SPI_finish().unwrap();
        assert!(!is_connected());
    }

    #[test]
    fn at_eoxact_pops_leaked_levels() {
        install_seams();
        reset_globals();

        SPI_connect().unwrap();
        SPI_connect().unwrap();
        assert_eq!(SPI_CONNECTED.with(|c| *c.borrow()), 1);

        // Abort path: pop everything, no warning.
        AtEOXact_SPI(false).unwrap();
        assert!(!is_connected());
        assert_eq!(SPI_STACK.with(|s| s.borrow().len()), 0);
    }

    #[test]
    fn at_eosubxact_pops_only_matching_subid() {
        install_seams();
        reset_globals();

        // Level created under subid 1 (the stub's cur_subid).
        SPI_connect().unwrap();
        assert_eq!(SPI_CONNECTED.with(|c| *c.borrow()), 0);

        // A different subid leaves it alone.
        AtEOSubXact_SPI(false, 99).unwrap();
        assert!(is_connected());

        // The matching subid pops it.
        AtEOSubXact_SPI(false, 1).unwrap();
        assert!(!is_connected());
    }

    #[test]
    fn inside_nonatomic_context_rules() {
        install_seams();
        reset_globals();

        // Not connected → false.
        assert!(!SPI_inside_nonatomic_context());

        // Atomic (default options) → false.
        SPI_connect().unwrap();
        assert!(!SPI_inside_nonatomic_context());
        SPI_finish().unwrap();

        // Non-atomic, not in subxact (stub) → true.
        SPI_connect_ext(crate::result_code::SPI_OPT_NONATOMIC).unwrap();
        assert!(SPI_inside_nonatomic_context());
        SPI_finish().unwrap();
    }
}
