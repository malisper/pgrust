//! Unit tests for the `pgstat_xact.c` port.
//!
//! Seams are installed exactly once per process (seam slots are `OnceLock`s)
//! with dispatchers that read a per-thread [`Recorder`], so tests stay
//! isolated: the test runner gives each test its own thread, hence its own
//! recorder and its own `pgStatXactStack`.

use std::cell::RefCell;
use std::sync::Once;

use ::mcx::MemoryContext;
use ::types_error::PgResult;

use super::*;

thread_local! {
    static REC: RefCell<Recorder> = RefCell::new(Recorder::new());
}

struct Recorder {
    /// (kind, dboid, objid) of each pgstat_drop_entry call.
    drops: Vec<(PgStat_Kind, Oid, u64)>,
    gc_requests: u32,
    clear_snapshots: u32,
    db_hooks: Vec<(bool, bool)>,
    /// (xact_state.nest_level, isCommit) of each AtEOXact_PgStat_Relations call.
    rel_eoxact: Vec<(i32, bool)>,
    /// (xact_state.nest_level, isCommit, nestDepth) of each
    /// AtEOSubXact_PgStat_Relations call.
    rel_eosubxact: Vec<(i32, bool, i32)>,
    prepare_rel: u32,
    postprepare_rel: u32,
    resets: Vec<(PgStat_Kind, Oid, u64)>,
    /// Configured: objids for which get_entry_ref_exists returns true.
    existing: Vec<u64>,
    /// Configured return of pgstat_drop_entry (true = freed).
    drop_ret: bool,
    /// Configured GetCurrentTransactionNestLevel.
    nest_level: i32,
}

impl Recorder {
    fn new() -> Self {
        Recorder {
            drops: Vec::new(),
            gc_requests: 0,
            clear_snapshots: 0,
            db_hooks: Vec::new(),
            rel_eoxact: Vec::new(),
            rel_eosubxact: Vec::new(),
            prepare_rel: 0,
            postprepare_rel: 0,
            resets: Vec::new(),
            existing: Vec::new(),
            drop_ret: true,
            nest_level: 1,
        }
    }
}

fn set_nest_level(nest_level: i32) {
    REC.with(|r| r.borrow_mut().nest_level = nest_level);
}

fn drop_entry(kind: PgStat_Kind, dboid: Oid, objid: u64) -> PgResult<bool> {
    REC.with(|r| {
        let mut r = r.borrow_mut();
        r.drops.push((kind, dboid, objid));
        Ok(r.drop_ret)
    })
}

fn install_seams() {
    static INSTALL: Once = Once::new();
    INSTALL.call_once(|| {
        shmem_seams::pgstat_drop_entry::set(drop_entry);
        shmem_seams::pgstat_request_entry_refs_gc::set(|| {
            REC.with(|r| r.borrow_mut().gc_requests += 1)
        });
        shmem_seams::pgstat_get_entry_ref_exists::set(|_k, _d, objid| {
            REC.with(|r| Ok(r.borrow().existing.contains(&objid)))
        });
        pgstat_seams::pgstat_clear_snapshot::set(|| {
            REC.with(|r| r.borrow_mut().clear_snapshots += 1)
        });
        pgstat_seams::pgstat_reset::set(|kind, dboid, objid| {
            REC.with(|r| r.borrow_mut().resets.push((kind, dboid, objid)));
            Ok(())
        });
        pgstat_seams::pgstat_get_kind_name::set(|_k| "relation");
        stat_seams::at_eoxact_pgstat_database::set(|c, p| {
            REC.with(|r| r.borrow_mut().db_hooks.push((c, p)))
        });
        stat_seams::at_eoxact_pgstat_relations::set(|xact_state, c| {
            REC.with(|r| {
                r.borrow_mut()
                    .rel_eoxact
                    .push((xact_state.nest_level, c))
            })
        });
        stat_seams::at_eosubxact_pgstat_relations::set(|xact_state, c, n| {
            REC.with(|r| {
                r.borrow_mut()
                    .rel_eosubxact
                    .push((xact_state.nest_level, c, n))
            });
            Ok(())
        });
        stat_seams::at_prepare_pgstat_relations::set(|_xact_state| {
            REC.with(|r| r.borrow_mut().prepare_rel += 1);
            Ok(())
        });
        stat_seams::post_prepare_pgstat_relations::set(|_xact_state| {
            REC.with(|r| r.borrow_mut().postprepare_rel += 1)
        });
        xact_seams::get_current_transaction_nest_level::set(|| {
            REC.with(|r| r.borrow().nest_level)
        });
    });
}

const REL: PgStat_Kind = types_pgstat::activity_pgstat::PGSTAT_KIND_RELATION;

fn drops_for(is_commit: bool) -> Vec<u64> {
    let ctx = MemoryContext::new("test");
    let objids: Vec<u64> = pgstat_get_transactional_drops(ctx.mcx(), is_commit)
        .unwrap()
        .iter()
        .map(|i| i.objid)
        .collect();
    objids
}

#[test]
fn commit_drops_dropped_objects_only() {
    install_seams();

    // dropped obj 100, created obj 200, at nest level 1.
    pgstat_drop_transactional(REL, 5, 100).unwrap();
    pgstat_create_transactional(REL, 5, 200).unwrap();

    AtEOXact_PgStat(true, false).unwrap();

    REC.with(|r| {
        let r = r.borrow();
        // db hook ran with (isCommit, parallel).
        assert_eq!(r.db_hooks, vec![(true, false)]);
        assert_eq!(r.rel_eoxact, vec![(1, true)]);
        // Only the dropped object's stats entry is dropped on commit.
        assert_eq!(r.drops, vec![(REL, 5, 100)]);
        assert_eq!(r.gc_requests, 0);
        assert_eq!(r.clear_snapshots, 1);
    });
    // Stack cleared.
    assert!(drops_for(false).is_empty());
}

#[test]
fn abort_drops_created_objects_only() {
    install_seams();

    pgstat_drop_transactional(REL, 5, 100).unwrap();
    pgstat_create_transactional(REL, 5, 200).unwrap();

    AtEOXact_PgStat(false, false).unwrap();

    REC.with(|r| {
        let r = r.borrow();
        assert_eq!(r.db_hooks, vec![(false, false)]);
        // Only the created object's stats entry is dropped on abort.
        assert_eq!(r.drops, vec![(REL, 5, 200)]);
        assert_eq!(r.gc_requests, 0);
    });
}

#[test]
fn drop_entry_not_freed_requests_gc() {
    install_seams();
    REC.with(|r| r.borrow_mut().drop_ret = false);

    pgstat_drop_transactional(REL, 5, 100).unwrap();
    AtEOXact_PgStat(true, false).unwrap();

    REC.with(|r| {
        let r = r.borrow();
        assert_eq!(r.drops, vec![(REL, 5, 100)]);
        assert_eq!(r.gc_requests, 1);
    });
}

#[test]
fn create_transactional_resets_existing_entry() {
    install_seams();
    REC.with(|r| r.borrow_mut().existing = vec![300]);

    pgstat_create_transactional(REL, 7, 300).unwrap();

    REC.with(|r| {
        let r = r.borrow();
        assert_eq!(r.resets, vec![(REL, 7, 300)]);
    });
}

#[test]
fn subxact_abort_drops_created_object() {
    install_seams();
    set_nest_level(2); // current nest level = 2 (a subxact)

    // create at subxact level 2
    pgstat_create_transactional(REL, 5, 400).unwrap();

    // subtransaction at depth 2 aborts
    AtEOSubXact_PgStat(false, 2).unwrap();

    REC.with(|r| {
        let r = r.borrow();
        assert_eq!(r.rel_eosubxact, vec![(2, false, 2)]);
        // created object stats dropped on subxact abort
        assert_eq!(r.drops, vec![(REL, 5, 400)]);
    });
}

#[test]
fn subxact_commit_passes_drop_to_parent() {
    install_seams();

    // Establish a parent level (nest 1) holding one dropped item.
    set_nest_level(1);
    pgstat_drop_transactional(REL, 5, 10).unwrap();

    // Now a subtransaction at level 2 drops an object.
    set_nest_level(2);
    pgstat_drop_transactional(REL, 5, 500).unwrap();

    // Subtransaction at depth 2 commits: its drop item moves to the parent,
    // no drop happens yet.
    AtEOSubXact_PgStat(true, 2).unwrap();

    REC.with(|r| {
        let r = r.borrow();
        assert_eq!(r.rel_eosubxact, vec![(2, true, 2)]);
        // No stats entry dropped at subxact commit.
        assert!(r.drops.is_empty());
    });

    // Back at level 1, the parent now holds both dropped items (10 and 500).
    set_nest_level(1);
    assert_eq!(drops_for(true), vec![10, 500]);

    // Top-level commit now drops both.
    AtEOXact_PgStat(true, false).unwrap();
    REC.with(|r| {
        let r = r.borrow();
        let dropped: Vec<u64> = r.drops.iter().map(|d| d.2).collect();
        assert_eq!(dropped, vec![10, 500]);
    });
}

#[test]
fn get_transactional_drops_filters_by_commit() {
    install_seams();

    pgstat_drop_transactional(REL, 5, 100).unwrap(); // is_create = false
    pgstat_create_transactional(REL, 5, 200).unwrap(); // is_create = true

    // Commit: only dropped (non-create) items.
    assert_eq!(drops_for(true), vec![100]);
    // Abort: only created items.
    assert_eq!(drops_for(false), vec![200]);
}

#[test]
fn execute_transactional_drops_drops_all() {
    install_seams();

    let items = vec![
        XlXactStatsItem { kind: REL.0 as i32, dboid: 5, objid: 1 },
        XlXactStatsItem { kind: REL.0 as i32, dboid: 5, objid: 2 },
    ];
    pgstat_execute_transactional_drops(&items, true).unwrap();

    REC.with(|r| {
        let r = r.borrow();
        assert_eq!(r.drops, vec![(REL, 5, 1), (REL, 5, 2)]);
        assert_eq!(r.gc_requests, 0);
    });
}

#[test]
fn execute_transactional_drops_empty_noop() {
    install_seams();
    pgstat_execute_transactional_drops(&[], false).unwrap();
    REC.with(|r| assert!(r.borrow().drops.is_empty()));
}

#[test]
fn prepare_and_postprepare_run_relation_hooks() {
    install_seams();

    pgstat_drop_transactional(REL, 5, 100).unwrap(); // make a level-1 node

    AtPrepare_PgStat().unwrap();
    REC.with(|r| assert_eq!(r.borrow().prepare_rel, 1));

    PostPrepare_PgStat();
    REC.with(|r| {
        let r = r.borrow();
        assert_eq!(r.postprepare_rel, 1);
        // PostPrepare clears the snapshot but does NOT call drop_entry.
        assert_eq!(r.clear_snapshots, 1);
        assert!(r.drops.is_empty());
    });
    // Stack cleared by PostPrepare.
    assert!(drops_for(false).is_empty());
}

#[test]
fn objid_round_trips_high_and_low_words() {
    install_seams();

    // An objid that uses both the high and low 32-bit words of C's
    // objid_lo/objid_hi split.
    let big: u64 = (0xABCD_u64 << 32) | 0x1234_5678;
    pgstat_drop_transactional(REL, 5, big).unwrap();
    AtEOXact_PgStat(true, false).unwrap();

    REC.with(|r| {
        let r = r.borrow();
        assert_eq!(r.drops, vec![(REL, 5, big)]);
    });
}
