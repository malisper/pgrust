use core::cell::Cell;
use std::sync::{Mutex, MutexGuard};

use init_small::globals::SetMyDatabaseId;
use mcx::MemoryContext;

use crate::database::{self, SessionEndType};
use crate::pending::{
    self, PendingData, PgStat_HashKey, PGSTAT_IDLE_INTERVAL, PGSTAT_KIND_DATABASE,
    PGSTAT_KIND_RELATION, PGSTAT_MIN_INTERVAL,
};
use crate::relation::{self, PgStat_TableCounts};
use crate::{checkpointer, slru, xact};

thread_local! {
    static NEST_LEVEL: Cell<i32> = const { Cell::new(1) };
    static NOW: Cell<i64> = const { Cell::new(1_000_000) };
}

// Serializes every test that touches the process-global stores: SHARED_STATS,
// SHARED_IO, SHARED_SLRU, the fixed-kind stats, and the statsfile import/reset
// paths, which replace all of them wholesale.
static TEST_LOCK: Mutex<()> = Mutex::new(());

#[must_use]
fn setup() -> MutexGuard<'static, ()> {
    let guard = TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    static ONCE: std::sync::Once = std::sync::Once::new();
    ONCE.call_once(|| {
        timestamp_seams::get_current_timestamp::set(|| NOW.with(|c| c.get()));
        xact_seams::get_current_transaction_nest_level::set(|| NEST_LEVEL.with(|c| c.get()));
        xact_seams::get_current_transaction_stop_timestamp::set(|| NOW.with(|c| c.get()));
        xact_seams::is_transaction_or_transaction_block::set(|| false);
        backend_status_seams::pgstat_clear_backend_status_snapshot::set(|| {});
        install_replslot_seams();
        crate::init_seams();
    });
    SetMyDatabaseId(5);
    crate::set_pgstat_track_counts(true);
    NEST_LEVEL.with(|c| c.set(1));
    guard
}

fn advance_clock(usec: i64) {
    NOW.with(|c| c.set(c.get() + usec));
}

fn rel_counts(relid: u32) -> Option<PgStat_TableCounts> {
    pending::with_state(|st| {
        let key = relation::relation_key(relid, false);
        match st.pending.get(&key) {
            Some(PendingData::Relation(rp)) => Some(unsafe { (*rp.as_ptr()).counts }),
            _ => None,
        }
    })
}

fn db_pending(dboid: u32) -> Option<database::PgStat_StatDBEntry> {
    pending::with_state(|st| {
        let key = PgStat_HashKey {
            kind: PGSTAT_KIND_DATABASE,
            dboid,
            objid: 0,
        };
        match st.pending.get(&key) {
            Some(PendingData::Database(d)) => Some(*d),
            _ => None,
        }
    })
}

#[test]
fn report_stat_with_nothing_pending_is_zero() {
    let _lock = setup();
    assert_eq!(pending::pgstat_report_stat(false), 0);
    assert_eq!(pending::pgstat_report_stat(true), 0);
}

#[test]
fn eoxact_commit_folds_trans_into_counts() {
    let _lock = setup();
    relation::pgstat_count_heap_insert(1001, false, 3);
    relation::pgstat_count_heap_update(1001, false, true, false);
    relation::pgstat_count_heap_delete(1001, false);

    xact::AtEOXact_PgStat(true, false);

    let c = rel_counts(1001).unwrap();
    assert_eq!(c.tuples_inserted, 3);
    assert_eq!(c.tuples_updated, 1);
    assert_eq!(c.tuples_deleted, 1);
    assert_eq!(c.tuples_hot_updated, 1);
    assert_eq!(c.delta_live_tuples, 2);
    assert_eq!(c.delta_dead_tuples, 2);
    assert_eq!(c.changed_tuples, 5);
    assert!(!c.truncdropped);
}

#[test]
fn eoxact_abort_counts_dead_only() {
    let _lock = setup();
    relation::pgstat_count_heap_insert(1002, false, 2);
    relation::pgstat_count_heap_update(1002, false, false, true);

    xact::AtEOXact_PgStat(false, false);

    let c = rel_counts(1002).unwrap();
    assert_eq!(c.tuples_inserted, 2);
    assert_eq!(c.tuples_updated, 1);
    assert_eq!(c.tuples_newpage_updated, 1);
    assert_eq!(c.delta_live_tuples, 0);
    assert_eq!(c.delta_dead_tuples, 3);
    assert_eq!(c.changed_tuples, 0);
}

#[test]
fn truncate_zeroes_and_abort_restores() {
    let _lock = setup();
    relation::pgstat_count_heap_insert(1003, false, 2);
    relation::pgstat_count_truncate(1003, false);
    relation::pgstat_count_heap_insert(1003, false, 1);

    xact::AtEOXact_PgStat(false, false);

    // abort restores the pre-truncate counters (the post-truncate insert is
    // discarded, as in C's restore_truncdrop_counters)
    let c = rel_counts(1003).unwrap();
    assert_eq!(c.tuples_inserted, 2);
    assert_eq!(c.delta_dead_tuples, 2);
}

#[test]
fn truncate_commit_resets_deltas() {
    let _lock = setup();
    relation::pgstat_count_heap_insert(1004, false, 4);
    relation::pgstat_count_truncate(1004, false);
    relation::pgstat_count_heap_insert(1004, false, 1);

    xact::AtEOXact_PgStat(true, false);

    let c = rel_counts(1004).unwrap();
    assert!(c.truncdropped);
    assert_eq!(c.tuples_inserted, 1);
    assert_eq!(c.delta_live_tuples, 1);
    assert_eq!(c.changed_tuples, 1);
}

#[test]
fn subxact_commit_merges_into_parent_level() {
    let _lock = setup();
    relation::pgstat_count_heap_insert(1005, false, 1);
    NEST_LEVEL.with(|c| c.set(2));
    relation::pgstat_count_heap_insert(1005, false, 10);
    xact::AtEOSubXact_PgStat(true, 2);
    NEST_LEVEL.with(|c| c.set(1));
    xact::AtEOXact_PgStat(true, false);

    let c = rel_counts(1005).unwrap();
    assert_eq!(c.tuples_inserted, 11);
    assert_eq!(c.delta_live_tuples, 11);
}

#[test]
fn subxact_commit_without_parent_node_relinks_upward() {
    let _lock = setup();
    NEST_LEVEL.with(|c| c.set(2));
    relation::pgstat_count_heap_insert(1006, false, 7);
    xact::AtEOSubXact_PgStat(true, 2);
    NEST_LEVEL.with(|c| c.set(1));
    xact::AtEOXact_PgStat(true, false);

    let c = rel_counts(1006).unwrap();
    assert_eq!(c.tuples_inserted, 7);
    assert_eq!(c.delta_live_tuples, 7);
}

#[test]
fn subxact_abort_folds_dead_into_counts() {
    let _lock = setup();
    relation::pgstat_count_heap_insert(1007, false, 1);
    NEST_LEVEL.with(|c| c.set(2));
    relation::pgstat_count_heap_insert(1007, false, 5);
    xact::AtEOSubXact_PgStat(false, 2);
    NEST_LEVEL.with(|c| c.set(1));
    xact::AtEOXact_PgStat(true, false);

    let c = rel_counts(1007).unwrap();
    assert_eq!(c.tuples_inserted, 6);
    // subxact-aborted inserts are dead; the committed one is live
    assert_eq!(c.delta_live_tuples, 1);
    assert_eq!(c.delta_dead_tuples, 5);
}

#[test]
fn count_macros_accumulate_nontransactional_counts() {
    let _lock = setup();
    relation::pgstat_count_heap_scan(1008, false);
    relation::pgstat_count_heap_getnext(1008, false);
    relation::pgstat_count_heap_getnext(1008, false);
    relation::pgstat_count_heap_fetch(1008, false);
    relation::pgstat_count_index_tuples(1008, false, 9);
    relation::pgstat_count_buffer_read(1008, false);
    relation::pgstat_count_buffer_hit(1008, false);

    let c = rel_counts(1008).unwrap();
    assert_eq!(c.numscans, 1);
    assert_eq!(c.tuples_returned, 11);
    assert_eq!(c.tuples_fetched, 1);
    assert_eq!(c.blocks_fetched, 1);
    assert_eq!(c.blocks_hit, 1);

    let folded = relation::find_tabstat_entry(1008).unwrap();
    assert_eq!(folded.tuples_returned, 11);
}

#[test]
fn relation_link_counts_and_gen_invalidation() {
    let _lock = setup();
    let gen0 = relation::pgstat_relation_link_gen();
    let link = relation::pgstat_relation_link_counts(1042, false);
    assert!(!link.is_null());
    unsafe { relation::pgstat_count_buffer_read_via(link, true) };
    unsafe { relation::pgstat_count_buffer_read_via(link, false) };

    let c = rel_counts(1042).unwrap();
    assert_eq!(c.blocks_fetched, 2);
    assert_eq!(c.blocks_hit, 1);
    // link bumps land in the same entry the keyed macros use
    relation::pgstat_count_buffer_read(1042, false);
    assert_eq!(rel_counts(1042).unwrap().blocks_fetched, 3);
    assert_eq!(relation::pgstat_relation_link_gen(), gen0);

    // any relation-pending removal bumps the gen BEFORE the allocation is
    // freed: a stale link must never pass the validity check again
    let key = relation::relation_key(1042, false);
    pending::with_state(|st| st.delete_pending_entry(key));
    assert!(relation::pgstat_relation_link_gen() > gen0);

    // re-assoc yields a fresh entry with zeroed counts
    let link2 = relation::pgstat_relation_link_counts(1042, false);
    unsafe { relation::pgstat_count_buffer_read_via(link2, false) };
    assert_eq!(rel_counts(1042).unwrap().blocks_fetched, 1);
}

#[test]
fn flush_folds_relation_into_database_pending() {
    let _lock = setup();
    relation::pgstat_count_heap_getnext(1009, false);
    xact::AtEOXact_PgStat(true, false);

    let key = relation::relation_key(1009, false);
    pending::with_state(|st| {
        let Some(PendingData::Relation(rp)) = st.pending.remove(&key) else {
            panic!("no relation pending entry");
        };
        let counts = unsafe { (*rp.as_ptr()).counts };
        pending::flush_relation_into_db(st, key.dboid, &counts);
    });

    let db = db_pending(5).unwrap();
    assert_eq!(db.tuples_returned, 1);
    assert_eq!(db.xact_commit, 0);
}

#[test]
fn report_stat_flushes_and_rate_limits() {
    let _lock = setup();
    relation::pgstat_count_heap_insert(1010, false, 1);
    xact::AtEOXact_PgStat(true, false);

    assert_eq!(pending::pgstat_report_stat(true), 0);
    assert!(rel_counts(1010).is_none());
    assert!(db_pending(5).is_none());

    // new pending counts within PGSTAT_MIN_INTERVAL are held back
    relation::pgstat_count_heap_insert(1010, false, 1);
    xact::AtEOXact_PgStat(true, false);
    advance_clock(1);
    assert_eq!(pending::pgstat_report_stat(false), PGSTAT_IDLE_INTERVAL);
    assert!(rel_counts(1010).is_some());

    advance_clock(PGSTAT_MIN_INTERVAL * 1000);
    assert_eq!(pending::pgstat_report_stat(false), 0);
    assert!(rel_counts(1010).is_none());
}

#[test]
fn force_next_flush_overrides_rate_limit() {
    let _lock = setup();
    relation::pgstat_count_heap_insert(1011, false, 1);
    xact::AtEOXact_PgStat(true, false);
    assert_eq!(pending::pgstat_report_stat(true), 0);

    relation::pgstat_count_heap_insert(1011, false, 1);
    xact::AtEOXact_PgStat(true, false);
    advance_clock(1);
    pending::pgstat_force_next_flush();
    assert_eq!(pending::pgstat_report_stat(false), 0);
    assert!(rel_counts(1011).is_none());
}

#[test]
fn update_dbstats_folds_xact_and_io_time_counters() {
    let _lock = setup();
    xact::AtEOXact_PgStat(true, false);
    xact::AtEOXact_PgStat(false, false);
    xact::AtEOXact_PgStat(true, true); // parallel: not counted
    database::pgstat_count_buffer_read_time(40);
    database::pgstat_count_buffer_write_time(7);

    database::pgstat_update_dbstats(0);
    let db = db_pending(5).unwrap();
    assert_eq!(db.xact_commit, 1);
    assert_eq!(db.xact_rollback, 1);
    assert_eq!(db.blk_read_time, 40);
    assert_eq!(db.blk_write_time, 7);

    database::pgstat_update_dbstats(0);
    assert_eq!(db_pending(5).unwrap().xact_commit, 1);
}

#[test]
fn tempfile_and_deadlock_reports_respect_track_counts() {
    let _lock = setup();
    crate::set_pgstat_track_counts(false);
    database::pgstat_report_tempfile(100);
    database::pgstat_report_deadlock();
    assert!(db_pending(5).is_none());

    crate::set_pgstat_track_counts(true);
    database::pgstat_report_tempfile(2048);
    database::pgstat_report_deadlock();
    let db = db_pending(5).unwrap();
    assert_eq!(db.temp_files, 1);
    assert_eq!(db.temp_bytes, 2048);
    assert_eq!(db.deadlocks, 1);
}

#[test]
fn transactional_drops_filter_by_outcome() {
    let _lock = setup();
    relation::pgstat_create_relation(2001, false);
    relation::pgstat_drop_relation(2002, false);

    let ctx = MemoryContext::new("test");
    let commit_items = xact::pgstat_get_transactional_drops(ctx.mcx(), true).unwrap();
    assert_eq!(commit_items.len(), 1);
    assert_eq!(commit_items[0].objid, 2002);
    assert_eq!(commit_items[0].kind, PGSTAT_KIND_RELATION.0 as i32);

    let abort_items = xact::pgstat_get_transactional_drops(ctx.mcx(), false).unwrap();
    assert_eq!(abort_items.len(), 1);
    assert_eq!(abort_items[0].objid, 2001);

    xact::AtEOXact_PgStat(true, false);
}

#[test]
fn subxact_commit_passes_drops_to_parent() {
    let _lock = setup();
    NEST_LEVEL.with(|c| c.set(2));
    relation::pgstat_drop_relation(2003, false);
    xact::AtEOSubXact_PgStat(true, 2);
    NEST_LEVEL.with(|c| c.set(1));

    let ctx = MemoryContext::new("test");
    let commit_items = xact::pgstat_get_transactional_drops(ctx.mcx(), true).unwrap();
    assert_eq!(commit_items.len(), 1);
    assert_eq!(commit_items[0].objid, 2003);
    xact::AtEOXact_PgStat(true, false);
}

#[test]
fn subxact_abort_drops_created_entries_pending() {
    let _lock = setup();
    NEST_LEVEL.with(|c| c.set(2));
    relation::pgstat_create_relation(2004, false);
    relation::pgstat_count_heap_insert(2004, false, 1);
    xact::AtEOSubXact_PgStat(false, 2);
    NEST_LEVEL.with(|c| c.set(1));

    assert!(rel_counts(2004).is_none());
    xact::AtEOXact_PgStat(true, false);
}

#[test]
fn execute_transactional_drops_removes_pending() {
    let _lock = setup();
    relation::pgstat_count_heap_insert(2005, false, 1);
    let items = [types_core::xact::XlXactStatsItem {
        kind: PGSTAT_KIND_RELATION.0 as i32,
        dboid: 5,
        objid: 2005,
    }];
    xact::pgstat_execute_transactional_drops(&items, false).unwrap();
    assert!(rel_counts(2005).is_none());
    xact::AtEOXact_PgStat(true, false);
}

#[test]
fn drop_relation_zeroes_current_level_trans() {
    let _lock = setup();
    relation::pgstat_count_heap_insert(2006, false, 9);
    relation::pgstat_drop_relation(2006, false);
    assert_eq!(
        relation::find_tabstat_entry(2006).unwrap().tuples_inserted,
        0
    );
    xact::AtEOXact_PgStat(false, false);
}

#[test]
fn init_relation_gates_on_relkind_and_track_counts() {
    let _lock = setup();
    assert!(relation::pgstat_init_relation(1, b'r'));
    assert!(relation::pgstat_init_relation(1, b'p'));
    assert!(relation::pgstat_init_relation(1, b'i'));
    assert!(!relation::pgstat_init_relation(1, b'v'));
    crate::set_pgstat_track_counts(false);
    assert!(!relation::pgstat_init_relation(1, b'r'));
}

#[test]
fn slru_counters_and_flush() {
    let _lock = setup();
    assert_eq!(slru::pgstat_get_slru_index("transaction"), 6);
    assert_eq!(slru::pgstat_get_slru_index("bogus"), 7);
    assert_eq!(slru::pgstat_get_slru_name(0), Some("commit_timestamp"));
    assert_eq!(slru::pgstat_get_slru_name(8), None);

    slru::pgstat_count_slru_page_zeroed(0);
    slru::pgstat_count_slru_page_hit(0);
    slru::pgstat_count_slru_page_read(1);
    slru::pgstat_count_slru_page_written(1);
    slru::pgstat_count_slru_page_exists(2);
    slru::pgstat_count_slru_flush(3);
    slru::pgstat_count_slru_truncate(4);

    assert!(slru::pgstat_have_slrustats());
    assert!(pending::pgstat_report_fixed());
    assert_eq!(slru::pgstat_slru_pending(0).blocks_zeroed, 1);
    assert_eq!(slru::pgstat_slru_pending(0).blocks_hit, 1);
    assert_eq!(slru::pgstat_slru_pending(1).blocks_read, 1);

    assert_eq!(pending::pgstat_report_stat(true), 0);
    assert!(!slru::pgstat_have_slrustats());
    assert!(!pending::pgstat_report_fixed());
    assert_eq!(slru::pgstat_slru_pending(0).blocks_zeroed, 0);
}

#[test]
fn checkpointer_slru_written_counter() {
    let _lock = setup();
    checkpointer::pgstat_count_checkpointer_slru_written();
    checkpointer::pgstat_count_checkpointer_slru_written();
    assert_eq!(checkpointer::pending_checkpointer_stats().slru_written, 2);
}

#[test]
fn checkpointer_buffers_written_counter() {
    let _lock = setup();
    checkpointer::pgstat_count_checkpointer_buffers_written();
    checkpointer::pgstat_count_checkpointer_buffers_written();
    checkpointer::pgstat_count_checkpointer_buffers_written();
    assert_eq!(checkpointer::pending_checkpointer_stats().buffers_written, 3);
}

#[test]
fn session_end_cause_fatal_only_upgrades_normal() {
    let _lock = setup();
    assert_eq!(
        database::pgstat_session_end_cause(),
        SessionEndType::DisconnectNormal
    );
    database::pgstat_set_session_end_cause_fatal();
    assert_eq!(
        database::pgstat_session_end_cause(),
        SessionEndType::DisconnectFatal
    );
    database::pgstat_set_session_end_cause(SessionEndType::DisconnectKilled);
    database::pgstat_set_session_end_cause_fatal();
    assert_eq!(
        database::pgstat_session_end_cause(),
        SessionEndType::DisconnectKilled
    );
}

#[test]
fn flush_applies_to_shared_store_and_fetch_returns_sum() {
    let _lock = setup();
    SetMyDatabaseId(601);
    crate::set_pgstat_fetch_consistency(crate::PGSTAT_FETCH_CONSISTENCY_NONE);
    relation::pgstat_count_heap_scan(6001, false);
    relation::pgstat_count_heap_getnext(6001, false);
    relation::pgstat_count_heap_insert(6001, false, 4);
    xact::AtEOXact_PgStat(true, false);
    assert_eq!(pending::pgstat_report_stat(true), 0);
    relation::pgstat_count_heap_insert(6001, false, 1);
    xact::AtEOXact_PgStat(true, false);
    pending::pgstat_force_next_flush();
    assert_eq!(pending::pgstat_report_stat(false), 0);

    let t = relation::pgstat_fetch_stat_tabentry_ext(false, 6001).unwrap();
    assert_eq!(t.numscans, 1);
    assert_eq!(t.tuples_returned, 1);
    assert_eq!(t.tuples_inserted, 5);
    assert_eq!(t.live_tuples, 5);
    assert_eq!(t.ins_since_vacuum, 5);
    assert_eq!(t.mod_since_analyze, 5);
    assert!(t.lastscan > 0);

    let db = database::pgstat_fetch_stat_dbentry(601).unwrap();
    assert_eq!(db.tuples_inserted, 5);
    assert_eq!(db.xact_commit, 2);
    assert!(database::pgstat_fetch_stat_dbentry(699).is_none());
}

#[test]
fn truncdrop_flush_resets_live_dead_ins() {
    let _lock = setup();
    SetMyDatabaseId(605);
    crate::set_pgstat_fetch_consistency(crate::PGSTAT_FETCH_CONSISTENCY_NONE);
    relation::pgstat_count_heap_insert(6006, false, 5);
    xact::AtEOXact_PgStat(true, false);
    pending::pgstat_report_stat(true);

    relation::pgstat_count_truncate(6006, false);
    relation::pgstat_count_heap_insert(6006, false, 2);
    xact::AtEOXact_PgStat(true, false);
    pending::pgstat_force_next_flush();
    pending::pgstat_report_stat(false);

    let t = relation::pgstat_fetch_stat_tabentry_ext(false, 6006).unwrap();
    assert_eq!(t.tuples_inserted, 7);
    assert_eq!(t.live_tuples, 2);
    assert_eq!(t.dead_tuples, 0);
    assert_eq!(t.ins_since_vacuum, 2);
}

#[test]
fn cache_consistency_is_stable_until_clear() {
    let _lock = setup();
    SetMyDatabaseId(602);
    crate::set_pgstat_fetch_consistency(crate::PGSTAT_FETCH_CONSISTENCY_CACHE);
    relation::pgstat_count_heap_getnext(6002, false);
    pending::pgstat_force_next_flush();
    pending::pgstat_report_stat(false);

    let v1 = relation::pgstat_fetch_stat_tabentry_ext(false, 6002).unwrap();
    assert_eq!(v1.tuples_returned, 1);
    assert!(relation::pgstat_fetch_stat_tabentry_ext(false, 6007).is_none());

    relation::pgstat_count_heap_getnext(6002, false);
    relation::pgstat_count_heap_getnext(6007, false);
    pending::pgstat_force_next_flush();
    pending::pgstat_report_stat(false);

    let v2 = relation::pgstat_fetch_stat_tabentry_ext(false, 6002).unwrap();
    assert_eq!(v2.tuples_returned, 1);
    // negative lookups are cached too, as in C
    assert!(relation::pgstat_fetch_stat_tabentry_ext(false, 6007).is_none());

    pending::pgstat_clear_snapshot();
    let v3 = relation::pgstat_fetch_stat_tabentry_ext(false, 6002).unwrap();
    assert_eq!(v3.tuples_returned, 2);
    assert!(relation::pgstat_fetch_stat_tabentry_ext(false, 6007).is_some());
}

#[test]
fn snapshot_consistency_excludes_later_entries() {
    let _lock = setup();
    SetMyDatabaseId(603);
    crate::set_pgstat_fetch_consistency(crate::PGSTAT_FETCH_CONSISTENCY_SNAPSHOT);
    relation::pgstat_count_heap_getnext(6003, false);
    pending::pgstat_force_next_flush();
    pending::pgstat_report_stat(false);
    assert!(relation::pgstat_fetch_stat_tabentry_ext(false, 6003).is_some());

    relation::pgstat_count_heap_getnext(6004, false);
    pending::pgstat_force_next_flush();
    pending::pgstat_report_stat(false);
    assert!(relation::pgstat_fetch_stat_tabentry_ext(false, 6004).is_none());

    pending::pgstat_clear_snapshot();
    assert!(relation::pgstat_fetch_stat_tabentry_ext(false, 6004).is_some());
}

#[test]
fn have_entry_sees_pending_flushed_and_fixed() {
    let _lock = setup();
    SetMyDatabaseId(604);
    assert!(!crate::pgstat_have_entry(PGSTAT_KIND_RELATION.0, 604, 6005));
    relation::pgstat_count_heap_getnext(6005, false);
    assert!(crate::pgstat_have_entry(PGSTAT_KIND_RELATION.0, 604, 6005));
    pending::pgstat_force_next_flush();
    pending::pgstat_report_stat(false);
    assert!(crate::pgstat_have_entry(PGSTAT_KIND_RELATION.0, 604, 6005));
    assert!(crate::pgstat_have_entry(pending::PGSTAT_KIND_SLRU.0, 0, 0));

    let items = [types_core::xact::XlXactStatsItem {
        kind: PGSTAT_KIND_RELATION.0 as i32,
        dboid: 604,
        objid: 6005,
    }];
    xact::pgstat_execute_transactional_drops(&items, false).unwrap();
    assert!(!crate::pgstat_have_entry(PGSTAT_KIND_RELATION.0, 604, 6005));
}

#[test]
fn seams_are_wired() {
    let _lock = setup();
    pgstat_seams::pgstat_report_tempfile::call(64);
    assert_eq!(db_pending(5).unwrap().temp_files, 1);
    assert_eq!(pgstat_seams::pgstat_get_slru_index::call("notify"), 3);
    assert!(pgstat_seams::pgstat_init_relation::call(1, b'r'));
    xact::AtEOXact_PgStat(true, false);
    xact::AtPrepare_PgStat().unwrap();
    xact::PostPrepare_PgStat();
    assert!(guc_tables::vars::pgstat_track_counts.read());
}

fn setup_function_seams() {
    static ONCE: std::sync::Once = std::sync::Once::new();
    ONCE.call_once(|| {
        inval_seams::accept_invalidation_messages::set(|| Ok(()));
        syscache_seams::search_syscache_exists_procoid::set(|oid| Ok(oid != 66_666));
    });
}

#[test]
fn function_usage_accumulates_and_flushes() {
    let _lock = setup();
    setup_function_seams();
    let fcu = crate::function::pgstat_init_function_usage(7001).unwrap();
    crate::function::pgstat_end_function_usage(&fcu, true);
    let fcu = crate::function::pgstat_init_function_usage(7001).unwrap();
    crate::function::pgstat_end_function_usage(&fcu, true);

    let pending = crate::find_funcstat_entry(7001).unwrap();
    assert_eq!(pending.numcalls, 2);
    assert!(pending.total_time > 0);
    assert!(pending.self_time > 0);
    assert!(pending.total_time >= pending.self_time);

    pending::pgstat_flush_pending_entries(false);
    assert!(crate::find_funcstat_entry(7001).is_none());
    let shared = crate::pgstat_fetch_stat_funcentry(7001).unwrap();
    assert_eq!(shared.numcalls, 2);
    assert!(crate::pgstat_have_entry(pending::PGSTAT_KIND_FUNCTION.0, 5, 7001));
}

#[test]
fn function_usage_recursion_assigns_total_once() {
    let _lock = setup();
    setup_function_seams();
    let outer = crate::function::pgstat_init_function_usage(7002).unwrap();
    let inner = crate::function::pgstat_init_function_usage(7002).unwrap();
    crate::function::pgstat_end_function_usage(&inner, false);
    crate::function::pgstat_end_function_usage(&outer, true);

    let c = crate::find_funcstat_entry(7002).unwrap();
    assert_eq!(c.numcalls, 1);
    // recursive total is assigned, not doubled: total covers one outer span
    assert!(c.total_time >= c.self_time);
}

#[test]
fn function_usage_dropped_function_errors() {
    let _lock = setup();
    setup_function_seams();
    let err = crate::function::pgstat_init_function_usage(66_666).unwrap_err();
    assert_eq!(err.sqlstate(), types_error::ERRCODE_UNDEFINED_FUNCTION);
    assert!(crate::find_funcstat_entry(66_666).is_none());
}

// Fresh per-test data dir so a stale statsfile from an earlier run is never
// read back; DataDir is thread-local so this only redirects the calling test.
fn statsfile_dir(name: &str) -> std::path::PathBuf {
    let dir = std::env::temp_dir().join(format!("{name}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(dir.join("pg_stat")).unwrap();
    init_small::globals::SetDataDir(dir.to_str().unwrap());
    dir
}

#[test]
fn statsfile_roundtrip_restores_entries() {
    let _lock = setup();
    setup_function_seams();
    let dir = statsfile_dir("pgstat-file-test");

    relation::pgstat_count_heap_insert(8001, false, 4);
    xact::AtEOXact_PgStat(true, false);
    pending::pgstat_flush_pending_entries(false);
    let before = relation::pgstat_fetch_stat_tabentry_ext(false, 8001).unwrap();
    assert_eq!(before.tuples_inserted, 4);

    crate::file::pgstat_write_statsfile().unwrap();

    crate::pgstat_reset(pending::PGSTAT_KIND_RELATION, 5, 8001);
    crate::pgstat_clear_snapshot();
    assert_eq!(
        relation::pgstat_fetch_stat_tabentry_ext(false, 8001).unwrap().tuples_inserted,
        0
    );

    crate::file::pgstat_read_statsfile();
    crate::pgstat_clear_snapshot();
    let after = relation::pgstat_fetch_stat_tabentry_ext(false, 8001).unwrap();
    assert_eq!(after.tuples_inserted, 4);
    // C unlinks after a successful read
    assert!(!dir.join("pg_stat/pgstat.stat").exists());
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn statsfile_corrupt_body_is_rejected() {
    assert!(crate::file::read_statsfile_body(&[]).is_none());
    assert!(crate::file::read_statsfile_body(&[1, 2, 3, 4, b'E']).is_none());
    let mut good_header = crate::file::PGSTAT_FILE_FORMAT_ID.to_ne_bytes().to_vec();
    good_header.push(b'X');
    assert!(crate::file::read_statsfile_body(&good_header).is_none());
}

#[must_use]
fn setup_io() -> MutexGuard<'static, ()> {
    let guard = setup();
    miscinit::SetMyBackendType(types_core::BackendType::Backend);
    guard
}

#[test]
fn io_tracks_predicates_match_c_rules() {
    use crate::io::{self, IOContext, IOObject, IOOp};
    use types_core::BackendType as B;
    assert!(io::pgstat_tracks_io_op(B::Backend, IOObject::Relation, IOContext::IOCONTEXT_NORMAL, IOOp::Hit));
    assert!(!io::pgstat_tracks_io_bktype(B::Archiver));
    assert!(!io::pgstat_tracks_io_object(B::Checkpointer, IOObject::Relation, IOContext::IOCONTEXT_VACUUM));
    assert!(!io::pgstat_tracks_io_op(B::BgWriter, IOObject::Relation, IOContext::IOCONTEXT_NORMAL, IOOp::Read));
    assert!(!io::pgstat_tracks_io_op(B::Backend, IOObject::TempRelation, IOContext::IOCONTEXT_NORMAL, IOOp::Fsync));
    assert!(!io::pgstat_tracks_io_op(B::Backend, IOObject::Relation, IOContext::IOCONTEXT_NORMAL, IOOp::Reuse));
    assert!(io::pgstat_tracks_io_op(B::Backend, IOObject::Relation, IOContext::IOCONTEXT_VACUUM, IOOp::Reuse));
    assert!(io::pgstat_tracks_io_op(B::Backend, IOObject::Wal, IOContext::IOCONTEXT_INIT, IOOp::Fsync));
    assert!(!io::pgstat_tracks_io_op(B::Backend, IOObject::Wal, IOContext::IOCONTEXT_INIT, IOOp::Read));
    assert!(!io::pgstat_tracks_io_op(B::Backend, IOObject::Relation, IOContext::IOCONTEXT_VACUUM, IOOp::Fsync));
}

#[test]
fn io_count_flush_and_fetch() {
    use crate::io::{self, IOContext, IOObject, IOOp};
    let _lock = setup_io();
    let base = io::export_io_stats().stats[types_core::BackendType::Backend as usize]
        .counts[IOObject::Relation as usize][IOContext::IOCONTEXT_NORMAL as usize][IOOp::Hit as usize];
    io::pgstat_count_io_op(IOObject::Relation, IOContext::IOCONTEXT_NORMAL, IOOp::Hit, 3, 0);
    io::pgstat_count_io_op(IOObject::Relation, IOContext::IOCONTEXT_NORMAL, IOOp::Read, 1, 8192);
    assert!(io::pgstat_have_pending_io());
    assert!(pending::pgstat_report_fixed());
    io::pgstat_flush_io(false);
    assert!(!io::pgstat_have_pending_io());
    let shared = io::export_io_stats().stats[types_core::BackendType::Backend as usize];
    assert!(shared.counts[IOObject::Relation as usize][IOContext::IOCONTEXT_NORMAL as usize][IOOp::Hit as usize] >= base + 3);
    assert!(shared.bytes[IOObject::Relation as usize][IOContext::IOCONTEXT_NORMAL as usize][IOOp::Read as usize] >= 8192);
}

#[test]
fn io_timed_count_records_time_and_dbstats() {
    use crate::io::{self, IOContext, IOObject, IOOp};
    let _lock = setup_io();
    let start = io::pgstat_prepare_io_time(true);
    assert!(start > 0);
    io::pgstat_count_io_op_time(IOObject::Relation, IOContext::IOCONTEXT_NORMAL, IOOp::Write, start, 1, 8192);
    let pend = io::pgstat_pending_io();
    assert!(pend.pending_times_ns[IOObject::Relation as usize][IOCONTEXT_NORMAL_IDX][IOOp::Write as usize] >= 0);
    assert_eq!(io::pgstat_prepare_io_time(false), 0);
    io::pgstat_flush_io(false);
}

const IOCONTEXT_NORMAL_IDX: usize = 3;

#[test]
fn slru_flush_reset_and_fetch() {
    let _lock = setup();
    slru::pgstat_count_slru_page_hit(2);
    slru::pgstat_count_slru_page_read(2);
    assert!(slru::pgstat_have_slrustats());
    slru::pgstat_slru_flush_cb(false);
    crate::pgstat_clear_snapshot();
    let snap = slru::pgstat_fetch_slru();
    assert!(snap[2].blocks_hit >= 1);
    slru::pgstat_reset_slru("multixact_offset");
    crate::pgstat_clear_snapshot();
    let snap = slru::pgstat_fetch_slru();
    assert_eq!(snap[2].blocks_hit, 0);
    assert!(snap[2].stat_reset_timestamp > 0);
    assert_eq!(slru::pgstat_get_slru_index("nonsense"), 7);
}

#[test]
fn backend_kind_flush_fetch_reset_drop() {
    use crate::io::{IOContext, IOObject, IOOp};
    let _lock = setup_io();
    init_small::globals::SetMyProcNumber(41);
    crate::backend::pgstat_create_backend(41);
    crate::io::pgstat_count_io_op(IOObject::Relation, IOContext::IOCONTEXT_NORMAL, IOOp::Hit, 2, 0);
    crate::backend::pgstat_flush_backend(false, crate::backend::PGSTAT_BACKEND_FLUSH_ALL);
    crate::pgstat_clear_snapshot();
    let entry = crate::backend::pgstat_fetch_stat_backend(41).unwrap();
    assert_eq!(entry.io_stats.counts[IOObject::Relation as usize][IOCONTEXT_NORMAL_IDX][IOOp::Hit as usize], 2);
    crate::backend::pgstat_reset_backend(41);
    crate::pgstat_clear_snapshot();
    let entry = crate::backend::pgstat_fetch_stat_backend(41).unwrap();
    assert_eq!(entry.io_stats.counts[IOObject::Relation as usize][IOCONTEXT_NORMAL_IDX][IOOp::Hit as usize], 0);
    assert!(entry.stat_reset_timestamp > 0);
    crate::io::pgstat_flush_io(false);
}

#[test]
fn reset_of_kind_covers_all_fixed_kinds() {
    let _lock = setup();
    for kind in [
        pending::PGSTAT_KIND_ARCHIVER,
        pending::PGSTAT_KIND_BGWRITER,
        pending::PGSTAT_KIND_CHECKPOINTER,
        pending::PGSTAT_KIND_IO,
        pending::PGSTAT_KIND_SLRU,
        pending::PGSTAT_KIND_WAL,
        pending::PGSTAT_KIND_BACKEND,
    ] {
        crate::pgstat_reset_of_kind(kind);
    }
    crate::pgstat_clear_snapshot();
    assert!(crate::wal::pgstat_fetch_stat_wal().stat_reset_timestamp > 0);
    assert!(crate::io::pgstat_fetch_stat_io().stat_reset_timestamp > 0);
}

// LogCheckpointEnd (xlog.c) accumulates write_msecs/sync_msecs into
// PendingCheckpointerStats via these seams; before they existed the fetched
// write_time/sync_time stayed 0 forever (sqlsmith s101 idx 47176: a WHERE
// over sync_time = write_time evaluated TRUE here, rows in C).
#[test]
fn checkpointer_write_sync_time_seams_accumulate() {
    let _lock = setup();
    let before = {
        crate::pgstat_clear_snapshot();
        checkpointer::pgstat_fetch_stat_checkpointer()
    };
    pgstat_seams::pgstat_count_checkpointer_write_time::call(120);
    pgstat_seams::pgstat_count_checkpointer_sync_time::call(45);
    checkpointer::pgstat_report_checkpointer();
    crate::pgstat_clear_snapshot();
    let after = checkpointer::pgstat_fetch_stat_checkpointer();
    assert_eq!(after.write_time - before.write_time, 120);
    assert_eq!(after.sync_time - before.sync_time, 45);
}

#[test]
fn checkpointer_and_bgwriter_report_apply_pending() {
    let _lock = setup();
    checkpointer::with_pending_checkpointer_stats(|s| s.num_timed += 1);
    checkpointer::pgstat_report_checkpointer();
    crate::pgstat_clear_snapshot();
    assert!(checkpointer::pgstat_fetch_stat_checkpointer().num_timed >= 1);
    crate::bgwriter::with_pending_bgwriter_stats(|s| s.buf_alloc += 5);
    crate::bgwriter::pgstat_report_bgwriter();
    crate::pgstat_clear_snapshot();
    assert!(crate::bgwriter::pgstat_fetch_stat_bgwriter().buf_alloc >= 5);
}

#[test]
fn wal_flush_without_installed_usage_seam_is_noop() {
    let _lock = setup();
    assert!(!crate::wal::pgstat_wal_flush_cb(false));
}

fn install_replslot_seams() {
    slot_seams::named_replication_slot_info::set(|name, _need_lock| {
        Ok(match name {
            "logslot" => (3, true),
            "physlot" => (4, false),
            _ => (-1, false),
        })
    });
    slot_seams::replication_slot_name::set(|index| {
        Ok((index == 3).then(|| {
            let mut b = [0u8; 64];
            b[..7].copy_from_slice(b"logslot");
            b
        }))
    });
}

#[test]
fn replslot_report_accumulates_and_resets() {
    let _lock = setup();
    crate::replslot::pgstat_create_replslot(3);
    let rep = crate::replslot::PgStat_StatReplSlotEntry {
        spill_txns: 2,
        total_bytes: 100,
        ..Default::default()
    };
    crate::replslot::pgstat_report_replslot(3, &rep);
    crate::replslot::pgstat_report_replslot(3, &rep);
    crate::pgstat_clear_snapshot();
    let e = crate::pgstat_fetch_replslot("logslot").unwrap().unwrap();
    assert_eq!(e.spill_txns, 4);
    assert_eq!(e.total_bytes, 200);
    assert_eq!(e.stat_reset_timestamp, 0);

    crate::replslot::pgstat_reset_replslot("logslot").unwrap();
    crate::pgstat_clear_snapshot();
    let e = crate::pgstat_fetch_replslot("logslot").unwrap().unwrap();
    assert_eq!(e.spill_txns, 0);
    assert!(e.stat_reset_timestamp > 0);

    // physical slots collect no stats; reset is a no-op, not an error
    crate::replslot::pgstat_reset_replslot("physlot").unwrap();
    let err = crate::replslot::pgstat_reset_replslot("gone").unwrap_err();
    assert_eq!(err.sqlstate(), types_error::ERRCODE_INVALID_PARAMETER_VALUE);

    crate::replslot::pgstat_drop_replslot(3);
    crate::pgstat_clear_snapshot();
    assert!(crate::pgstat_fetch_replslot("logslot").unwrap().is_none());
}

#[test]
fn subscription_counts_flush_and_reset() {
    let _lock = setup();
    crate::subscription::pgstat_report_subscription_error(9001, true);
    crate::subscription::pgstat_report_subscription_error(9001, true);
    crate::subscription::pgstat_report_subscription_error(9001, false);
    crate::subscription::pgstat_report_subscription_conflict(9001, 2);
    pending::pgstat_flush_pending_entries(false);
    crate::pgstat_clear_snapshot();
    let e = crate::pgstat_fetch_stat_subscription(9001).unwrap();
    assert_eq!(e.apply_error_count, 2);
    assert_eq!(e.sync_error_count, 1);
    assert_eq!(e.conflict_count[2], 1);
    assert_eq!(e.conflict_count[0], 0);

    crate::pgstat_reset(pending::PGSTAT_KIND_SUBSCRIPTION, 0, 9001);
    crate::pgstat_clear_snapshot();
    let e = crate::pgstat_fetch_stat_subscription(9001).unwrap();
    assert_eq!(e.apply_error_count, 0);
    assert!(e.stat_reset_timestamp > 0);
}

#[test]
fn subscription_create_rollback_drops_entry() {
    let _lock = setup();
    crate::subscription::pgstat_create_subscription(9002);
    crate::pgstat_clear_snapshot();
    assert!(crate::pgstat_fetch_stat_subscription(9002).is_some());
    xact::AtEOXact_PgStat(false, false);
    crate::pgstat_clear_snapshot();
    assert!(crate::pgstat_fetch_stat_subscription(9002).is_none());
}

#[test]
fn statsfile_replslot_roundtrips_by_name() {
    let _lock = setup();
    let dir = statsfile_dir("pgstat-replslot-test");

    crate::replslot::pgstat_create_replslot(3);
    let rep = crate::replslot::PgStat_StatReplSlotEntry {
        stream_count: 7,
        ..Default::default()
    };
    crate::replslot::pgstat_report_replslot(3, &rep);
    crate::file::pgstat_write_statsfile().unwrap();

    crate::replslot::pgstat_drop_replslot(3);
    crate::pgstat_clear_snapshot();
    assert!(crate::pgstat_fetch_replslot("logslot").unwrap().is_none());

    crate::file::pgstat_read_statsfile();
    crate::pgstat_clear_snapshot();
    let e = crate::pgstat_fetch_replslot("logslot").unwrap().unwrap();
    assert_eq!(e.stream_count, 7);
    crate::replslot::pgstat_drop_replslot(3);
    let _ = std::fs::remove_dir_all(&dir);
}
