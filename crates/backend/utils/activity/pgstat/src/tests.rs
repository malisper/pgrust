use core::cell::Cell;

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

fn setup() {
    static ONCE: std::sync::Once = std::sync::Once::new();
    ONCE.call_once(|| {
        timestamp_seams::get_current_timestamp::set(|| NOW.with(|c| c.get()));
        xact_seams::get_current_transaction_nest_level::set(|| NEST_LEVEL.with(|c| c.get()));
        xact_seams::get_current_transaction_stop_timestamp::set(|| NOW.with(|c| c.get()));
        xact_seams::is_transaction_or_transaction_block::set(|| false);
        backend_status_seams::pgstat_clear_backend_status_snapshot::set(|| {});
        crate::init_seams();
    });
    SetMyDatabaseId(5);
    crate::set_pgstat_track_counts(true);
    NEST_LEVEL.with(|c| c.set(1));
}

fn advance_clock(usec: i64) {
    NOW.with(|c| c.set(c.get() + usec));
}

fn rel_counts(relid: u32) -> Option<PgStat_TableCounts> {
    pending::with_state(|st| {
        let key = relation::relation_key(relid, false);
        match st.pending.get(&key) {
            Some(PendingData::Relation(t)) => Some(t.counts),
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
    setup();
    assert_eq!(pending::pgstat_report_stat(false), 0);
    assert_eq!(pending::pgstat_report_stat(true), 0);
}

#[test]
fn eoxact_commit_folds_trans_into_counts() {
    setup();
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
    setup();
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
    setup();
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
    setup();
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
    setup();
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
    setup();
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
    setup();
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
    setup();
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
fn flush_folds_relation_into_database_pending() {
    setup();
    relation::pgstat_count_heap_getnext(1009, false);
    xact::AtEOXact_PgStat(true, false);

    let key = relation::relation_key(1009, false);
    pending::with_state(|st| {
        let Some(PendingData::Relation(t)) = st.pending.remove(&key) else {
            panic!("no relation pending entry");
        };
        pending::flush_relation_into_db(st, key.dboid, &t.counts);
    });

    let db = db_pending(5).unwrap();
    assert_eq!(db.tuples_returned, 1);
    assert_eq!(db.xact_commit, 0);
}

#[test]
fn report_stat_flushes_and_rate_limits() {
    setup();
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
    setup();
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
    setup();
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
    setup();
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
    setup();
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
    setup();
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
    setup();
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
    setup();
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
    setup();
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
    setup();
    assert!(relation::pgstat_init_relation(1, b'r'));
    assert!(relation::pgstat_init_relation(1, b'p'));
    assert!(relation::pgstat_init_relation(1, b'i'));
    assert!(!relation::pgstat_init_relation(1, b'v'));
    crate::set_pgstat_track_counts(false);
    assert!(!relation::pgstat_init_relation(1, b'r'));
}

#[test]
fn slru_counters_and_flush() {
    setup();
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
    setup();
    checkpointer::pgstat_count_checkpointer_slru_written();
    checkpointer::pgstat_count_checkpointer_slru_written();
    assert_eq!(checkpointer::pending_checkpointer_stats().slru_written, 2);
}

#[test]
fn session_end_cause_fatal_only_upgrades_normal() {
    setup();
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
    setup();
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
    setup();
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
    setup();
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
    setup();
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
    setup();
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
    setup();
    pgstat_seams::pgstat_report_tempfile::call(64);
    assert_eq!(db_pending(5).unwrap().temp_files, 1);
    assert_eq!(pgstat_seams::pgstat_get_slru_index::call("notify"), 3);
    assert!(pgstat_seams::pgstat_init_relation::call(1, b'r'));
    xact::AtEOXact_PgStat(true, false);
    xact::AtPrepare_PgStat().unwrap();
    xact::PostPrepare_PgStat();
    assert!(guc_tables::vars::pgstat_track_counts.read());
}
