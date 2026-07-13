// pgstat_shmem.c's shared store + pgstat.c's fetch/snapshot half. C keeps the
// entries in DSM dshash outside memory contexts; one process-global mutex
// replaces the dshash partitions and per-entry lwlocks on this cold path.

use core::cell::{Cell, RefCell};
use std::collections::HashMap;
use std::sync::Mutex;

use rustc_hash::FxBuildHasher;
use types_core::{InvalidOid, TimestampTz};

use crate::database::PgStat_StatDBEntry;
use crate::pending::{
    PgStat_HashKey, PgStat_Kind, PGSTAT_KIND_ARCHIVER, PGSTAT_KIND_BACKEND, PGSTAT_KIND_BGWRITER,
    PGSTAT_KIND_CHECKPOINTER, PGSTAT_KIND_DATABASE, PGSTAT_KIND_FUNCTION, PGSTAT_KIND_IO,
    PGSTAT_KIND_RELATION, PGSTAT_KIND_REPLSLOT, PGSTAT_KIND_SLRU, PGSTAT_KIND_SUBSCRIPTION,
    PGSTAT_KIND_WAL,
};
use crate::relation::PgStat_TableCounts;
use crate::{
    PgStat_Counter, PGSTAT_FETCH_CONSISTENCY_CACHE, PGSTAT_FETCH_CONSISTENCY_NONE,
    PGSTAT_FETCH_CONSISTENCY_SNAPSHOT,
};

// repr(C), all-i64 fields: statsfile serialization copies these as bytes.
#[derive(Clone, Copy, Default, PartialEq, Debug)]
#[repr(C)]
pub struct PgStat_StatTabEntry {
    pub numscans: PgStat_Counter,
    pub lastscan: TimestampTz,
    pub tuples_returned: PgStat_Counter,
    pub tuples_fetched: PgStat_Counter,
    pub tuples_inserted: PgStat_Counter,
    pub tuples_updated: PgStat_Counter,
    pub tuples_deleted: PgStat_Counter,
    pub tuples_hot_updated: PgStat_Counter,
    pub tuples_newpage_updated: PgStat_Counter,
    pub live_tuples: PgStat_Counter,
    pub dead_tuples: PgStat_Counter,
    pub mod_since_analyze: PgStat_Counter,
    pub ins_since_vacuum: PgStat_Counter,
    pub blocks_fetched: PgStat_Counter,
    pub blocks_hit: PgStat_Counter,
    pub last_vacuum_time: TimestampTz,
    pub vacuum_count: PgStat_Counter,
    pub last_autovacuum_time: TimestampTz,
    pub autovacuum_count: PgStat_Counter,
    pub last_analyze_time: TimestampTz,
    pub analyze_count: PgStat_Counter,
    pub last_autoanalyze_time: TimestampTz,
    pub autoanalyze_count: PgStat_Counter,
    // times in milliseconds
    pub total_vacuum_time: PgStat_Counter,
    pub total_autovacuum_time: PgStat_Counter,
    pub total_analyze_time: PgStat_Counter,
    pub total_autoanalyze_time: PgStat_Counter,
}

#[derive(Clone, Copy, Debug)]
pub enum SharedEntry {
    Relation(PgStat_StatTabEntry),
    Database(PgStat_StatDBEntry),
    Function(crate::function::PgStat_StatFuncEntry),
    Backend(crate::backend::PgStat_Backend),
    ReplSlot(crate::replslot::PgStat_StatReplSlotEntry),
    Subscription(crate::subscription::PgStat_StatSubEntry),
}

type Store = HashMap<PgStat_HashKey, SharedEntry, FxBuildHasher>;

static SHARED_STATS: Mutex<Store> = Mutex::new(HashMap::with_hasher(FxBuildHasher));

pub(crate) fn flush_relation(key: PgStat_HashKey, counts: &PgStat_TableCounts) {
    let scan_ts = if counts.numscans != 0 {
        xact_seams::get_current_transaction_stop_timestamp::call()
    } else {
        0
    };
    let mut store = SHARED_STATS.lock().unwrap();
    // prep_pending_entry created the shared entry eagerly; absence means the
    // object was dropped concurrently — discard the pending counts instead of
    // resurrecting (C flushes into the dropped-but-refcounted copy, which
    // readers can no longer find).
    let Some(SharedEntry::Relation(tabentry)) = store.get_mut(&key) else {
        return;
    };

    tabentry.numscans += counts.numscans;
    if counts.numscans != 0 && scan_ts > tabentry.lastscan {
        tabentry.lastscan = scan_ts;
    }
    tabentry.tuples_returned += counts.tuples_returned;
    tabentry.tuples_fetched += counts.tuples_fetched;
    tabentry.tuples_inserted += counts.tuples_inserted;
    tabentry.tuples_updated += counts.tuples_updated;
    tabentry.tuples_deleted += counts.tuples_deleted;
    tabentry.tuples_hot_updated += counts.tuples_hot_updated;
    tabentry.tuples_newpage_updated += counts.tuples_newpage_updated;

    if counts.truncdropped {
        tabentry.live_tuples = 0;
        tabentry.dead_tuples = 0;
        tabentry.ins_since_vacuum = 0;
    }

    tabentry.live_tuples += counts.delta_live_tuples;
    tabentry.dead_tuples += counts.delta_dead_tuples;
    tabentry.mod_since_analyze += counts.changed_tuples;
    tabentry.ins_since_vacuum += counts.tuples_inserted;
    tabentry.blocks_fetched += counts.blocks_fetched;
    tabentry.blocks_hit += counts.blocks_hit;

    tabentry.live_tuples = tabentry.live_tuples.max(0);
    tabentry.dead_tuples = tabentry.dead_tuples.max(0);
}

pub(crate) fn flush_database(key: PgStat_HashKey, pending: &PgStat_StatDBEntry) {
    let mut store = SHARED_STATS.lock().unwrap();
    // See flush_relation: absent = dropped concurrently, discard.
    let Some(SharedEntry::Database(shared)) = store.get_mut(&key) else {
        return;
    };

    shared.xact_commit += pending.xact_commit;
    shared.xact_rollback += pending.xact_rollback;
    shared.blocks_fetched += pending.blocks_fetched;
    shared.blocks_hit += pending.blocks_hit;
    shared.tuples_returned += pending.tuples_returned;
    shared.tuples_fetched += pending.tuples_fetched;
    shared.tuples_inserted += pending.tuples_inserted;
    shared.tuples_updated += pending.tuples_updated;
    shared.tuples_deleted += pending.tuples_deleted;
    // last_autovac_time / checksum failures are reported immediately in C;
    // stat_reset_timestamp is never accumulated.
    debug_assert_eq!(pending.last_autovac_time, 0);
    debug_assert_eq!(pending.checksum_failures, 0);
    debug_assert_eq!(pending.last_checksum_failure, 0);
    shared.conflict_tablespace += pending.conflict_tablespace;
    shared.conflict_lock += pending.conflict_lock;
    shared.conflict_snapshot += pending.conflict_snapshot;
    shared.conflict_logicalslot += pending.conflict_logicalslot;
    shared.conflict_bufferpin += pending.conflict_bufferpin;
    shared.conflict_startup_deadlock += pending.conflict_startup_deadlock;
    shared.temp_bytes += pending.temp_bytes;
    shared.temp_files += pending.temp_files;
    shared.deadlocks += pending.deadlocks;
    shared.blk_read_time += pending.blk_read_time;
    shared.blk_write_time += pending.blk_write_time;
    shared.sessions += pending.sessions;
    shared.session_time += pending.session_time;
    shared.active_time += pending.active_time;
    shared.idle_in_transaction_time += pending.idle_in_transaction_time;
    shared.sessions_abandoned += pending.sessions_abandoned;
    shared.sessions_fatal += pending.sessions_fatal;
    shared.sessions_killed += pending.sessions_killed;
    shared.parallel_workers_to_launch += pending.parallel_workers_to_launch;
    shared.parallel_workers_launched += pending.parallel_workers_launched;
}

pub(crate) fn flush_function(key: PgStat_HashKey, pending: &crate::function::PgStat_FunctionCounts) {
    let mut store = SHARED_STATS.lock().unwrap();
    // See flush_relation: absent = dropped concurrently, discard.
    let Some(SharedEntry::Function(shared)) = store.get_mut(&key) else {
        return;
    };
    shared.numcalls += pending.numcalls;
    shared.total_time += pending.total_time / 1000;
    shared.self_time += pending.self_time / 1000;
}

pub(crate) fn flush_subscription(
    key: PgStat_HashKey,
    pending: &crate::subscription::PgStat_BackendSubEntry,
) {
    let mut store = SHARED_STATS.lock().unwrap();
    // See flush_relation: absent = dropped concurrently, discard.
    let Some(SharedEntry::Subscription(shared)) = store.get_mut(&key) else {
        return;
    };
    shared.apply_error_count += pending.apply_error_count;
    shared.sync_error_count += pending.sync_error_count;
    for i in 0..crate::subscription::CONFLICT_NUM_TYPES {
        shared.conflict_count[i] += pending.conflict_count[i];
    }
}

pub(crate) fn drop_entry(key: PgStat_HashKey) {
    let mut store = SHARED_STATS.lock().unwrap();
    let existed = store.remove(&key).is_some();
    // pgstat_drop_entry (pgstat_shmem.c:1027): database stats contain other
    // stats — dropping the database entry also drops every entry of that
    // dboid (pgstat_drop_database_and_contents). C only cascades when the
    // shared entry was found; mirror that.
    if existed && key.kind == PGSTAT_KIND_DATABASE {
        store.retain(|k, _| k.dboid != key.dboid);
    }
}

// pgstat_copy_relation_stats' shared-entry copy.
// pgstat_prep_pending_entry's pgstat_get_entry_ref(create=true) leg: C
// creates the zeroed shared-hash entry at first count, so readers observe it
// before any flush (pg_stat_get_function_calls = 0 mid-transaction,
// pg_stat_have_stats after an index scan, pgstat_copy_relation_stats source
// lookup after REINDEX CONCURRENTLY).
pub(crate) fn ensure_entry_for_pending(key: PgStat_HashKey) {
    use crate::pending::{
        PGSTAT_KIND_DATABASE as KD, PGSTAT_KIND_FUNCTION as KF, PGSTAT_KIND_RELATION as KR,
        PGSTAT_KIND_SUBSCRIPTION as KS,
    };
    let mut store = SHARED_STATS.lock().unwrap();
    store.entry(key).or_insert_with(|| match key.kind {
        KR => SharedEntry::Relation(PgStat_StatTabEntry::default()),
        KD => SharedEntry::Database(PgStat_StatDBEntry::default()),
        KF => SharedEntry::Function(Default::default()),
        KS => SharedEntry::Subscription(Default::default()),
        other => panic!("pending entry for unported stats kind {other:?}"),
    });
}

pub(crate) fn copy_entry(src: PgStat_HashKey, dst: PgStat_HashKey) {
    let mut store = SHARED_STATS.lock().unwrap();
    if let Some(&e) = store.get(&src) {
        store.insert(dst, e);
    }
}

pub(crate) fn update_relation_entry(key: PgStat_HashKey, f: impl FnOnce(&mut PgStat_StatTabEntry)) {
    let mut store = SHARED_STATS.lock().unwrap();
    let SharedEntry::Relation(tabentry) = store
        .entry(key)
        .or_insert(SharedEntry::Relation(PgStat_StatTabEntry::default()))
    else {
        unreachable!("relation key holds non-relation shared entry")
    };
    f(tabentry);
}

pub(crate) fn update_backend_entry(
    key: PgStat_HashKey,
    f: impl FnOnce(&mut crate::backend::PgStat_Backend),
) {
    let mut store = SHARED_STATS.lock().unwrap();
    let SharedEntry::Backend(entry) = store
        .entry(key)
        .or_insert(SharedEntry::Backend(Default::default()))
    else {
        unreachable!("backend key holds non-backend shared entry")
    };
    f(entry);
}

pub(crate) fn update_replslot_entry(
    key: PgStat_HashKey,
    f: impl FnOnce(&mut crate::replslot::PgStat_StatReplSlotEntry),
) {
    let mut store = SHARED_STATS.lock().unwrap();
    let SharedEntry::ReplSlot(entry) = store
        .entry(key)
        .or_insert(SharedEntry::ReplSlot(Default::default()))
    else {
        unreachable!("replslot key holds non-replslot shared entry")
    };
    f(entry);
}

pub(crate) fn update_subscription_entry(
    key: PgStat_HashKey,
    f: impl FnOnce(&mut crate::subscription::PgStat_StatSubEntry),
) {
    let mut store = SHARED_STATS.lock().unwrap();
    let SharedEntry::Subscription(entry) = store
        .entry(key)
        .or_insert(SharedEntry::Subscription(Default::default()))
    else {
        unreachable!("subscription key holds non-subscription shared entry")
    };
    f(entry);
}

pub(crate) fn update_database_entry(key: PgStat_HashKey, f: impl FnOnce(&mut PgStat_StatDBEntry)) {
    let mut store = SHARED_STATS.lock().unwrap();
    let SharedEntry::Database(dbentry) = store
        .entry(key)
        .or_insert(SharedEntry::Database(PgStat_StatDBEntry::default()))
    else {
        unreachable!("database key holds non-database shared entry")
    };
    f(dbentry);
}

// C force_stats_snapshot_clear (pgstat.c): a stats_fetch_consistency change
// discards the snapshot on the next access, not at assign time.
thread_local! {
    static FORCE_SNAPSHOT_CLEAR: Cell<bool> = const { Cell::new(false) };
}

pub(crate) fn force_snapshot_clear_on_next_access() {
    FORCE_SNAPSHOT_CLEAR.with(|c| c.set(true));
}

pub(crate) fn consume_forced_snapshot_clear() {
    if FORCE_SNAPSHOT_CLEAR.with(|c| c.replace(false)) {
        clear_snapshot();
    }
}

struct SnapshotState {
    mode: i32,
    snapshot_timestamp: TimestampTz,
    stats: HashMap<PgStat_HashKey, Option<SharedEntry>, FxBuildHasher>,
}

thread_local! {
    static SNAPSHOT: RefCell<SnapshotState> = RefCell::new(SnapshotState {
        mode: PGSTAT_FETCH_CONSISTENCY_NONE,
        snapshot_timestamp: 0,
        stats: HashMap::with_hasher(FxBuildHasher),
    });
}

pub(crate) fn build_snapshot() {
    consume_forced_snapshot_clear();
    let built = SNAPSHOT.with(|s| {
        let mut snap = s.borrow_mut();
        if snap.mode == PGSTAT_FETCH_CONSISTENCY_SNAPSHOT {
            return false;
        }
        debug_assert!(snap.stats.is_empty());
        let my_dboid = init_small::globals::MyDatabaseId();
        let store = SHARED_STATS.lock().unwrap();
        for (&key, &entry) in store.iter() {
            // database stats are accessed_across_databases in C
            if key.dboid != my_dboid
                && key.dboid != InvalidOid
                && key.kind != PGSTAT_KIND_DATABASE
            {
                continue;
            }
            snap.stats.insert(key, Some(entry));
        }
        snap.snapshot_timestamp = timestamp_seams::get_current_timestamp::call();
        snap.mode = PGSTAT_FETCH_CONSISTENCY_SNAPSHOT;
        true
    });
    // C's pgstat_build_snapshot copies every fixed-amount kind in the same pass.
    if built {
        crate::archiver::pgstat_archiver_snapshot_cb();
        crate::bgwriter::pgstat_bgwriter_snapshot_cb();
        crate::checkpointer::pgstat_checkpointer_snapshot_cb();
        crate::io::pgstat_io_snapshot_cb();
        crate::slru::pgstat_slru_snapshot_cb();
        crate::wal::pgstat_wal_snapshot_cb();
    }
}

pub fn pgstat_get_stat_snapshot_timestamp() -> Option<TimestampTz> {
    consume_forced_snapshot_clear();
    SNAPSHOT.with(|s| {
        let snap = s.borrow();
        (snap.mode == PGSTAT_FETCH_CONSISTENCY_SNAPSHOT).then_some(snap.snapshot_timestamp)
    })
}

// Returns an owned copy; C hands back a pointer into snapshot memory. The
// negative-lookup caching in CACHE mode mirrors C's entry->data = NULL.
pub(crate) fn fetch_entry(key: PgStat_HashKey) -> Option<SharedEntry> {
    consume_forced_snapshot_clear();
    let consistency = crate::pgstat_fetch_consistency();

    if consistency == PGSTAT_FETCH_CONSISTENCY_SNAPSHOT {
        build_snapshot();
    }

    if consistency > PGSTAT_FETCH_CONSISTENCY_NONE {
        let cached = SNAPSHOT.with(|s| s.borrow().stats.get(&key).copied());
        if let Some(entry) = cached {
            return entry;
        }
        if consistency == PGSTAT_FETCH_CONSISTENCY_SNAPSHOT {
            return None;
        }
    }

    let entry = SHARED_STATS.lock().unwrap().get(&key).copied();
    if consistency == PGSTAT_FETCH_CONSISTENCY_CACHE {
        SNAPSHOT.with(|s| {
            let mut snap = s.borrow_mut();
            snap.mode = PGSTAT_FETCH_CONSISTENCY_CACHE;
            snap.stats.insert(key, entry);
        });
    }
    entry
}

pub(crate) fn clear_snapshot() {
    SNAPSHOT.with(|s| {
        let mut snap = s.borrow_mut();
        snap.mode = PGSTAT_FETCH_CONSISTENCY_NONE;
        snap.stats.clear();
    });
    crate::archiver::pgstat_archiver_snapshot_clear();
    crate::bgwriter::pgstat_bgwriter_snapshot_clear();
    crate::checkpointer::pgstat_checkpointer_snapshot_clear();
    crate::io::pgstat_io_snapshot_clear();
    crate::slru::pgstat_slru_snapshot_clear();
    crate::wal::pgstat_wal_snapshot_clear();
}

pub fn pgstat_have_entry(kind: u32, dboid: types_core::Oid, objid: u64) -> bool {
    if (PGSTAT_KIND_ARCHIVER.0..=PGSTAT_KIND_WAL.0).contains(&kind) {
        return true;
    }
    let key = PgStat_HashKey {
        kind: crate::pending::PgStat_Kind(kind),
        dboid,
        objid,
    };
    // C creates the shared entry at pending-prep time, so unflushed pending
    // counts already answer true there; here shared entries appear at flush.
    SHARED_STATS.lock().unwrap().contains_key(&key)
        || crate::pending::pgstat_have_pending(key)
}

pub(crate) fn export_entries(mut f: impl FnMut(PgStat_HashKey, SharedEntry)) {
    let store = SHARED_STATS.lock().unwrap();
    for (&key, &entry) in store.iter() {
        f(key, entry);
    }
}

pub(crate) fn import_entry(key: PgStat_HashKey, entry: SharedEntry) {
    SHARED_STATS.lock().unwrap().insert(key, entry);
}

pub(crate) fn clear_all_entries() {
    SHARED_STATS.lock().unwrap().clear();
}

fn reset_entry_contents(entry: &mut SharedEntry, ts: TimestampTz) {
    match entry {
        SharedEntry::Relation(t) => *t = PgStat_StatTabEntry::default(),
        SharedEntry::Function(f) => *f = Default::default(),
        SharedEntry::Database(d) => {
            *d = PgStat_StatDBEntry::default();
            d.stat_reset_timestamp = ts;
        }
        SharedEntry::Backend(b) => {
            *b = Default::default();
            b.stat_reset_timestamp = ts;
        }
        SharedEntry::ReplSlot(r) => {
            *r = Default::default();
            r.stat_reset_timestamp = ts;
        }
        SharedEntry::Subscription(s) => {
            *s = Default::default();
            s.stat_reset_timestamp = ts;
        }
    }
}

fn reset_database_timestamp(store: &mut Store, dboid: types_core::Oid, ts: TimestampTz) {
    let key = PgStat_HashKey { kind: PGSTAT_KIND_DATABASE, dboid, objid: 0 };
    let SharedEntry::Database(d) =
        store.entry(key).or_insert(SharedEntry::Database(PgStat_StatDBEntry::default()))
    else {
        unreachable!("database key holds non-database shared entry")
    };
    d.stat_reset_timestamp = ts;
}

pub fn pgstat_reset_counters() {
    let ts = timestamp_seams::get_current_timestamp::call();
    let my_dboid = init_small::globals::MyDatabaseId();
    let mut store = SHARED_STATS.lock().unwrap();
    for (key, entry) in store.iter_mut() {
        if key.dboid == my_dboid {
            reset_entry_contents(entry, ts);
        }
    }
}

pub fn pgstat_reset(kind: PgStat_Kind, dboid: types_core::Oid, objid: u64) {
    let ts = timestamp_seams::get_current_timestamp::call();
    let mut store = SHARED_STATS.lock().unwrap();
    if let Some(entry) = store.get_mut(&PgStat_HashKey { kind, dboid, objid }) {
        reset_entry_contents(entry, ts);
    }
    // accessed_across_databases kinds (C's kind table) skip the db timestamp.
    if kind != PGSTAT_KIND_DATABASE
        && kind != PGSTAT_KIND_BACKEND
        && kind != PGSTAT_KIND_REPLSLOT
        && kind != PGSTAT_KIND_SUBSCRIPTION
    {
        reset_database_timestamp(&mut store, dboid, ts);
    }
}

pub fn pgstat_reset_of_kind(kind: PgStat_Kind) {
    let ts = timestamp_seams::get_current_timestamp::call();
    match kind {
        PGSTAT_KIND_DATABASE | PGSTAT_KIND_RELATION | PGSTAT_KIND_FUNCTION
        | PGSTAT_KIND_BACKEND | PGSTAT_KIND_REPLSLOT | PGSTAT_KIND_SUBSCRIPTION => {
            let mut store = SHARED_STATS.lock().unwrap();
            for (key, entry) in store.iter_mut() {
                if key.kind == kind {
                    reset_entry_contents(entry, ts);
                }
            }
        }
        PGSTAT_KIND_ARCHIVER => crate::archiver::pgstat_archiver_reset_all_cb(ts),
        PGSTAT_KIND_BGWRITER => crate::bgwriter::pgstat_bgwriter_reset_all_cb(ts),
        PGSTAT_KIND_CHECKPOINTER => crate::checkpointer::pgstat_checkpointer_reset_all_cb(ts),
        PGSTAT_KIND_IO => crate::io::pgstat_io_reset_all_cb(ts),
        PGSTAT_KIND_SLRU => crate::slru::pgstat_slru_reset_all_cb(ts),
        PGSTAT_KIND_WAL => crate::wal::pgstat_wal_reset_all_cb(ts),
        _ => panic!(
            "pgstat_reset_of_kind (pgstat.c:875): shared stats for kind {:?} unported",
            kind
        ),
    }
}
