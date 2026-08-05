// pgstat_relation.c — relation counting keyed by (dboid, relid); the relcache
// carries only `pgstat_enabled` (checked by callers, C's macro branch), and
// C's rel->pgstat_info/trans/parent pointer chases become by-key map access.
// The per-table trans/upper chain is a per-table stack vec (innermost last);
// each xact level's `first` list carries table keys.

use init_small::globals::MyDatabaseId;
use mcx::{Mcx, PgVec};
use types_core::{InvalidOid, Oid};
use types_rel::{RELKIND_HAS_STORAGE, RELKIND_PARTITIONED_TABLE};

use crate::pending::{self, PendingData, PgStatState, PgStat_HashKey, PGSTAT_KIND_RELATION};
use crate::xact;
use crate::PgStat_Counter;

#[derive(Clone, Copy, Default, PartialEq, Debug)]
pub struct PgStat_TableCounts {
    pub numscans: PgStat_Counter,
    pub tuples_returned: PgStat_Counter,
    pub tuples_fetched: PgStat_Counter,
    pub tuples_inserted: PgStat_Counter,
    pub tuples_updated: PgStat_Counter,
    pub tuples_deleted: PgStat_Counter,
    pub tuples_hot_updated: PgStat_Counter,
    pub tuples_newpage_updated: PgStat_Counter,
    pub truncdropped: bool,
    pub delta_live_tuples: PgStat_Counter,
    pub delta_dead_tuples: PgStat_Counter,
    pub changed_tuples: PgStat_Counter,
    pub blocks_fetched: PgStat_Counter,
    pub blocks_hit: PgStat_Counter,
}

#[derive(Clone, Copy, Default, Debug)]
pub struct PgStat_TableXactStatus {
    pub tuples_inserted: PgStat_Counter,
    pub tuples_updated: PgStat_Counter,
    pub tuples_deleted: PgStat_Counter,
    pub truncdropped: bool,
    pub inserted_pre_truncdrop: PgStat_Counter,
    pub updated_pre_truncdrop: PgStat_Counter,
    pub deleted_pre_truncdrop: PgStat_Counter,
    pub nest_level: i32,
}

pub struct PgStat_TableStatus {
    pub id: Oid,
    pub shared: bool,
    pub trans: PgVec<'static, PgStat_TableXactStatus>,
    pub counts: PgStat_TableCounts,
}

impl PgStat_TableStatus {
    pub(crate) fn new(mcx: Mcx<'static>) -> Self {
        PgStat_TableStatus {
            id: InvalidOid,
            shared: false,
            trans: PgVec::new_in(mcx),
            counts: PgStat_TableCounts::default(),
        }
    }
}

pub(crate) fn relation_key(relid: Oid, relisshared: bool) -> PgStat_HashKey {
    PgStat_HashKey {
        kind: PGSTAT_KIND_RELATION,
        dboid: if relisshared {
            InvalidOid
        } else {
            MyDatabaseId()
        },
        objid: relid as u64,
    }
}

pub fn pgstat_init_relation(_relid: Oid, relkind: u8) -> bool {
    if !RELKIND_HAS_STORAGE(relkind) && relkind != RELKIND_PARTITIONED_TABLE {
        return false;
    }
    // C also unlinks rel->pgstat_info here; the keyed model has no link, and
    // the count paths gate on pgstat_enabled, never on stale pending presence.
    crate::pgstat_track_counts()
}

pub fn pgstat_assoc_relation(relid: Oid, relisshared: bool) {
    pending::with_state(|st| {
        pgstat_prep_relation_pending(st, relid, relisshared);
    });
}

pub fn pgstat_unlink_relation(_relid: Oid, _relisshared: bool) {}

// Both helpers reborrow through the RelationPendingPtr allocation root (never
// through an intermediate reference), so relcache-cached count links derived
// from the same root stay aliasing-clean. Sound: the allocation outlives the
// returned borrow (freed only via removal paths, which need &mut PgStatState).
fn pgstat_prep_relation_pending<'a>(
    st: &'a mut PgStatState,
    relid: Oid,
    relisshared: bool,
) -> &'a mut PgStat_TableStatus {
    let key = relation_key(relid, relisshared);
    match st.prep_pending_entry(key) {
        PendingData::Relation(rp) => {
            let t = unsafe { &mut *rp.as_ptr() };
            t.id = relid;
            t.shared = relisshared;
            t
        }
        _ => unreachable!("relation key holds non-relation pending data"),
    }
}

fn table_mut<'a>(st: &'a mut PgStatState, key: PgStat_HashKey) -> &'a mut PgStat_TableStatus {
    match st.pending.get_mut(&key) {
        Some(PendingData::Relation(rp)) => unsafe { &mut *rp.as_ptr() },
        _ => unreachable!("relation pending entry vanished"),
    }
}

fn ensure_tabstat_xact_level(st: &mut PgStatState, relid: Oid, relisshared: bool) {
    let nest_level = xact_seams::get_current_transaction_nest_level::call();
    let key = relation_key(relid, relisshared);
    let need = {
        let t = pgstat_prep_relation_pending(st, relid, relisshared);
        t.trans.last().is_none_or(|tr| tr.nest_level != nest_level)
    };
    if need {
        xact::pgstat_get_xact_stack_level_mut(st, nest_level)
            .first
            .push(key);
        table_mut(st, key).trans.push(PgStat_TableXactStatus {
            nest_level,
            ..Default::default()
        });
    }
}

fn save_truncdrop_counters(trans: &mut PgStat_TableXactStatus, is_drop: bool) {
    if !trans.truncdropped || is_drop {
        trans.inserted_pre_truncdrop = trans.tuples_inserted;
        trans.updated_pre_truncdrop = trans.tuples_updated;
        trans.deleted_pre_truncdrop = trans.tuples_deleted;
        trans.truncdropped = true;
    }
}

fn restore_truncdrop_counters(trans: &mut PgStat_TableXactStatus) {
    if trans.truncdropped {
        trans.tuples_inserted = trans.inserted_pre_truncdrop;
        trans.tuples_updated = trans.updated_pre_truncdrop;
        trans.tuples_deleted = trans.deleted_pre_truncdrop;
    }
}

pub fn pgstat_count_heap_insert(relid: Oid, relisshared: bool, n: PgStat_Counter) {
    pending::with_state(|st| {
        ensure_tabstat_xact_level(st, relid, relisshared);
        let t = table_mut(st, relation_key(relid, relisshared));
        t.trans.last_mut().unwrap().tuples_inserted += n;
    });
}

pub fn pgstat_count_heap_update(relid: Oid, relisshared: bool, hot: bool, newpage: bool) {
    debug_assert!(!(hot && newpage));
    pending::with_state(|st| {
        ensure_tabstat_xact_level(st, relid, relisshared);
        let t = table_mut(st, relation_key(relid, relisshared));
        t.trans.last_mut().unwrap().tuples_updated += 1;
        if hot {
            t.counts.tuples_hot_updated += 1;
        } else if newpage {
            t.counts.tuples_newpage_updated += 1;
        }
    });
}

pub fn pgstat_count_heap_delete(relid: Oid, relisshared: bool) {
    pending::with_state(|st| {
        ensure_tabstat_xact_level(st, relid, relisshared);
        let t = table_mut(st, relation_key(relid, relisshared));
        t.trans.last_mut().unwrap().tuples_deleted += 1;
    });
}

pub fn pgstat_count_truncate(relid: Oid, relisshared: bool) {
    pending::with_state(|st| {
        ensure_tabstat_xact_level(st, relid, relisshared);
        let t = table_mut(st, relation_key(relid, relisshared));
        let trans = t.trans.last_mut().unwrap();
        save_truncdrop_counters(trans, false);
        trans.tuples_inserted = 0;
        trans.tuples_updated = 0;
        trans.tuples_deleted = 0;
    });
}

pub fn pgstat_update_heap_dead_tuples(relid: Oid, relisshared: bool, delta: i32) {
    with_counts(relid, relisshared, |c| {
        c.delta_dead_tuples -= delta as PgStat_Counter;
    });
}

// The pgstat.h count macros. The caller holds C's pgstat_enabled branch (the
// relcache Cell); these are the pgstat_info-side add, one map probe with the
// lazy assoc folded into the same probe (fabled #368 single-probe shape).
fn with_counts(relid: Oid, relisshared: bool, f: impl FnOnce(&mut PgStat_TableCounts)) {
    pending::with_state(|st| {
        f(&mut pgstat_prep_relation_pending(st, relid, relisshared).counts);
    });
}

pub fn pgstat_count_heap_scan(relid: Oid, relisshared: bool) {
    with_counts(relid, relisshared, |c| c.numscans += 1);
}

// Drain of heapam's per-scan batched counters (one probe per scan, not per row).
pub fn pgstat_count_heap_scan_batched(
    relid: Oid,
    relisshared: bool,
    numscans: u64,
    tuples_returned: u64,
) {
    if numscans == 0 && tuples_returned == 0 {
        return;
    }
    with_counts(relid, relisshared, |c| {
        c.numscans += numscans as PgStat_Counter;
        c.tuples_returned += tuples_returned as PgStat_Counter;
    });
}

pub fn pgstat_count_heap_getnext(relid: Oid, relisshared: bool) {
    with_counts(relid, relisshared, |c| c.tuples_returned += 1);
}

pub fn pgstat_count_heap_fetch(relid: Oid, relisshared: bool) {
    with_counts(relid, relisshared, |c| c.tuples_fetched += 1);
}

pub fn pgstat_count_index_scan(relid: Oid, relisshared: bool) {
    with_counts(relid, relisshared, |c| c.numscans += 1);
}

pub fn pgstat_count_index_tuples(relid: Oid, relisshared: bool, n: PgStat_Counter) {
    with_counts(relid, relisshared, |c| c.tuples_returned += n);
}

// Drain of indexam's per-scan batched counters (the pgstat_count_index_scan /
// pgstat_count_index_tuples / pgstat_count_heap_fetch macros, one probe per
// scan). heap_fetches is idx_tup_fetch: C counts it against the index rel.
pub fn pgstat_count_index_scan_batched(
    relid: Oid,
    relisshared: bool,
    numscans: u64,
    tuples_returned: u64,
    tuples_fetched: u64,
) {
    if numscans == 0 && tuples_returned == 0 && tuples_fetched == 0 {
        return;
    }
    with_counts(relid, relisshared, |c| {
        c.numscans += numscans as PgStat_Counter;
        c.tuples_returned += tuples_returned as PgStat_Counter;
        c.tuples_fetched += tuples_fetched as PgStat_Counter;
    });
}

pub fn pgstat_count_buffer_read(relid: Oid, relisshared: bool) {
    with_counts(relid, relisshared, |c| c.blocks_fetched += 1);
}

pub fn pgstat_count_buffer_hit(relid: Oid, relisshared: bool) {
    with_counts(relid, relisshared, |c| c.blocks_hit += 1);
}

// C rel->pgstat_info link (pgstat_relation.c pgstat_assoc_relation): the
// relcache caches (gen, counts ptr) so per-buffer-access counting is a
// pointer bump, not a map probe. Validity contract: the pointer may be
// dereferenced only while pgstat_relation_link_gen() equals the gen stored
// beside it (pending.rs RELATION_PENDING_GEN).
pub fn pgstat_relation_link_gen() -> u64 {
    pending::relation_pending_gen()
}

pub fn pgstat_relation_link_counts(relid: Oid, relisshared: bool) -> *mut () {
    let key = relation_key(relid, relisshared);
    pending::with_state(|st| {
        pgstat_prep_relation_pending(st, relid, relisshared);
        match st.pending.get_mut(&key) {
            Some(PendingData::Relation(rp)) => {
                unsafe { &raw mut (*rp.as_ptr()).counts }.cast()
            }
            _ => unreachable!("relation pending entry vanished"),
        }
    })
}

/// Bump blocks_fetched (and blocks_hit) through a still-valid link.
///
/// # Safety
/// `counts` must come from `pgstat_relation_link_counts` and the caller must
/// have checked `pgstat_relation_link_gen()` against the gen captured with it.
pub unsafe fn pgstat_count_buffer_read_via(counts: *mut (), hit: bool) {
    let c = counts.cast::<PgStat_TableCounts>();
    unsafe {
        (*c).blocks_fetched += 1;
        if hit {
            (*c).blocks_hit += 1;
        }
    }
}

pub fn pgstat_create_relation(relid: Oid, relisshared: bool) {
    xact::pgstat_create_transactional(
        PGSTAT_KIND_RELATION,
        if relisshared {
            InvalidOid
        } else {
            MyDatabaseId()
        },
        relid as u64,
    );
}

pub fn pgstat_drop_relation(relid: Oid, relisshared: bool) {
    let nest_level = xact_seams::get_current_transaction_nest_level::call();
    xact::pgstat_drop_transactional(
        PGSTAT_KIND_RELATION,
        if relisshared {
            InvalidOid
        } else {
            MyDatabaseId()
        },
        relid as u64,
    );

    let key = relation_key(relid, relisshared);
    pending::with_state(|st| {
        if !st.have_pending(key) {
            return;
        }
        // Transactionally zero counters so pg_stat_xact_all_tables shows 0.
        let t = table_mut(st, key);
        if let Some(trans) = t.trans.last_mut() {
            if trans.nest_level == nest_level {
                save_truncdrop_counters(trans, true);
                trans.tuples_inserted = 0;
                trans.tuples_updated = 0;
                trans.tuples_deleted = 0;
            }
        }
    });
}

// find_tabstat_entry: the copy's counts with live subxact i/u/d reconciled
// (C returns a palloc'd PgStat_TableStatus copy with trans cleared).
pub fn find_tabstat_entry(rel_id: Oid) -> Option<PgStat_TableCounts> {
    let local_key = PgStat_HashKey {
        kind: PGSTAT_KIND_RELATION,
        dboid: MyDatabaseId(),
        objid: rel_id as u64,
    };
    let shared_key = PgStat_HashKey {
        kind: PGSTAT_KIND_RELATION,
        dboid: InvalidOid,
        objid: rel_id as u64,
    };
    pending::with_state(|st| {
        let key = if st.have_pending(local_key) {
            local_key
        } else if st.have_pending(shared_key) {
            shared_key
        } else {
            return None;
        };
        let t = table_mut(st, key);
        let mut counts = t.counts;
        for trans in &t.trans {
            counts.tuples_inserted += trans.tuples_inserted;
            counts.tuples_updated += trans.tuples_updated;
            counts.tuples_deleted += trans.tuples_deleted;
        }
        Some(counts)
    })
}

// pgstat_copy_relation_stats (pgstat_relation.c): REINDEX CONCURRENTLY swap.
pub fn pgstat_copy_relation_stats(
    dst_relid: Oid,
    dst_shared: bool,
    src_relid: Oid,
    src_shared: bool,
) {
    crate::shmem::copy_entry(
        relation_key(src_relid, src_shared),
        relation_key(dst_relid, dst_shared),
    );
}

fn timestamp_difference_milliseconds(
    start: types_core::TimestampTz,
    stop: types_core::TimestampTz,
) -> PgStat_Counter {
    if start >= stop {
        return 0;
    }
    let Some(diff) = stop.checked_sub(start) else {
        return i32::MAX as PgStat_Counter;
    };
    if diff >= i32::MAX as i64 * 1000 - 999 {
        i32::MAX as PgStat_Counter
    } else {
        (diff + 999) / 1000
    }
}

fn am_autovacuum_worker() -> bool {
    miscinit::GetMyBackendType() == types_core::BackendType::AutovacWorker
}

pub fn pgstat_report_vacuum(
    tableoid: Oid,
    shared: bool,
    livetuples: PgStat_Counter,
    deadtuples: PgStat_Counter,
    starttime: types_core::TimestampTz,
) {
    if !crate::pgstat_track_counts() {
        return;
    }
    let ts = timestamp_seams::get_current_timestamp::call();
    let elapsedtime = timestamp_difference_milliseconds(starttime, ts);
    crate::shmem::update_relation_entry(relation_key(tableoid, shared), |tabentry| {
        tabentry.live_tuples = livetuples;
        tabentry.dead_tuples = deadtuples;
        tabentry.ins_since_vacuum = 0;
        if am_autovacuum_worker() {
            tabentry.last_autovacuum_time = ts;
            tabentry.autovacuum_count += 1;
            tabentry.total_autovacuum_time += elapsedtime;
        } else {
            tabentry.last_vacuum_time = ts;
            tabentry.vacuum_count += 1;
            tabentry.total_vacuum_time += elapsedtime;
        }
    });
    // C flushes IO stats here (pgstat_flush_io/pgstat_flush_backend): IO-stats lane.
}

pub fn pgstat_report_analyze(
    relid: Oid,
    relisshared: bool,
    relkind: u8,
    pgstat_enabled: bool,
    mut livetuples: PgStat_Counter,
    mut deadtuples: PgStat_Counter,
    resetcounter: bool,
    starttime: types_core::TimestampTz,
) {
    if !crate::pgstat_track_counts() {
        return;
    }

    // Subtract this transaction's own not-yet-flushed counts, else they'd be
    // double-counted at commit (C walks rel->pgstat_info->trans->upper).
    if pgstat_enabled && relkind != RELKIND_PARTITIONED_TABLE {
        let key = relation_key(relid, relisshared);
        pending::with_state(|st| {
            if !st.have_pending(key) {
                return;
            }
            let t = table_mut(st, key);
            for trans in &t.trans {
                livetuples -= trans.tuples_inserted - trans.tuples_deleted;
                deadtuples -= trans.tuples_updated + trans.tuples_deleted;
            }
            deadtuples -= t.counts.delta_dead_tuples;
        });
        livetuples = livetuples.max(0);
        deadtuples = deadtuples.max(0);
    }

    let ts = timestamp_seams::get_current_timestamp::call();
    let elapsedtime = timestamp_difference_milliseconds(starttime, ts);
    crate::shmem::update_relation_entry(relation_key(relid, relisshared), |tabentry| {
        tabentry.live_tuples = livetuples;
        tabentry.dead_tuples = deadtuples;
        if resetcounter {
            tabentry.mod_since_analyze = 0;
        }
        if am_autovacuum_worker() {
            tabentry.last_autoanalyze_time = ts;
            tabentry.autoanalyze_count += 1;
            tabentry.total_autoanalyze_time += elapsedtime;
        } else {
            tabentry.last_analyze_time = ts;
            tabentry.analyze_count += 1;
            tabentry.total_analyze_time += elapsedtime;
        }
    });
    // C flushes IO stats here (pgstat_flush_io/pgstat_flush_backend): IO-stats lane.
}

pub fn pgstat_fetch_stat_tabentry(relid: Oid) -> Option<crate::shmem::PgStat_StatTabEntry> {
    pgstat_fetch_stat_tabentry_ext(catalog_seams::is_shared_relation::call(relid), relid)
}

pub fn pgstat_fetch_stat_tabentry_ext(
    shared: bool,
    reloid: Oid,
) -> Option<crate::shmem::PgStat_StatTabEntry> {
    match crate::shmem::fetch_entry(relation_key(reloid, shared)) {
        Some(crate::shmem::SharedEntry::Relation(t)) => Some(t),
        Some(_) => unreachable!("relation key holds non-relation shared entry"),
        None => None,
    }
}

pub(crate) fn AtEOXact_PgStat_Relations(
    st: &mut PgStatState,
    xact_state: &xact::PgStat_SubXactStatus,
    isCommit: bool,
) {
    for &key in &xact_state.first {
        if !st.have_pending(key) {
            continue;
        }
        let tabstat = table_mut(st, key);
        let Some(mut trans) = tabstat.trans.pop() else {
            continue;
        };
        debug_assert_eq!(trans.nest_level, 1);
        debug_assert!(tabstat.trans.is_empty());

        if !isCommit {
            restore_truncdrop_counters(&mut trans);
        }
        tabstat.counts.tuples_inserted += trans.tuples_inserted;
        tabstat.counts.tuples_updated += trans.tuples_updated;
        tabstat.counts.tuples_deleted += trans.tuples_deleted;
        if isCommit {
            tabstat.counts.truncdropped = trans.truncdropped;
            if trans.truncdropped {
                // forget live/dead stats seen by backend thus far
                tabstat.counts.delta_live_tuples = 0;
                tabstat.counts.delta_dead_tuples = 0;
            }
            tabstat.counts.delta_live_tuples += trans.tuples_inserted - trans.tuples_deleted;
            tabstat.counts.delta_dead_tuples += trans.tuples_updated + trans.tuples_deleted;
            tabstat.counts.changed_tuples +=
                trans.tuples_inserted + trans.tuples_updated + trans.tuples_deleted;
        } else {
            // inserted tuples are dead, deleted tuples are unaffected
            tabstat.counts.delta_dead_tuples += trans.tuples_inserted + trans.tuples_updated;
        }
    }
}

pub(crate) fn AtEOSubXact_PgStat_Relations(
    st: &mut PgStatState,
    xact_state: &xact::PgStat_SubXactStatus,
    isCommit: bool,
    nestDepth: i32,
) {
    for &key in &xact_state.first {
        if !st.have_pending(key) {
            continue;
        }
        let mut push_to_parent = false;
        {
            let tabstat = table_mut(st, key);
            let Some(mut trans) = tabstat.trans.pop() else {
                continue;
            };
            debug_assert_eq!(trans.nest_level, nestDepth);

            if isCommit {
                let upper_is_immediate_parent = tabstat
                    .trans
                    .last()
                    .is_some_and(|u| u.nest_level == nestDepth - 1);
                if upper_is_immediate_parent {
                    let upper = tabstat.trans.last_mut().unwrap();
                    if trans.truncdropped {
                        // propagate truncate/drop one level up, replacing stats
                        save_truncdrop_counters(upper, false);
                        upper.tuples_inserted = trans.tuples_inserted;
                        upper.tuples_updated = trans.tuples_updated;
                        upper.tuples_deleted = trans.tuples_deleted;
                    } else {
                        upper.tuples_inserted += trans.tuples_inserted;
                        upper.tuples_updated += trans.tuples_updated;
                        upper.tuples_deleted += trans.tuples_deleted;
                    }
                } else {
                    // no immediate parent: re-stamp the node one level up and
                    // re-link it into the parent level's list
                    trans.nest_level = nestDepth - 1;
                    tabstat.trans.push(trans);
                    push_to_parent = true;
                }
            } else {
                restore_truncdrop_counters(&mut trans);
                tabstat.counts.tuples_inserted += trans.tuples_inserted;
                tabstat.counts.tuples_updated += trans.tuples_updated;
                tabstat.counts.tuples_deleted += trans.tuples_deleted;
                tabstat.counts.delta_dead_tuples += trans.tuples_inserted + trans.tuples_updated;
            }
        }
        if push_to_parent {
            xact::pgstat_get_xact_stack_level_mut(st, nestDepth - 1)
                .first
                .push(key);
        }
    }
}

pub(crate) fn PostPrepare_PgStat_Relations(
    st: &mut PgStatState,
    xact_state: &xact::PgStat_SubXactStatus,
) {
    for &key in &xact_state.first {
        if st.have_pending(key) {
            table_mut(st, key).trans.clear();
        }
    }
}

pub(crate) const TWOPHASE_RM_PGSTAT_ID: u8 = 2;

pub(crate) const SIZEOF_TWOPHASE_PGSTAT_RECORD: usize = 56;

fn pgstat_record_bytes(
    trans: &PgStat_TableXactStatus,
    id: Oid,
    shared: bool,
) -> [u8; SIZEOF_TWOPHASE_PGSTAT_RECORD] {
    let mut b = [0u8; SIZEOF_TWOPHASE_PGSTAT_RECORD];
    b[0..8].copy_from_slice(&trans.tuples_inserted.to_ne_bytes());
    b[8..16].copy_from_slice(&trans.tuples_updated.to_ne_bytes());
    b[16..24].copy_from_slice(&trans.tuples_deleted.to_ne_bytes());
    b[24..32].copy_from_slice(&trans.inserted_pre_truncdrop.to_ne_bytes());
    b[32..40].copy_from_slice(&trans.updated_pre_truncdrop.to_ne_bytes());
    b[40..48].copy_from_slice(&trans.deleted_pre_truncdrop.to_ne_bytes());
    b[48..52].copy_from_slice(&id.to_ne_bytes());
    b[52] = shared as u8;
    b[53] = trans.truncdropped as u8;
    b
}

struct TwoPhasePgStatRecord {
    tuples_inserted: PgStat_Counter,
    tuples_updated: PgStat_Counter,
    tuples_deleted: PgStat_Counter,
    inserted_pre_truncdrop: PgStat_Counter,
    updated_pre_truncdrop: PgStat_Counter,
    deleted_pre_truncdrop: PgStat_Counter,
    id: Oid,
    shared: bool,
    truncdropped: bool,
}

fn decode_pgstat_record(recdata: &[u8]) -> TwoPhasePgStatRecord {
    assert_eq!(recdata.len(), SIZEOF_TWOPHASE_PGSTAT_RECORD);
    let rd_i64 = |o: usize| i64::from_ne_bytes(recdata[o..o + 8].try_into().unwrap());
    TwoPhasePgStatRecord {
        tuples_inserted: rd_i64(0),
        tuples_updated: rd_i64(8),
        tuples_deleted: rd_i64(16),
        inserted_pre_truncdrop: rd_i64(24),
        updated_pre_truncdrop: rd_i64(32),
        deleted_pre_truncdrop: rd_i64(40),
        id: Oid::from_ne_bytes(recdata[48..52].try_into().unwrap()),
        shared: recdata[52] != 0,
        truncdropped: recdata[53] != 0,
    }
}

pub(crate) fn AtPrepare_PgStat_Relations(
    st: &mut PgStatState,
    xact_state: &xact::PgStat_SubXactStatus,
) -> types_error::PgResult<()> {
    for &key in &xact_state.first {
        if !st.have_pending(key) {
            continue;
        }
        let tabstat = table_mut(st, key);
        let Some(trans) = tabstat.trans.last() else {
            continue;
        };
        debug_assert_eq!(trans.nest_level, 1);
        let record = pgstat_record_bytes(trans, tabstat.id, tabstat.shared);
        twophase_seams::register_two_phase_record::call(TWOPHASE_RM_PGSTAT_ID, 0, &record)?;
    }
    Ok(())
}

pub fn pgstat_twophase_postcommit(
    _xid: types_core::TransactionId,
    _info: u16,
    recdata: &[u8],
) -> types_error::PgResult<()> {
    let rec = decode_pgstat_record(recdata);
    pending::with_state(|st| {
        let t = pgstat_prep_relation_pending(st, rec.id, rec.shared);
        t.counts.tuples_inserted += rec.tuples_inserted;
        t.counts.tuples_updated += rec.tuples_updated;
        t.counts.tuples_deleted += rec.tuples_deleted;
        t.counts.truncdropped = rec.truncdropped;
        if rec.truncdropped {
            t.counts.delta_live_tuples = 0;
            t.counts.delta_dead_tuples = 0;
        }
        t.counts.delta_live_tuples += rec.tuples_inserted - rec.tuples_deleted;
        t.counts.delta_dead_tuples += rec.tuples_updated + rec.tuples_deleted;
        t.counts.changed_tuples += rec.tuples_inserted + rec.tuples_updated + rec.tuples_deleted;
    });
    Ok(())
}

pub fn pgstat_twophase_postabort(
    _xid: types_core::TransactionId,
    _info: u16,
    recdata: &[u8],
) -> types_error::PgResult<()> {
    let mut rec = decode_pgstat_record(recdata);
    pending::with_state(|st| {
        let t = pgstat_prep_relation_pending(st, rec.id, rec.shared);
        if rec.truncdropped {
            rec.tuples_inserted = rec.inserted_pre_truncdrop;
            rec.tuples_updated = rec.updated_pre_truncdrop;
            rec.tuples_deleted = rec.deleted_pre_truncdrop;
        }
        t.counts.tuples_inserted += rec.tuples_inserted;
        t.counts.tuples_updated += rec.tuples_updated;
        t.counts.tuples_deleted += rec.tuples_deleted;
        t.counts.delta_dead_tuples += rec.tuples_inserted + rec.tuples_updated;
    });
    Ok(())
}
