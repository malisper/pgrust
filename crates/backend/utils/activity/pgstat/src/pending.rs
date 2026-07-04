// pgstat.c core: the backend-local pending-entry model (pgStatPending +
// pgStatEntryRefHash collapse into one key->pending map, so the report_stat
// "anything to do?" gate is one O(1) is_empty load — C's dlist_is_empty) and
// pgstat_report_stat's flush batching.

use core::cell::{Cell, RefCell};
use core::mem::ManuallyDrop;

use hashbrown::hash_map::Entry;
use mcx::{Mcx, MemoryContext, PgHashMap, PgVec};
use types_core::{Oid, TimestampTz};

use crate::database;
use crate::relation::{PgStat_TableCounts, PgStat_TableStatus};
use crate::slru;
use crate::xact::PgStat_SubXactStatus;

#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub struct PgStat_Kind(pub u32);

pub const PGSTAT_KIND_INVALID: PgStat_Kind = PgStat_Kind(0);
pub const PGSTAT_KIND_DATABASE: PgStat_Kind = PgStat_Kind(1);
pub const PGSTAT_KIND_RELATION: PgStat_Kind = PgStat_Kind(2);
pub const PGSTAT_KIND_FUNCTION: PgStat_Kind = PgStat_Kind(3);
pub const PGSTAT_KIND_REPLSLOT: PgStat_Kind = PgStat_Kind(4);
pub const PGSTAT_KIND_SUBSCRIPTION: PgStat_Kind = PgStat_Kind(5);
pub const PGSTAT_KIND_BACKEND: PgStat_Kind = PgStat_Kind(6);
pub const PGSTAT_KIND_ARCHIVER: PgStat_Kind = PgStat_Kind(7);
pub const PGSTAT_KIND_BGWRITER: PgStat_Kind = PgStat_Kind(8);
pub const PGSTAT_KIND_CHECKPOINTER: PgStat_Kind = PgStat_Kind(9);
pub const PGSTAT_KIND_IO: PgStat_Kind = PgStat_Kind(10);
pub const PGSTAT_KIND_SLRU: PgStat_Kind = PgStat_Kind(11);
pub const PGSTAT_KIND_WAL: PgStat_Kind = PgStat_Kind(12);

#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub struct PgStat_HashKey {
    pub kind: PgStat_Kind,
    pub dboid: Oid,
    pub objid: u64,
}

pub enum PendingData {
    Relation(PgStat_TableStatus),
    Database(database::PgStat_StatDBEntry),
    Function(crate::function::PgStat_FunctionCounts),
}

pub const PGSTAT_ENTRY_REF_HASH_SIZE: usize = 128;

pub struct PgStatState {
    pub(crate) ctx: &'static MemoryContext,
    pub(crate) pending: PgHashMap<'static, PgStat_HashKey, PendingData>,
    // pgStatPending's insertion order; stale keys (deleted outside a flush
    // pass) are skipped and swept at the end of each flush pass.
    pub(crate) pending_order: PgVec<'static, PgStat_HashKey>,
    pub(crate) xact_stack: PgVec<'static, PgStat_SubXactStatus>,
}

impl PgStatState {
    pub(crate) fn prep_pending_entry(&mut self, key: PgStat_HashKey) -> &mut PendingData {
        let mcx = self.ctx.mcx();
        match self.pending.entry(key) {
            Entry::Occupied(e) => e.into_mut(),
            Entry::Vacant(v) => {
                self.pending_order.push(key);
                // Stale-true on a test-local state is harmless (see HAVE_PENDING).
                HAVE_PENDING.with(|c| c.set(true));
                v.insert(new_pending_data(key, mcx))
            }
        }
    }

    pub(crate) fn delete_pending_entry(&mut self, key: PgStat_HashKey) -> bool {
        self.pending.remove(&key).is_some()
    }

    pub(crate) fn have_pending(&self, key: PgStat_HashKey) -> bool {
        self.pending.contains_key(&key)
    }
}

fn new_pending_data(key: PgStat_HashKey, mcx: Mcx<'static>) -> PendingData {
    if key.kind == PGSTAT_KIND_RELATION {
        PendingData::Relation(PgStat_TableStatus::new(mcx))
    } else if key.kind == PGSTAT_KIND_DATABASE {
        PendingData::Database(database::PgStat_StatDBEntry::default())
    } else if key.kind == PGSTAT_KIND_FUNCTION {
        PendingData::Function(crate::function::PgStat_FunctionCounts::default())
    } else {
        panic!("pending entry for unported stats kind {:?}", key.kind)
    }
}

thread_local! {
    static STATE: RefCell<Option<ManuallyDrop<PgStatState>>> = const { RefCell::new(None) };
    static REPORT_FIXED: Cell<bool> = const { Cell::new(false) };
    static FORCE_NEXT_FLUSH: Cell<bool> = const { Cell::new(false) };
    static PENDING_SINCE: Cell<TimestampTz> = const { Cell::new(0) };
    static LAST_FLUSH: Cell<TimestampTz> = const { Cell::new(0) };
    // Mirror of !pending.is_empty(), C's bare dlist_is_empty(&pgStatPending)
    // load: the nothing-pending statement must not pay the STATE RefCell
    // borrow. Contract: never false while pending is non-empty; stale TRUE
    // falls through to the real check (which then re-clears it).
    static HAVE_PENDING: Cell<bool> = const { Cell::new(false) };
}

pub(crate) fn with_state<R>(f: impl FnOnce(&mut PgStatState) -> R) -> R {
    STATE.with(|s| {
        let mut slot = s.borrow_mut();
        let st = slot.get_or_insert_with(|| {
            // C's lazily-created pgStatPendingContext; leaked: backend-lifetime.
            let ctx: &'static MemoryContext =
                Box::leak(Box::new(MemoryContext::new("PgStat Pending")));
            let m = ctx.mcx();
            ManuallyDrop::new(PgStatState {
                ctx,
                pending: PgHashMap::with_capacity_in(PGSTAT_ENTRY_REF_HASH_SIZE, m),
                pending_order: PgVec::new_in(m),
                xact_stack: PgVec::new_in(m),
            })
        });
        f(st)
    })
}

pub fn pgstat_have_pending(key: PgStat_HashKey) -> bool {
    STATE.with(|s| s.borrow().as_ref().is_some_and(|st| st.have_pending(key)))
}

fn pending_is_empty() -> bool {
    STATE.with(|s| s.borrow().as_ref().is_none_or(|st| st.pending.is_empty()))
}

pub fn pgstat_report_fixed_set() {
    REPORT_FIXED.with(|c| c.set(true));
}

pub fn pgstat_report_fixed() -> bool {
    REPORT_FIXED.with(|c| c.get())
}

pub fn pgstat_force_next_flush() {
    FORCE_NEXT_FLUSH.with(|c| c.set(true));
}

pub fn pgstat_clear_snapshot() {
    crate::shmem::clear_snapshot();
    backend_status_seams::pgstat_clear_backend_status_snapshot::call();
}

pub const PGSTAT_MIN_INTERVAL: i64 = 1000;
pub const PGSTAT_MAX_INTERVAL: i64 = 60000;
pub const PGSTAT_IDLE_INTERVAL: i64 = 10000;

fn timestamp_difference_exceeds(start: TimestampTz, stop: TimestampTz, msec: i64) -> bool {
    stop - start >= msec * 1000
}

pub fn pgstat_report_stat(mut force: bool) -> i64 {
    debug_assert!(!xact_seams::is_transaction_or_transaction_block::call());

    // C's early-exit shape: one TLS block, plain loads, nothing-pending returns 0.
    if FORCE_NEXT_FLUSH.with(|c| c.get()) {
        FORCE_NEXT_FLUSH.with(|c| c.set(false));
        force = true;
    }

    if !HAVE_PENDING.with(|c| c.get()) && !pgstat_report_fixed() {
        return 0;
    }

    report_stat_slow(force)
}

// Outlined so the nothing-pending exit stays frameless (the 39-v-7 regression).
#[cold]
#[inline(never)]
fn report_stat_slow(mut force: bool) -> i64 {
    if pending_is_empty() && !pgstat_report_fixed() {
        HAVE_PENDING.with(|c| c.set(false));
        return 0;
    }

    let now;
    if force {
        now = timestamp_seams::get_current_timestamp::call();
    } else {
        now = xact_seams::get_current_transaction_stop_timestamp::call();
        let pending_since = PENDING_SINCE.with(|c| c.get());
        let last_flush = LAST_FLUSH.with(|c| c.get());
        if pending_since > 0
            && timestamp_difference_exceeds(pending_since, now, PGSTAT_MAX_INTERVAL)
        {
            force = true;
        } else if last_flush > 0
            && !timestamp_difference_exceeds(last_flush, now, PGSTAT_MIN_INTERVAL)
        {
            if pending_since == 0 {
                PENDING_SINCE.with(|c| c.set(now));
            }
            return PGSTAT_IDLE_INTERVAL;
        }
    }

    database::pgstat_update_dbstats(now);

    let nowait = !force;
    let mut partial_flush = pgstat_flush_pending_entries(nowait);
    // flush_static_cbs in kind order: BACKEND, IO, SLRU, WAL (pgstat.c:783).
    if pgstat_report_fixed() {
        partial_flush |= crate::backend::pgstat_backend_flush_cb(nowait);
        partial_flush |= crate::io::pgstat_io_flush_cb(nowait);
        partial_flush |= slru::pgstat_slru_flush_cb(nowait);
        partial_flush |= crate::wal::pgstat_wal_flush_cb(nowait);
    }

    LAST_FLUSH.with(|c| c.set(now));

    if partial_flush {
        debug_assert!(!force);
        PENDING_SINCE.with(|c| {
            if c.get() == 0 {
                c.set(now);
            }
        });
        return PGSTAT_IDLE_INTERVAL;
    }

    PENDING_SINCE.with(|c| c.set(0));
    REPORT_FIXED.with(|c| c.set(false));
    HAVE_PENDING.with(|c| c.set(false));
    0
}

// pgstat_flush_pending_entries: relation entries fold into their database's
// pending entry (pgstat_relation_flush_cb's tail), which this same pass then
// flushes (C's append-during-iteration dlist walk). Local flush never
// contends, so this never reports partial.
pub(crate) fn pgstat_flush_pending_entries(_nowait: bool) -> bool {
    with_state(|st| {
        let mut i = 0;
        while i < st.pending_order.len() {
            let key = st.pending_order[i];
            i += 1;
            if key.kind == PGSTAT_KIND_RELATION {
                let Some(PendingData::Relation(tab)) = st.pending.remove(&key) else {
                    continue;
                };
                // Ignore entries that never accumulated counts (planner-only opens).
                if tab.counts == PgStat_TableCounts::default() {
                    continue;
                }
                crate::shmem::flush_relation(key, &tab.counts);
                flush_relation_into_db(st, key.dboid, &tab.counts);
            } else if key.kind == PGSTAT_KIND_FUNCTION {
                let Some(PendingData::Function(f)) = st.pending.remove(&key) else {
                    continue;
                };
                crate::shmem::flush_function(key, &f);
            } else {
                debug_assert_eq!(key.kind, PGSTAT_KIND_DATABASE);
                let Some(PendingData::Database(db)) = st.pending.remove(&key) else {
                    continue;
                };
                crate::shmem::flush_database(key, &db);
            }
        }
        st.pending_order.clear();
        debug_assert!(st.pending.is_empty());
        false
    })
}

pub(crate) fn flush_relation_into_db(
    st: &mut PgStatState,
    dboid: Oid,
    counts: &PgStat_TableCounts,
) {
    let dbentry = database::pgstat_prep_database_pending_in(st, dboid);
    dbentry.tuples_returned += counts.tuples_returned;
    dbentry.tuples_fetched += counts.tuples_fetched;
    dbentry.tuples_inserted += counts.tuples_inserted;
    dbentry.tuples_updated += counts.tuples_updated;
    dbentry.tuples_deleted += counts.tuples_deleted;
    dbentry.blocks_fetched += counts.blocks_fetched;
    dbentry.blocks_hit += counts.blocks_hit;
}
