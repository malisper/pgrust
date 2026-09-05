//! The objkv table access method: rows are key/value entries whose durable
//! form is immutable objects in an object store.
//!
//! Unlike heap: no pages, so no buffer or storage manager; no xmin/xmax, since
//! visibility follows the commit sequence number that wrote the row; no WAL,
//! because the numbered commit objects are both log and data. Writes buffer
//! per backend and are numbered and validated at pre-commit; one writer
//! thread then lands everything queued in one PUT, so a 1000-row INSERT
//! costs one PUT and so do eight transactions committing at once. Two
//! transactions writing one row do not block: the first wins, the second
//! gets 40001.
//!
//! Missing on purpose, and errors rather than pretending: TABLESAMPLE,
//! ANALYZE, parallel scan.

use std::cell::RefCell;
use std::collections::{BTreeMap, HashMap};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex, OnceLock};

use ::datum::Datum;
use ::objkv::commit::Op;
use ::objkv::db::{Db, Outcome};
use ::objkv::store::{MemStore, Store};
use ::types_core::xact::SubTransactionId;
use ::types_error::{PgError, PgResult};
use ::types_tuple::TupleDescData;

use ::mcx::Mcx;
use ::types_rel::Relation;
use ::types_slot::SlotData;
use ::types_snapshot::SnapshotData;

use ::types_tuple::{
    HeapTupleData, ItemPointerData, ItemPointerGetBlockNumber, ItemPointerGetOffsetNumber,
    ItemPointerSet,
};

use crate::{TableScanDesc, TableScanDescData};

/// Process-global: a thread per backend, so thread_local would give every
/// connection its own copy of the database.
static STORE: OnceLock<Arc<dyn Store>> = OnceLock::new();
static DB: Mutex<Option<Db>> = Mutex::new(None);
static NEXT_ROW: Mutex<Option<HashMap<(u32, u32), u64>>> = Mutex::new(None);
/// Live (bytes, rows) per relation: the local file is 0 bytes and ANALYZE is
/// unsupported, so without this the planner sees an empty table.
static REL_STATS: Mutex<Option<HashMap<(u32, u32), (u64, u64)>>> = Mutex::new(None);

/// One subtransaction's writes, keyed by id: a savepoint opened before our
/// first write delivers no START_SUB.
struct Frame {
    subid: SubTransactionId,
    /// Keyed by object key; the number is where in this transaction the write
    /// happened, which is how a TRUNCATE knows which staged rows it covers.
    writes: BTreeMap<Vec<u8>, (u64, Op)>,
}

thread_local! {
    /// This backend's uncommitted writes, outermost frame first. The
    /// process-global memtable would let one session read another's; a stack so
    /// ROLLBACK TO SAVEPOINT can discard part of it.
    static PENDING: RefCell<Vec<Frame>> = const { RefCell::new(Vec::new()) };

    static XACT_REGISTERED: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };

        static MY_COMMIT_SEQ: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };

    static XACT_SNAPSHOT: std::cell::Cell<u64> = const { std::cell::Cell::new(u64::MAX) };

}

/// Which table each objkv index belongs to, learned as they are used: the
/// collector needs it and cannot read pg_index while holding the storage lock.
/// An untouched index's entries are left alone.
static INDEX_TABLES: Mutex<Option<BTreeMap<u32, u32>>> = Mutex::new(None);

pub(crate) fn note_index_table(index: ::types_core::Oid, relid: ::types_core::Oid) {
    let mut g = INDEX_TABLES.lock().unwrap();
    g.get_or_insert_with(BTreeMap::new).insert(index, relid);
}

fn index_tables() -> BTreeMap<u32, u32> {
    INDEX_TABLES.lock().unwrap().clone().unwrap_or_default()
}

/// The oldest commit each backend is reading at; the collector must not
/// discard history one of these can ask for.
static IN_USE: Mutex<Option<BTreeMap<u64, u64>>> = Mutex::new(None);

fn my_slot() -> u64 {
    let t = std::thread::current().id();
    // ThreadId has no stable numeric form; a collision only makes the horizon
    // more conservative.
    use std::hash::{Hash, Hasher};
    let mut h = std::collections::hash_map::DefaultHasher::new();
    t.hash(&mut h);
    h.finish()
}

fn note_in_use(seq: u64) {
    // Armed here, not only where writes are: a read-only session takes no other
    // path, and its leftover read point froze collection for good.
    ensure_xact_callback();
    let mut g = IN_USE.lock().unwrap();
    let m = g.get_or_insert_with(BTreeMap::new);
    let e = m.entry(my_slot()).or_insert(u64::MAX);
    *e = (*e).min(seq);
}

fn release_in_use() {
    if let Some(m) = IN_USE.lock().unwrap().as_mut() {
        m.remove(&my_slot());
    }
}

fn oldest_in_use() -> u64 {
    IN_USE
        .lock()
        .unwrap()
        .as_ref()
        .and_then(|m| m.values().copied().min())
        .unwrap_or(u64::MAX)
}

/// Where objkv stands, republished so [`note_snapshot`] need not open the Db.
static SEQ_NOW: AtomicU64 = AtomicU64::new(0);
static DB_OPEN: AtomicBool = AtomicBool::new(false);

/// Stamps a snapshot with where objkv stands when it is taken, so a commit a
/// millisecond later cannot pass for one it should see; deciding at first read
/// makes every commit in between visible.
fn note_snapshot(sn: &SnapshotData<'static>) {
    if DB_OPEN.load(Ordering::Relaxed) {
        sn.am_commit_seq.set(SEQ_NOW.load(Ordering::Relaxed));
    }
}

pub fn snapshot_seq(snapshot: Option<&SnapshotData<'_>>) -> PgResult<u64> {
    let Some(sn) = snapshot else {
        return Ok(::objkv::key::LATEST);
    };
    if let Some(forced) = time_travel_seq() {
        return Ok(forced);
    }
    let seq = match sn.am_commit_seq.get() {
        0 => {
            let seq = with_db(|db| db.current_seq())?;
            sn.am_commit_seq.set(seq);
            seq
        }
        seq => seq,
    };
    // What writes validate against, and what collection must not go under.
    XACT_SNAPSHOT.set(XACT_SNAPSHOT.get().min(seq));
    note_in_use(seq);
    Ok(seq)
}

pub fn init_seams() {
    ::snapmgr::tap_snapshot_taken::install(note_snapshot);
}

/// Publishes what is confirmed on the way out. A Postgres commit object is
/// self-confirmed and needs none of this; the watermark records the point
/// for readers of the header chain, and drains asynchronous commits first.
fn publish_watermark_at_exit(_code: i32, _arg: usize) {
    // A backend that leaves without a transaction end must not pin the horizon.
    release_in_use();
    if !DB_OPEN.load(Ordering::Relaxed) {
        return;
    }
    // Whatever asynchronous commits are still queued go first, or the
    // watermark would stop below them and a clean shutdown would look like a
    // crash to the next open.
    drain_writes();
    if let Ok(Err(e)) = with_db_raw(|db| db.flush_watermark()) {
        eprintln!("objkv: could not publish the closing watermark: {e}");
    }
}

fn arm_exit_watermark() {
    if ::ipc_seams::on_proc_exit::is_installed() {
        ::ipc_seams::on_proc_exit::call(publish_watermark_at_exit, 0);
    }
}

/// How far back collection may reach. Held down by
/// `pgrust.objkv_retain_commits` (0 promises for ever) and by open reads.
fn collection_horizon(now: u64) -> u64 {
    let retain = ::guc_tables::vars::pgrust_objkv_retain_commits.read();
    if retain <= 0 {
        return 0;
    }
    now.saturating_sub(retain as u64).min(oldest_in_use())
}

fn time_travel_seq() -> Option<u64> {
    let v = ::guc_tables::vars::pgrust_objkv_snapshot_seq.read();
    (v > 0).then_some(v as u64)
}

/// Whether this session is reading history rather than the present.
///
/// It decides one thing: whether a read sees this transaction's own
/// uncommitted writes. It must, except when the read is deliberately of a past
/// they are not part of. This was once decided by "the snapshot is not
/// LATEST", which is true of an ordinary MVCC snapshot too -- so a transaction
/// that wrote and then read got the bucket without its own writes, and only
/// multi-statement transactions ever noticed.
pub fn reading_the_past() -> bool {
    time_travel_seq().is_some()
}

/// Counts staged writes, so a TRUNCATE can tell which of them it covers.
thread_local! {
    static STAGE_ORD: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
}

pub(crate) fn stage(key: Vec<u8>, op: Op) {
    ensure_xact_callback();
    let ord = STAGE_ORD.with(|c| {
        let n = c.get() + 1;
        c.set(n);
        n
    });
    let subid = ::xact::GetCurrentSubTransactionId();
    PENDING.with(|p| {
        let mut stack = p.borrow_mut();
        if stack.last().map(|f| f.subid) != Some(subid) {
            stack.push(Frame { subid, writes: BTreeMap::new() });
        }
        stack.last_mut().unwrap().writes.insert(key, (ord, op));
    });
}

/// Where the transaction's writes have got to, for a TRUNCATE to record.
pub(crate) fn stage_mark() -> u64 {
    STAGE_ORD.with(|c| c.get())
}

/// Records that this transaction decided something from the bucket at `seq`.
/// An insert-only transaction never takes an objkv snapshot, so without this
/// two inserts of one unique value both commit.
pub(crate) fn observe_read_at(seq: u64) {
    XACT_SNAPSHOT.set(XACT_SNAPSHOT.get().min(seq));
    note_in_use(seq);
}

/// What this transaction has staged for `key`, innermost frame first. A
/// uniqueness check needs it: two rows with one value write the same index
/// key, and the second would overwrite the first -- one entry, no error.
pub(crate) fn staged_op(key: &[u8]) -> Option<Op> {
    PENDING.with(|p| {
        p.borrow()
            .iter()
            .rev()
            .find_map(|f| f.writes.get(key).map(|(_, op)| op.clone()))
    })
}

/// Staged writes under `prefix`, outermost first so an inner one wins.
pub(crate) fn staged_prefix(prefix: &[u8]) -> BTreeMap<Vec<u8>, (u64, Op)> {
    PENDING.with(|p| {
        let mut out = BTreeMap::new();
        for f in p.borrow().iter() {
            for (k, (ord, op)) in f.writes.range(prefix.to_vec()..) {
                if !k.starts_with(prefix) {
                    break;
                }
                out.insert(k.clone(), (*ord, op.clone()));
            }
        }
        out
    })
}

pub(crate) fn staged_range(lo: &[u8], hi: &[u8]) -> BTreeMap<Vec<u8>, (u64, Op)> {
    PENDING.with(|p| {
        let mut out = BTreeMap::new();
        for f in p.borrow().iter() {
            for (k, (ord, op)) in f.writes.range(lo.to_vec()..hi.to_vec()) {
                out.insert(k.clone(), (*ord, op.clone()));
            }
        }
        out
    })
}

fn flatten_pending() -> BTreeMap<Vec<u8>, Op> {
    // Writes a TRUNCATE covered are dropped here, not when it ran, since until
    // now a savepoint could bring them back. Everything a transaction writes
    // shares one sequence number, so a row from before the truncate would
    // otherwise look like one from after it.
    let covered: Vec<(Vec<Vec<u8>>, u64)> = EMPTIED.with(|e| {
        e.borrow()
            .iter()
            .map(|(marker, since)| (covered_prefixes(marker), *since))
            .collect()
    });
    PENDING.with(|p| {
        let stack = std::mem::take(&mut *p.borrow_mut());
        let mut out = BTreeMap::new();
        for f in stack {
            for (k, (ord, op)) in f.writes {
                let dropped = covered.iter().any(|(prefixes, since)| {
                    ord <= *since && prefixes.iter().any(|pre| k.starts_with(pre))
                });
                if !dropped {
                    out.insert(k, op);
                }
            }
        }
        out
    })
}

/// What a truncation covers: rows, and any index's entries. No catalog.
fn covered_prefixes(marker: &[u8]) -> Vec<Vec<u8>> {
    // t/{db:08x}/{oid:08x}
    let mut parts = marker.split(|&b| b == b'/');
    let (Some(b"t"), Some(db), Some(oid)) = (parts.next(), parts.next(), parts.next()) else {
        return Vec::new();
    };
    let (db, oid) = (String::from_utf8_lossy(db), String::from_utf8_lossy(oid));
    vec![
        format!("{db}/{oid}/").into_bytes(),
        format!("{db}/u/{oid}/").into_bytes(),
        format!("{db}/i/{oid}/").into_bytes(),
    ]
}

fn ensure_xact_callback() {
    XACT_REGISTERED.with(|c| {
        if !c.get() {
            ::xact::RegisterXactCallback(objkv_xact_callback, Datum::null());
            ::xact::RegisterSubXactCallback(objkv_subxact_callback, Datum::null());
            arm_exit_watermark();
            c.set(true);
        }
    });
}

fn objkv_subxact_callback(
    event: ::types_core::xact::SubXactEvent,
    my_subid: SubTransactionId,
    parent_subid: SubTransactionId,
    _arg: Datum,
) -> PgResult<()> {
    use ::types_core::xact::SubXactEvent::*;
    match event {
        SUBXACT_EVENT_START_SUB | SUBXACT_EVENT_PRE_COMMIT_SUB => {}
        SUBXACT_EVENT_COMMIT_SUB => PENDING.with(|p| {
            let mut stack = p.borrow_mut();
            while stack.last().is_some_and(|f| f.subid == my_subid) {
                let f = stack.pop().unwrap();
                match stack.last_mut() {
                    Some(parent) => parent.writes.extend(f.writes),
                    None => stack.push(Frame { subid: parent_subid, writes: f.writes }),
                }
            }
        }),
        SUBXACT_EVENT_ABORT_SUB => {
            PENDING.with(|p| {
                let mut stack = p.borrow_mut();
                while stack.last().is_some_and(|f| f.subid >= my_subid) {
                    stack.pop();
                }
            });
            // A truncate in the rolled-back subtransaction goes with it: its
            // marker was one of the writes just dropped.
            EMPTIED.with(|e| {
                e.borrow_mut()
                    .retain(|k, _| PENDING.with(|p| p.borrow().iter().any(|f| f.writes.contains_key(k))));
            });
        }
    }
    Ok(())
}

fn objkv_xact_callback(
    event: ::types_core::xact::XactEvent,
    _arg: Datum,
) -> PgResult<()> {
    use ::types_core::xact::XactEvent::*;
    match event {
        // PRE_COMMIT: the last point a failed PUT can still abort the transaction.
        XACT_EVENT_PRE_COMMIT | XACT_EVENT_PARALLEL_PRE_COMMIT => at_pre_commit(),
        XACT_EVENT_ABORT | XACT_EVENT_PARALLEL_ABORT => {
            // An abort after pre-commit leaves an object nothing stands behind.
            let seq = MY_COMMIT_SEQ.replace(0);
            if seq != 0 {
                with_db_raw(|db| db.discard_staged(seq, ::objkv::db::Discard::Aborted))?;
            }
            discard_pending();
            forget_snapshots();
            forget_emptied();
            Ok(())
        }
        XACT_EVENT_COMMIT | XACT_EVENT_PARALLEL_COMMIT => {
            let seq = MY_COMMIT_SEQ.replace(0);
            if seq != 0 {
                with_db(|db| db.mark_confirmed(seq))?;
            }
            forget_snapshots();
            forget_emptied();
            Ok(())
        }
        XACT_EVENT_PRE_PREPARE | XACT_EVENT_PREPARE => {
            if PENDING.with(|p| p.borrow().iter().all(|f| f.writes.is_empty())) {
                Ok(())
            } else {
                Err(unsupported("PREPARE TRANSACTION"))
            }
        }
    }
}

/// One thread writes commit objects; everything else queues behind it.
///
/// A backend at pre-commit numbers and validates its writes under the storage
/// lock, then either waits for the writer to report its ticket durable or --
/// with `pgrust.objkv_async_commit` on -- carries on. The writer takes all
/// that is queued as one object, PUTs it with no lock held, and reports back.
/// The lock and condition variable here carry only the wake-ups; the queue
/// itself is in the [`Db`].
static SIGNAL: (Mutex<u64>, Condvar) = (Mutex::new(0), Condvar::new());
static WRITER: OnceLock<()> = OnceLock::new();
/// One thread per object that may be in flight. Each takes whatever is queued
/// when it wakes, so under load the flights pipeline instead of queueing
/// behind one round trip.
const WRITER_THREADS: usize = ::objkv::db::MAX_IN_FLIGHT;

fn signal() {
    let mut g = SIGNAL.0.lock().unwrap_or_else(|e| e.into_inner());
    *g += 1;
    SIGNAL.1.notify_all();
}

fn ensure_writer() {
    WRITER.get_or_init(|| {
        for i in 0..WRITER_THREADS {
            std::thread::Builder::new()
                .name(format!("objkv-writer-{i}"))
                .spawn(writer_loop)
                .expect("spawning an objkv writer thread");
        }
        std::thread::Builder::new()
            .name("objkv-compactor".into())
            .spawn(compactor_loop)
            .expect("spawning the objkv compactor thread");
    });
}

/// The compactor's wake-up, and the collection horizon the requesters have
/// asked for: the highest wins, and a request with none leaves it be.
static COMPACT: (Mutex<u64>, Condvar) = (Mutex::new(0), Condvar::new());
static HORIZON_WANTED: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

fn request_compaction(horizon: u64) {
    HORIZON_WANTED.fetch_max(horizon, Ordering::Relaxed);
    let mut g = COMPACT.0.lock().unwrap_or_else(|e| e.into_inner());
    *g += 1;
    COMPACT.1.notify_all();
}

/// Folds commits into runs whenever asked, with every GET, PUT and DELETE
/// made with no lock held: the plan and the swap are the only steps under
/// it, and both are memory-only. A fold that fails is logged and retried
/// on the next request; the data is already durable in its commit objects.
fn compactor_loop() {
    loop {
        {
            let g = COMPACT.0.lock().unwrap_or_else(|e| e.into_inner());
            let _ = COMPACT.1.wait_timeout(g, std::time::Duration::from_secs(1));
        }
        let horizon = HORIZON_WANTED.swap(0, Ordering::Relaxed);
        loop {
            let plan = match with_db_raw(|db| db.needs_compaction().then(|| db.fold_plan()).flatten()) {
                Ok(Some(p)) => p,
                Ok(None) => break,
                Err(e) => {
                    eprintln!("objkv compactor: {e}");
                    break;
                }
            };
            let tables = index_tables();
            let store = store();
            let folded = match ::objkv::db::build_fold(&plan, horizon, &tables)
                .and_then(|f| ::objkv::db::put_fold(&store, &f).map(|_| f))
            {
                Ok(f) => f,
                Err(e) => {
                    eprintln!("objkv compactor: fold failed, commit chain keeps growing: {e}");
                    break;
                }
            };
            let sweep = match with_db_raw(|db| db.apply_fold(plan, &folded, horizon)) {
                Ok(Ok(s)) => s,
                Ok(Err(e)) => {
                    eprintln!("objkv compactor: could not open the run it wrote: {e}");
                    break;
                }
                Err(e) => {
                    eprintln!("objkv compactor: {e}");
                    break;
                }
            };
            let result = ::objkv::db::execute_sweep(&store, sweep);
            let _ = with_db_raw(|db| db.sweep_done(result));
        }
    }
}

fn writer_loop() {
    const ATTEMPTS: u32 = 3;
    loop {
        let flight = {
            // Checked and waited on under the one lock, so a kick between
            // the check and the wait cannot be lost.
            let g = SIGNAL.0.lock().unwrap_or_else(|e| e.into_inner());
            match with_db_raw(|db| db.take_flight()) {
                Ok(Some(f)) => f,
                Ok(None) => {
                    let _ = SIGNAL.1.wait_timeout(g, std::time::Duration::from_secs(1));
                    continue;
                }
                Err(e) => {
                    eprintln!("objkv writer: {e}");
                    let _ = SIGNAL.1.wait_timeout(g, std::time::Duration::from_secs(1));
                    continue;
                }
            }
        };
        let mut attempt = 0;
        loop {
            attempt += 1;
            let done = match store().put_if_absent(&flight.key, &flight.bytes) {
                Ok(::objkv::s3::PutOutcome::Written) => with_db_raw(|db| db.flight_written(flight.first)).map(|_| true),
                Ok(::objkv::s3::PutOutcome::AlreadyExists) => {
                    with_db_raw(|db| db.flight_lost(&flight)).map(|r| {
                        if let Err(e) = r {
                            eprintln!("objkv writer: {e}");
                        }
                        true
                    })
                }
                Err(e) if attempt < ATTEMPTS => {
                    eprintln!("objkv writer: PUT of {} failed ({e}); retrying", flight.key);
                    std::thread::sleep(std::time::Duration::from_millis(100 * attempt as u64));
                    Ok(false)
                }
                Err(e) => with_db_raw(|db| db.flight_failed(flight.first, &e.to_string())).map(|_| true),
            };
            match done {
                Ok(true) => break,
                Ok(false) => continue,
                Err(e) => {
                    eprintln!("objkv writer: {e}");
                    break;
                }
            }
        }
        signal();
    }
}

/// Blocks until the writer has dealt with `ticket`.
fn wait_outcome(ticket: u64) -> PgResult<Outcome> {
    loop {
        let g = SIGNAL.0.lock().unwrap_or_else(|e| e.into_inner());
        if let Some(o) = with_db_raw(|db| db.take_outcome(ticket))? {
            return Ok(o);
        }
        let _ = SIGNAL.1.wait_timeout(g, std::time::Duration::from_secs(1));
    }
}

/// Waits until nothing is owed to the bucket. Used on the way out, so an
/// asynchronous commit is only ever lost to a crash, never to a shutdown.
fn drain_writes() {
    loop {
        let g = SIGNAL.0.lock().unwrap_or_else(|e| e.into_inner());
        match with_db_raw(|db| db.has_unwritten()) {
            Ok(true) => {}
            _ => return,
        }
        let _ = SIGNAL.1.wait_timeout(g, std::time::Duration::from_secs(1));
    }
}

fn serialization_failure(c: &::objkv::db::Conflict) -> Box<PgError> {
    let (what, detail) = describe_conflict(c);
    Box::new(
        PgError::error(format!("could not serialize access due to concurrent update of {what}"))
            .with_detail(detail)
            .with_hint("Retry the transaction.".to_string())
            .with_sqlstate(::types_error::ERRCODE_T_R_SERIALIZATION_FAILURE),
    )
}

fn at_pre_commit() -> PgResult<()> {
    let writes = flatten_pending();
    if writes.is_empty() {
        return Ok(());
    }
    let n = writes.len();
    // Only so a discarded object can name its transaction in the log; often 0,
    // since an objkv-only transaction writes no WAL.
    let xid = ::xact::GetCurrentTransactionIdIfAny();
    // Against the oldest snapshot read at, so a row changed since then is a
    // conflict; u64::MAX means we never read and nothing can have moved.
    let snap = XACT_SNAPSHOT.replace(u64::MAX);
    let wants_async = ::guc_tables::vars::pgrust_objkv_async_commit.read();
    let cap = ::guc_tables::vars::pgrust_objkv_async_queue.read().max(1) as usize;
    // Decided under the lock, with the queue as it is at that moment: an
    // asynchronous commit behind `cap` acknowledged-but-unwritten ones waits
    // like a synchronous one. That bounds what a crash can lose and what a
    // writer that cannot reach the bucket can fence, and turns a stuck
    // writer into slow commits rather than a growing queue.
    let (staged, sync) = with_db(|db| {
        let sync = !wants_async || db.async_backlog() >= cap;
        (db.stage_commit(writes, xid, snap, sync), sync)
    })?;
    let staged = staged
        .map_err(|e| Box::new(PgError::error(format!("objkv: commit of {n} changes failed: {e}"))))?;
    let (ticket, seq) = match staged {
        Ok(Some(x)) => x,
        Ok(None) => return Ok(()),
        Err(c) => return Err(serialization_failure(&c)),
    };
    MY_COMMIT_SEQ.set(seq);
    ensure_writer();
    signal();

    if sync {
        match wait_outcome(ticket)? {
            Outcome::Durable(landed) => {
                MY_COMMIT_SEQ.set(landed);
                // Fault points for the tests, here and only here: the object
                // is durable, and the client has been told nothing yet. An
                // asynchronous commit never reaches this arm, so the hooks
                // cannot fire ahead of the PUT.
                // A crash here keeps the commit, as a lost COMMIT reply does
                // under WAL. abort(), not panic!, so nothing unwinds -- as
                // kill -9 would.
                if std::env::var_os("OBJKV_FAULT_AFTER_COMMIT_PUT").is_some() {
                    eprintln!("objkv: OBJKV_FAULT_AFTER_COMMIT_PUT -- aborting after the PUT, before commit");
                    std::process::abort();
                }
                // An error here aborts the transaction: the abort path
                // writes the discard marker that keeps the object from ever
                // being applied.
                if std::env::var_os("OBJKV_FAULT_ERROR_AFTER_COMMIT_PUT").is_some() {
                    return Err(Box::new(PgError::error(
                        "objkv: OBJKV_FAULT_ERROR_AFTER_COMMIT_PUT -- failing after the PUT, before commit"
                            .to_string(),
                    )));
                }
            }
            // Nothing landed under any of these, so there is nothing for the
            // abort path to discard.
            Outcome::Refused(c) => {
                MY_COMMIT_SEQ.set(0);
                return Err(serialization_failure(&c));
            }
            Outcome::Failed(why) | Outcome::Fenced(why) => {
                MY_COMMIT_SEQ.set(0);
                return Err(Box::new(PgError::error(format!(
                    "objkv: commit of {n} changes failed: {why}"
                ))));
            }
        }
    }

    // Fold the chain into a run, or every scan replays all history. Done by
    // the compactor thread, off this backend and off the lock: the horizon
    // is decided here, where this session's retention setting and the open
    // snapshots are known.
    let (wanted, now) = with_db(|db| (db.needs_compaction(), db.current_seq()))?;
    if wanted {
        request_compaction(collection_horizon(now));
    }
    Ok(())
}

/// Names what collided: "row changed" for a duplicate key misdirects.
fn describe_conflict(c: &::objkv::db::Conflict) -> (String, String) {
    let key = String::from_utf8_lossy(&c.key).into_owned();
    let index_oid = key
        .strip_prefix("u/")
        .or_else(|| key.strip_prefix("i/"))
        .and_then(|rest| rest.split('/').next())
        .and_then(|hex| u32::from_str_radix(hex, 16).ok());
    match index_oid {
        Some(oid) => (
            format!("an index entry in objkv index {oid}"),
            format!(
                "Commit {} wrote the same entry. On a unique index this is a duplicate \
                 value inserted concurrently; the retry will report it as one.",
                c.by
            ),
        ),
        None => (
            format!("objkv row {key}"),
            format!("The row was changed by commit {}.", c.by),
        ),
    }
}

fn forget_snapshots() {
    XACT_SNAPSHOT.set(u64::MAX);
    release_in_use();
}

fn discard_pending() {
    PENDING.with(|p| p.borrow_mut().clear());
    MY_COMMIT_SEQ.set(0);
}

fn staged_with_ord(key: &[u8]) -> Option<(u64, Op)> {
    PENDING.with(|p| {
        p.borrow()
            .iter()
            .rev()
            .find_map(|f| f.writes.get(key).map(|(ord, op)| (*ord, op.clone())))
    })
}

fn pending_op(key: &[u8]) -> Option<Op> {
    PENDING.with(|p| {
        p.borrow().iter().rev().find_map(|f| f.writes.get(key).map(|(_, op)| op.clone()))
    })
}

/// Chooses the backing store once per process. With no `OBJKV_S3_*` set it
/// falls back to memory, which suits tests and nothing else.
fn store() -> Arc<dyn Store> {
    Arc::clone(STORE.get_or_init(|| {
        let Ok(endpoint) = std::env::var("OBJKV_S3_ENDPOINT") else {
            return Arc::new(MemStore::new()) as Arc<dyn Store>;
        };
        object_store(&endpoint)
    }))
}

/// The object-store client, when this build has one.
#[cfg(feature = "objkv-s3")]
fn object_store(endpoint: &str) -> Arc<dyn Store> {
    let env = |k: &str, d: &str| std::env::var(k).unwrap_or_else(|_| d.to_string());
    let key = env("OBJKV_S3_KEY", "minioadmin");
    let secret = env("OBJKV_S3_SECRET", "minioadmin");
    let bucket = env("OBJKV_S3_BUCKET", "objkv");
    let region = env("OBJKV_S3_REGION", "us-east-1");
    let built = match std::env::var("OBJKV_S3_TOKEN") {
        Ok(tok) => {
            ::objkv::s3::Client::new_with_token(endpoint, &bucket, &region, &key, &secret, &tok)
        }
        Err(_) => ::objkv::s3::Client::new(endpoint, &bucket, &region, &key, &secret),
    };
    // A config error must not read as "your data vanished on restart".
    match built {
        Ok(c) => Arc::new(c) as Arc<dyn Store>,
        Err(e) => panic!("OBJKV_S3_ENDPOINT is set but the S3 client could not be built: {e}"),
    }
}

/// And when it has not. Falling back to memory here would be the same silence
/// the branch above refuses: the server would start, accept writes, and lose
/// them at shutdown, with the configuration that asked for a bucket ignored.
#[cfg(not(feature = "objkv-s3"))]
fn object_store(endpoint: &str) -> Arc<dyn Store> {
    panic!(
        "OBJKV_S3_ENDPOINT is set to {endpoint}, but this server was built without          the `objkv-s3` feature and has no object-store client. Rebuild with          `cargo build --bin postgres --features objkv-s3`."
    );
}

pub fn unsupported(what: &str) -> Box<PgError> {
    Box::new(
        PgError::error(format!("objkv does not support {what}"))
            .with_sqlstate(::types_error::ERRCODE_FEATURE_NOT_SUPPORTED),
    )
}

/// One log for the whole database: relations are namespaced by key prefix, and
/// two `Db` instances over one store would race for the same commit number.
///
/// Refuses once the bucket has been fenced: another writer took a number this
/// server had already acknowledged, so its picture of the data is not to be
/// trusted, reads included.
pub(crate) fn with_db<R>(f: impl FnOnce(&mut Db) -> R) -> PgResult<R> {
    with_db_raw(|db| {
        if db.is_fenced() {
            return Err(Box::new(PgError::error(
                "objkv: this server has lost the bucket to another writer and must be restarted"
                    .to_string(),
            )));
        }
        Ok(f(db))
    })?
}

/// A read view, taken under the lock and used without it: every S3 GET a
/// read makes happens with no lock held, so a cold read never holds up a
/// commit or another reader.
pub(crate) fn view() -> PgResult<::objkv::db::View> {
    with_db(|db| db.view())
}

/// The same, fenced or not: for the writer, the abort path and the exit
/// path, which have to finish what they were doing either way.
pub(crate) fn with_db_raw<R>(f: impl FnOnce(&mut Db) -> R) -> PgResult<R> {
    let mut guard = DB.lock().unwrap_or_else(|e| e.into_inner());
    if guard.is_none() {
        // An object written at pre-commit by a transaction that then died looks
        // like a real commit; clog knows the difference, so recovery asks.
        *guard = Some(
            Db::open(store())
                .map_err(|e| Box::new(PgError::error(format!("objkv: cannot open storage: {e}"))))?,
        );
    }
    let db = guard.as_mut().unwrap();
    let r = f(db);
    SEQ_NOW.store(db.current_seq(), Ordering::Relaxed);
    DB_OPEN.store(true, Ordering::Relaxed);
    Ok(r)
}

fn row_key(db: u32, relid: u32, rowid: u64) -> Vec<u8> {
    format!("{db:08x}/{relid:08x}/{rowid:016x}").into_bytes()
}

fn table_prefix(db: u32, relid: u32) -> Vec<u8> {
    format!("{db:08x}/{relid:08x}/").into_bytes()
}

fn hi_of(prefix: &[u8]) -> Vec<u8> {
    let mut hi = prefix.to_vec();
    hi.push(0xff);
    hi
}

/// Where "this relation was emptied" is recorded. `t` is not a hex digit, so
/// these can never be mistaken for a row or an index entry.
pub fn empty_marker_key(db: u32, oid: u32) -> Vec<u8> {
    format!("t/{db:08x}/{oid:08x}").into_bytes()
}

/// Which of this transaction's writes a TRUNCATE it ran covers. Removing them
/// outright would defeat a rollback to a savepoint, so it records where the
/// writes had got to and reads skip the earlier ones.
thread_local! {
    static EMPTIED: RefCell<BTreeMap<Vec<u8>, u64>> = const { RefCell::new(BTreeMap::new()) };
}

/// The line below which this transaction's own staged writes for `key` are
/// covered by a truncate it performed.
fn staged_empty_mark(marker: &[u8]) -> u64 {
    EMPTIED.with(|e| e.borrow().get(marker).copied().unwrap_or(0))
}

/// Empties a relation as of now: one small object, not a tombstone per row.
pub fn empty_relation(db: u32, oid: u32) {
    let key = empty_marker_key(db, oid);
    stage(key.clone(), Op::Put(Vec::new()));
    EMPTIED.with(|e| {
        e.borrow_mut().insert(key, stage_mark());
    });
}

pub(crate) fn forget_emptied() {
    EMPTIED.with(|e| e.borrow_mut().clear());
    STAGE_ORD.with(|c| c.set(0));
}

/// Where this transaction's own writes for a relation were covered by a
/// TRUNCATE it ran; 0 if it ran none.
pub fn staged_empty_mark_for(db: u32, oid: u32) -> u64 {
    staged_empty_mark(&empty_marker_key(db, oid))
}

/// The commit at or below `at` where this relation was last emptied.
pub fn emptied_at(db: u32, oid: u32, at: u64) -> PgResult<Option<u64>> {
    let key = empty_marker_key(db, oid);
    view()?.emptied_at(&key, at)
        .map_err(|e| Box::new(PgError::error(format!("objkv: {e}"))))
}

/// Which database's key space a relation lives in. Oids are unique within a
/// database, not a cluster -- CREATE DATABASE copies catalog rows keeping
/// them -- so without this two databases' tables share rows. Shared: scope 0.
pub fn scope(rel: &Relation<'_>) -> u32 {
    if rel.rd_rel.relisshared {
        0
    } else {
        ::init_small::globals::MyDatabaseId()
    }
}

fn rowid_from_key(key: &[u8]) -> Option<u64> {
    let s = std::str::from_utf8(key).ok()?;
    u64::from_str_radix(s.rsplit('/').next()?, 16).ok()
}

// --- Synthetic TIDs ---------------------------------------------------------
//
// Postgres addresses tuples as (block, offset) and entries store that pair.
// There are no blocks, so a row id splits across the two: a block number and a
// 1-based offset, since zero is invalid.
//
// The block holds as many rows as a real page could. Wider would waste less of
// the block number, but a bitmap -- the structure that lets one query combine
// two indexes -- rejects any offset a heap page could not have produced. None
// of this reaches the bucket: keys carry the row id itself.

pub const ROWS_PER_BLOCK: u64 = ::types_storage::bufpage::MaxHeapTuplesPerPage as u64;
pub const MAX_ROWID: u64 = (u32::MAX as u64) * ROWS_PER_BLOCK + (ROWS_PER_BLOCK - 1);

pub fn tid_of(rowid: u64) -> ItemPointerData {
    debug_assert!(rowid <= MAX_ROWID);
    let mut tid = ItemPointerData::invalid();
    ItemPointerSet(
        &mut tid,
        (rowid / ROWS_PER_BLOCK) as u32,
        ((rowid % ROWS_PER_BLOCK) + 1) as u16,
    );
    tid
}

pub fn rowid_of(tid: &ItemPointerData) -> u64 {
    let block = ItemPointerGetBlockNumber(tid) as u64;
    let offset = ItemPointerGetOffsetNumber(tid) as u64;
    block * ROWS_PER_BLOCK + offset.saturating_sub(1)
}

// --- Row images -------------------------------------------------------------
//
// A row is its heap-tuple image, which is what makes every column type work.

pub fn encode_row<'mcx>(
    mcx: Mcx<'mcx>,
    desc: &TupleDescData<'mcx>,
    values: &[Datum],
    isnull: &[bool],
) -> PgResult<Vec<u8>> {
    Ok(::heaptuple::heap_form_tuple(mcx, desc, values, isnull)?.image().to_vec())
}

pub fn store_image<'mcx>(
    mcx: Mcx<'mcx>,
    slot: &mut SlotData<'mcx>,
    image: &[u8],
    tid: ItemPointerData,
) -> PgResult<()> {
    let mut tuple = ::heaptuple::HeapTuple::alloc_zeroed(mcx, image.len())?;
    tuple.image_mut().copy_from_slice(image);
    tuple.as_tuple_mut().t_self = tid;
    ::exectuples::exec_store_heap_tuple_owned(slot, mcx, tuple);
    Ok(())
}

/// A block of object ids from the bucket; ordinary clusters use the WAL.
pub fn claim_oid_block(want: u32, prefetch: u32) -> PgResult<u32> {
    with_db(|db| db.claim_oid_block(want, prefetch))?
        .map_err(|e| Box::new(PgError::error(format!("objkv: cannot claim object ids: {e}"))))
}

pub fn insert_row(db: u32, relid: u32, image: Vec<u8>) -> PgResult<u64> {
    // Seeded from what is stored, so a restart cannot reuse row ids. Taking the
    // id and advancing it is one acquisition: a separate read and write-back
    // let two backends get the same id, and the later Put replaced the earlier
    // row with both clients told they had succeeded. 159 of 160.
    let seeded = NEXT_ROW
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .as_ref()
        .and_then(|m| m.get(&(db, relid)).copied());
    let scanned = match seeded {
        Some(_) => None,
        None => Some(
            scan_rows(db, relid, ::objkv::key::LATEST)?
                .iter()
                .map(|(id, _)| *id)
                .max()
                .map_or(0, |m| m + 1),
        ),
    };
    let rowid = {
        let mut guard = NEXT_ROW.lock().unwrap_or_else(|e| e.into_inner());
        let map = guard.get_or_insert_with(HashMap::new);
        let next = map.entry((db, relid)).or_insert_with(|| scanned.unwrap_or(0));
        let id = *next;
        *next = id + 1;
        id
    };
    add_stats(db, relid, image.len() as i64, 1);
    stage(row_key(db, relid, rowid), Op::Put(image));
    Ok(rowid)
}

fn add_stats(db: u32, relid: u32, byte_delta: i64, row_delta: i64) {
    fn apply(cur: u64, delta: i64) -> u64 {
        if delta >= 0 {
            cur.saturating_add(delta as u64)
        } else {
            cur.saturating_sub(delta.unsigned_abs())
        }
    }
    let mut guard = REL_STATS.lock().unwrap_or_else(|e| e.into_inner());
    let map = guard.get_or_insert_with(HashMap::new);
    let (b, r) = map.get(&(db, relid)).copied().unwrap_or((0, 0));
    map.insert((db, relid), (apply(b, byte_delta), apply(r, row_delta)));
}

/// Seeded by one scan, then tracked incrementally. Drifts: for the planner.
pub fn relation_stats(db: u32, relid: u32) -> PgResult<(u64, u64)> {
    if let Some(s) = REL_STATS
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .as_ref()
        .and_then(|m| m.get(&(db, relid)).copied())
    {
        return Ok(s);
    }
    let rows = scan_rows(db, relid, ::objkv::key::LATEST)?;
    let stats = (rows.iter().map(|(_, v)| v.len() as u64).sum(), rows.len() as u64);
    REL_STATS
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .get_or_insert_with(HashMap::new)
        .insert((db, relid), stats);
    Ok(stats)
}

pub fn relation_bytes(db: u32, relid: u32) -> PgResult<u64> {
    Ok(relation_stats(db, relid)?.0)
}

/// One raw key/value: the lift's records, which are not rows but must land in
/// the same commit object as the rows they describe.
pub fn stage_raw(key: Vec<u8>, value: Vec<u8>) {
    stage(key, Op::Put(value));
}

pub fn key_exists(key: &[u8]) -> PgResult<bool> {
    if let Some(op) = staged_op(key) {
        return Ok(matches!(op, Op::Put(_)));
    }
    let found = view()?.get(key)
        .map_err(|e| Box::new(PgError::error(format!("objkv: read failed: {e}"))))?;
    Ok(found.is_some())
}

/// A durable watermark. Confirmation normally rides in the next commit's
/// header, which is useless when the lift is the last thing before a
/// shutdown: the next boot would throw the catalogs away.
pub fn publish_watermark() -> PgResult<bool> {
    with_db(|db| db.flush_watermark())?
        .map_err(|e| Box::new(PgError::error(format!("objkv: cannot publish watermark: {e}"))))
}

/// The lift's access-method oids, before any catalog is opened: in bucket mode
/// pg_am is itself an objkv relation, so asking it is circular.
pub fn register_lifted_ams() {
    for oid in lifted_am_oids("am=") {
        ::tableam_vocab::register_objkv_table_am(oid);
    }
}

/// The oids a lift recorded under one field name; no catalog is reachable.
pub fn lifted_am_oids(field: &'static str) -> Vec<u32> {
    let Ok(records) = lift_records() else { return Vec::new() };
    records
        .iter()
        .flat_map(|r| r.split_whitespace())
        .filter_map(|f| f.strip_prefix(field)?.parse::<u32>().ok())
        .filter(|&oid| oid != 0)
        .collect()
}

/// Every `lift/...` record, as written text. Nobody else writes there.
pub fn lift_records() -> PgResult<Vec<String>> {
    Ok(lift_records_keyed()?.into_iter().map(|(_, v)| v).collect())
}

/// The same records with their keys, for a message that can name the scope.
pub fn lift_records_keyed() -> PgResult<Vec<(String, String)>> {
    let found = view()?.scan_prefix_at(b"lift/", ::objkv::key::LATEST)
        .map_err(|e| Box::new(PgError::error(format!("objkv: scan failed: {e}"))))?;
    Ok(found
        .into_iter()
        .map(|(k, v)| {
            (String::from_utf8_lossy(&k).into_owned(), String::from_utf8_lossy(&v).into_owned())
        })
        .collect())
}

/// Whether any objkv data belongs to a database; `createdb` asks once.
pub fn database_has_rows(db: u32) -> PgResult<bool> {
    let prefix = format!("{db:08x}/").into_bytes();
    let found = with_db(|d| d.scan_prefix_at(&prefix, ::objkv::key::LATEST))?
        .map_err(|e| Box::new(PgError::error(format!("objkv: scan failed: {e}"))))?;
    Ok(!found.is_empty())
}

pub fn scan_rows(db: u32, relid: u32, at: u64) -> PgResult<Vec<(u64, Vec<u8>)>> {
    let prefix = table_prefix(db, relid);
    scan_rows_between(db, relid, prefix.clone(), hi_of(&prefix), at)
}

/// Every row whose key falls in `[lo, hi)`, newest version each.
fn scan_rows_between(
    db: u32,
    relid: u32,
    lo: Vec<u8>,
    hi: Vec<u8>,
    at: u64,
) -> PgResult<Vec<(u64, Vec<u8>)>> {
    // Rows older than the last TRUNCATE are still in the bucket, for a snapshot
    // taken before it. They are not in this table any more.
    let since = staged_empty_mark(&empty_marker_key(db, relid));
    // An uncommitted TRUNCATE has no sequence number, and covers everything.
    let emptied = if since > 0 {
        u64::MAX
    } else {
        emptied_at(db, relid, at)?.unwrap_or(0)
    };
    let durable = view()?.scan_window_stamped_at(&lo, &hi, at, usize::MAX)
        .map_err(|e| Box::new(PgError::error(format!("objkv: scan failed: {e}"))))?
        .0;

    let mut merged: BTreeMap<Vec<u8>, Vec<u8>> = durable
        .into_iter()
        .filter(|(_, _, seq)| *seq >= emptied)
        .map(|(k, v, _)| (k, v))
        .collect();
    // A read into the past must not see our uncommitted writes; they belong to
    // the present. Every other read must.
    if reading_the_past() {
        return Ok(merged
            .into_iter()
            .filter_map(|(k, v)| rowid_from_key(&k).map(|id| (id, v)))
            .collect());
    }
    PENDING.with(|p| {
        for f in p.borrow().iter() {
            for (k, (ord, op)) in f.writes.range(lo.clone()..hi.clone()) {
                // Written before a TRUNCATE this transaction ran: covered by it.
                if *ord <= since {
                    merged.remove(k);
                    continue;
                }
                match op {
                    Op::Put(v) => {
                        merged.insert(k.clone(), v.clone());
                    }
                    Op::Delete => {
                        merged.remove(k);
                    }
                }
            }
        }
    });

    Ok(merged
        .into_iter()
        .filter_map(|(k, v)| rowid_from_key(&k).map(|id| (id, v)))
        .collect())
}

/// Replaces a row's contents at the row id it already has.
///
/// The catalog's in-place update: Postgres rewrites a pg_class row inside its
/// buffer with no MVCC version, so TIDs and index entries keep pointing at it.
/// Here that is a new version under the same row key. Like Postgres's, it
/// leaves indexes alone, which is sound only because the fields updated this
/// way are never indexed ones.
pub fn update_row_in_place(db: u32, relid: u32, rowid: u64, image: Vec<u8>) -> PgResult<()> {
    stage(row_key(db, relid, rowid), Op::Put(image));
    Ok(())
}

/// The newest value this row ever had, tombstones included: the SnapshotAny
/// re-fetch the executor does while updating a row.
pub fn fetch_row_any(db: u32, relid: u32, rowid: u64) -> PgResult<Option<Vec<u8>>> {
    let key = row_key(db, relid, rowid);
    if let Some(Op::Put(v)) = pending_op(&key) {
        return Ok(Some(v));
    }
    view()?.get_any(&key)
        .map_err(|e| Box::new(PgError::error(format!("objkv: fetch failed: {e}"))))
}

pub fn fetch_row(db: u32, relid: u32, rowid: u64, at: u64) -> PgResult<Option<Vec<u8>>> {
    let key = row_key(db, relid, rowid);
    let since = staged_empty_mark(&empty_marker_key(db, relid));
    if !reading_the_past() {
        match staged_with_ord(&key) {
            // Staged before a TRUNCATE this transaction ran: covered by it.
            Some((ord, _)) if ord <= since => return Ok(None),
            Some((_, Op::Put(v))) => return Ok(Some(v)),
            Some((_, Op::Delete)) => return Ok(None),
            None => {}
        }
    }
    // An uncommitted TRUNCATE has no sequence number, and covers everything.
    let emptied = if since > 0 { u64::MAX } else { emptied_at(db, relid, at)?.unwrap_or(0) };
    let found = with_db(|d| d.get_stamped_at(&key, at))?
        .map_err(|e| Box::new(PgError::error(format!("objkv: fetch failed: {e}"))))?;
    Ok(found.filter(|(_, seq)| *seq >= emptied).map(|(v, _)| v))
}

/// A tombstone. Old versions stay until compaction drops them: no vacuum.
pub fn delete_row(db: u32, relid: u32, rowid: u64) -> PgResult<()> {
    if let Some(v) = fetch_row(db, relid, rowid, ::objkv::key::LATEST)? {
        add_stats(db, relid, -(v.len() as i64), -1);
    }
    stage(row_key(db, relid, rowid), Op::Delete);
    Ok(())
}

/// Materialised at scan_begin: fine for a prototype, wrong for real volumes.
pub struct ObjkvScanDescData<'mcx> {
    pub rs_base: TableScanDescData<'mcx>,
    pub rows: Vec<(u64, Vec<u8>)>,
    pub next: usize,
}

impl<'mcx> ObjkvScanDescData<'mcx> {
    pub fn new(rs_base: TableScanDescData<'mcx>, rows: Vec<(u64, Vec<u8>)>) -> Self {
        ObjkvScanDescData { rs_base, rows, next: 0 }
    }
    pub fn take_next(&mut self) -> Option<(u64, Vec<u8>)> {
        let r = self.rows.get(self.next).cloned();
        if r.is_some() {
            self.next += 1;
        }
        r
    }
    pub fn rewind(&mut self) {
        self.next = 0;
    }
    /// Replaces the scan keys. next_slot filters against rs_key, so a rescan
    /// that dropped them answered from the previous call's predicate -- and a
    /// filter that is merely wrong still returns rows.
    pub fn set_keys(&mut self, key: &[::types_scan::scankey::ScanKeyData]) {
        self.rs_base.rs_key.clear();
        for k in key {
            self.rs_base.rs_key.push(k.clone());
        }
        self.rs_base.rs_nkeys = key.len() as i32;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const DB: u32 = 5;

    /// The read paths consult `pgrust.objkv_snapshot_seq`, whose slot is
    /// installed by the server's boot sequence and by nothing in a unit test.
    fn gucs() {
        use ::guc_tables::{backing, vars, GucVarAccessors};
        vars::pgrust_objkv_snapshot_seq.install_if_absent(GucVarAccessors {
            get: backing::pgrust_objkv_snapshot_seq,
            set: backing::set_pgrust_objkv_snapshot_seq,
        });
        vars::pgrust_objkv_retain_commits.install_if_absent(GucVarAccessors {
            get: backing::pgrust_objkv_retain_commits,
            set: backing::set_pgrust_objkv_retain_commits,
        });
    }

    #[test]
    fn row_keys_sort_within_a_table_and_separate_tables() {
        assert!(row_key(DB, 7, 9) < row_key(DB, 7, 10));
        assert!(row_key(DB, 7, u64::MAX) < row_key(DB, 8, 0));
        assert!(row_key(DB, 7, 0).starts_with(&table_prefix(DB, 7)));
        assert!(!row_key(DB, 8, 0).starts_with(&table_prefix(DB, 7)));
    }

    #[test]
    fn two_databases_with_one_relid_do_not_share_rows() {
        gucs();
        assert_ne!(row_key(1, 7, 0), row_key(2, 7, 0));
        assert!(!row_key(2, 7, 0).starts_with(&table_prefix(1, 7)));

        insert_row(1, 9100, vec![1; 8]).unwrap();
        insert_row(2, 9100, vec![2; 8]).unwrap();
        let one = scan_rows(1, 9100, ::objkv::key::LATEST).unwrap();
        let two = scan_rows(2, 9100, ::objkv::key::LATEST).unwrap();
        assert_eq!(one.len(), 1);
        assert_eq!(two.len(), 1);
        assert_eq!(one[0].1, vec![1; 8]);
        assert_eq!(two[0].1, vec![2; 8], "one database must not read another's rows");

        delete_row(1, 9100, one[0].0).unwrap();
        assert_eq!(scan_rows(2, 9100, ::objkv::key::LATEST).unwrap().len(), 1);
    }

    #[test]
    fn rowids_survive_the_trip_through_a_tid() {
        for id in [0u64, 1, ROWS_PER_BLOCK - 1, ROWS_PER_BLOCK, ROWS_PER_BLOCK + 1, 0x1_2345, MAX_ROWID] {
            assert_eq!(rowid_of(&tid_of(id)), id, "rowid {id:#x} round-trips");
        }
        assert_ne!(tid_of(ROWS_PER_BLOCK - 1), tid_of(ROWS_PER_BLOCK));
        assert_ne!(ItemPointerGetOffsetNumber(&tid_of(0)), 0);
    }

    #[test]
    fn keys_parse_back_to_rowids() {
        assert_eq!(rowid_from_key(&row_key(DB, 3, 0x2a)), Some(0x2a));
        assert_eq!(rowid_from_key(b"not-a-key"), None);
    }

    #[test]
    fn insert_scan_fetch_delete_round_trip() {
        gucs();
        let relid = 4242;
        let mut ids = Vec::new();
        for i in 0..5u8 {
            ids.push(insert_row(DB, relid, vec![i; 32]).unwrap());
        }
        assert_eq!(ids, vec![0, 1, 2, 3, 4], "rowids are dense and ordered");

        let rows = scan_rows(DB, relid, ::objkv::key::LATEST).unwrap();
        assert_eq!(rows.len(), 5);
        assert!(rows.windows(2).all(|w| w[0].0 < w[1].0), "scan is in rowid order");

        assert_eq!(fetch_row(DB, relid, 2, ::objkv::key::LATEST).unwrap(), Some(vec![2u8; 32]));
        assert_eq!(fetch_row(DB, relid, 99, ::objkv::key::LATEST).unwrap(), None);

        delete_row(DB, relid, 2).unwrap();
        assert_eq!(fetch_row(DB, relid, 2, ::objkv::key::LATEST).unwrap(), None, "tombstone hides the row");
        let after = scan_rows(DB, relid, ::objkv::key::LATEST).unwrap();
        assert_eq!(after.len(), 4, "deleted row leaves the scan");
        assert!(!after.iter().any(|(id, _)| *id == 2));
    }

    #[test]
    fn relations_do_not_see_each_others_rows() {
        gucs();
        insert_row(DB, 7001, vec![1; 8]).unwrap();
        insert_row(DB, 7002, vec![2; 8]).unwrap();
        assert_eq!(scan_rows(DB, 7001, ::objkv::key::LATEST).unwrap().len(), 1);
        assert_eq!(scan_rows(DB, 7002, ::objkv::key::LATEST).unwrap().len(), 1);
        assert_ne!(
            scan_rows(DB, 7001, ::objkv::key::LATEST).unwrap()[0].1,
            scan_rows(DB, 7002, ::objkv::key::LATEST).unwrap()[0].1
        );
        assert!(scan_rows(DB, 7003, ::objkv::key::LATEST).unwrap().is_empty());
    }
}

pub fn relid(rel: &Relation<'_>) -> u32 {
    rel.rd_id
}

pub fn begin_scan<'mcx>(
    relation: &Relation<'mcx>,
    snapshot: ::tableam_vocab::Snapshot<'mcx>,
    nkeys: i32,
    key: ::mcx::PgVec<'mcx, ::types_scan::scankey::ScanKeyData>,
    flags: u32,
) -> PgResult<TableScanDesc<'mcx>> {
    let rows = scan_rows(scope(relation), relid(relation), snapshot_seq(snapshot.as_deref())?)?;
    Ok(desc(relation, snapshot, nkeys, key, flags, rows))
}

/// A bitmap scan reads nothing up front: the row ids arrive from the index
/// side a block at a time, and each block is fetched when it comes.
pub fn begin_scan_bitmap<'mcx>(
    mcx: Mcx<'mcx>,
    relation: &Relation<'mcx>,
    snapshot: ::tableam_vocab::Snapshot<'mcx>,
    flags: u32,
) -> PgResult<TableScanDesc<'mcx>> {
    Ok(desc(relation, snapshot, 0, ::mcx::PgVec::new_in(mcx), flags, Vec::new()))
}

fn desc<'mcx>(
    relation: &Relation<'mcx>,
    snapshot: ::tableam_vocab::Snapshot<'mcx>,
    nkeys: i32,
    key: ::mcx::PgVec<'mcx, ::types_scan::scankey::ScanKeyData>,
    flags: u32,
    rows: Vec<(u64, Vec<u8>)>,
) -> TableScanDesc<'mcx> {
    TableScanDesc::Objkv(std::boxed::Box::new(ObjkvScanDescData::new(
        TableScanDescData {
            rs_rd: relation.alias(),
            rs_snapshot: snapshot,
            rs_nkeys: nkeys,
            rs_key: key,
            rs_mintid: ItemPointerData::invalid(),
            rs_maxtid: ItemPointerData::invalid(),
            rs_flags: flags,
            rs_parallel: None,
            rs_am: ::tableam_vocab::TableAm::Objkv,
        },
        rows,
    )))
}

/// Stages the next bitmap block's rows; 0 means the bitmap is finished.
///
/// A block here is a range of row ids rather than a page on a disk, so a lossy
/// entry -- one the bitmap shrank to "some row on this block" under memory
/// pressure -- is a range read, and the caller rechecks.
pub fn scan_bitmap_next_pagebatch(
    scan: &mut ObjkvScanDescData<'_>,
    tbm: Option<&::tidbitmap::TIDBitmap<'_>>,
    iterator: &mut ::tidbitmap::TbmIterator,
    recheck: &mut bool,
    lossy_pages: &mut u64,
    exact_pages: &mut u64,
) -> PgResult<u32> {
    let db = scope(&scan.rs_base.rs_rd);
    let rel = relid(&scan.rs_base.rs_rd);
    let at = snapshot_seq(scan.rs_base.rs_snapshot.as_deref())?;
    loop {
        let Some(page) = iterator.next(tbm) else { return Ok(0) };
        let base = page.blockno as u64 * ROWS_PER_BLOCK;
        let rows = if page.lossy {
            *lossy_pages += 1;
            *recheck = true;
            scan_rows_between(
                db,
                rel,
                row_key(db, rel, base),
                row_key(db, rel, base + ROWS_PER_BLOCK),
                at,
            )?
        } else {
            *exact_pages += 1;
            *recheck = page.recheck;
            let mut offsets = [0u16; ROWS_PER_BLOCK as usize];
            let n = page.extract_page_tuples(&mut offsets);
            let mut rows = Vec::with_capacity(n);
            for &off in &offsets[..n.min(offsets.len())] {
                let rowid = base + off as u64 - 1;
                if let Some(image) = fetch_row(db, rel, rowid, at)? {
                    rows.push((rowid, image));
                }
            }
            rows
        };
        if !rows.is_empty() {
            let n = rows.len() as u32;
            scan.rows = rows;
            scan.rewind();
            return Ok(n);
        }
    }
}

pub fn scan_bitmap_batch_store<'mcx>(
    mcx: Mcx<'mcx>,
    scan: &mut ObjkvScanDescData<'mcx>,
    i: u32,
    slot: &mut SlotData<'mcx>,
) {
    let (rowid, image) = scan.rows[i as usize].clone();
    let _ = store_image(mcx, slot, &image, tid_of(rowid));
}

pub fn scan_bitmap_next_tuple<'mcx>(
    mcx: Mcx<'mcx>,
    scan: &mut ObjkvScanDescData<'mcx>,
    tbm: Option<&::tidbitmap::TIDBitmap<'_>>,
    iterator: &mut ::tidbitmap::TbmIterator,
    slot: &mut SlotData<'mcx>,
    recheck: &mut bool,
    lossy_pages: &mut u64,
    exact_pages: &mut u64,
) -> PgResult<bool> {
    while scan.next >= scan.rows.len() {
        if scan_bitmap_next_pagebatch(scan, tbm, iterator, recheck, lossy_pages, exact_pages)? == 0 {
            ::exectuples::exec_clear_tuple(slot, mcx);
            return Ok(false);
        }
    }
    let (rowid, image) = scan.rows[scan.next].clone();
    scan.next += 1;
    store_image(mcx, slot, &image, tid_of(rowid))?;
    Ok(true)
}

/// No visibility recheck: the layer merge dropped tombstones already.
pub fn next_slot<'mcx>(
    mcx: Mcx<'mcx>,
    scan: &mut ObjkvScanDescData<'mcx>,
    slot: &mut SlotData<'mcx>,
) -> PgResult<bool> {
    loop {
        let Some((rowid, image)) = scan.take_next() else {
            ::exectuples::exec_clear_tuple(slot, mcx);
            return Ok(false);
        };
        store_image(mcx, slot, &image, tid_of(rowid))?;

        // Scan keys are the AM's job: a catalog scan through genam has no filter
        // node above it and believes every row it gets.
        if scan.rs_base.rs_nkeys > 0 {
            let mut tuple = ::heaptuple::HeapTuple::alloc_zeroed(mcx, image.len())?;
            tuple.image_mut().copy_from_slice(&image);
            tuple.as_tuple_mut().t_self = tid_of(rowid);
            let desc = scan.rs_base.rs_rd.rd_att.clone();
            if !::heapam::heap_key_test(tuple.as_tuple(), &desc, &mut scan.rs_base.rs_key)? {
                continue;
            }
        }
        return Ok(true);
    }
}

pub fn tuple_insert<'mcx>(
    mcx: Mcx<'mcx>,
    rel: &Relation<'mcx>,
    slot: &mut SlotData<'mcx>,
) -> PgResult<()> {
    ::exectuples::slot_getallattrs(slot);
    let image = {
        let base = slot.base();
        let desc = base
            .tts_tupleDescriptor
            .as_ref()
            .expect("objkv insert slot without a descriptor");
        encode_row(mcx, desc, &base.tts_values, &base.tts_isnull)?
    };
    let rowid = insert_row(scope(rel), relid(rel), image)?;
    slot.base_mut().tts_tid = tid_of(rowid);
    Ok(())
}

/// Inserts a catalog row that is already formed, and stamps its TID. The
/// catalog path forms its tuple before it knows where it will live, so it
/// arrives as an image rather than a slot; nothing else differs.
pub fn insert_tuple_image(rel: &Relation<'_>, tup: &mut HeapTupleData<'_>) -> PgResult<()> {
    // SAFETY: a formed catalog tuple whose header is live for t_len bytes.
    let image =
        unsafe { core::slice::from_raw_parts(tup.header_ptr(), tup.t_len as usize) }.to_vec();
    let rowid = insert_row(scope(rel), relid(rel), image)?;
    tup.t_self = tid_of(rowid);
    // heap_insert stamps this too, and the index path asserts on it.
    tup.t_tableOid = rel.rd_id;
    Ok(())
}

/// Replaces a catalog row's contents at the row id it has. Not the ordinary
/// UPDATE, which writes at a fresh one: entries name the row id, and the
/// catalog's own indexes would be left pointing at nothing.
pub fn update_tuple_image(
    rel: &Relation<'_>,
    otid: &ItemPointerData,
    tup: &mut HeapTupleData<'_>,
) -> PgResult<()> {
    // Same row id, new contents: the entries for the old contents would
    // otherwise stay, pointing at a row that no longer carries that value.
    let cx = ::mcx::MemoryContext::new("objkv retire entries");
    crate::objkv_index::retire_entries(cx.mcx(), rel, rowid_of(otid))?;
    // SAFETY: a formed catalog tuple, live for t_len bytes.
    let image =
        unsafe { core::slice::from_raw_parts(tup.header_ptr(), tup.t_len as usize) }.to_vec();
    let rowid = rowid_of(otid);
    update_row_in_place(scope(rel), relid(rel), rowid, image)?;
    tup.t_self = *otid;
    tup.t_tableOid = rel.rd_id;
    Ok(())
}

pub fn tuple_delete(rel: &Relation<'_>, tid: &ItemPointerData) -> PgResult<()> {
    // Before the row goes: the entry keys are read off the row as it stands.
    // Its own context because eighty places delete a catalog row and none hand
    // one down; nothing allocated here outlives the call.
    let cx = ::mcx::MemoryContext::new("objkv retire entries");
    crate::objkv_index::retire_entries(cx.mcx(), rel, rowid_of(tid))?;
    delete_row(scope(rel), relid(rel), rowid_of(tid))
}

/// Delete plus insert, as MVCC does anyway -- except the old version becomes
/// an old object rather than a dead tuple somebody has to vacuum.
pub fn tuple_update<'mcx>(
    mcx: Mcx<'mcx>,
    rel: &Relation<'mcx>,
    old_tid: &ItemPointerData,
    slot: &mut SlotData<'mcx>,
) -> PgResult<()> {
    crate::objkv_index::retire_entries(mcx, rel, rowid_of(old_tid))?;
    delete_row(scope(rel), relid(rel), rowid_of(old_tid))?;
    tuple_insert(mcx, rel, slot)
}

pub fn satisfies_snapshot(
    rel: &Relation<'_>,
    tid: &ItemPointerData,
    snapshot: Option<&SnapshotData<'_>>,
) -> PgResult<bool> {
    Ok(fetch_row(scope(rel), relid(rel), rowid_of(tid), snapshot_seq(snapshot)?)?.is_some())
}

pub fn row_exists(rel: &Relation<'_>, tid: &ItemPointerData) -> PgResult<bool> {
    Ok(fetch_row(scope(rel), relid(rel), rowid_of(tid), ::objkv::key::LATEST)?.is_some())
}

pub fn index_fetch<'mcx>(
    mcx: Mcx<'mcx>,
    rel: &Relation<'mcx>,
    tid: &ItemPointerData,
    slot: &mut SlotData<'mcx>,
    snapshot: Option<&SnapshotData<'_>>,
) -> PgResult<bool> {
    let any = snapshot.is_some_and(|s| {
        matches!(s.snapshot_type, ::types_snapshot::SnapshotType::SNAPSHOT_ANY)
    });
    let found = if any {
        fetch_row_any(scope(rel), relid(rel), rowid_of(tid))?
    } else {
        fetch_row(scope(rel), relid(rel), rowid_of(tid), snapshot_seq(snapshot)?)?
    };
    match found {
        Some(image) => {
            store_image(mcx, slot, &image, *tid)?;
            Ok(true)
        }
        None => Ok(false),
    }
}
