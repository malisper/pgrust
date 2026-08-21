//! The AM-layer cache composition: placement of the process-shared M3-F
//! [`PartRegistry`], the relid → part-key map that closes the
//! **ino-reuse hole** (pgrc2_read crate doc "Identity staleness": sealed
//! parts are immutable and cache identity is `(dev, ino, len)`; the one
//! hole is inode reuse after unlink+recreate at equal length — closed HERE
//! with relcache-style invalidation, the named M3-F→M3-H seam), and the
//! per-table publish serialization mutex.
//!
//! ## Registration law (the part_cache precedent, verbatim)
//!
//! Callback registration PRECEDES first map insert: a registration failure
//! must not leave a cache that caches without invalidation. The relcache
//! callback array is per-backend TLS in `inval`, so EACH backend registers
//! its own callback before its first recorded open; the per-backend latch
//! cell lives in [`crate::session`]'s single census-pinned `thread_local!`
//! block. See [`ensure_inval_registered`].

use std::collections::BTreeMap;
use std::sync::Arc;

use pgrc2_read::registry::{PartKey, PartRegistry};
use pgrc2_write::publish::TxnProbe;
use pgrc2_write::wvfs::RealVfs;
use types_core::Oid;
use types_error::PgResult;

/// Shared part-cache budget (capacity guidance; pins are law — M3-F).
/// Deliberately a const, not a GUC: O-M3-2 forbids new control surfaces in
/// this lane; sizing becomes a ruled knob when M3-G's scan path owns it.
const PART_CACHE_BUDGET_BYTES: u64 = 256 << 20;

pgsync::process_global! {
    /// The process-shared part registry (M3-F contract; placement is M3-H's).
    static REGISTRY: pgsync::Mutex<Option<Arc<PartRegistry>>> = pgsync::Mutex::new(None);
    /// relid → the part cache keys opened for it (the ino-reuse ledger).
    static RELID_PARTS: pgsync::Mutex<BTreeMap<u32, Vec<PartKey>>> =
        pgsync::Mutex::new(BTreeMap::new());
    /// relfilenumber-keyed publish mutexes (single-process serialization of
    /// generational manifest publish; crate doc "Publish serialization").
    static PUBLISH_LOCKS: pgsync::Mutex<BTreeMap<u64, Arc<pgsync::Mutex<()>>>> =
        pgsync::Mutex::new(BTreeMap::new());
    /// dir path → recovery scans run there this postmaster lifetime (the
    /// once-per-lifetime startup-recovery guard; the count is the test
    /// witness that later opens run ZERO scans). Presence = recovered.
    static RECOVERED_DIRS: pgsync::Mutex<BTreeMap<String, u64>> =
        pgsync::Mutex::new(BTreeMap::new());
}

/// The shared registry (created on first use; const-init static, no
/// OnceLock — the determinism ledger's `once` category stays empty).
pub fn registry() -> Arc<PartRegistry> {
    let mut slot = pgsync::lock(&REGISTRY);
    match &*slot {
        Some(r) => Arc::clone(r),
        None => {
            let r = Arc::new(PartRegistry::new(PART_CACHE_BUDGET_BYTES));
            *slot = Some(Arc::clone(&r));
            r
        }
    }
}

/// Record that `relid`'s scan opened the part under `key` (called with the
/// pin held; the relcache callback later invalidates exactly these).
pub fn record_part_key(relid: Oid, key: PartKey) {
    let mut map = pgsync::lock(&RELID_PARTS);
    let keys = map.entry(relid).or_default();
    if !keys.contains(&key) {
        keys.push(key);
    }
}

/// The engine-registry flush hook (the sqe statement plane's standing
/// engine cache rides the SAME relcache-invalidation face as the part
/// registry and the fact cache): DDL/TRUNCATE-class events drop the
/// per-relation engine handle so the next statement re-resolves. Set once
/// at executor seam-init (process-global; a null hook is a no-op).
static ENGINE_FLUSH_HOOK: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);

/// Register the engine-registry flush face (`relid == 0` = flush all).
pub fn set_engine_flush_hook(f: fn(Oid)) {
    ENGINE_FLUSH_HOOK.store(f as usize, std::sync::atomic::Ordering::Release);
}

fn engine_flush(relid: Oid) {
    let p = ENGINE_FLUSH_HOOK.load(std::sync::atomic::Ordering::Acquire);
    if p != 0 {
        // SAFETY: only ever stored from a `fn(Oid)` in set_engine_flush_hook.
        let f: fn(Oid) = unsafe { std::mem::transmute::<usize, fn(Oid)>(p) };
        f(relid);
    }
}

/// Drop every recorded key for `relid` from the shared registry.
/// `relid == 0` (InvalidOid) = a full-cache invalidation event.
pub fn invalidate_relid(relid: Oid) {
    engine_flush(relid);
    // M5a: the process-grain footer-fact cache rides the same
    // invalidation face (hygiene — its staleness law is content
    // addressing + resolve-precedes-read, factcache module doc).
    crate::factcache::invalidate_relid(relid);
    let reg = registry();
    if relid == 0 {
        let mut map = pgsync::lock(&RELID_PARTS);
        map.clear();
        drop(map);
        reg.clear();
        return;
    }
    let keys = {
        let mut map = pgsync::lock(&RELID_PARTS);
        map.remove(&relid)
    };
    if let Some(keys) = keys {
        for key in keys {
            reg.invalidate(key);
        }
    }
}

fn relcache_callback(_arg: datum::Datum, relid: Oid) {
    invalidate_relid(relid);
}

/// Register this backend's relcache callback (BEFORE the first recorded
/// open — the part_cache register-before-install law). The latch lives in
/// the session TLS block (census-pinned single block).
pub fn ensure_inval_registered() -> PgResult<()> {
    crate::session::ensure_session_hooks();
    crate::session::with_inval_latch(|c| {
        if !c.get() {
            inval::invalidate::CacheRegisterRelcacheCallback(
                relcache_callback,
                datum::Datum::null(),
            )?;
            c.set(true);
        }
        Ok(())
    })
}

/// The per-table publish lock (held across finish+publish, and across the
/// once-per-lifetime recovery scan — `recover_and_clean`'s documented
/// precondition is "no concurrent publisher on this directory").
pub fn publish_lock(relfilenumber: u64) -> Arc<pgsync::Mutex<()>> {
    let mut map = pgsync::lock(&PUBLISH_LOCKS);
    Arc::clone(map.entry(relfilenumber).or_insert_with(|| Arc::new(pgsync::Mutex::new(()))))
}

/// The probe capability recovery needs: transaction verdicts PLUS the
/// recorded-error re-raise (probe errors force conservative verdicts, so an
/// error-tainted recovery must be retried on the next open, never cached as
/// done — the guard checks this BEFORE marking the directory recovered).
pub trait RecoveryProbe: TxnProbe {
    fn take_recorded_error(&self) -> PgResult<()>;
    /// The `TxnProbe` view of self (explicit upcast).
    fn as_txn_probe(&self) -> &dyn TxnProbe;
}

impl RecoveryProbe for crate::probe::ClogTxnProbe {
    fn take_recorded_error(&self) -> PgResult<()> {
        self.take_error()
    }
    fn as_txn_probe(&self) -> &dyn TxnProbe {
        self
    }
}

/// The startup-recovery choke point (the #480 gap wiring): make `dir`
/// recovered-before-read for this postmaster lifetime. EVERY directory open
/// — reader scan begin ([`crate::scan`]) and writer open
/// ([`crate::ingest`]) — funnels through here, so no path can observe an
/// un-recovered crashed directory (spec §13.3 "recovery runs before
/// readers"; without this a pure SELECT after a crash serves the reader
/// walk's strict `ManifestMissing` refusal on a healthy table).
///
/// Entry cost: after the first open the whole call is one process-global
/// mutex lock + one `BTreeMap` probe — ZERO syscalls on the hot SELECT
/// path. The recovery SCAN runs at most once per directory per lifetime
/// (idempotence is #480-proven, so over-calling would be safe but wasteful;
/// in-lifetime abort residue never needs it: writer temps die in
/// `WriterRegistry::at_eoxact`, and an aborted PUBLISHED generation is
/// clog-fenced dead-band garbage reclaimed next lifetime).
///
/// Concurrency: first-opens race through the per-table publish lock — one
/// runs the scan, the rest block then hit the double-checked fast path.
/// Holding the PUBLISH lock (not a private once-lock) is load-bearing: it
/// excludes a concurrent publisher's rename window, whose freshly-renamed
/// final-name parts a concurrent recovery would reclaim as orphans.
pub fn ensure_dir_recovered(
    relfilenumber: u64,
    dir: &str,
    probe: &dyn RecoveryProbe,
) -> PgResult<()> {
    if pgsync::lock(&RECOVERED_DIRS).contains_key(dir) {
        return Ok(());
    }
    let plock = publish_lock(relfilenumber);
    let _guard = pgsync::lock(&plock);
    if pgsync::lock(&RECOVERED_DIRS).contains_key(dir) {
        return Ok(());
    }
    let mut scans = 0u64;
    if crate::dirpath::dir_exists(dir)? {
        let mut vfs = RealVfs;
        pgrc2_write::publish::recover_and_clean(&mut vfs, dir, probe.as_txn_probe())
            .map_err(crate::write_error)?;
        probe.take_recorded_error()?;
        scans = 1;
    }
    // Absent directory = never-ingested table (the reader's empty posture);
    // if a writer creates it later, it is born clean and marks itself via
    // [`mark_dir_recovered`].
    pgsync::lock(&RECOVERED_DIRS).insert(dir.to_string(), scans);
    Ok(())
}

/// Mark a FRESHLY-CREATED (born-clean) directory recovered without a scan
/// (the writer's `mkdir_if_absent`-created arm).
pub fn mark_dir_recovered(dir: &str) {
    pgsync::lock(&RECOVERED_DIRS).entry(dir.to_string()).or_insert(0);
}

/// Test witness: how many recovery scans ran for `dir` this lifetime
/// (`None` = the guard never saw the directory).
pub fn recovery_scans(dir: &str) -> Option<u64> {
    pgsync::lock(&RECOVERED_DIRS).get(dir).copied()
}
