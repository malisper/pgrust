//! Session-local Part cache: relation OID -> parsed Part (footer, granule
//! directory, block zone maps, blooms — the whole mmap'd part), reused
//! across statements (docs/design/pgrcolumnar-part-cache.md).
//!
//! STALENESS RULE (the design note's §3, kept in sync verbatim): a cached
//! parsed footer for relation R may be reused by a statement iff
//!   (a) no relcache invalidation for R has been delivered since the entry
//!       was built (the sinval-driven callback below drops the entry when
//!       one is), AND
//!   (b) the validity probe at lookup observes, in order: the relation's
//!       main-fork path unchanged; the part's column count unchanged;
//!       seg0's (st_dev, st_ino, st_size) equal to the values captured
//!       immediately BEFORE the cached footer was read+CRC-validated; and
//!       the header's footer_off word (read from the live shared mapping)
//!       equal to the offset the cached footer was parsed from.
//! On any mismatch the entry is discarded and the footer is re-read,
//! re-parsed and CRC-revalidated from the current header pointer.
//! Reuse under (a)+(b) is snapshot-safe in both directions:
//!   lower bound — footer publish is fsync-ordered before the appending
//!   transaction's commit, so every row group whose xmin any statement
//!   snapshot can see is contained in the footer the current header points
//!   at; a matching probe proves that footer IS the cached one, hence no
//!   snapshot-visible row group is missing;
//!   upper bound — the footer is a snapshot-agnostic superset: row groups
//!   newer than the snapshot are excluded per-RG at scan time by the
//!   xmin-vs-snapshot visibility gate (rg_visible / rg_wholly_visible),
//!   never answered from cached metadata.

use std::cell::RefCell;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering::Relaxed};
use std::sync::{Arc, Mutex, OnceLock, Weak};

use ::datum::Datum;
use ::types_core::{InvalidOid, Oid};
use ::types_error::PgResult;

use crate::format::get_u64;
use crate::reader::Part;

struct Entry {
    part: Arc<Part>,
    path: String,
    ncols: usize,
    dev: u64,
    ino: u64,
    len: u64,
    footer_off: u64,
}

thread_local! {
    static CACHE: RefCell<Option<HashMap<Oid, Entry>>> = const { RefCell::new(None) };
}

// Compile-time proof that the shared registry below is sound to hand a Part
// to any thread: every Part field is Send+Sync (SegMap by the coldio-lane
// impl in segfile.rs; the lazy caches are OnceLock; the rest is plain data).
#[allow(dead_code)]
fn _assert_part_send_sync() {
    fn f<T: Send + Sync>() {}
    f::<Part>();
}

// Process-shared parsed-Part registry (partdir lane; proportionality-audit
// census B1/P6). The thread-local CACHE above never hits in runtime pool
// workers: they are ephemeral (one scan, then exit), so at 100M every worker
// re-preads ~5.8MB of footer sections and re-parses the O(nrgs x ncols)
// chunk+stitch directory (~320K entries, ~5MB of owned Vecs) per query, x
// DOP. This registry shares ONE live parsed Part per part state across
// threads, keyed by the part-cache identity vocabulary (seg0 dev, ino, len)
// with the same footer_off header-word revalidation as `probe`.
//
// LAW POSTURE (no-large-memory-caches): the registry holds Weak only — it
// never extends any Part's lifetime and retains no derived state of its own;
// a Part stays alive exactly while some scan or the (pre-existing,
// design-doc'd, sinval-invalidated) session part cache holds the Arc. Dead
// entries are swept on every publish. Serving a Part whose footer is newer
// than the prober's stat is snapshot-safe by the header comment's two-sided
// argument (footers are snapshot-agnostic supersets; per-RG xmin gates
// exclude too-new row groups) — identical to Part::open itself, which always
// reads the CURRENT header regardless of when the caller stat'd.
//
// Kill switch: PGRUST_CBSTORE_SHARED_PART=0/off (per-thread parse, the
// historical behavior). PGRUST_CBSTORE_PART_CACHE=0 disables this too (the
// registry is probed only from the cache-miss path).
static SHARED_PARTS: Mutex<Option<HashMap<(u64, u64, u64), Weak<Part>>>> = Mutex::new(None);

// Per-process counters (always on, uncontended increments): full directory
// parses at Part::open vs registry shares. The A/B evidence channel for the
// per-worker-redundancy claim; PGRUST_PARTCACHE_DEBUG=1 additionally prints
// one line per event (PARTCACHE|parse us=.. / PARTCACHE|share).
pub static PART_DIR_PARSES: AtomicU64 = AtomicU64::new(0);
pub static PART_DIR_SHARES: AtomicU64 = AtomicU64::new(0);

fn shared_part_enabled() -> bool {
    static ON: OnceLock<bool> = OnceLock::new();
    *ON.get_or_init(|| {
        !matches!(
            std::env::var("PGRUST_CBSTORE_SHARED_PART").as_deref(),
            Ok("0") | Ok("off") | Ok("OFF"),
        )
    })
}

// Upgrade the live shared Part for this file state, revalidating exactly as
// `probe` does: ncols unchanged and the live mapping's header footer_off
// word equal to the offset the cached footer was parsed from (the header
// page is MAP_SHARED, so a publish is visible through the old mapping).
fn shared_lookup(key: (u64, u64, u64), ncols: usize) -> Option<Arc<Part>> {
    if !shared_part_enabled() {
        return None;
    }
    let live = {
        let guard = SHARED_PARTS.lock().unwrap();
        guard.as_ref()?.get(&key).and_then(Weak::upgrade)?
    };
    if live.ncols != ncols || get_u64(live.bytes(), 16) != live.footer_off {
        return None;
    }
    Some(live)
}

fn shared_publish(key: (u64, u64, u64), part: &Arc<Part>) {
    if !shared_part_enabled() {
        return;
    }
    let mut guard = SHARED_PARTS.lock().unwrap();
    let map = guard.get_or_insert_with(HashMap::new);
    map.retain(|_, w| w.strong_count() > 0);
    map.insert(key, Arc::downgrade(part));
}

// Was the hidden GUC `pgrcolumnar_part_cache` on the old branch; re-homed to an
// env off-switch (default ON) — the lane-v2 line adds no SQL GUCs (see
// costsize::gucs' pgrcolumnar knob note). `PGRUST_CBSTORE_PART_CACHE=0`/`off`
// disables (byte-identical A/B gate).
fn enabled() -> bool {
    static ON: OnceLock<bool> = OnceLock::new();
    *ON.get_or_init(|| {
        !matches!(
            std::env::var("PGRUST_CBSTORE_PART_CACHE").as_deref(),
            Ok("0") | Ok("off")
        )
    })
}

fn debug_log() -> bool {
    static ON: OnceLock<bool> = OnceLock::new();
    *ON.get_or_init(|| std::env::var("PGRUST_PARTCACHE_DEBUG").is_ok_and(|v| v == "1"))
}

fn invalidate_callback(_arg: Datum, relid: Oid) {
    CACHE.with(|cell| {
        if let Some(map) = cell.borrow_mut().as_mut() {
            let before = map.len();
            if relid == InvalidOid {
                map.clear();
            } else {
                map.remove(&relid);
            }
            if debug_log() && map.len() != before {
                eprintln!("PARTCACHE|inval relid={relid}");
            }
        }
    });
}

// Callback registration precedes map install: a registration failure must
// not leave a map that caches without invalidation.
fn init() -> PgResult<()> {
    inval::invalidate::CacheRegisterRelcacheCallback(invalidate_callback, Datum::null())?;
    CACHE.with(|cell| *cell.borrow_mut() = Some(HashMap::new()));
    Ok(())
}

/// The (st_dev, st_ino, st_size) identity stat, via the Vfs boundary
/// (DST P1 WS-B, contract v1.1 FileInfo dev+ino): one stat(2), exactly like
/// the std::fs::metadata call it replaces; None on any failure.
fn stat_info(path: &str) -> Option<vfs::FileInfo> {
    let c = std::ffi::CString::new(path).ok()?;
    let mut fi = vfs::FileInfo::zeroed();
    if vfs::stat(&c, &mut fi) != 0 {
        return None;
    }
    Some(fi)
}

fn probe(e: &Entry, path: &str, ncols: usize) -> bool {
    if e.path != path || e.ncols != ncols {
        return false;
    }
    let Some(md) = stat_info(path) else { return false };
    if (md.dev, md.ino, md.size as u64) != (e.dev, e.ino, e.len) {
        return false;
    }
    // len matched, so the header page is inside the live mapping.
    get_u64(e.part.bytes(), 16) == e.footer_off
}

/// Cache-or-open the relation's Part. None: no committed footer yet
/// (deliberately uncached — the next COPY changes everything anyway).
pub fn cached_part(rel: &::types_rel::Relation<'_>) -> PgResult<Option<Arc<Part>>> {
    let ncols = crate::writer::coltypes_of(rel)?.len();
    let path = crate::rel_main_path(rel);
    if !enabled() {
        PART_DIR_PARSES.fetch_add(1, Relaxed);
        return Ok(Part::open(&path, ncols)?.map(Arc::new));
    }
    if CACHE.with(|cell| cell.borrow().is_none()) {
        init()?;
    }
    let relid = rel.rd_id;
    let hit = CACHE.with(|cell| {
        let b = cell.borrow();
        let map = b.as_ref().expect("part cache initialized");
        map.get(&relid).filter(|e| probe(e, &path, ncols)).map(|e| Arc::clone(&e.part))
    });
    if let Some(part) = hit {
        if debug_log() {
            eprintln!("PARTCACHE|hit relid={relid}");
        }
        return Ok(Some(part));
    }
    // Stat BEFORE the read: a publish racing the build makes the parsed
    // footer newer than the probe values, so the next lookup rebuilds
    // (spurious miss, never a stale hit).
    let md = stat_info(&path);
    // Thread-local miss: another thread (in the common shape, the leader at
    // plan time) may already hold the parsed directory for this exact file
    // state — share it instead of re-parsing (census B1: the per-worker
    // O(nrgs x ncols) tax).
    let shared_key = md.as_ref().map(|m| (m.dev, m.ino, m.size as u64));
    let part = match shared_key.and_then(|key| shared_lookup(key, ncols)) {
        Some(live) => {
            PART_DIR_SHARES.fetch_add(1, Relaxed);
            if debug_log() {
                eprintln!("PARTCACHE|share relid={relid}");
            }
            live
        }
        None => {
            let t0 = debug_log().then(std::time::Instant::now);
            PART_DIR_PARSES.fetch_add(1, Relaxed);
            let Some(part) = Part::open(&path, ncols)? else {
                CACHE.with(|cell| {
                    if let Some(map) = cell.borrow_mut().as_mut() {
                        map.remove(&relid);
                    }
                });
                return Ok(None);
            };
            let part = Arc::new(part);
            if let Some(t0) = t0 {
                eprintln!(
                    "PARTCACHE|parse relid={relid} nrgs={} ncols={ncols} us={}",
                    part.rgs.len(),
                    t0.elapsed().as_micros()
                );
            }
            if let Some(key) = shared_key {
                shared_publish(key, &part);
            }
            part
        }
    };
    if let Some(md) = md {
        if debug_log() {
            eprintln!("PARTCACHE|fill relid={relid}");
        }
        CACHE.with(|cell| {
            if let Some(map) = cell.borrow_mut().as_mut() {
                map.insert(
                    relid,
                    Entry {
                        part: Arc::clone(&part),
                        path,
                        ncols,
                        dev: md.dev,
                        ino: md.ino,
                        len: md.size as u64,
                        footer_off: part.footer_off,
                    },
                );
            }
        });
    }
    Ok(Some(part))
}

#[cfg(test)]
mod tests {
    use std::os::unix::fs::MetadataExt;

    use super::*;

    fn entry_for(path: &str, ncols: usize) -> Entry {
        let md = std::fs::metadata(path).unwrap();
        let part = Part::open(path, ncols).unwrap().unwrap();
        Entry {
            footer_off: part.footer_off,
            part: Arc::new(part),
            path: path.to_string(),
            ncols,
            dev: md.dev(),
            ino: md.ino(),
            len: md.len(),
        }
    }

    #[test]
    fn probe_matches_then_detects_republish_growth_and_recreate() {
        let path = crate::reader::test_part(
            &format!("partcache-probe-{}", std::process::id()),
            &[1000, 2000],
            3,
        );
        let e = entry_for(&path, 3);
        assert!(probe(&e, &path, 3));
        assert!(!probe(&e, &path, 4));
        assert!(!probe(&e, "/nonexistent", 3));

        // Append growth without publish: len probe fails.
        let mut bytes = std::fs::read(&path).unwrap();
        bytes.extend_from_slice(&[0u8; 64]);
        std::fs::write(&path, &bytes).unwrap();
        assert!(!probe(&e, &path, 3));

        // Unlink + recreate byte-identical: inode probe fails.
        let orig = {
            std::fs::remove_file(&path).unwrap();
            bytes.truncate(bytes.len() - 64);
            std::fs::write(&path, &bytes).unwrap();
            entry_for(&path, 3)
        };
        assert!(probe(&orig, &path, 3));
        std::fs::remove_file(&path).unwrap();
        std::fs::write(&path, &bytes).unwrap();
        assert!(!probe(&orig, &path, 3));
        std::fs::remove_file(&path).unwrap();
    }

    // partdir lane (census B1): one live parsed Part per part state, shared
    // across threads; revalidation refuses ncols drift and a republished
    // header; the Weak registry never keeps a dead Part alive.
    #[test]
    fn shared_part_registry_shares_and_revalidates() {
        let path = crate::reader::test_part(
            &format!("partdir-shared-{}", std::process::id()),
            &[1000, 2000],
            3,
        );
        let md = std::fs::metadata(&path).unwrap();
        let key = (md.dev(), md.ino(), md.len());
        let part = Arc::new(Part::open(&path, 3).unwrap().unwrap());
        shared_publish(key, &part);

        // Same file state upgrades to the SAME allocation, from any thread.
        let live = shared_lookup(key, 3).expect("live entry upgrades");
        assert!(Arc::ptr_eq(&part, &live));
        let (k2, p2) = (key, Arc::clone(&part));
        let cross = std::thread::spawn(move || {
            shared_lookup(k2, 3).map(|l| Arc::ptr_eq(&p2, &l))
        })
        .join()
        .unwrap();
        assert_eq!(cross, Some(true), "cross-thread share must hit the same Part");

        // ncols drift refuses the share.
        assert!(shared_lookup(key, 4).is_none());

        // Republish detection: rewrite the header footer_off word in place
        // (same inode/len — the case the (dev,ino,len) key alone cannot
        // see); the MAP_SHARED header page makes it visible through the
        // cached mapping and the probe-word revalidation refuses.
        let orig_word = get_u64(part.bytes(), 16);
        {
            use std::io::{Seek, SeekFrom, Write};
            let mut f = std::fs::OpenOptions::new().write(true).open(&path).unwrap();
            f.seek(SeekFrom::Start(16)).unwrap();
            f.write_all(&(orig_word + 8).to_le_bytes()).unwrap();
        }
        assert!(shared_lookup(key, 3).is_none(), "stale footer_off must refuse");
        {
            use std::io::{Seek, SeekFrom, Write};
            let mut f = std::fs::OpenOptions::new().write(true).open(&path).unwrap();
            f.seek(SeekFrom::Start(16)).unwrap();
            f.write_all(&orig_word.to_le_bytes()).unwrap();
        }
        assert!(shared_lookup(key, 3).is_some());

        // Weak lifetime: with every holder dropped the entry dies.
        drop((part, live));
        assert!(shared_lookup(key, 3).is_none(), "Weak registry must not retain");
        std::fs::remove_file(&path).unwrap();
    }
}
